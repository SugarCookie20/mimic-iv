
CREATE OR REPLACE TABLE `my-mimic-research.my_results.arf_cohort_table` AS
(
WITH

Qualifying_Admissions AS (
   SELECT
       adm.subject_id, adm.hadm_id, adm.admittime
   FROM `physionet-data.mimiciv_3_1_hosp.admissions` AS adm
   INNER JOIN `physionet-data.mimiciv_3_1_hosp.patients` AS pat ON adm.subject_id = pat.subject_id
   WHERE
       (EXTRACT(YEAR FROM adm.admittime) - pat.anchor_year + pat.anchor_age) > 18
       AND EXISTS (
           SELECT 1 FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` AS d
           WHERE d.hadm_id = adm.hadm_id AND d.icd_code IN ('J9600', 'J9601', 'J9602', 'J9620', 'J9621', 'J9622', 'J9690', 'J9691', 'J9692', '51881', '51884')
       )
       AND EXISTS (
           SELECT 1 FROM `physionet-data.mimiciv_3_1_icu.icustays` AS icu
           WHERE icu.hadm_id = adm.hadm_id AND icu.los > 1
       )
),

Ranked_Admissions AS (
   SELECT
       subject_id, hadm_id, ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY admittime ASC) as admission_rank
   FROM Qualifying_Admissions
),
Core_Cohort AS (
   SELECT
       ra.subject_id, ra.hadm_id, icu.stay_id
   FROM (
       SELECT subject_id, hadm_id FROM Ranked_Admissions WHERE admission_rank = 1
   ) AS ra
   INNER JOIN (
       SELECT hadm_id, stay_id, ROW_NUMBER() OVER (PARTITION BY hadm_id ORDER BY intime ASC) as icu_rank
       FROM `physionet-data.mimiciv_3_1_icu.icustays` WHERE los > 1
   ) AS icu ON ra.hadm_id = icu.hadm_id
   WHERE icu.icu_rank = 1
),

ARF_Inclusion_Codes AS (
   SELECT hadm_id, STRING_AGG(icd_code, ', ') AS arf_icd_code FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
   WHERE icd_code IN ('J9600', 'J9601', 'J9602', 'J9620', 'J9621', 'J9622', 'J9690', 'J9691', 'J9692', '51881', '51884')
   GROUP BY hadm_id
),

Pregnancy_Codes AS (
   SELECT hadm_id, STRING_AGG(icd_code, ', ') AS pregnancy_icd_codes FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
   WHERE (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^Z3[3469]')) OR (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^V2[234]')) GROUP BY hadm_id
),
Congenital_Codes AS (
   SELECT hadm_id, STRING_AGG(icd_code, ', ') AS congenital_anomaly_icd_codes FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
   WHERE (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(74|75)')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^Q')) GROUP BY hadm_id
),
Hematolymphoid_Malignancy_Codes AS (
    SELECT
        hadm_id,
        STRING_AGG(icd_code, ', ' ORDER BY icd_code) AS hematolymphoid_icd_codes
    FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
    WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
      AND (
        (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(20[0-8]|1985|2384|2387[2-6])'))
        OR
        (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^(C8[1-68]|C9[0-5]|D4[567]|C7952)'))
      )
    GROUP BY hadm_id
),
Malignancy_Codes AS (
   SELECT hadm_id, STRING_AGG(icd_code, ', ') AS malignancy_icd_codes FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
   WHERE (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(1[4-9]|20)')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C')) GROUP BY hadm_id
),
AIDS_Codes AS (
  SELECT hadm_id, STRING_AGG(icd_code, ', ') AS aids_icd_codes
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
    AND (
         icd_code IN ('B20', 'V08', '042')
         OR (icd_version = 10 AND icd_code LIKE 'O987%')
    )
  GROUP BY hadm_id
),
DNI_DNR_From_Events AS (
  SELECT stay_id, STRING_AGG(DISTINCT value, '; ') AS dni_dnr_status
  FROM `physionet-data.mimiciv_3_1_icu.chartevents`
  WHERE itemid IN (223758, 228687) AND value IN ('DNI (do not intubate)', 'DNR (do not resuscitate)', 'DNI/DNR')
  GROUP BY stay_id
),
DNI_DNR_From_ICD AS (
  SELECT hadm_id, STRING_AGG(icd_code, '; ') AS dni_dnr_icd
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE icd_code IN ('V4986', 'Z66')
  GROUP BY hadm_id
),


Comorbidities_Flags AS (
   SELECT hadm_id,
       MAX(CASE WHEN (icd_version = 10 AND icd_code LIKE 'E10%') OR (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^250\d[13]')) THEN 1 ELSE 0 END) AS has_t1d,
       MAX(CASE WHEN (icd_version = 10 AND icd_code LIKE 'E11%') OR (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^250\d[02]')) THEN 1 ELSE 0 END) AS has_t2d,
       MAX(CASE WHEN icd_code IN ('4010', '4011', '4019') OR (icd_version = 10 AND icd_code LIKE 'I10%') THEN 1 ELSE 0 END) AS has_hypertension,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '272%') OR (icd_version = 10 AND icd_code LIKE 'E78%') THEN 1 ELSE 0 END) AS has_dyslipidemia,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^41[0-4]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I2[0-5]')) THEN 1 ELSE 0 END) AS has_ischemic_heart_disease,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '413%') OR (icd_version = 10 AND icd_code LIKE 'I20%') THEN 1 ELSE 0 END) AS has_angina,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '4140%' OR icd_code = '41181')) OR (icd_version = 10 AND icd_code IN ('I210', 'I211', 'I212', 'I213', 'I21B', 'I240', 'I251', 'I257', 'I2581', 'I2582', 'I2583', 'I2584', 'I2481')) THEN 1 ELSE 0 END) AS has_coronary_artery_disease,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '410%' OR icd_code LIKE '412%')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I2[1-3]|^I25')) THEN 1 ELSE 0 END) AS has_myocardial_infarction,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '428%') OR (icd_version = 10 AND icd_code IN ('I110', 'I13', 'I9713', 'I0981') OR icd_code LIKE 'I50%') THEN 1 ELSE 0 END) AS has_heart_failure,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '4275') OR (icd_version = 10 AND (icd_code LIKE 'I46%' OR icd_code IN ('I97710', 'I9712', 'I9771'))) THEN 1 ELSE 0 END) AS has_cardiac_arrest,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '7855%' OR icd_code IN ('99800', '99801', '99802', '99809'))) OR (icd_version = 10 AND (icd_code LIKE 'R57%' OR icd_code IN ('T780', 'T782', 'T886', 'T805', 'T811'))) THEN 1 ELSE 0 END) AS has_shock,
       MAX(CASE WHEN icd_code IN ('78551', 'R570') THEN 1 ELSE 0 END) AS has_cardiogenic_shock,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '42731') OR (icd_version = 10 AND icd_code IN ('I480', 'I481', 'I482', 'I4891')) THEN 1 ELSE 0 END) AS has_atrial_fibrillation,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^74[56]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^Q2[0-4]')) THEN 1 ELSE 0 END) AS has_congenital_heart_defect,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '421%' OR icd_code IN ('42490', '42491', '07422', '09884', '11504', '11514', '11594', '03642', '11281'))) OR (icd_version = 10 AND (icd_code LIKE 'I33%' OR icd_code LIKE 'I38%' OR icd_code IN ('B376', 'A5483', 'M3211', 'A3282', 'A3951', 'M0531', 'A5203', 'A1884', 'A0102'))) THEN 1 ELSE 0 END) AS has_infective_endocarditis,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^43[0-8]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I6[0-9]')) THEN 1 ELSE 0 END) AS has_cerebrovascular_disease,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('45340', '45342', '45350', '45352')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^(I8240|I8243|I8244|I8245|I8246|I8249|I824Z|I8250|I8253|I8254|I8255|I8256|I8259|I825Z)')) THEN 1 ELSE 0 END) AS has_dvt,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^48[0-8]')) OR (icd_version = 10 AND (REGEXP_CONTAINS(icd_code, r'^J1[2-8]') OR icd_code = 'J95851')) THEN 1 ELSE 0 END) AS has_pneumonia,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^48[12]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^J1[3-5]')) THEN 1 ELSE 0 END) AS has_bacterial_pneumonia,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '480%') OR (icd_version = 10 AND icd_code LIKE 'J12%') THEN 1 ELSE 0 END) AS has_viral_pneumonia,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '491%' OR icd_code LIKE '492%' OR icd_code LIKE '496%')) OR (icd_version = 10 AND (icd_code LIKE 'J41%' OR icd_code LIKE 'J42%' OR icd_code LIKE 'J43%' OR icd_code LIKE 'J44%')) THEN 1 ELSE 0 END) AS has_copd,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '491%') OR (icd_version = 10 AND icd_code LIKE 'J41%') THEN 1 ELSE 0 END) AS has_chronic_bronchitis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '492%') OR (icd_version = 10 AND icd_code LIKE 'J43%') THEN 1 ELSE 0 END) AS has_emphysema,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '493%') OR (icd_version = 10 AND icd_code LIKE 'J45%') THEN 1 ELSE 0 END) AS has_asthma,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '494%') OR (icd_version = 10 AND icd_code LIKE 'J47%') THEN 1 ELSE 0 END) AS has_bronchiectasis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('515', '5160', '5161', '5162', '51630', '51631', '51632', '51633', '51634', '51635', '51636', '51637', '5164', '5165', '51661', '51662', '51663', '51664', '51669', '5169', '135', '5178', '7100', '7101', '7102', '7103', '7104', '71481')) OR (icd_version = 10 AND icd_code IN ('J8410', 'J8489', 'J8401', 'J8403', 'J8402', 'J84111', 'J84112', 'J84113', 'J84114', 'J84115', 'J842', 'J84116', 'J84117', 'J8481', 'J8482', 'J84841', 'J84842', 'J8483', 'J84843', 'J84848', 'J849', 'D860', 'D861', 'D862', 'J99', 'M3213', 'M3401', 'M3301', 'M3502', 'M3311', 'M3391', 'M3321', 'M0501', 'M0511', 'M0517', 'M0519')) THEN 1 ELSE 0 END) AS has_ild,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '515') OR (icd_version = 10 AND icd_code IN ('J8410', 'J8489')) THEN 1 ELSE 0 END) AS has_post_inflammatory_pulmonary_fibrosis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '5160') OR (icd_version = 10 AND icd_code = 'J8401') THEN 1 ELSE 0 END) AS has_pulmonary_alveolar_proteinosis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '5161') OR (icd_version = 10 AND icd_code = 'J8403') THEN 1 ELSE 0 END) AS has_idiopathic_pulmonary_hemosiderosis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('5162', '7100', '5178')) OR (icd_version = 10 AND icd_code IN ('J8402', 'M3213')) THEN 1 ELSE 0 END) AS has_pulmonary_alveolar_microlithiasis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51630') OR (icd_version = 10 AND icd_code = 'J84111') THEN 1 ELSE 0 END) AS has_idiopathic_interstitial_pneumonia_nos,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51631') OR (icd_version = 10 AND icd_code = 'J84112') THEN 1 ELSE 0 END) AS has_idiopathic_pulmonary_fibrosis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51632') OR (icd_version = 10 AND icd_code = 'J84113') THEN 1 ELSE 0 END) AS has_idiopathic_non_specific_inter_pneumo,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51633') OR (icd_version = 10 AND icd_code = 'J84114') THEN 1 ELSE 0 END) AS has_acute_interstitial_pneumonitis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51634') OR (icd_version = 10 AND icd_code = 'J84115') THEN 1 ELSE 0 END) AS has_respiratory_bronchiolitis_inter_lung_dis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51635') OR (icd_version = 10 AND icd_code = 'J842') THEN 1 ELSE 0 END) AS has_idiopathic_lymphoid_inter_pneumonia,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51636') OR (icd_version = 10 AND icd_code = 'J84116') THEN 1 ELSE 0 END) AS has_cryptogenic_organizing_pneumonia,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51637') OR (icd_version = 10 AND icd_code = 'J84117') THEN 1 ELSE 0 END) AS has_desquamative_interstitial_pneumonia,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '5164') OR (icd_version = 10 AND icd_code = 'J8481') THEN 1 ELSE 0 END) AS has_lymphangioleiomyomatosis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '5165') OR (icd_version = 10 AND icd_code = 'J8482') THEN 1 ELSE 0 END) AS has_adult_pulm_langerhans_cell_histiocytos,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51661') OR (icd_version = 10 AND icd_code = 'J84841') THEN 1 ELSE 0 END) AS has_neuroendocrine_cell_hyperpi_of_infancy,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51662') OR (icd_version = 10 AND icd_code = 'J84842') THEN 1 ELSE 0 END) AS has_pulmonary_interstitial_glycogenosis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51663') OR (icd_version = 10 AND icd_code = 'J8483') THEN 1 ELSE 0 END) AS has_surfactant_mutatons_of_the_lung,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51664') OR (icd_version = 10 AND icd_code = 'J84843') THEN 1 ELSE 0 END) AS has_alveol_cap_dysplasia_w_vein_misalign,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '51669') OR (icd_version = 10 AND icd_code = 'J84848') THEN 1 ELSE 0 END) AS has_other_interstital_lung_dis_of_childhood,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '5169') OR (icd_version = 10 AND icd_code = 'J849') THEN 1 ELSE 0 END) AS has_other_nonspec_alveol_parietoalveol_pneumopathies,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('135', '5178')) OR (icd_version = 10 AND icd_code IN ('D860', 'D861', 'D862')) THEN 1 ELSE 0 END) AS has_sarcoidosis,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '5178') OR (icd_version = 10 AND icd_code = 'J99') THEN 1 ELSE 0 END) AS has_lung_involvement_in_other_lung_diseases,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('7101', '5178')) OR (icd_version = 10 AND icd_code IN ('M3401', 'M3301')) THEN 1 ELSE 0 END) AS has_systemic_sclerosis_w_lung_involve,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('7102', '5178')) OR (icd_version = 10 AND icd_code = 'M3502') THEN 1 ELSE 0 END) AS has_sicca_syndrome_w_lung_involve,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('7103', '5178')) OR (icd_version = 10 AND icd_code IN ('M3311', 'M3391')) THEN 1 ELSE 0 END) AS has_dermatomyositis_with_lung_involve,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('7104', '5178')) OR (icd_version = 10 AND icd_code = 'M3321') THEN 1 ELSE 0 END) AS has_polymyositis_w_lung_involvement,
       MAX(CASE WHEN (icd_version = 9 AND icd_code = '71481') OR (icd_version = 10 AND icd_code IN ('M0501', 'M0511', 'M0517', 'M0519')) THEN 1 ELSE 0 END) AS has_rheumatoid_lung_disease,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code = 'V1255' OR icd_code LIKE '4151%' OR icd_code LIKE '4162%')) OR (icd_version = 10 AND (icd_code LIKE 'I26%' OR icd_code LIKE 'I2782%' OR icd_code IN ('T790', 'T791', 'T800', 'T817', 'T828'))) THEN 1 ELSE 0 END) AS has_pulmonary_embolism,
       MAX(CASE WHEN icd_code IN ('0117', '860', 'A15') OR (icd_version = 9 AND icd_code LIKE '512%') OR (icd_version = 10 AND (icd_code LIKE 'J93%' OR icd_code = 'J86' OR icd_code = 'J95811' OR icd_code = 'S270')) THEN 1 ELSE 0 END) AS has_pneumothorax,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '4160%') OR (icd_version = 10 AND (icd_code = 'I270' OR icd_code LIKE 'I272%')) THEN 1 ELSE 0 END) AS has_pulmonary_hypertension,
       MAX(CASE WHEN icd_code = 'J80' THEN 1 ELSE 0 END) AS has_ards,
       MAX(CASE WHEN icd_code IN ('51881', '51884', '51883', '51882') OR (icd_version = 10 AND icd_code LIKE 'J96%') THEN 1 ELSE 0 END) AS has_respiratory_failure,
       MAX(CASE WHEN icd_code IN ('J9601', 'J9621', 'J9691') THEN 1 ELSE 0 END) AS has_arf_t1,
       MAX(CASE WHEN icd_code IN ('J9602', 'J9622', 'J9692') THEN 1 ELSE 0 END) AS has_arf_t2,
       MAX(CASE WHEN icd_code IN ('0202', '0223', 'A427', 'B377', 'A267', 'A282', 'A5486', 'A327', 'A392', 'A393', 'A394', 'A207', 'A217', 'A483', 'A227') OR (icd_version = 10 AND (icd_code LIKE 'A40%' OR icd_code LIKE 'A41%' OR icd_code = 'O85')) OR (icd_version = 9 AND icd_code LIKE '038%') THEN 1 ELSE 0 END) AS has_sepsis,
       MAX(CASE WHEN icd_code IN ('K702', 'K704', 'K740', 'K741', 'K742', 'K743', 'K744', 'K745', 'K746', '5715', '5712', '5716') OR (icd_version = 10 AND icd_code LIKE 'K703%') OR (icd_version = 10 AND icd_code LIKE 'K721%') THEN 1 ELSE 0 END) AS has_cirrhosis,
       MAX(CASE WHEN icd_code IN ('5731', '5732', '5733', 'K754', 'K7581') OR (icd_version = 9 AND icd_code LIKE '070%') OR (icd_version = 10 AND (icd_code LIKE 'B15%' OR icd_code LIKE 'B16%' OR icd_code LIKE 'B17%' OR icd_code LIKE 'B18%' OR icd_code LIKE 'B19%' OR icd_code LIKE 'K701%' OR icd_code LIKE 'K73%')) THEN 1 ELSE 0 END) AS has_hepatitis,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^53[1-4]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^K2[5-8]')) THEN 1 ELSE 0 END) AS has_peptic_ulcer_disease,
       MAX(CASE WHEN icd_code IN ('I150', 'I151','N19', 'N11', 'N12', 'N14', 'N15', 'N16', 'N00', 'N01', 'N02', 'N06', 'N07', 'N08', '5851', '5852', '5853', '5854', '5855', '5856', '5859', '28521', '403', '404', 'M3214', 'M3215', 'M3504', 'M350A') OR (icd_version = 10 AND (icd_code LIKE 'E102%' OR icd_code LIKE 'E112%' OR icd_code LIKE 'E132%' OR icd_code LIKE 'I12%' OR icd_code LIKE 'I13%' OR icd_code LIKE 'N18%' OR icd_code LIKE 'N03%' OR icd_code LIKE 'N04%' OR icd_code LIKE 'N05%')) THEN 1 ELSE 0 END) AS has_ckd,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '584%' OR icd_code = '586')) OR (icd_version = 10 AND (icd_code LIKE 'N17%' OR icd_code = 'N19')) THEN 1 ELSE 0 END) AS has_aki_comorbidity,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '342%') OR (icd_version = 10 AND icd_code LIKE 'G81%') THEN 1 ELSE 0 END) AS has_hemiplegia,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '290%') OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^F0[1-4]')) THEN 1 ELSE 0 END) AS has_dementia,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^20[4-8]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C9[1-5]')) THEN 1 ELSE 0 END) AS has_leukemia,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^20[0-2]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C8[1-68]')) THEN 1 ELSE 0 END) AS has_lymphoma,
       MAX(CASE WHEN icd_code IN ('B20', 'V08', '042') OR (icd_version = 10 AND icd_code LIKE 'O987%') THEN 1 ELSE 0 END) AS has_aids,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '710%') OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^M3[0-6]')) THEN 1 ELSE 0 END) AS has_connective_tissue_disease,
       MAX(CASE WHEN (icd_version = 10 AND (icd_code LIKE 'M05%' OR icd_code LIKE 'M06%')) OR (icd_version = 9 AND icd_code LIKE '714%') THEN 1 ELSE 0 END) AS has_rheumatoid_arthritis,
       MAX(CASE WHEN icd_code = 'Z66' THEN 1 ELSE 0 END) AS has_dnr_status,
       MAX(CASE WHEN icd_code = 'Z942' THEN 1 ELSE 0 END) AS has_lung_transplant
   FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
   GROUP BY hadm_id
),

Medication_Flags AS (
    SELECT
        ie.hadm_id,
        MAX(CASE WHEN itemid = 221906 THEN 1 ELSE 0 END) AS had_norepinephrine,
        MAX(CASE WHEN itemid = 221289 THEN 1 ELSE 0 END) AS had_epinephrine,
        MAX(CASE WHEN itemid = 221662 THEN 1 ELSE 0 END) AS had_dopamine,
        MAX(CASE WHEN itemid = 222315 THEN 1 ELSE 0 END) AS had_vasopressin,
        MAX(CASE WHEN itemid = 221653 THEN 1 ELSE 0 END) AS had_dobutamine,
        MAX(CASE WHEN itemid = 221749 THEN 1 ELSE 0 END) AS had_phenylephrine,
        MAX(CASE WHEN itemid = 221986 THEN 1 ELSE 0 END) AS had_milrinone
    FROM `physionet-data.mimiciv_3_1_icu.inputevents` ie
    WHERE ie.hadm_id IN (SELECT hadm_id FROM Core_Cohort)
    GROUP BY ie.hadm_id
),

Baseline_Features AS (
    WITH
    Time_Window AS (
      SELECT
        stay_id,
        intime,
        DATETIME_SUB(intime, INTERVAL 24 HOUR) as window_start,
        DATETIME_ADD(intime, INTERVAL 24 HOUR) as window_end
      FROM `physionet-data.mimiciv_3_1_icu.icustays`
      WHERE stay_id IN (SELECT stay_id FROM Core_Cohort)
    ),
    all_events_unioned AS (
        SELECT
            tw.stay_id, cc.hadm_id, ev.charttime, ev.itemid,
            CASE
                WHEN ev.itemid IN (229355, 229359, 229361, 229360) THEN ev.valuenum * 1000
                ELSE ev.valuenum
            END AS valuenum,
            ev.value
        FROM Time_Window tw
        INNER JOIN Core_Cohort cc ON tw.stay_id = cc.stay_id
        INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ev ON tw.stay_id = ev.stay_id
        WHERE ev.charttime BETWEEN tw.window_start AND tw.window_end

        UNION ALL

        SELECT
            tw.stay_id, cc.hadm_id, ev.charttime, ev.itemid,
            CASE
                WHEN ev.itemid IN (52075, 51133, 52074, 52069, 52073) THEN ev.valuenum * 1000
                WHEN ev.itemid = 51199 THEN ev.valuenum / 1000
                ELSE ev.valuenum
            END AS valuenum,
            ev.comments AS value
        FROM Time_Window tw
        INNER JOIN Core_Cohort cc ON tw.stay_id = cc.stay_id
        INNER JOIN `physionet-data.mimiciv_3_1_hosp.labevents` ev ON cc.hadm_id = ev.hadm_id
        WHERE ev.charttime BETWEEN tw.window_start AND tw.window_end

        UNION ALL

        SELECT
            tw.stay_id, cc.hadm_id, ev.charttime, ev.itemid,
            ev.value AS valuenum,
            CAST(NULL AS STRING) AS value
        FROM Time_Window tw
        INNER JOIN Core_Cohort cc ON tw.stay_id = cc.stay_id
        INNER JOIN `physionet-data.mimiciv_3_1_icu.outputevents` ev ON tw.stay_id = ev.stay_id
        WHERE ev.itemid = 227519 AND ev.charttime BETWEEN tw.window_start AND tw.window_end
    ),
    events_ranked AS (
        SELECT stay_id, valuenum, value as value_str,
            CASE
                WHEN itemid=226707 THEN 'height_in' WHEN itemid=226730 THEN 'height_cm' WHEN itemid=226531 THEN 'weight_lbs' WHEN itemid=226512 THEN 'weight_kg'
                WHEN itemid=229770 THEN 'resting_pulse' WHEN itemid=220045 THEN 'heart_rate' WHEN itemid IN (220210, 224690) THEN 'resp_rate'
                WHEN itemid IN (220050, 220179, 225309) THEN 'sbp' WHEN itemid IN (220051, 220180, 225310) THEN 'dbp' WHEN itemid IN (220052, 220181, 225312) THEN 'mbp'
                WHEN itemid=223761 THEN 'temp_f' WHEN itemid=223762 THEN 'temp_c' WHEN itemid IN (226755, 227013) THEN 'gcs' WHEN itemid=223901 THEN 'gcs_motor' WHEN itemid=223900 THEN 'gcs_verbal' WHEN itemid=220739 THEN 'gcs_eye'
                WHEN itemid IN (50811, 51640, 51222) THEN 'hb' WHEN itemid IN (51279, 52170) THEN 'rbc' WHEN itemid=50810 THEN 'hematocrit_calc' WHEN itemid IN (52028, 51638, 51639, 51221) THEN 'hematocrit'
                WHEN itemid IN (51300, 51301, 51755, 220546) THEN 'wbc' WHEN itemid=51248 THEN 'mch' WHEN itemid=51249 THEN 'mchc' WHEN itemid IN (51691, 51250) THEN 'mcv'
                WHEN itemid=52172 THEN 'rdw_sd' WHEN itemid=51277 THEN 'rdw_cv' WHEN itemid IN (51704, 51265) THEN 'platelet_count' WHEN itemid=51240 THEN 'p_lcc'
                WHEN itemid IN (51256, 225643) THEN 'neutrophils_pct' WHEN itemid=52075 THEN 'abs_neutrophil_lab' WHEN itemid=229355 THEN 'abs_neutrophil_chart'
                WHEN itemid IN (51244, 51245, 51690, 225641) THEN 'lymphocytes_pct' WHEN itemid IN (52769, 51133, 53132) THEN 'abs_lymphocyte_lab' WHEN itemid=229358 THEN 'abs_lymphocyte_chart'
                WHEN itemid IN (51254, 225642) THEN 'monocyte_pct' WHEN itemid IN (51253, 52074) THEN 'abs_monocyte_lab' WHEN itemid=229359 THEN 'abs_monocyte_chart'
                WHEN itemid IN (51146, 225639) THEN 'basophil_pct' WHEN itemid=229361 THEN 'abs_basophil_chart' WHEN itemid=52069 THEN 'abs_basophil_lab'
                WHEN itemid IN (51200, 225640) THEN 'eosinophil_pct' WHEN itemid=51199 THEN 'eosinophil_count' WHEN itemid=52073 THEN 'abs_eosinophil_lab' WHEN itemid=229360 THEN 'abs_eosinophil_chart'
                WHEN itemid=50889 THEN 'crp_lab' WHEN itemid=227444 THEN 'crp_chart' WHEN itemid=51288 THEN 'esr'
                WHEN itemid IN (52921, 51274) THEN 'pt' WHEN itemid IN (51275, 52923) THEN 'aptt' WHEN itemid IN (51675, 51237) THEN 'inr'
                WHEN itemid IN (50931, 50809, 52569, 52027) THEN 'glucose_lab' WHEN itemid=50854 THEN 'abs_a1c' WHEN itemid=51631 THEN 'glycated_hb' WHEN itemid=50852 THEN 'hba1c_pct'
                WHEN itemid IN (50907, 50906) THEN 'total_cholesterol' WHEN itemid IN (50905, 50906) THEN 'ldl' WHEN itemid=50904 THEN 'hdl' WHEN itemid=51000 THEN 'triglycerides'
                WHEN itemid IN (50885, 53089) THEN 'bilirubin_total' WHEN itemid IN (50883, 51592) THEN 'bilirubin_direct' WHEN itemid IN (50884, 51751) THEN 'bilirubin_indirect'
                WHEN itemid=50878 THEN 'ast' WHEN itemid=50861 THEN 'alt' WHEN itemid IN (53086, 50863) THEN 'alp' WHEN itemid IN (50927, 53093) THEN 'ggt'
                WHEN itemid IN (50912, 52546) THEN 'creatinine_lab' WHEN itemid=52024 THEN 'creatinine_wb_lab' WHEN itemid IN (51842, 52647, 51006) THEN 'bun_lab' WHEN itemid IN (50920, 52026, 51770) THEN 'egfr'
                WHEN itemid IN (50983, 52623) THEN 'sodium_lab' WHEN itemid IN (50824, 52455) THEN 'sodium_wb_lab'
                WHEN itemid IN (50971, 50833, 52610) THEN 'potassium_lab' WHEN itemid IN (52452, 50822) THEN 'potassium_wb_lab'
                WHEN itemid IN (50902, 52535) THEN 'chloride_lab' WHEN itemid IN (50806, 52434) THEN 'chloride_wb_lab'
                WHEN itemid=50882 THEN 'bicarbonate_lab' WHEN itemid=50803 THEN 'bicarbonate_calc_wb_lab' WHEN itemid=52039 THEN 'bicarbonate_calc_lab'
                WHEN itemid IN (50808, 51624) THEN 'calcium_free_lab' WHEN itemid IN (52035, 52034, 50893) THEN 'calcium_total_lab'
                WHEN itemid=50970 THEN 'phosphate_lab' WHEN itemid IN (50862, 53085, 52022, 53138) THEN 'albumin_lab'
                WHEN itemid IN (50821, 220224) THEN 'pao2' WHEN itemid=220277 THEN 'spo2' WHEN itemid IN (229407, 229393, 229405) THEN 'pf_ratio' WHEN itemid=223835 THEN 'fio2'
                WHEN itemid=50818 THEN 'paco2' WHEN itemid IN (50820, 223830) THEN 'ph_arterial'
                WHEN itemid=227519 THEN 'urine_output' WHEN itemid IN (51994, 51498) THEN 'urine_spec_gravity' WHEN itemid=52045 THEN 'urine_ph' WHEN itemid IN (52044, 51093) THEN 'urine_osmolality'
                WHEN itemid=51102 THEN 'urine_protein' WHEN itemid IN (51069, 52703) THEN 'urine_albumin' WHEN itemid IN (51084, 51981, 51478) THEN 'urine_glucose'
                WHEN itemid IN (51106, 52000, 51082) THEN 'urine_creatinine' WHEN itemid IN (51984, 51484) THEN 'urine_ketone'
                WHEN itemid = 224700 THEN 'total_peep' WHEN itemid = 220339 THEN 'peep_set'
                WHEN itemid IN (227580, 227581, 227579, 227578, 227577, 227582) THEN 'bipap_ipap' WHEN itemid=227583 THEN 'cpap'
                WHEN itemid=226732 THEN 'o2_device'
            END AS concept,
            ROW_NUMBER() OVER (PARTITION BY stay_id,
                CASE
                    WHEN itemid=226707 THEN 'height_in' WHEN itemid=226730 THEN 'height_cm' WHEN itemid=226531 THEN 'weight_lbs' WHEN itemid=226512 THEN 'weight_kg'
                    WHEN itemid=229770 THEN 'resting_pulse' WHEN itemid=220045 THEN 'heart_rate' WHEN itemid IN (220210, 224690) THEN 'resp_rate'
                    WHEN itemid IN (220050, 220179, 225309) THEN 'sbp' WHEN itemid IN (220051, 220180, 225310) THEN 'dbp' WHEN itemid IN (220052, 220181, 225312) THEN 'mbp'
                    WHEN itemid=223761 THEN 'temp_f' WHEN itemid=223762 THEN 'temp_c' WHEN itemid IN (226755, 227013) THEN 'gcs' WHEN itemid=223901 THEN 'gcs_motor' WHEN itemid=223900 THEN 'gcs_verbal' WHEN itemid=220739 THEN 'gcs_eye'
                    WHEN itemid IN (50811, 51640, 51222) THEN 'hb' WHEN itemid IN (51279, 52170) THEN 'rbc' WHEN itemid=50810 THEN 'hematocrit_calc' WHEN itemid IN (52028, 51638, 51639, 51221) THEN 'hematocrit'
                    WHEN itemid IN (51300, 51301, 51755, 220546) THEN 'wbc' WHEN itemid=51248 THEN 'mch' WHEN itemid=51249 THEN 'mchc' WHEN itemid IN (51691, 51250) THEN 'mcv'
                    WHEN itemid=52172 THEN 'rdw_sd' WHEN itemid=51277 THEN 'rdw_cv' WHEN itemid IN (51704, 51265) THEN 'platelet_count' WHEN itemid=51240 THEN 'p_lcc'
                    WHEN itemid IN (51256, 225643) THEN 'neutrophils_pct' WHEN itemid=52075 THEN 'abs_neutrophil_lab' WHEN itemid=229355 THEN 'abs_neutrophil_chart'
                    WHEN itemid IN (51244, 51245, 51690, 225641) THEN 'lymphocytes_pct' WHEN itemid IN (52769, 51133, 53132) THEN 'abs_lymphocyte_lab' WHEN itemid=229358 THEN 'abs_lymphocyte_chart'
                    WHEN itemid IN (51254, 225642) THEN 'monocyte_pct' WHEN itemid IN (51253, 52074) THEN 'abs_monocyte_lab' WHEN itemid=229359 THEN 'abs_monocyte_chart'
                    WHEN itemid IN (51146, 225639) THEN 'basophil_pct' WHEN itemid=229361 THEN 'abs_basophil_chart' WHEN itemid=52069 THEN 'abs_basophil_lab'
                    WHEN itemid IN (51200, 225640) THEN 'eosinophil_pct' WHEN itemid=51199 THEN 'eosinophil_count' WHEN itemid=52073 THEN 'abs_eosinophil_lab' WHEN itemid=229360 THEN 'abs_eosinophil_chart'
                    WHEN itemid=50889 THEN 'crp_lab' WHEN itemid=227444 THEN 'crp_chart' WHEN itemid=51288 THEN 'esr'
                    WHEN itemid IN (52921, 51274) THEN 'pt' WHEN itemid IN (51275, 52923) THEN 'aptt' WHEN itemid IN (51675, 51237) THEN 'inr'
                    WHEN itemid IN (50931, 50809, 52569, 52027) THEN 'glucose_lab' WHEN itemid=50854 THEN 'abs_a1c' WHEN itemid=51631 THEN 'glycated_hb' WHEN itemid=50852 THEN 'hba1c_pct'
                    WHEN itemid IN (50907, 50906) THEN 'total_cholesterol' WHEN itemid IN (50905, 50906) THEN 'ldl' WHEN itemid=50904 THEN 'hdl' WHEN itemid=51000 THEN 'triglycerides'
                    WHEN itemid IN (50885, 53089) THEN 'bilirubin_total' WHEN itemid IN (50883, 51592) THEN 'bilirubin_direct' WHEN itemid IN (50884, 51751) THEN 'bilirubin_indirect'
                    WHEN itemid=50878 THEN 'ast' WHEN itemid=50861 THEN 'alt' WHEN itemid IN (53086, 50863) THEN 'alp' WHEN itemid IN (50927, 53093) THEN 'ggt'
                    WHEN itemid IN (50912, 52546) THEN 'creatinine_lab' WHEN itemid=52024 THEN 'creatinine_wb_lab' WHEN itemid IN (51842, 52647, 51006) THEN 'bun_lab' WHEN itemid IN (50920, 52026, 51770) THEN 'egfr'
                    WHEN itemid IN (50983, 52623) THEN 'sodium_lab' WHEN itemid IN (50824, 52455) THEN 'sodium_wb_lab'
                    WHEN itemid IN (50971, 50833, 52610) THEN 'potassium_lab' WHEN itemid IN (52452, 50822) THEN 'potassium_wb_lab'
                    WHEN itemid IN (50902, 52535) THEN 'chloride_lab' WHEN itemid IN (50806, 52434) THEN 'chloride_wb_lab'
                    WHEN itemid=50882 THEN 'bicarbonate_lab' WHEN itemid=50803 THEN 'bicarbonate_calc_wb_lab' WHEN itemid=52039 THEN 'bicarbonate_calc_lab'
                    WHEN itemid IN (50808, 51624) THEN 'calcium_free_lab' WHEN itemid IN (52035, 52034, 50893) THEN 'calcium_total_lab'
                    WHEN itemid=50970 THEN 'phosphate_lab' WHEN itemid IN (50862, 53085, 52022, 53138) THEN 'albumin_lab'
                    WHEN itemid IN (50821, 220224) THEN 'pao2' WHEN itemid=220277 THEN 'spo2' WHEN itemid IN (229407, 229393, 229405) THEN 'pf_ratio' WHEN itemid=223835 THEN 'fio2'
                    WHEN itemid=50818 THEN 'paco2' WHEN itemid IN (50820, 223830) THEN 'ph_arterial'
                    WHEN itemid=227519 THEN 'urine_output' WHEN itemid IN (51994, 51498) THEN 'urine_spec_gravity' WHEN itemid=52045 THEN 'urine_ph' WHEN itemid IN (52044, 51093) THEN 'urine_osmolality'
                    WHEN itemid=51102 THEN 'urine_protein' WHEN itemid IN (51069, 52703) THEN 'urine_albumin' WHEN itemid IN (51084, 51981, 51478) THEN 'urine_glucose'
                    WHEN itemid IN (51106, 52000, 51082) THEN 'urine_creatinine' WHEN itemid IN (51984, 51484) THEN 'urine_ketone'
                    WHEN itemid = 224700 THEN 'total_peep' WHEN itemid = 220339 THEN 'peep_set'
                    WHEN itemid IN (227580, 227581, 227579, 227578, 227577, 227582) THEN 'bipap_ipap' WHEN itemid=227583 THEN 'cpap'
                    WHEN itemid=226732 THEN 'o2_device'
                END
            ORDER BY charttime ASC) AS rn
        FROM all_events_unioned
    ),
    First_Day_Values AS (
        SELECT stay_id,
            MAX(CASE WHEN concept = 'height_in' THEN valuenum END) AS first_height_in, MAX(CASE WHEN concept = 'height_cm' THEN valuenum END) AS first_height_cm, MAX(CASE WHEN concept = 'weight_lbs' THEN valuenum END) AS first_weight_lbs, MAX(CASE WHEN concept = 'weight_kg' THEN valuenum END) AS first_weight_kg,
            MAX(CASE WHEN concept = 'resting_pulse' THEN valuenum END) AS first_resting_pulse, MAX(CASE WHEN concept = 'heart_rate' THEN valuenum END) AS first_heart_rate,
            MAX(CASE WHEN concept = 'resp_rate' THEN valuenum END) AS first_resp_rate, MAX(CASE WHEN concept = 'sbp' THEN valuenum END) AS first_sbp, MAX(CASE WHEN concept = 'dbp' THEN valuenum END) AS first_dbp, MAX(CASE WHEN concept = 'mbp' THEN valuenum END) AS first_mbp,
            MAX(CASE WHEN concept = 'temp_f' THEN valuenum END) AS first_temp_f, MAX(CASE WHEN concept = 'temp_c' THEN valuenum END) AS first_temp_c, MAX(CASE WHEN concept = 'gcs' THEN valuenum END) AS first_gcs,
            MAX(CASE WHEN concept = 'gcs_motor' THEN valuenum END) AS first_gcs_motor, MAX(CASE WHEN concept = 'gcs_verbal' THEN valuenum END) AS first_gcs_verbal, MAX(CASE WHEN concept = 'gcs_eye' THEN valuenum END) AS first_gcs_eye,
            MAX(CASE WHEN concept = 'hb' THEN valuenum END) AS first_hb, MAX(CASE WHEN concept = 'rbc' THEN valuenum END) AS first_rbc, MAX(CASE WHEN concept = 'hematocrit_calc' THEN valuenum END) AS first_hematocrit_calc, MAX(CASE WHEN concept = 'hematocrit' THEN valuenum END) AS first_hematocrit,
            MAX(CASE WHEN concept = 'wbc' THEN valuenum END) AS first_wbc, MAX(CASE WHEN concept = 'mch' THEN valuenum END) AS first_mch, MAX(CASE WHEN concept = 'mchc' THEN valuenum END) AS first_mchc, MAX(CASE WHEN concept = 'mcv' THEN valuenum END) AS first_mcv,
            MAX(CASE WHEN concept = 'rdw_sd' THEN valuenum END) AS first_rdw_sd, MAX(CASE WHEN concept = 'rdw_cv' THEN valuenum END) AS first_rdw_cv, MAX(CASE WHEN concept = 'platelet_count' THEN valuenum END) AS first_platelet_count, MAX(CASE WHEN concept = 'p_lcc' THEN valuenum END) AS first_p_lcc,
            MAX(CASE WHEN concept = 'neutrophils_pct' THEN valuenum END) AS first_neutrophils_pct, MAX(CASE WHEN concept = 'abs_neutrophil_lab' THEN valuenum END) AS first_abs_neutrophil_lab, MAX(CASE WHEN concept = 'abs_neutrophil_chart' THEN valuenum END) AS first_abs_neutrophil_chart,
            MAX(CASE WHEN concept = 'lymphocytes_pct' THEN valuenum END) AS first_lymphocytes_pct, MAX(CASE WHEN concept = 'abs_lymphocyte_lab' THEN valuenum END) AS first_abs_lymphocyte_lab, MAX(CASE WHEN concept = 'abs_lymphocyte_chart' THEN valuenum END) AS first_abs_lymphocyte_chart,
            MAX(CASE WHEN concept = 'monocyte_pct' THEN valuenum END) AS first_monocyte_pct, MAX(CASE WHEN concept = 'abs_monocyte_lab' THEN valuenum END) AS first_abs_monocyte_lab, MAX(CASE WHEN concept = 'abs_monocyte_chart' THEN valuenum END) AS first_abs_monocyte_chart,
            MAX(CASE WHEN concept = 'basophil_pct' THEN valuenum END) AS first_basophil_pct, MAX(CASE WHEN concept = 'abs_basophil_chart' THEN valuenum END) AS first_abs_basophil_chart, MAX(CASE WHEN concept = 'abs_basophil_lab' THEN valuenum END) AS first_abs_basophil_lab,
            MAX(CASE WHEN concept = 'eosinophil_pct' THEN valuenum END) AS first_eosinophil_pct, MAX(CASE WHEN concept = 'eosinophil_count' THEN valuenum END) AS first_eosinophil_count, MAX(CASE WHEN concept = 'abs_eosinophil_lab' THEN valuenum END) AS first_abs_eosinophil_lab, MAX(CASE WHEN concept = 'abs_eosinophil_chart' THEN valuenum END) AS first_abs_eosinophil_chart,
            MAX(CASE WHEN concept = 'crp_lab' THEN valuenum END) AS first_crp_lab, MAX(CASE WHEN concept = 'crp_chart' THEN valuenum END) AS first_crp_chart, MAX(CASE WHEN concept = 'esr' THEN valuenum END) AS first_esr,
            MAX(CASE WHEN concept = 'pt' THEN valuenum END) AS first_pt, MAX(CASE WHEN concept = 'aptt' THEN valuenum END) AS first_aptt, MAX(CASE WHEN concept = 'inr' THEN valuenum END) AS first_inr,
            MAX(CASE WHEN concept = 'glucose_lab' THEN valuenum END) AS first_glucose_lab, MAX(CASE WHEN concept = 'abs_a1c' THEN valuenum END) AS first_abs_a1c, MAX(CASE WHEN concept = 'glycated_hb' THEN valuenum END) AS first_glycated_hb, MAX(CASE WHEN concept = 'hba1c_pct' THEN valuenum END) AS first_hba1c_pct,
            MAX(CASE WHEN concept = 'total_cholesterol' THEN valuenum END) AS first_total_cholesterol, MAX(CASE WHEN concept = 'ldl' THEN valuenum END) AS first_ldl, MAX(CASE WHEN concept = 'hdl' THEN valuenum END) AS first_hdl, MAX(CASE WHEN concept = 'triglycerides' THEN valuenum END) AS first_triglycerides,
            MAX(CASE WHEN concept = 'bilirubin_total' THEN valuenum END) AS first_bilirubin_total, MAX(CASE WHEN concept = 'bilirubin_direct' THEN valuenum END) AS first_bilirubin_direct, MAX(CASE WHEN concept = 'bilirubin_indirect' THEN valuenum END) AS first_bilirubin_indirect,
            MAX(CASE WHEN concept = 'ast' THEN valuenum END) AS first_ast, MAX(CASE WHEN concept = 'alt' THEN valuenum END) AS first_alt, MAX(CASE WHEN concept = 'alp' THEN valuenum END) AS first_alp, MAX(CASE WHEN concept = 'ggt' THEN valuenum END) AS first_ggt,
            MAX(CASE WHEN concept = 'creatinine_lab' THEN valuenum END) AS first_creatinine_lab, MAX(CASE WHEN concept = 'creatinine_wb_lab' THEN valuenum END) AS first_creatinine_wb_lab, MAX(CASE WHEN concept = 'bun_lab' THEN valuenum END) AS first_bun_lab, MAX(CASE WHEN concept = 'egfr' THEN value_str END) AS first_egfr_comment,
            MAX(CASE WHEN concept = 'sodium_lab' THEN valuenum END) AS first_sodium_lab, MAX(CASE WHEN concept = 'sodium_wb_lab' THEN valuenum END) AS first_sodium_wb_lab,
            MAX(CASE WHEN concept = 'potassium_lab' THEN valuenum END) AS first_potassium_lab, MAX(CASE WHEN concept = 'potassium_wb_lab' THEN valuenum END) AS first_potassium_wb_lab,
            MAX(CASE WHEN concept = 'chloride_lab' THEN valuenum END) AS first_chloride_lab, MAX(CASE WHEN concept = 'chloride_wb_lab' THEN valuenum END) AS first_chloride_wb_lab,
            MAX(CASE WHEN concept = 'bicarbonate_lab' THEN valuenum END) AS first_bicarbonate_lab, MAX(CASE WHEN concept = 'bicarbonate_calc_wb_lab' THEN valuenum END) AS first_bicarbonate_calc_wb_lab, MAX(CASE WHEN concept = 'bicarbonate_calc_lab' THEN valuenum END) AS first_bicarbonate_calc_lab,
            MAX(CASE WHEN concept = 'calcium_free_lab' THEN valuenum END) AS first_calcium_free_lab, MAX(CASE WHEN concept = 'calcium_total_lab' THEN valuenum END) AS first_calcium_total_lab,
            MAX(CASE WHEN concept = 'phosphate_lab' THEN valuenum END) AS first_phosphate_lab, MAX(CASE WHEN concept = 'albumin_lab' THEN valuenum END) AS first_albumin_lab,
            MAX(CASE WHEN concept = 'pao2' THEN valuenum END) AS first_pao2, MAX(CASE WHEN concept = 'spo2' THEN valuenum END) AS first_spo2, MAX(CASE WHEN concept = 'pf_ratio' THEN valuenum END) AS first_pf_ratio, MAX(CASE WHEN concept = 'fio2' THEN valuenum END) AS first_fio2,
            MAX(CASE WHEN concept = 'paco2' THEN valuenum END) AS first_paco2, MAX(CASE WHEN concept = 'ph_arterial' THEN valuenum END) AS first_ph_arterial,
            MAX(CASE WHEN concept = 'urine_output' THEN valuenum END) AS first_urine_output, MAX(CASE WHEN concept = 'urine_spec_gravity' THEN valuenum END) AS first_urine_spec_gravity, MAX(CASE WHEN concept = 'urine_ph' THEN valuenum END) AS first_urine_ph,
            MAX(CASE WHEN concept = 'urine_osmolality' THEN valuenum END) AS first_urine_osmolality, MAX(CASE WHEN concept = 'urine_protein' THEN valuenum END) AS first_urine_protein, MAX(CASE WHEN concept = 'urine_albumin' THEN valuenum END) AS first_urine_albumin,
            MAX(CASE WHEN concept = 'urine_glucose' THEN valuenum END) AS first_urine_glucose, MAX(CASE WHEN concept = 'urine_creatinine' THEN valuenum END) AS first_urine_creatinine, MAX(CASE WHEN concept = 'urine_ketone' THEN valuenum END) AS first_urine_ketone,
            MAX(CASE WHEN concept = 'total_peep' THEN valuenum END) AS first_total_peep, MAX(CASE WHEN concept = 'peep_set' THEN valuenum END) AS first_peep_set,
            MAX(CASE WHEN concept = 'bipap_ipap' THEN valuenum END) AS first_bipap_ipap, MAX(CASE WHEN concept = 'bipap_epap' THEN valuenum END) AS first_bipap_epap, MAX(CASE WHEN concept = 'cpap' THEN valuenum END) AS first_cpap,
            MAX(CASE WHEN concept = 'o2_device' AND value_str = 'High Flow Nasal Cannula' THEN 1 ELSE 0 END) AS hfnc,
            MAX(CASE WHEN concept = 'o2_device' AND value_str IN ('Non-rebreather', 'Face tent', 'Aerosol-cool', 'Venti mask', 'Medium conc mask', 'Ultrasonic neb', 'Vapomist', 'Oxymizer', 'High flow neb', 'Nasal cannula') THEN 1 ELSE 0 END) AS supplemental_oxygen
        FROM events_ranked WHERE rn = 1
        GROUP BY stay_id
    )
    SELECT
        cohort.subject_id, cohort.hadm_id, cohort.stay_id,
        fdv.* EXCEPT(stay_id),
        mv.min_heart_rate
    FROM Core_Cohort AS cohort
    LEFT JOIN First_Day_Values as fdv ON cohort.stay_id = fdv.stay_id
    LEFT JOIN (SELECT stay_id, MIN(valuenum) as min_heart_rate FROM all_events_unioned WHERE itemid = 220045 GROUP BY stay_id) as mv ON cohort.stay_id = mv.stay_id
),

Outcomes_Base AS (
  SELECT
      core.subject_id, core.hadm_id, core.stay_id,
      icu.intime, adm.admittime, adm.dischtime, adm.deathtime, pat.dod
  FROM Core_Cohort core
  LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON core.stay_id = icu.stay_id
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON core.hadm_id = adm.hadm_id
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.patients` pat ON core.subject_id = pat.subject_id
),
Comprehensive_Outcomes AS (
  SELECT
      ob.stay_id,
      COALESCE(
          ob.deathtime,
          IF(adm.hospital_expire_flag = 1, ob.dischtime, NULL),
          CAST(ob.dod AS DATETIME)
      ) AS death_datetime,
      CASE
          WHEN ob.dod IS NOT NULL OR ob.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN 1
          ELSE 0
      END AS overall_mortality_flag,
      CASE
          WHEN ob.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN 1
          ELSE 0
      END AS in_hospital_mortality_flag,
      DATETIME_DIFF(ob.dischtime, ob.admittime, HOUR) / 24.0 AS hospital_los_days,
      LEAST(icu.los, DATETIME_DIFF(ob.dischtime, ob.admittime, HOUR) / 24.0) AS icu_los_days,
      CASE
          WHEN ob.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN DATETIME_DIFF(COALESCE(ob.deathtime, ob.dischtime), ob.admittime, HOUR) / 24.0
          ELSE NULL
      END AS in_hospital_mortality_duration,
      CASE
          WHEN ob.dod IS NOT NULL OR ob.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN DATETIME_DIFF(COALESCE(ob.deathtime, CAST(ob.dod AS DATETIME), ob.dischtime), ob.admittime, HOUR) / 24.0
          ELSE NULL
      END AS overall_mortality_duration
  FROM Outcomes_Base ob
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON ob.hadm_id = adm.hadm_id
  LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ob.stay_id = icu.stay_id
),
Proc_Events_Outcomes AS (
   SELECT
       stay_id,
       SUM(CASE WHEN itemid = 225303 THEN value END) AS duration_mask_ventilation,
       SUM(CASE WHEN itemid = 225792 THEN value END) AS duration_invasive_vent,
       MAX(CASE WHEN itemid = 227194 THEN 1 ELSE 0 END) AS extubation_flag
   FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
   WHERE stay_id IN (SELECT stay_id FROM Outcomes_Base)
   GROUP BY stay_id
),
Other_Flags AS (
   SELECT
       c.hadm_id,
       MAX(CASE WHEN ce.itemid = 224833 THEN 1 ELSE 0 END) AS weaning_deferred_flag,
       MAX(CASE WHEN proc.icd_code = '3965' THEN 1 ELSE 0 END) AS ecmo_flag,
       MAX(CASE WHEN proc.icd_code IN ('3352', '3350', '3351', '336', '0BYH0Z0', '0BYM0Z2', '0BYK0Z2', '0BYJ0Z0', '0BYJ0Z1', '0BYC0Z2', '0BYL0Z0', '0BYC0Z0', '0BYM0Z0', '0BYG0Z0', '0BYC0Z1', '0BYH0Z2', '0BYL0Z1', '0BYL0Z2', '0BYG0Z1', '0BYD0Z2', '0BYF0Z0', '0BYK0Z1', '0BYF0Z2', '0BYG0Z2', '0BYK0Z0', '0BYJ0Z2', '0BYF0Z1') THEN 1 ELSE 0 END) AS lung_transplant_current
   FROM Outcomes_Base c
   LEFT JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON c.stay_id = ce.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_hosp.procedures_icd` proc ON c.hadm_id = proc.hadm_id
   GROUP BY c.hadm_id
),
Hospital_Readmission AS (
  WITH next_admission AS (
      SELECT
          hadm_id,
          subject_id,
          dischtime,
          LEAD(admittime, 1) OVER (PARTITION BY subject_id ORDER BY admittime) as next_admittime
      FROM `physionet-data.mimiciv_3_1_hosp.admissions`
      WHERE subject_id IN (SELECT subject_id FROM Core_Cohort)
  )
  SELECT
      hadm_id,
      CASE
          WHEN next_admittime IS NOT NULL AND DATETIME_DIFF(next_admittime, dischtime, DAY) <= 30 THEN 1
          ELSE 0
      END AS readmission_30_day
  FROM next_admission
  WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
),
Ventilation_Events AS (
   SELECT stay_id, starttime as event_time, 'Invasive' as vent_type FROM `physionet-data.mimiciv_3_1_icu.procedureevents` WHERE itemid = 225792 AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, charttime as event_time, 'Invasive' as vent_type FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 226732 AND value IN ('Endotracheal tube', 'Trach mask') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, charttime as event_time, 'Invasive' as vent_type FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IN ('CMV', 'APRV', 'APRV/Biphasic+ApnPress', 'APRV/Biphasic+ApnVol', 'APV (cmv)', 'Ambient', 'Apnea Ventilation', 'CMV/ASSIST', 'CMV/ASSIST/AutoFlow', 'CMV/AutoFlow', 'P-CMV', 'PCV+', 'PCV+/PSV', 'PCV+Assist', 'PRES/AC', 'PRVC/AC', 'PRVC/SIMV', 'SIMV', 'SIMV/AutoFlow', 'SIMV/PRES', 'SIMV/PSV', 'SIMV/PSV/AutoFlow', 'SIMV/VOL', 'SYNCHRON MASTER', 'SYNCHRON SLAVE', 'VOL/AC') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, charttime as event_time, 'Invasive' as vent_type FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 229314 AND value IN ('APRV', 'APV (cmv)', 'Ambient', '(S) CMV', 'P-CMV', 'SIMV', 'APV (simv)', 'P-SIMV', 'VS', 'ASV') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, starttime as event_time, 'NIV' as vent_type FROM `physionet-data.mimiciv_3_1_icu.procedureevents` WHERE itemid = 225794 AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, charttime as event_time, 'NIV' as vent_type FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 226732 AND value IN ('Bipap mask', 'CPAP mask') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, charttime as event_time, 'NIV' as vent_type FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 229314 AND value IN ('DuoPaP', 'NIV', 'NIV-ST') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
),
Invasive_Events_No_Trach AS (
   SELECT stay_id, starttime as event_time FROM `physionet-data.mimiciv_3_1_icu.procedureevents` WHERE itemid = 225792 AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, charttime as event_time FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 226732 AND value IN ('Endotracheal tube') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, charttime as event_time FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IN ('CMV', 'APRV', 'APRV/Biphasic+ApnPress', 'APRV/Biphasic+ApnVol', 'APV (cmv)', 'Ambient', 'Apnea Ventilation', 'CMV/ASSIST', 'CMV/ASSIST/AutoFlow', 'CMV/AutoFlow', 'P-CMV', 'PCV+', 'PCV+/PSV', 'PCV+Assist', 'PRES/AC', 'PRVC/AC', 'PRVC/SIMV', 'SIMV', 'SIMV/AutoFlow', 'SIMV/PRES', 'SIMV/PSV', 'SIMV/PSV/AutoFlow', 'SIMV/VOL', 'SYNCHRON MASTER', 'SYNCHRON SLAVE', 'VOL/AC') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL
   SELECT stay_id, charttime as event_time FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 229314 AND value IN ('APRV', 'APV (cmv)', 'Ambient', '(S) CMV', 'P-CMV', 'SIMV', 'APV (simv)', 'P-SIMV', 'VS', 'ASV') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
),

All_NIV_Events AS (
    SELECT stay_id, starttime AS event_time
    FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
    WHERE itemid = 225794 AND stay_id IN (SELECT stay_id FROM Core_Cohort)

    UNION ALL

    SELECT stay_id, charttime AS event_time
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid = 226732 AND value IN ('Bipap mask', 'CPAP mask') AND stay_id IN (SELECT stay_id FROM Core_Cohort)

    UNION ALL

    SELECT stay_id, charttime AS event_time
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid = 229314 AND value IN ('DuoPaP', 'NIV', 'NIV-ST') AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),

All_IMV_Events AS (
    SELECT stay_id, starttime AS event_time
    FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
    WHERE itemid = 225792 AND stay_id IN (SELECT stay_id FROM Core_Cohort)

    UNION ALL

    SELECT stay_id, charttime AS event_time
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid = 226732 AND value IN ('Endotracheal tube', 'Trach mask') AND stay_id IN (SELECT stay_id FROM Core_Cohort)

    UNION ALL

    SELECT stay_id, charttime AS event_time
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid = 223849 AND value IN (
        'CMV', 'APRV', 'APRV/Biphasic+ApnPress', 'APRV/Biphasic+ApnVol', 'APV (cmv)', 'Ambient', 'Apnea Ventilation',
        'CMV/ASSIST', 'CMV/ASSIST/AutoFlow', 'CMV/AutoFlow', 'CPAP/PPS', 'CPAP/PSV', 'CPAP/PSV+Apn TCPL',
        'CPAP/PSV+ApnPres', 'CPAP/PSV+ApnVol', 'MMV', 'MMV/AutoFlow', 'MMV/PSV', 'MMV/PSV/AutoFlow', 'P-CMV',
        'PCV+', 'PCV+/PSV', 'PCV+Assist', 'PRES/AC', 'PRVC/AC', 'PRVC/SIMV', 'PSV/SBT', 'SIMV', 'SIMV/AutoFlow',
        'SIMV/PRES', 'SIMV/PSV', 'SIMV/PSV/AutoFlow', 'SIMV/VOL', 'SYNCHRON MASTER', 'SYNCHRON SLAVE', 'VOL/AC'
    ) AND stay_id IN (SELECT stay_id FROM Core_Cohort)

    UNION ALL

    SELECT stay_id, charttime AS event_time
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid = 229314 AND value IN (
        'APRV', 'APV (cmv)', 'Ambient', '(S) CMV', 'P-CMV', 'SIMV', 'APV (simv)', 'P-SIMV', 'VS', 'ASV'
    ) AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),

First_SBT_Time AS (
    SELECT stay_id, MIN(charttime) AS sbt_time
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid = 224715 AND value = 'Yes'
      AND stay_id IN (SELECT stay_id FROM Core_Cohort)
    GROUP BY stay_id
),

First_Extubation_Time AS (
    SELECT stay_id, MIN(starttime) AS extubation_time
    FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
    WHERE itemid = 227194
      AND stay_id IN (SELECT stay_id FROM Core_Cohort)
    GROUP BY stay_id
),

First_IMV_Time AS (
    SELECT stay_id, MIN(event_time) AS first_imv_time
    FROM All_IMV_Events
    GROUP BY stay_id
),

Weaning_Attempt AS (
    SELECT
        imv.stay_id,
        ext.extubation_time
    FROM First_IMV_Time imv
    INNER JOIN First_SBT_Time sbt ON imv.stay_id = sbt.stay_id
    INNER JOIN First_Extubation_Time ext ON imv.stay_id = ext.stay_id
    WHERE imv.first_imv_time < ext.extubation_time
      AND sbt.sbt_time <= ext.extubation_time
),

Post_Extubation_Reintubation AS (
    SELECT
        wa.stay_id,
        wa.extubation_time,
        MIN(imv.event_time) as reintubation_time
    FROM Weaning_Attempt wa
    LEFT JOIN All_IMV_Events imv ON wa.stay_id = imv.stay_id AND imv.event_time > wa.extubation_time
    GROUP BY wa.stay_id, wa.extubation_time
),

Weaning_Outcomes_Calculation AS (
    SELECT
        att.stay_id,
        CASE
            WHEN (re.reintubation_time IS NOT NULL AND DATETIME_DIFF(re.reintubation_time, att.extubation_time, HOUR) <= 48)
              OR (ob.deathtime IS NOT NULL AND DATETIME_DIFF(ob.deathtime, att.extubation_time, HOUR) <= 48)
            THEN 1
            ELSE 0
        END AS weaning_failure,
        CASE
            WHEN (re.reintubation_time IS NOT NULL AND DATETIME_DIFF(re.reintubation_time, att.extubation_time, HOUR) > 48 AND DATETIME_DIFF(re.reintubation_time, att.extubation_time, DAY) <= 7)
              OR (ob.deathtime IS NOT NULL AND DATETIME_DIFF(ob.deathtime, att.extubation_time, HOUR) > 48 AND DATETIME_DIFF(ob.deathtime, att.extubation_time, DAY) <= 7)
            THEN 1
            ELSE 0
        END AS weaning_indeterminate,
        CASE
            WHEN (re.reintubation_time IS NULL OR DATETIME_DIFF(re.reintubation_time, att.extubation_time, DAY) > 7)
             AND (ob.deathtime IS NULL OR DATETIME_DIFF(ob.deathtime, att.extubation_time, DAY) > 7)
            THEN 1
            ELSE 0
        END AS weaning_success

    FROM Weaning_Attempt att
    LEFT JOIN Post_Extubation_Reintubation re ON att.stay_id = re.stay_id
    LEFT JOIN Outcomes_Base ob ON att.stay_id = ob.stay_id
),

Weaning_Status_Final AS (
    SELECT
        stay_id,
        weaning_failure,
        CASE WHEN weaning_failure = 1 THEN 0 ELSE weaning_indeterminate END as weaning_indeterminate,
        CASE WHEN weaning_failure = 1 OR weaning_indeterminate = 1 THEN 0 ELSE weaning_success END as weaning_success,
        CASE
            WHEN weaning_failure = 1 THEN 1
            WHEN weaning_indeterminate = 1 AND weaning_failure = 0 THEN 2
            WHEN weaning_success = 1 AND weaning_failure = 0 AND weaning_indeterminate = 0 THEN 3
            ELSE 0
        END AS weaning_outcome_status
    FROM Weaning_Outcomes_Calculation
),

First_Vent_Times AS (
    SELECT
        stay_id,
        MIN(CASE WHEN vent_type = 'NIV' THEN event_time END) as first_niv_time,
        MIN(CASE WHEN vent_type = 'IMV' THEN event_time END) as first_imv_time
    FROM (
        SELECT stay_id, event_time, 'NIV' as vent_type FROM All_NIV_Events
        UNION ALL
        SELECT stay_id, event_time, 'IMV' as vent_type FROM All_IMV_Events
    ) AS all_vents
    GROUP BY stay_id
),

NIV_Failure_Calculation AS (
    SELECT
        c.stay_id,
        CASE
            WHEN fvt.first_niv_time IS NULL AND fvt.first_imv_time IS NULL THEN 'N/A'

            WHEN fvt.first_imv_time IS NOT NULL AND (fvt.first_niv_time IS NULL OR fvt.first_imv_time <= fvt.first_niv_time) THEN 'IMV_FIRST'

            WHEN fvt.first_imv_time IS NOT NULL AND fvt.first_imv_time > fvt.first_niv_time THEN 'Yes'

            WHEN ob.deathtime IS NOT NULL AND ob.deathtime > fvt.first_niv_time AND fvt.first_imv_time IS NULL THEN 'Yes'

            WHEN fvt.first_niv_time IS NOT NULL THEN 'No'

            ELSE 'N/A'
        END AS niv_failure,
        CAST(NULL AS STRING) AS niv_failure_no_trachmask
    FROM Core_Cohort c
    LEFT JOIN First_Vent_Times fvt ON c.stay_id = fvt.stay_id
    LEFT JOIN Outcomes_Base ob ON c.stay_id = ob.stay_id
),

aps_ventilation_events AS (
   SELECT
     stay_id, charttime
   FROM `physionet-data.mimiciv_3_1_icu.chartevents`
   WHERE itemid = 223849 AND value IS NOT NULL
     AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
pa AS (
   SELECT ie.stay_id, bg.charttime
       , po2 AS pao2
       , ROW_NUMBER() OVER (PARTITION BY ie.stay_id ORDER BY bg.po2 DESC) AS rn
   FROM `physionet-data.mimiciv_3_1_derived.bg` bg
   INNER JOIN Core_Cohort ie ON bg.hadm_id = ie.hadm_id
   LEFT JOIN aps_ventilation_events vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime
   WHERE vd.stay_id IS NULL AND COALESCE(fio2, fio2_chartevents, 21) < 50 AND bg.po2 IS NOT NULL AND bg.specimen = 'ART.'
),
aa AS (
   SELECT ie.stay_id, bg.charttime
       , bg.aado2
       , ROW_NUMBER() OVER (PARTITION BY ie.stay_id ORDER BY bg.aado2 DESC) AS rn
   FROM `physionet-data.mimiciv_3_1_derived.bg` bg
   INNER JOIN Core_Cohort ie ON bg.hadm_id = ie.hadm_id
   INNER JOIN aps_ventilation_events vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime
   WHERE vd.stay_id IS NOT NULL AND COALESCE(fio2, fio2_chartevents) >= 50 AND bg.aado2 IS NOT NULL AND bg.specimen = 'ART.'
),
acidbase AS (
   SELECT ie.stay_id, ph, pco2 AS paco2,
       CASE
           WHEN ph < 7.20 THEN CASE WHEN pco2 < 50 THEN 12 ELSE 4 END
           WHEN ph < 7.30 THEN CASE WHEN pco2 < 30 THEN 9 WHEN pco2 < 40 THEN 6 WHEN pco2 < 50 THEN 3 ELSE 2 END
           WHEN ph < 7.35 THEN CASE WHEN pco2 < 30 THEN 9 WHEN pco2 < 45 THEN 0 ELSE 1 END
           WHEN ph < 7.45 THEN CASE WHEN pco2 < 30 THEN 5 WHEN pco2 < 45 THEN 0 ELSE 1 END
           WHEN ph < 7.50 THEN CASE WHEN pco2 < 30 THEN 5 WHEN pco2 < 35 THEN 0 WHEN pco2 < 45 THEN 2 ELSE 12 END
           WHEN ph < 7.60 THEN CASE WHEN pco2 < 40 THEN 3 ELSE 12 END
           ELSE CASE WHEN pco2 < 25 THEN 0 WHEN pco2 < 40 THEN 3 ELSE 12 END
       END AS acidbase_score
   FROM `physionet-data.mimiciv_3_1_derived.bg` bg
   INNER JOIN Core_Cohort ie ON bg.hadm_id = ie.hadm_id
   WHERE ph IS NOT NULL AND pco2 IS NOT NULL AND bg.specimen = 'ART.'
),
acidbase_max AS (
   SELECT stay_id, acidbase_score, ph, paco2, ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY acidbase_score DESC) AS acidbase_rn
   FROM acidbase
),
arf_aps AS (
   SELECT ie.stay_id,
       CASE WHEN labs.creatinine_max >= 1.5 AND uo.urineoutput < 410 AND icd.ckd = 0 THEN 1 ELSE 0 END AS arf
   FROM Core_Cohort ie
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
   LEFT JOIN (
       SELECT hadm_id, MAX(CASE
               WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 4) IN ('5854', '5855', '5856') THEN 1
               WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 4) IN ('N184', 'N185', 'N186') THEN 1
               ELSE 0 END
           ) AS ckd
       FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` GROUP BY hadm_id
   ) icd ON ie.hadm_id = icd.hadm_id
),
vent AS (
   SELECT ie.stay_id, MAX(CASE WHEN v.stay_id IS NOT NULL THEN 1 ELSE 0 END) AS vent
   FROM Core_Cohort ie
   LEFT JOIN aps_ventilation_events v ON ie.stay_id = v.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
   WHERE DATETIME_DIFF(v.charttime, icu.intime, HOUR) < 24
   GROUP BY ie.stay_id
),
apsiii_cohort AS (
   SELECT ie.subject_id, ie.hadm_id, ie.stay_id,
       vital.heart_rate_min, vital.heart_rate_max, vital.mbp_min, vital.mbp_max, vital.temperature_min,
       vital.temperature_max, vital.resp_rate_min, vital.resp_rate_max,
       pa.pao2, aa.aado2, ab.ph, ab.paco2, ab.acidbase_score,
       labs.hematocrit_min, labs.hematocrit_max, labs.wbc_min, labs.wbc_max, labs.creatinine_min,
       labs.creatinine_max, labs.bun_min, labs.bun_max, labs.sodium_min, labs.sodium_max,
       labs.albumin_min, labs.albumin_max, labs.bilirubin_total_min AS bilirubin_min,
       labs.bilirubin_total_max AS bilirubin_max,
       GREATEST(labs.glucose_max, vital.glucose_max) AS glucose_max,
       LEAST(labs.glucose_min, vital.glucose_min) AS glucose_min,
       vent.vent, uo.urineoutput,
       gcs.gcs_min AS mingcs, gcs.gcs_motor, gcs.gcs_verbal, gcs.gcs_eyes, gcs.gcs_unable,
       arf_aps.arf AS arf
   FROM Core_Cohort ie
   LEFT JOIN pa ON ie.stay_id = pa.stay_id AND pa.rn = 1
   LEFT JOIN aa ON ie.stay_id = aa.stay_id AND aa.rn = 1
   LEFT JOIN acidbase_max ab ON ie.stay_id = ab.stay_id AND ab.acidbase_rn = 1
   LEFT JOIN arf_aps ON ie.stay_id = arf_aps.stay_id
   LEFT JOIN vent ON ie.stay_id = vent.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
),
score_min AS (
   SELECT c.subject_id, c.hadm_id, c.stay_id,
       CASE WHEN heart_rate_min < 40 THEN 8 WHEN heart_rate_min < 50 THEN 5 WHEN heart_rate_min < 100 THEN 0 WHEN heart_rate_min < 110 THEN 1 WHEN heart_rate_min < 120 THEN 5 WHEN heart_rate_min < 140 THEN 7 WHEN heart_rate_min < 155 THEN 13 WHEN heart_rate_min >= 155 THEN 17 END AS hr_score,
       CASE WHEN mbp_min < 40 THEN 23 WHEN mbp_min < 60 THEN 15 WHEN mbp_min < 70 THEN 7 WHEN mbp_min < 80 THEN 6 WHEN mbp_min < 100 THEN 0 WHEN mbp_min < 120 THEN 4 WHEN mbp_min < 130 THEN 7 WHEN mbp_min < 140 THEN 9 WHEN mbp_min >= 140 THEN 10 END AS mbp_score,
       CASE WHEN temperature_min < 33.0 THEN 20 WHEN temperature_min < 33.5 THEN 16 WHEN temperature_min < 34.0 THEN 13 WHEN temperature_min < 35.0 THEN 8 WHEN temperature_min < 36.0 THEN 2 WHEN temperature_min < 40.0 THEN 0 WHEN temperature_min >= 40.0 THEN 4 END AS temp_score,
       CASE WHEN vent = 1 AND resp_rate_min < 14 THEN 0 WHEN resp_rate_min < 6 THEN 17 WHEN resp_rate_min < 12 THEN 8 WHEN resp_rate_min < 14 THEN 7 WHEN resp_rate_min < 25 THEN 0 WHEN resp_rate_min < 35 THEN 6 WHEN resp_rate_min < 40 THEN 9 WHEN resp_rate_min < 50 THEN 11 WHEN resp_rate_min >= 50 THEN 18 END AS resp_rate_score,
       CASE WHEN hematocrit_min < 41.0 THEN 3 WHEN hematocrit_min < 50.0 THEN 0 WHEN hematocrit_min >= 50.0 THEN 3 END AS hematocrit_score,
       CASE WHEN wbc_min < 1.0 THEN 19 WHEN wbc_min < 3.0 THEN 5 WHEN wbc_min < 20.0 THEN 0 WHEN wbc_min < 25.0 THEN 1 WHEN wbc_min >= 25.0 THEN 5 END AS wbc_score,
       CASE WHEN arf = 1 AND creatinine_min >= 1.5 THEN 10 WHEN arf = 1 THEN 0 WHEN creatinine_min < 0.5 THEN 3 WHEN creatinine_min < 1.5 THEN 0 WHEN creatinine_min < 1.95 THEN 4 WHEN creatinine_min >= 1.95 THEN 7 END AS creatinine_score,
       CASE WHEN bun_min < 17.0 THEN 0 WHEN bun_min < 20.0 THEN 2 WHEN bun_min < 40.0 THEN 7 WHEN bun_min < 80.0 THEN 11 WHEN bun_min >= 80.0 THEN 12 END AS bun_score,
       CASE WHEN sodium_min < 120 THEN 3 WHEN sodium_min < 135 THEN 2 WHEN sodium_min < 155 THEN 0 WHEN sodium_min >= 155 THEN 4 END AS sodium_score,
       CASE WHEN albumin_min < 2.0 THEN 11 WHEN albumin_min < 2.5 THEN 6 WHEN albumin_min < 4.5 THEN 0 WHEN albumin_min >= 4.5 THEN 4 END AS albumin_score,
       CASE WHEN bilirubin_min < 2.0 THEN 0 WHEN bilirubin_min < 3.0 THEN 5 WHEN bilirubin_min < 5.0 THEN 6 WHEN bilirubin_min < 8.0 THEN 8 WHEN bilirubin_min >= 8.0 THEN 16 END AS bilirubin_score,
       CASE WHEN glucose_min < 40 THEN 8 WHEN glucose_min < 60 THEN 9 WHEN glucose_min < 200 THEN 0 WHEN glucose_min < 350 THEN 3 WHEN glucose_min >= 350 THEN 5 END AS glucose_score
   FROM apsiii_cohort c
),
score_max AS (
   SELECT c.subject_id, c.hadm_id, c.stay_id,
       CASE WHEN heart_rate_max < 40 THEN 8 WHEN heart_rate_max < 50 THEN 5 WHEN heart_rate_max < 100 THEN 0 WHEN heart_rate_max < 110 THEN 1 WHEN heart_rate_max < 120 THEN 5 WHEN heart_rate_max < 140 THEN 7 WHEN heart_rate_max < 155 THEN 13 WHEN heart_rate_max >= 155 THEN 17 END AS hr_score,
       CASE WHEN mbp_max < 40 THEN 23 WHEN mbp_max < 60 THEN 15 WHEN mbp_max < 70 THEN 7 WHEN mbp_max < 80 THEN 6 WHEN mbp_max < 100 THEN 0 WHEN mbp_max < 120 THEN 4 WHEN mbp_max < 130 THEN 7 WHEN mbp_max < 140 THEN 9 WHEN mbp_max >= 140 THEN 10 END AS mbp_score,
       CASE WHEN temperature_max < 33.0 THEN 20 WHEN temperature_max < 33.5 THEN 16 WHEN temperature_max < 34.0 THEN 13 WHEN temperature_max < 35.0 THEN 8 WHEN temperature_max < 36.0 THEN 2 WHEN temperature_max < 40.0 THEN 0 WHEN temperature_max >= 40.0 THEN 4 END AS temp_score,
       CASE WHEN vent = 1 AND resp_rate_max < 14 THEN 0 WHEN resp_rate_max < 6 THEN 17 WHEN resp_rate_max < 12 THEN 8 WHEN resp_rate_max < 14 THEN 7 WHEN resp_rate_max < 25 THEN 0 WHEN resp_rate_max < 35 THEN 6 WHEN resp_rate_max < 40 THEN 9 WHEN resp_rate_max < 50 THEN 11 WHEN resp_rate_max >= 50 THEN 18 END AS resp_rate_score,
       CASE WHEN hematocrit_max < 41.0 THEN 3 WHEN hematocrit_max < 50.0 THEN 0 WHEN hematocrit_max >= 50.0 THEN 3 END AS hematocrit_score,
       CASE WHEN wbc_max < 1.0 THEN 19 WHEN wbc_max < 3.0 THEN 5 WHEN wbc_max < 20.0 THEN 0 WHEN wbc_max < 25.0 THEN 1 WHEN wbc_max >= 25.0 THEN 5 END AS wbc_score,
       CASE WHEN arf = 1 AND creatinine_max >= 1.5 THEN 10 WHEN arf = 1 THEN 0 WHEN creatinine_max < 0.5 THEN 3 WHEN creatinine_max < 1.5 THEN 0 WHEN creatinine_max < 1.95 THEN 4 WHEN creatinine_max >= 1.95 THEN 7 END AS creatinine_score,
       CASE WHEN bun_max < 17.0 THEN 0 WHEN bun_max < 20.0 THEN 2 WHEN bun_max < 40.0 THEN 7 WHEN bun_max < 80.0 THEN 11 WHEN bun_max >= 80.0 THEN 12 END AS bun_score,
       CASE WHEN sodium_max < 120 THEN 3 WHEN sodium_max < 135 THEN 2 WHEN sodium_max < 155 THEN 0 WHEN sodium_max >= 155 THEN 4 END AS sodium_score,
       CASE WHEN albumin_max < 2.0 THEN 11 WHEN albumin_max < 2.5 THEN 6 WHEN albumin_max < 4.5 THEN 0 WHEN albumin_max >= 4.5 THEN 4 END AS albumin_score,
       CASE WHEN bilirubin_max < 2.0 THEN 0 WHEN bilirubin_max < 3.0 THEN 5 WHEN bilirubin_max < 5.0 THEN 6 WHEN bilirubin_max < 8.0 THEN 8 WHEN bilirubin_max >= 8.0 THEN 16 END AS bilirubin_score,
       CASE WHEN glucose_max < 40 THEN 8 WHEN glucose_max < 60 THEN 9 WHEN glucose_max < 200 THEN 0 WHEN glucose_max < 350 THEN 3 WHEN glucose_max >= 350 THEN 5 END AS glucose_score
   FROM apsiii_cohort c
),
scorecomp AS (
   SELECT co.*,
       CASE WHEN ABS(heart_rate_max - 75) > ABS(heart_rate_min - 75) THEN smax.hr_score WHEN ABS(heart_rate_max - 75) < ABS(heart_rate_min - 75) THEN smin.hr_score WHEN smax.hr_score >= smin.hr_score THEN smax.hr_score ELSE smin.hr_score END AS hr_score,
       CASE WHEN ABS(mbp_max - 90) > ABS(mbp_min - 90) THEN smax.mbp_score WHEN ABS(mbp_max - 90) < ABS(mbp_min - 90) THEN smin.mbp_score WHEN smax.mbp_score >= smin.mbp_score THEN smax.mbp_score ELSE smin.mbp_score END AS mbp_score,
       CASE WHEN ABS(temperature_max - 38) > ABS(temperature_min - 38) THEN smax.temp_score WHEN ABS(temperature_max - 38) < ABS(temperature_min - 38) THEN smin.temp_score WHEN smax.temp_score >= smin.temp_score THEN smax.temp_score ELSE smin.temp_score END AS temp_score,
       CASE WHEN ABS(resp_rate_max - 19) > ABS(resp_rate_min - 19) THEN smax.resp_rate_score WHEN ABS(resp_rate_max - 19) < ABS(resp_rate_min - 19) THEN smin.resp_rate_score WHEN smax.resp_rate_score >= smin.resp_rate_score THEN smax.resp_rate_score ELSE smin.resp_rate_score END AS resp_rate_score,
       CASE WHEN ABS(hematocrit_max - 45.5) > ABS(hematocrit_min - 45.5) THEN smax.hematocrit_score WHEN ABS(hematocrit_max - 45.5) < ABS(hematocrit_min - 45.5) THEN smin.hematocrit_score WHEN smax.hematocrit_score >= smin.hematocrit_score THEN smax.hematocrit_score ELSE smin.hematocrit_score END AS hematocrit_score,
       CASE WHEN ABS(wbc_max - 11.5) > ABS(wbc_min - 11.5) THEN smax.wbc_score WHEN ABS(wbc_max - 11.5) < ABS(wbc_min - 11.5) THEN smin.wbc_score WHEN smax.wbc_score >= smin.wbc_score THEN smax.wbc_score ELSE smin.wbc_score END AS wbc_score,
       CASE WHEN arf = 1 THEN smax.creatinine_score WHEN ABS(creatinine_max - 1) > ABS(creatinine_min - 1) THEN smax.creatinine_score WHEN ABS(creatinine_max - 1) < ABS(creatinine_min - 1) THEN smin.creatinine_score WHEN smax.creatinine_score >= smin.creatinine_score THEN smax.creatinine_score ELSE smin.creatinine_score END AS creatinine_score,
       smax.bun_score AS bun_score,
       CASE WHEN ABS(sodium_max - 145.5) > ABS(sodium_min - 145.5) THEN smax.sodium_score WHEN ABS(sodium_max - 145.5) < ABS(sodium_min - 145.5) THEN smin.sodium_score WHEN smax.sodium_score >= smin.sodium_score THEN smax.sodium_score ELSE smin.sodium_score END AS sodium_score,
       CASE WHEN ABS(albumin_max - 3.5) > ABS(albumin_min - 3.5) THEN smax.albumin_score WHEN ABS(albumin_max - 3.5) < ABS(albumin_min - 3.5) THEN smin.albumin_score WHEN smax.albumin_score >= smin.albumin_score THEN smax.albumin_score ELSE smin.albumin_score END AS albumin_score,
       smax.bilirubin_score AS bilirubin_score,
       CASE WHEN ABS(glucose_max - 130) > ABS(glucose_min - 130) THEN smax.glucose_score WHEN ABS(glucose_max - 130) < ABS(glucose_min - 130) THEN smin.glucose_score WHEN smax.glucose_score >= smin.glucose_score THEN smax.glucose_score ELSE smin.glucose_score END AS glucose_score,
       CASE WHEN urineoutput < 400 THEN 15 WHEN urineoutput < 600 THEN 8 WHEN urineoutput < 900 THEN 7 WHEN urineoutput < 1500 THEN 5 WHEN urineoutput < 2000 THEN 4 WHEN urineoutput < 4000 THEN 0 WHEN urineoutput >= 4000 THEN 1 END AS uo_score,
       CASE WHEN gcs_unable = 1 THEN 0 WHEN gcs_eyes = 1 THEN CASE WHEN gcs_verbal = 1 AND gcs_motor IN (1, 2) THEN 48 WHEN gcs_verbal = 1 AND gcs_motor IN (3, 4) THEN 33 WHEN gcs_verbal = 1 AND gcs_motor IN (5, 6) THEN 16 WHEN gcs_verbal IN (2, 3) AND gcs_motor IN (1, 2) THEN 29 WHEN gcs_verbal IN (2, 3) AND gcs_motor IN (3, 4) THEN 24 END WHEN gcs_eyes > 1 THEN CASE WHEN gcs_verbal = 1 AND gcs_motor IN (1, 2) THEN 29 WHEN gcs_verbal = 1 AND gcs_motor IN (3, 4) THEN 24 WHEN gcs_verbal = 1 AND gcs_motor IN (5, 6) THEN 15 WHEN gcs_verbal IN (2, 3) AND gcs_motor IN (1, 2) THEN 29 WHEN gcs_verbal IN (2, 3) AND gcs_motor IN (3, 4) THEN 24 WHEN gcs_verbal IN (2, 3) AND gcs_motor = 5 THEN 13 WHEN gcs_verbal IN (2, 3) AND gcs_motor = 6 THEN 10 WHEN gcs_verbal = 4 AND gcs_motor IN (1, 2, 3, 4) THEN 13 WHEN gcs_verbal = 4 AND gcs_motor = 5 THEN 8 WHEN gcs_verbal = 4 AND gcs_motor = 6 THEN 3 WHEN gcs_verbal = 5 AND gcs_motor IN (1, 2, 3, 4, 5) THEN 3 WHEN gcs_verbal = 5 AND gcs_motor = 6 THEN 0 END END AS gcs_score,
       CASE WHEN pao2 IS NOT NULL THEN CASE WHEN pao2 < 50 THEN 15 WHEN pao2 < 70 THEN 5 WHEN pao2 < 80 THEN 2 ELSE 0 END WHEN aado2 IS NOT NULL THEN CASE WHEN aado2 < 100 THEN 0 WHEN aado2 < 250 THEN 7 WHEN aado2 < 350 THEN 9 WHEN aado2 < 500 THEN 11 WHEN aado2 >= 500 THEN 14 ELSE 0 END END AS pao2_aado2_score
   FROM apsiii_cohort co
   LEFT JOIN score_min smin ON co.stay_id = smin.stay_id
   LEFT JOIN score_max smax ON co.stay_id = smax.stay_id
),
apsiii_scores AS (
   SELECT s.stay_id,
       (COALESCE(hr_score, 0) + COALESCE(mbp_score, 0) + COALESCE(temp_score, 0) + COALESCE(resp_rate_score, 0)
       + COALESCE(pao2_aado2_score, 0) + COALESCE(hematocrit_score, 0) + COALESCE(wbc_score, 0)
       + COALESCE(creatinine_score, 0) + COALESCE(uo_score, 0) + COALESCE(bun_score, 0) + COALESCE(sodium_score, 0)
       + COALESCE(albumin_score, 0) + COALESCE(bilirubin_score, 0) + COALESCE(glucose_score, 0)
       + COALESCE(acidbase_score, 0) + COALESCE(gcs_score, 0)) AS apsiii
   FROM scorecomp s
),
sbt_start_times AS (
  SELECT stay_id, MIN(charttime) AS sbt_starttime
  FROM `physionet-data.mimiciv_3_1_icu.chartevents`
  WHERE itemid = 224715 AND value = 'Yes'
    AND stay_id IN (SELECT stay_id FROM Core_Cohort)
  GROUP BY stay_id
),

raw_hacor_data AS (
  SELECT c.stay_id, c.sbt_starttime, ce.charttime,
    CASE
      WHEN ce.itemid = 220045 THEN 'heart_rate'
      WHEN ce.itemid IN (50820, 223830) THEN 'ph'
      WHEN ce.itemid = 223901 THEN 'gcs_motor'
      WHEN ce.itemid = 223900 THEN 'gcs_verbal'
      WHEN ce.itemid = 220739 THEN 'gcs_eye'
      WHEN ce.itemid IN (220224, 50821) THEN 'pao2'
      WHEN ce.itemid = 223835 THEN 'fio2'
      WHEN ce.itemid IN (229407, 229393, 229405) THEN 'pf_ratio'
      WHEN ce.itemid IN (220210, 224690) THEN 'resp_rate'
    END as item_group,
    CASE WHEN ce.itemid = 223835 AND ce.valuenum > 1 THEN ce.valuenum / 100.0 ELSE ce.valuenum END AS valuenum
  FROM sbt_start_times c
  JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON c.stay_id = ce.stay_id
  WHERE ce.itemid IN (220045, 223830, 223901, 223900, 220739, 220224, 223835, 229407, 229393, 229405, 220210, 224690)
  UNION ALL
  SELECT c.stay_id, c.sbt_starttime, le.charttime,
    CASE WHEN le.itemid = 50820 THEN 'ph' WHEN le.itemid = 50821 THEN 'pao2' END as item_group,
    le.valuenum
  FROM sbt_start_times c
  JOIN Core_Cohort cc ON c.stay_id = cc.stay_id
  JOIN `physionet-data.mimiciv_3_1_hosp.labevents` le ON cc.hadm_id = le.hadm_id
  WHERE le.itemid IN (50820, 50821)
),

values_in_windows_ranked AS (
   SELECT stay_id, item_group, valuenum, time_window
   FROM (
       SELECT *,
           ROW_NUMBER() OVER (PARTITION BY stay_id, item_group, time_window ORDER BY charttime ASC) as rn
       FROM (
           SELECT stay_id, item_group, valuenum, charttime, 't0_t1' as time_window FROM raw_hacor_data WHERE charttime BETWEEN sbt_starttime AND DATETIME_ADD(sbt_starttime, INTERVAL 1 HOUR)
           UNION ALL
           SELECT stay_id, item_group, valuenum, charttime, 't0_t3' as time_window FROM raw_hacor_data WHERE charttime BETWEEN sbt_starttime AND DATETIME_ADD(sbt_starttime, INTERVAL 3 HOUR)
       )
   )
   WHERE rn = 1
),
pivoted_values AS (
   SELECT
       stay_id,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'heart_rate' THEN valuenum END) AS hr_t1,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'ph' THEN valuenum END) AS ph_t1,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'gcs_motor' THEN valuenum END) AS gcs_motor_t1,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'gcs_verbal' THEN valuenum END) AS gcs_verbal_t1,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'gcs_eye' THEN valuenum END) AS gcs_eye_t1,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'pao2' THEN valuenum END) AS pao2_t1,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'fio2' THEN valuenum END) AS fio2_t1,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'pf_ratio' THEN valuenum END) AS pf_ratio_t1,
       MAX(CASE WHEN time_window = 't0_t1' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_t1,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'heart_rate' THEN valuenum END) AS hr_t3,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'ph' THEN valuenum END) AS ph_t3,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'gcs_motor' THEN valuenum END) AS gcs_motor_t3,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'gcs_verbal' THEN valuenum END) AS gcs_verbal_t3,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'gcs_eye' THEN valuenum END) AS gcs_eye_t3,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'pao2' THEN valuenum END) AS pao2_t3,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'fio2' THEN valuenum END) AS fio2_t3,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'pf_ratio' THEN valuenum END) AS pf_ratio_t3,
       MAX(CASE WHEN time_window = 't0_t3' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_t3
   FROM values_in_windows_ranked
   GROUP BY stay_id
),
hacor_scores AS (
   SELECT
       pv.stay_id,
       sbt.sbt_starttime,
       pv.hr_t1, pv.ph_t1, pv.resp_rate_t1,
       CASE WHEN pv.gcs_motor_t1 IS NOT NULL AND pv.gcs_verbal_t1 IS NOT NULL AND pv.gcs_eye_t1 IS NOT NULL THEN pv.gcs_motor_t1 + pv.gcs_verbal_t1 + pv.gcs_eye_t1 END AS gcs_total_t1,
       IFNULL(pv.pf_ratio_t1, pv.pao2_t1 / NULLIF(pv.fio2_t1, 0)) AS pf_ratio_calc_t1,
       pv.hr_t3, pv.ph_t3, pv.resp_rate_t3,
       CASE WHEN pv.gcs_motor_t3 IS NOT NULL AND pv.gcs_verbal_t3 IS NOT NULL AND pv.gcs_eye_t3 IS NOT NULL THEN pv.gcs_motor_t3 + pv.gcs_verbal_t3 + pv.gcs_eye_t3 END AS gcs_total_t3,
       IFNULL(pv.pf_ratio_t3, pv.pao2_t3 / NULLIF(pv.fio2_t3, 0)) AS pf_ratio_calc_t3,
       CASE WHEN pv.hr_t1 <= 120 THEN 0 WHEN pv.hr_t1 > 120 THEN 1 END AS hacor_score_hr_t1,
       CASE WHEN pv.ph_t1 >= 7.35 THEN 0 WHEN pv.ph_t1 >= 7.30 THEN 2 WHEN pv.ph_t1 >= 7.25 THEN 3 WHEN pv.ph_t1 < 7.25 THEN 4 END AS hacor_score_ph_t1,
       CASE WHEN (pv.gcs_motor_t1 + pv.gcs_verbal_t1 + pv.gcs_eye_t1) = 15 THEN 0 WHEN (pv.gcs_motor_t1 + pv.gcs_verbal_t1 + pv.gcs_eye_t1) >= 13 THEN 2 WHEN (pv.gcs_motor_t1 + pv.gcs_verbal_t1 + pv.gcs_eye_t1) >= 11 THEN 5 WHEN (pv.gcs_motor_t1 + pv.gcs_verbal_t1 + pv.gcs_eye_t1) <= 10 THEN 10 END AS hacor_score_gcs_t1,
       CASE WHEN IFNULL(pv.pf_ratio_t1, pv.pao2_t1 / NULLIF(pv.fio2_t1, 0)) >= 201 THEN 0 WHEN IFNULL(pv.pf_ratio_t1, pv.pao2_t1 / NULLIF(pv.fio2_t1, 0)) >= 176 THEN 2 WHEN IFNULL(pv.pf_ratio_t1, pv.pao2_t1 / NULLIF(pv.fio2_t1, 0)) >= 151 THEN 3 WHEN IFNULL(pv.pf_ratio_t1, pv.pao2_t1 / NULLIF(pv.fio2_t1, 0)) >= 126 THEN 4 WHEN IFNULL(pv.pf_ratio_t1, pv.pao2_t1 / NULLIF(pv.fio2_t1, 0)) >= 101 THEN 5 WHEN IFNULL(pv.pf_ratio_t1, pv.pao2_t1 / NULLIF(pv.fio2_t1, 0)) <= 100 THEN 6 END AS hacor_score_pf_t1,
       CASE WHEN pv.resp_rate_t1 <= 30 THEN 0 WHEN pv.resp_rate_t1 <= 35 THEN 1 WHEN pv.resp_rate_t1 <= 40 THEN 2 WHEN pv.resp_rate_t1 <= 45 THEN 3 WHEN pv.resp_rate_t1 > 45 THEN 4 END AS hacor_score_rr_t1,
       CASE WHEN pv.hr_t3 <= 120 THEN 0 WHEN pv.hr_t3 > 120 THEN 1 END AS hacor_score_hr_t3,
       CASE WHEN pv.ph_t3 >= 7.35 THEN 0 WHEN pv.ph_t3 >= 7.30 THEN 2 WHEN pv.ph_t3 >= 7.25 THEN 3 WHEN pv.ph_t3 < 7.25 THEN 4 END AS hacor_score_ph_t3,
       CASE WHEN (pv.gcs_motor_t3 + pv.gcs_verbal_t3 + pv.gcs_eye_t3) = 15 THEN 0 WHEN (pv.gcs_motor_t3 + pv.gcs_verbal_t3 + pv.gcs_eye_t3) >= 13 THEN 2 WHEN (pv.gcs_motor_t3 + pv.gcs_verbal_t3 + pv.gcs_eye_t3) >= 11 THEN 5 WHEN (pv.gcs_motor_t3 + pv.gcs_verbal_t3 + pv.gcs_eye_t3) <= 10 THEN 10 END AS hacor_score_gcs_t3,
       CASE WHEN IFNULL(pv.pf_ratio_t3, pv.pao2_t3 / NULLIF(pv.fio2_t3, 0)) >= 201 THEN 0 WHEN IFNULL(pv.pf_ratio_t3, pv.pao2_t3 / NULLIF(pv.fio2_t3, 0)) >= 176 THEN 2 WHEN IFNULL(pv.pf_ratio_t3, pv.pao2_t3 / NULLIF(pv.fio2_t3, 0)) >= 151 THEN 3 WHEN IFNULL(pv.pf_ratio_t3, pv.pao2_t3 / NULLIF(pv.fio2_t3, 0)) >= 126 THEN 4 WHEN IFNULL(pv.pf_ratio_t3, pv.pao2_t3 / NULLIF(pv.fio2_t3, 0)) >= 101 THEN 5 WHEN IFNULL(pv.pf_ratio_t3, pv.pao2_t3 / NULLIF(pv.fio2_t3, 0)) <= 100 THEN 6 END AS hacor_score_pf_t3,
       CASE WHEN pv.resp_rate_t3 <= 30 THEN 0 WHEN pv.resp_rate_t3 <= 35 THEN 1 WHEN pv.resp_rate_t3 <= 40 THEN 2 WHEN pv.resp_rate_t3 <= 45 THEN 3 WHEN pv.resp_rate_t3 > 45 THEN 4 END AS hacor_score_rr_t3
   FROM pivoted_values pv
   LEFT JOIN sbt_start_times sbt ON pv.stay_id = sbt.stay_id
),

niv_start_times AS (
   SELECT stay_id, MIN(charttime) AS niv_starttime
   FROM `physionet-data.mimiciv_3_1_icu.chartevents`
   WHERE (itemid IN (227577, 227578, 227579, 227580, 227581, 227582, 225949, 227583)
     OR (itemid = 226732 AND value IN ('Bipap mask', 'CPAP mask'))
     OR (itemid = 229314 AND value IN ('DuoPaP', 'NIV', 'NIV-ST')))
     AND stay_id IN (SELECT stay_id FROM Core_Cohort)
   GROUP BY stay_id
 ),

raw_hacor_data_niv AS (
   SELECT c.stay_id, c.niv_starttime, ce.charttime,
     CASE
       WHEN ce.itemid = 220045 THEN 'heart_rate'
       WHEN ce.itemid IN (50820, 223830) THEN 'ph'
       WHEN ce.itemid = 223901 THEN 'gcs_motor'
       WHEN ce.itemid = 223900 THEN 'gcs_verbal'
       WHEN ce.itemid = 220739 THEN 'gcs_eye'
       WHEN ce.itemid IN (220224, 50821) THEN 'pao2'
       WHEN ce.itemid = 223835 THEN 'fio2'
       WHEN ce.itemid IN (229407, 229393, 229405) THEN 'pf_ratio'
       WHEN ce.itemid IN (220210, 224690) THEN 'resp_rate'
     END as item_group,
     CASE WHEN ce.itemid = 223835 AND ce.valuenum > 1 THEN ce.valuenum / 100.0 ELSE ce.valuenum END AS valuenum
   FROM niv_start_times c
   JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON c.stay_id = ce.stay_id
   WHERE ce.itemid IN (220045, 223830, 223901, 223900, 220739, 220224, 223835, 229407, 229393, 229405, 220210, 224690)
   UNION ALL
   SELECT c.stay_id, c.niv_starttime, le.charttime,
     CASE WHEN le.itemid = 50820 THEN 'ph' WHEN le.itemid = 50821 THEN 'pao2' END as item_group,
     le.valuenum
   FROM niv_start_times c
   JOIN Core_Cohort bc ON c.stay_id = bc.stay_id
   JOIN `physionet-data.mimiciv_3_1_hosp.labevents` le ON bc.hadm_id = le.hadm_id
   WHERE le.itemid IN (50820, 50821)
),

niv_values_in_windows_ranked AS (
    SELECT stay_id, item_group, valuenum, time_window
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY stay_id, item_group, time_window ORDER BY charttime ASC) as rn
        FROM (
            SELECT stay_id, item_group, valuenum, charttime, 't0_t6' as time_window
            FROM raw_hacor_data_niv
            WHERE charttime BETWEEN niv_starttime AND DATETIME_ADD(niv_starttime, INTERVAL 6 HOUR)

            UNION ALL

            SELECT stay_id, item_group, valuenum, charttime, 't6_t12' as time_window
            FROM raw_hacor_data_niv
            WHERE charttime BETWEEN DATETIME_ADD(niv_starttime, INTERVAL 6 HOUR) AND DATETIME_ADD(niv_starttime, INTERVAL 12 HOUR)

            UNION ALL

            SELECT stay_id, item_group, valuenum, charttime, 't12_t24' as time_window
            FROM raw_hacor_data_niv
            WHERE charttime BETWEEN DATETIME_ADD(niv_starttime, INTERVAL 12 HOUR) AND DATETIME_ADD(niv_starttime, INTERVAL 24 HOUR)
        )
    )
    WHERE rn = 1
),

niv_pivoted_values AS (
    SELECT
        stay_id,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'heart_rate' THEN valuenum END) AS hr_t6,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'ph' THEN valuenum END) AS ph_t6,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'gcs_motor' THEN valuenum END) AS gcs_motor_t6,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'gcs_verbal' THEN valuenum END) AS gcs_verbal_t6,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'gcs_eye' THEN valuenum END) AS gcs_eye_t6,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'pao2' THEN valuenum END) AS pao2_t6,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'fio2' THEN valuenum END) AS fio2_t6,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'pf_ratio' THEN valuenum END) AS pf_ratio_t6,
        MAX(CASE WHEN time_window = 't0_t6' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_t6,

        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'heart_rate' THEN valuenum END) AS hr_t12,
        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'ph' THEN valuenum END) AS ph_t12,
        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'gcs_motor' THEN valuenum END) AS gcs_motor_t12,
        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'gcs_verbal' THEN valuenum END) AS gcs_verbal_t12,
        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'gcs_eye' THEN valuenum END) AS gcs_eye_t12,
        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'pao2' THEN valuenum END) AS pao2_t12,
        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'fio2' THEN valuenum END) AS fio2_t12,
        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'pf_ratio' THEN valuenum END) AS pf_ratio_t12,
        MAX(CASE WHEN time_window = 't6_t12' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_t12,

        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'heart_rate' THEN valuenum END) AS hr_t24,
        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'ph' THEN valuenum END) AS ph_t24,
        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'gcs_motor' THEN valuenum END) AS gcs_motor_t24,
        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'gcs_verbal' THEN valuenum END) AS gcs_verbal_t24,
        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'gcs_eye' THEN valuenum END) AS gcs_eye_t24,
        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'pao2' THEN valuenum END) AS pao2_t24,
        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'fio2' THEN valuenum END) AS fio2_t24,
        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'pf_ratio' THEN valuenum END) AS pf_ratio_t24,
        MAX(CASE WHEN time_window = 't12_t24' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_t24
    FROM niv_values_in_windows_ranked
    GROUP BY stay_id
),

NIV_Intubation_Flags AS (
    SELECT
        c.stay_id,
        CASE
            WHEN fiv.first_imv_time IS NOT NULL AND nst.niv_starttime IS NOT NULL
                 AND fiv.first_imv_time <= DATETIME_ADD(nst.niv_starttime, INTERVAL 6 HOUR)
            THEN 1 ELSE 0
        END AS intubated_within_6h_of_niv,
        CASE
            WHEN fiv.first_imv_time IS NOT NULL AND nst.niv_starttime IS NOT NULL
                 AND fiv.first_imv_time <= DATETIME_ADD(nst.niv_starttime, INTERVAL 12 HOUR)
            THEN 1 ELSE 0
        END AS intubated_within_12h_of_niv
    FROM Core_Cohort c
    LEFT JOIN niv_start_times nst ON c.stay_id = nst.stay_id
    LEFT JOIN First_IMV_Time fiv ON c.stay_id = fiv.stay_id
),

niv_hacor_scores AS (
    SELECT
        pv.stay_id,
        nst.niv_starttime,
        flags.intubated_within_6h_of_niv,
        flags.intubated_within_12h_of_niv,

        pv.hr_t6 AS niv_hr_t0_t6,
        pv.ph_t6 AS niv_ph_t0_t6,
        pv.resp_rate_t6 AS niv_rr_t0_t6,
        CASE WHEN pv.gcs_motor_t6 IS NOT NULL AND pv.gcs_verbal_t6 IS NOT NULL AND pv.gcs_eye_t6 IS NOT NULL THEN pv.gcs_motor_t6 + pv.gcs_verbal_t6 + pv.gcs_eye_t6 END AS niv_gcs_t0_t6,
        IFNULL(pv.pf_ratio_t6, pv.pao2_t6 / NULLIF(pv.fio2_t6, 0)) AS niv_pf_ratio_t0_t6,
        CASE WHEN pv.hr_t6 <= 120 THEN 0 WHEN pv.hr_t6 > 120 THEN 1 END AS niv_hacor_hr_t0_t6,
        CASE WHEN pv.ph_t6 >= 7.35 THEN 0 WHEN pv.ph_t6 >= 7.30 THEN 2 WHEN pv.ph_t6 >= 7.25 THEN 3 WHEN pv.ph_t6 < 7.25 THEN 4 END AS niv_hacor_ph_t0_t6,
        CASE WHEN (pv.gcs_motor_t6 + pv.gcs_verbal_t6 + pv.gcs_eye_t6) = 15 THEN 0 WHEN (pv.gcs_motor_t6 + pv.gcs_verbal_t6 + pv.gcs_eye_t6) >= 13 THEN 2 WHEN (pv.gcs_motor_t6 + pv.gcs_verbal_t6 + pv.gcs_eye_t6) >= 11 THEN 5 WHEN (pv.gcs_motor_t6 + pv.gcs_verbal_t6 + pv.gcs_eye_t6) <= 10 THEN 10 END AS niv_hacor_gcs_t0_t6,
        CASE WHEN IFNULL(pv.pf_ratio_t6, pv.pao2_t6 / NULLIF(pv.fio2_t6, 0)) >= 201 THEN 0 WHEN IFNULL(pv.pf_ratio_t6, pv.pao2_t6 / NULLIF(pv.fio2_t6, 0)) >= 176 THEN 2 WHEN IFNULL(pv.pf_ratio_t6, pv.pao2_t6 / NULLIF(pv.fio2_t6, 0)) >= 151 THEN 3 WHEN IFNULL(pv.pf_ratio_t6, pv.pao2_t6 / NULLIF(pv.fio2_t6, 0)) >= 126 THEN 4 WHEN IFNULL(pv.pf_ratio_t6, pv.pao2_t6 / NULLIF(pv.fio2_t6, 0)) >= 101 THEN 5 WHEN IFNULL(pv.pf_ratio_t6, pv.pao2_t6 / NULLIF(pv.fio2_t6, 0)) <= 100 THEN 6 END AS niv_hacor_pf_t0_t6,
        CASE WHEN pv.resp_rate_t6 <= 30 THEN 0 WHEN pv.resp_rate_t6 <= 35 THEN 1 WHEN pv.resp_rate_t6 <= 40 THEN 2 WHEN pv.resp_rate_t6 <= 45 THEN 3 WHEN pv.resp_rate_t6 > 45 THEN 4 END AS niv_hacor_rr_t0_t6,

        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL ELSE pv.hr_t12 END AS niv_hr_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL ELSE pv.ph_t12 END AS niv_ph_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL ELSE pv.resp_rate_t12 END AS niv_rr_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL WHEN pv.gcs_motor_t12 IS NOT NULL AND pv.gcs_verbal_t12 IS NOT NULL AND pv.gcs_eye_t12 IS NOT NULL THEN pv.gcs_motor_t12 + pv.gcs_verbal_t12 + pv.gcs_eye_t12 END AS niv_gcs_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL ELSE IFNULL(pv.pf_ratio_t12, pv.pao2_t12 / NULLIF(pv.fio2_t12, 0)) END AS niv_pf_ratio_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL WHEN pv.hr_t12 <= 120 THEN 0 WHEN pv.hr_t12 > 120 THEN 1 END AS niv_hacor_hr_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL WHEN pv.ph_t12 >= 7.35 THEN 0 WHEN pv.ph_t12 >= 7.30 THEN 2 WHEN pv.ph_t12 >= 7.25 THEN 3 WHEN pv.ph_t12 < 7.25 THEN 4 END AS niv_hacor_ph_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL WHEN (pv.gcs_motor_t12 + pv.gcs_verbal_t12 + pv.gcs_eye_t12) = 15 THEN 0 WHEN (pv.gcs_motor_t12 + pv.gcs_verbal_t12 + pv.gcs_eye_t12) >= 13 THEN 2 WHEN (pv.gcs_motor_t12 + pv.gcs_verbal_t12 + pv.gcs_eye_t12) >= 11 THEN 5 WHEN (pv.gcs_motor_t12 + pv.gcs_verbal_t12 + pv.gcs_eye_t12) <= 10 THEN 10 END AS niv_hacor_gcs_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL WHEN IFNULL(pv.pf_ratio_t12, pv.pao2_t12 / NULLIF(pv.fio2_t12, 0)) >= 201 THEN 0 WHEN IFNULL(pv.pf_ratio_t12, pv.pao2_t12 / NULLIF(pv.fio2_t12, 0)) >= 176 THEN 2 WHEN IFNULL(pv.pf_ratio_t12, pv.pao2_t12 / NULLIF(pv.fio2_t12, 0)) >= 151 THEN 3 WHEN IFNULL(pv.pf_ratio_t12, pv.pao2_t12 / NULLIF(pv.fio2_t12, 0)) >= 126 THEN 4 WHEN IFNULL(pv.pf_ratio_t12, pv.pao2_t12 / NULLIF(pv.fio2_t12, 0)) >= 101 THEN 5 WHEN IFNULL(pv.pf_ratio_t12, pv.pao2_t12 / NULLIF(pv.fio2_t12, 0)) <= 100 THEN 6 END AS niv_hacor_pf_t6_t12,
        CASE WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL WHEN pv.resp_rate_t12 <= 30 THEN 0 WHEN pv.resp_rate_t12 <= 35 THEN 1 WHEN pv.resp_rate_t12 <= 40 THEN 2 WHEN pv.resp_rate_t12 <= 45 THEN 3 WHEN pv.resp_rate_t12 > 45 THEN 4 END AS niv_hacor_rr_t6_t12,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL ELSE pv.hr_t24 END AS niv_hr_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL ELSE pv.ph_t24 END AS niv_ph_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL ELSE pv.resp_rate_t24 END AS niv_rr_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL WHEN pv.gcs_motor_t24 IS NOT NULL AND pv.gcs_verbal_t24 IS NOT NULL AND pv.gcs_eye_t24 IS NOT NULL THEN pv.gcs_motor_t24 + pv.gcs_verbal_t24 + pv.gcs_eye_t24 END AS niv_gcs_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL ELSE IFNULL(pv.pf_ratio_t24, pv.pao2_t24 / NULLIF(pv.fio2_t24, 0)) END AS niv_pf_ratio_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL WHEN pv.hr_t24 <= 120 THEN 0 WHEN pv.hr_t24 > 120 THEN 1 END AS niv_hacor_hr_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL WHEN pv.ph_t24 >= 7.35 THEN 0 WHEN pv.ph_t24 >= 7.30 THEN 2 WHEN pv.ph_t24 >= 7.25 THEN 3 WHEN pv.ph_t24 < 7.25 THEN 4 END AS niv_hacor_ph_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL WHEN (pv.gcs_motor_t24 + pv.gcs_verbal_t24 + pv.gcs_eye_t24) = 15 THEN 0 WHEN (pv.gcs_motor_t24 + pv.gcs_verbal_t24 + pv.gcs_eye_t24) >= 13 THEN 2 WHEN (pv.gcs_motor_t24 + pv.gcs_verbal_t24 + pv.gcs_eye_t24) >= 11 THEN 5 WHEN (pv.gcs_motor_t24 + pv.gcs_verbal_t24 + pv.gcs_eye_t24) <= 10 THEN 10 END AS niv_hacor_gcs_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL WHEN IFNULL(pv.pf_ratio_t24, pv.pao2_t24 / NULLIF(pv.fio2_t24, 0)) >= 201 THEN 0 WHEN IFNULL(pv.pf_ratio_t24, pv.pao2_t24 / NULLIF(pv.fio2_t24, 0)) >= 176 THEN 2 WHEN IFNULL(pv.pf_ratio_t24, pv.pao2_t24 / NULLIF(pv.fio2_t24, 0)) >= 151 THEN 3 WHEN IFNULL(pv.pf_ratio_t24, pv.pao2_t24 / NULLIF(pv.fio2_t24, 0)) >= 126 THEN 4 WHEN IFNULL(pv.pf_ratio_t24, pv.pao2_t24 / NULLIF(pv.fio2_t24, 0)) >= 101 THEN 5 WHEN IFNULL(pv.pf_ratio_t24, pv.pao2_t24 / NULLIF(pv.fio2_t24, 0)) <= 100 THEN 6 END AS niv_hacor_pf_t12_t24,
        CASE WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL WHEN pv.resp_rate_t24 <= 30 THEN 0 WHEN pv.resp_rate_t24 <= 35 THEN 1 WHEN pv.resp_rate_t24 <= 40 THEN 2 WHEN pv.resp_rate_t24 <= 45 THEN 3 WHEN pv.resp_rate_t24 > 45 THEN 4 END AS niv_hacor_rr_t12_t24

    FROM niv_pivoted_values pv
    LEFT JOIN niv_start_times nst ON pv.stay_id = nst.stay_id
    LEFT JOIN NIV_Intubation_Flags flags ON pv.stay_id = flags.stay_id
),

ventilation_events_sofa AS (
   SELECT
     stay_id, charttime
   FROM `physionet-data.mimiciv_3_1_icu.chartevents`
   WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
vasoactive_agent_sofa AS (
   SELECT
       stay_id, itemid,
       rate AS vaso_rate
   FROM `physionet-data.mimiciv_3_1_icu.inputevents`
   WHERE itemid IN (221906, 221289, 221662, 221653)
     AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
pafi_sofa AS (
   SELECT ie.stay_id,
       MIN(CASE WHEN vd.stay_id IS NULL THEN pao2fio2ratio END) AS pao2fio2ratio_novent,
       MIN(CASE WHEN vd.stay_id IS NOT NULL THEN pao2fio2ratio END) AS pao2fio2ratio_vent
   FROM Core_Cohort ie
   INNER JOIN `physionet-data.mimiciv_3_1_derived.bg` bg ON ie.hadm_id = bg.hadm_id
   LEFT JOIN ventilation_events_sofa vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime
   LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
   WHERE bg.specimen = 'ART.' AND bg.charttime BETWEEN icu.intime AND DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
   GROUP BY ie.stay_id
),
vasopressors_sofa AS (
   SELECT
       stay_id,
       MAX(CASE WHEN itemid = 221906 THEN vaso_rate END) AS rate_norepinephrine,
       MAX(CASE WHEN itemid = 221289 THEN vaso_rate END) AS rate_epinephrine,
       MAX(CASE WHEN itemid = 221662 THEN vaso_rate END) AS rate_dopamine,
       MAX(CASE WHEN itemid = 221653 THEN vaso_rate END) AS rate_dobutamine
   FROM vasoactive_agent_sofa
   GROUP BY stay_id
),
sofa_scorecomp AS (
   SELECT
       ie.stay_id,
       pf.pao2fio2ratio_novent, pf.pao2fio2ratio_vent,
       labs.platelets_min,
       labs.bilirubin_total_max AS bilirubin_max,
       vital.mbp_min,
       vaso.rate_norepinephrine, vaso.rate_epinephrine, vaso.rate_dopamine, vaso.rate_dobutamine,
       gcs.gcs_min,
       labs.creatinine_max,
       uo.urineoutput AS uo_24hr
   FROM Core_Cohort ie
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
   LEFT JOIN pafi_sofa pf ON ie.stay_id = pf.stay_id
   LEFT JOIN vasopressors_sofa vaso ON ie.stay_id = vaso.stay_id
),
final_sofa_scores AS (
   SELECT stay_id,
       CASE
           WHEN pao2fio2ratio_vent < 100 THEN 4
           WHEN pao2fio2ratio_vent < 200 THEN 3
           WHEN pao2fio2ratio_novent < 300 THEN 2
           WHEN pao2fio2ratio_vent < 300 THEN 2
           WHEN pao2fio2ratio_novent < 400 THEN 1
           WHEN pao2fio2ratio_vent < 400 THEN 1
           ELSE 0
       END AS respiration_sofa,
       CASE
           WHEN platelets_min < 20 THEN 4
           WHEN platelets_min < 50 THEN 3
           WHEN platelets_min < 100 THEN 2
           WHEN platelets_min < 150 THEN 1
           ELSE 0
       END AS coagulation_sofa,
       CASE
           WHEN bilirubin_max >= 12.0 THEN 4
           WHEN bilirubin_max >= 6.0 THEN 3
           WHEN bilirubin_max >= 2.0 THEN 2
           WHEN bilirubin_max >= 1.2 THEN 1
           ELSE 0
       END AS liver_sofa,
       CASE
           WHEN rate_dopamine > 15 OR rate_epinephrine > 0.1 OR rate_norepinephrine > 0.1 THEN 4
           WHEN rate_dopamine > 5 OR rate_epinephrine <= 0.1 OR rate_norepinephrine <= 0.1 THEN 3
           WHEN rate_dopamine > 0 OR rate_dobutamine > 0 THEN 2
           WHEN mbp_min < 70 THEN 1
           ELSE 0
       END AS cardiovascular_sofa,
       CASE
           WHEN (gcs_min >= 13 AND gcs_min <= 14) THEN 1
           WHEN (gcs_min >= 10 AND gcs_min <= 12) THEN 2
           WHEN (gcs_min >= 6 AND gcs_min <= 9) THEN 3
           WHEN gcs_min < 6 THEN 4
           ELSE 0
       END AS cns_sofa,
       CASE
           WHEN (creatinine_max >= 5.0) THEN 4
           WHEN uo_24hr < 200 THEN 4
           WHEN (creatinine_max >= 3.5 AND creatinine_max < 5.0) THEN 3
           WHEN uo_24hr < 500 THEN 3
           WHEN (creatinine_max >= 2.0 AND creatinine_max < 3.5) THEN 2
           WHEN (creatinine_max >= 1.2 AND creatinine_max < 2.0) THEN 1
           ELSE 0
       END AS renal_sofa
   FROM sofa_scorecomp
),

sirs_scorecomp AS (
   SELECT
       ie.stay_id,
       v.temperature_min, v.temperature_max,
       v.heart_rate_max,
       v.resp_rate_max,
       bg.pco2_min AS paco2_min,
       l.wbc_min, l.wbc_max, l.bands_max
   FROM Core_Cohort ie
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_bg_art` bg ON ie.stay_id = bg.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` v ON ie.stay_id = v.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` l ON ie.stay_id = l.stay_id
),
sirs_scorecalc AS (
   SELECT
       stay_id,
       CASE
           WHEN temperature_min < 36.0 THEN 1
           WHEN temperature_max > 38.0 THEN 1
           ELSE 0
       END AS temp_score,
       CASE
           WHEN heart_rate_max > 90.0 THEN 1
           ELSE 0
       END AS heart_rate_score,
       CASE
           WHEN resp_rate_max > 20.0 THEN 1
           WHEN paco2_min < 32.0 THEN 1
           ELSE 0
       END AS resp_score,
       CASE
           WHEN wbc_min < 4.0 THEN 1
           WHEN wbc_max > 12.0 THEN 1
           WHEN bands_max > 10 THEN 1
           ELSE 0
       END AS wbc_score
   FROM sirs_scorecomp
),
co_sapsii AS (
   SELECT
       ie.subject_id, ie.hadm_id, ie.stay_id,
       icu.intime AS starttime,
       DATETIME_ADD(icu.intime, INTERVAL '24' HOUR) AS endtime
   FROM Core_Cohort ie
   INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
),
ventilation_events_sapsii AS (
   SELECT stay_id, charttime, CASE WHEN itemid = 223849 THEN 'InvasiveVent' END AS ventilation_status
   FROM `physionet-data.mimiciv_3_1_icu.chartevents`
   WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
cpap_sapsii AS (
   SELECT co.stay_id,
       GREATEST(MIN(DATETIME_SUB(charttime, INTERVAL '1' HOUR)), co.starttime) AS starttime,
       LEAST(MAX(DATETIME_ADD(charttime, INTERVAL '4' HOUR)), co.endtime) AS endtime
   FROM co_sapsii co
   INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON co.stay_id = ce.stay_id
       AND ce.charttime > co.starttime AND ce.charttime <= co.endtime
   WHERE ce.itemid = 226732 AND REGEXP_CONTAINS(LOWER(ce.value), '(cpap mask|bipap)')
   GROUP BY co.stay_id, co.starttime, co.endtime
),
surgflag_sapsii AS (
   SELECT adm.hadm_id,
       CASE WHEN LOWER(curr_service) LIKE '%surg%' THEN 1 ELSE 0 END AS surgical,
       ROW_NUMBER() OVER (PARTITION BY adm.hadm_id ORDER BY transfertime) AS serviceorder
   FROM `physionet-data.mimiciv_3_1_hosp.admissions` adm
   LEFT JOIN `physionet-data.mimiciv_3_1_hosp.services` se ON adm.hadm_id = se.hadm_id
   WHERE adm.hadm_id IN (SELECT hadm_id FROM Core_Cohort)
),
comorb_sapsii AS (
   SELECT hadm_id,
       MAX(CASE WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 3) BETWEEN '042' AND '044' THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'B20' AND 'B22' THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) = 'B24' THEN 1 ELSE 0 END) AS aids,
       MAX(CASE WHEN icd_version = 9 AND (SUBSTR(icd_code, 1, 5) BETWEEN '20000' AND '20302' OR SUBSTR(icd_code, 1, 5) BETWEEN '20310' AND '20312' OR SUBSTR(icd_code, 1, 5) BETWEEN '20302' AND '20382' OR SUBSTR(icd_code, 1, 5) BETWEEN '20400' AND '20892' OR SUBSTR(icd_code, 1, 4) IN ('2386', '2733')) THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'C81' AND 'C96' THEN 1 ELSE 0 END) AS hem,
       MAX(CASE WHEN icd_version = 9 AND (SUBSTR(icd_code, 1, 4) BETWEEN '1960' AND '1991' OR SUBSTR(icd_code, 1, 5) BETWEEN '20970' AND '20975' OR SUBSTR(icd_code, 1, 5) IN ('20979', '78951')) THEN 1 WHEN icd_version = 10 AND (SUBSTR(icd_code, 1, 3) BETWEEN 'C77' AND 'C79' OR SUBSTR(icd_code, 1, 4) = 'C800') THEN 1 ELSE 0 END) AS mets
   FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
   WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
   GROUP BY hadm_id
),
pafi1_sapsii AS (
   SELECT
       co.stay_id, bg.charttime, pao2fio2ratio AS pao2fio2,
       CASE WHEN vd.stay_id IS NOT NULL THEN 1 ELSE 0 END AS vent,
       CASE WHEN cp.stay_id IS NOT NULL THEN 1 ELSE 0 END AS cpap
   FROM co_sapsii co
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.bg` bg ON co.subject_id = bg.subject_id AND bg.specimen = 'ART.' AND bg.charttime > co.starttime AND bg.charttime <= co.endtime
   LEFT JOIN ventilation_events_sapsii vd ON co.stay_id = vd.stay_id AND bg.charttime = vd.charttime
   LEFT JOIN cpap_sapsii cp ON co.stay_id = cp.stay_id AND bg.charttime > cp.starttime AND bg.charttime <= cp.endtime
),
pafi2_sapsii AS (
   SELECT stay_id, MIN(pao2fio2) AS pao2fio2_vent_min
   FROM pafi1_sapsii
   WHERE vent = 1 OR cpap = 1
   GROUP BY stay_id
),
sapsii_cohort AS (
   SELECT
       ie.subject_id, ie.hadm_id, ie.stay_id,
       va.age,
       vital.heart_rate_max, vital.heart_rate_min,
       vital.sbp_max, vital.sbp_min,
       vital.temperature_max AS tempc_max, vital.temperature_min AS tempc_min,
       pf.pao2fio2_vent_min,
       uo.urineoutput,
       labs.bun_min, labs.bun_max,
       labs.wbc_min, labs.wbc_max,
       labs.potassium_min, labs.potassium_max,
       labs.sodium_min, labs.sodium_max,
       labs.bicarbonate_min, labs.bicarbonate_max,
       labs.bilirubin_total_min AS bilirubin_min, labs.bilirubin_total_max AS bilirubin_max,
       gcs.gcs_min AS mingcs,
       comorb.aids, comorb.hem, comorb.mets,
       CASE
           WHEN adm.admission_type = 'ELECTIVE' AND sf.surgical = 1 THEN 'ScheduledSurgical'
           WHEN adm.admission_type != 'ELECTIVE' AND sf.surgical = 1 THEN 'UnscheduledSurgical'
           ELSE 'Medical'
       END AS admissiontype
   FROM Core_Cohort ie
   INNER JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON ie.hadm_id = adm.hadm_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.age` va ON ie.hadm_id = va.hadm_id
   LEFT JOIN pafi2_sapsii pf ON ie.stay_id = pf.stay_id
   LEFT JOIN surgflag_sapsii sf ON adm.hadm_id = sf.hadm_id AND sf.serviceorder = 1
   LEFT JOIN comorb_sapsii comorb ON ie.hadm_id = comorb.hadm_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
),
sapsii_scorecomp AS (
   SELECT
       cohort.*,
       CASE WHEN age < 40 THEN 0 WHEN age < 60 THEN 7 WHEN age < 70 THEN 12 WHEN age < 75 THEN 15 WHEN age < 80 THEN 16 WHEN age >= 80 THEN 18 END AS age_score,
       CASE WHEN heart_rate_min < 40 THEN 11 WHEN heart_rate_max >= 160 THEN 7 WHEN heart_rate_max >= 120 THEN 4 WHEN heart_rate_min < 70 THEN 2 ELSE 0 END AS hr_score,
       CASE WHEN sbp_min < 70 THEN 13 WHEN sbp_min < 100 THEN 5 WHEN sbp_max >= 200 THEN 2 ELSE 0 END AS sysbp_score,
       CASE WHEN tempc_max >= 39.0 THEN 3 ELSE 0 END AS temp_score,
       CASE WHEN pao2fio2_vent_min < 100 THEN 11 WHEN pao2fio2_vent_min < 200 THEN 9 WHEN pao2fio2_vent_min >= 200 THEN 6 END AS pao2fio2_score,
       CASE WHEN urineoutput < 500.0 THEN 11 WHEN urineoutput < 1000.0 THEN 4 ELSE 0 END AS uo_score,
       CASE WHEN bun_max < 28.0 THEN 0 WHEN bun_max < 84.0 THEN 6 WHEN bun_max >= 84.0 THEN 10 END AS bun_score,
       CASE WHEN wbc_min < 1.0 THEN 12 WHEN wbc_max >= 20.0 THEN 3 ELSE 0 END AS wbc_score,
       CASE WHEN potassium_min < 3.0 THEN 3 WHEN potassium_max >= 5.0 THEN 3 ELSE 0 END AS potassium_score,
       CASE WHEN sodium_min < 125 THEN 5 WHEN sodium_max >= 145 THEN 1 ELSE 0 END AS sodium_score,
       CASE WHEN bicarbonate_min < 15.0 THEN 6 WHEN bicarbonate_min < 20.0 THEN 3 ELSE 0 END AS bicarbonate_score,
       CASE WHEN bilirubin_max < 4.0 THEN 0 WHEN bilirubin_max < 6.0 THEN 4 WHEN bilirubin_max >= 6.0 THEN 9 END AS bilirubin_score,
       CASE WHEN mingcs < 3 THEN NULL WHEN mingcs < 6 THEN 26 WHEN mingcs < 9 THEN 13 WHEN mingcs < 11 THEN 7 WHEN mingcs < 14 THEN 5 WHEN mingcs >= 14 THEN 0 END AS gcs_score,
       CASE WHEN aids = 1 THEN 17 WHEN hem = 1 THEN 10 WHEN mets = 1 THEN 9 ELSE 0 END AS comorbidity_score,
       CASE WHEN admissiontype = 'ScheduledSurgical' THEN 0 WHEN admissiontype = 'Medical' THEN 6 WHEN admissiontype = 'UnscheduledSurgical' THEN 8 END AS admissiontype_score
   FROM sapsii_cohort cohort
),
sapsii_scores AS (
   SELECT s.*,
       (COALESCE(age_score, 0) + COALESCE(hr_score, 0) + COALESCE(sysbp_score, 0) + COALESCE(temp_score, 0)
       + COALESCE(pao2fio2_score, 0) + COALESCE(uo_score, 0) + COALESCE(bun_score, 0) + COALESCE(wbc_score, 0)
       + COALESCE(potassium_score, 0) + COALESCE(sodium_score, 0) + COALESCE(bicarbonate_score, 0)
       + COALESCE(bilirubin_score, 0) + COALESCE(gcs_score, 0) + COALESCE(comorbidity_score, 0) + COALESCE(admissiontype_score, 0)
       ) AS sapsii
   FROM sapsii_scorecomp s
),

ventilation_events_oasis AS (
   SELECT
     stay_id, charttime
   FROM `physionet-data.mimiciv_3_1_icu.chartevents`
   WHERE itemid = 223849 AND value IS NOT NULL
     AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
surgflag_oasis AS (
   SELECT ie.stay_id,
       MAX(CASE
           WHEN LOWER(curr_service) LIKE '%surg%' THEN 1
           WHEN curr_service = 'ORTHO' THEN 1
           ELSE 0 END) AS surgical
   FROM Core_Cohort ie
   LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_hosp.services` se
       ON ie.hadm_id = se.hadm_id
           AND se.transfertime < DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
   GROUP BY ie.stay_id
),
vent_oasis AS (
   SELECT ie.stay_id,
       MAX(CASE WHEN v.stay_id IS NOT NULL THEN 1 ELSE 0 END) AS vent
   FROM Core_Cohort ie
   LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
   LEFT JOIN ventilation_events_oasis v
       ON ie.stay_id = v.stay_id
           AND v.charttime >= icu.intime
           AND v.charttime <= DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
   GROUP BY ie.stay_id
),
cohort_oasis AS (
   SELECT ie.subject_id, ie.hadm_id, ie.stay_id,
       ie.intime, ie.outtime, adm.deathtime,
       DATETIME_DIFF(ie.intime, adm.admittime, MINUTE) AS preiculos,
       ag.age,
       gcs.gcs_min,
       vital.heart_rate_max, vital.heart_rate_min,
       vital.mbp_max, vital.mbp_min,
       vital.resp_rate_max, vital.resp_rate_min,
       vital.temperature_max, vital.temperature_min,
       vent.vent AS mechvent,
       uo.urineoutput,
       CASE
           WHEN adm.admission_type = 'ELECTIVE' AND sf.surgical = 1 THEN 1
           ELSE 0
       END AS electivesurgery,
       adm.hospital_expire_flag
   FROM `physionet-data.mimiciv_3_1_icu.icustays` ie
   INNER JOIN Core_Cohort bc ON ie.stay_id = bc.stay_id
   INNER JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON ie.hadm_id = adm.hadm_id
   INNER JOIN `physionet-data.mimiciv_3_1_hosp.patients` pat ON ie.subject_id = pat.subject_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.age` ag ON ie.hadm_id = ag.hadm_id
   LEFT JOIN surgflag_oasis sf ON ie.stay_id = sf.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
   LEFT JOIN vent_oasis vent ON ie.stay_id = vent.stay_id
),
scorecomp_oasis AS (
   SELECT co.subject_id, co.hadm_id, co.stay_id,
       CASE WHEN preiculos < 10.2 THEN 5 WHEN preiculos < 297 THEN 3 WHEN preiculos < 1440 THEN 0 WHEN preiculos < 18708 THEN 2 ELSE 1 END AS preiculos_score,
       CASE WHEN age < 24 THEN 0 WHEN age <= 53 THEN 3 WHEN age <= 77 THEN 6 WHEN age <= 89 THEN 9 WHEN age >= 90 THEN 7 ELSE 0 END AS age_score,
       CASE WHEN gcs_min <= 7 THEN 10 WHEN gcs_min < 14 THEN 4 WHEN gcs_min = 14 THEN 3 ELSE 0 END AS gcs_score,
       CASE WHEN heart_rate_max > 125 THEN 6 WHEN heart_rate_min < 33 THEN 4 WHEN heart_rate_max >= 107 AND heart_rate_max <= 125 THEN 3 WHEN heart_rate_max >= 89 AND heart_rate_max <= 106 THEN 1 ELSE 0 END AS heart_rate_score,
       CASE WHEN mbp_min < 20.65 THEN 4 WHEN mbp_min < 51 THEN 3 WHEN mbp_max > 143.44 THEN 3 WHEN mbp_min >= 51 AND mbp_min < 61.33 THEN 2 ELSE 0 END AS mbp_score,
       CASE WHEN resp_rate_min < 6 THEN 10 WHEN resp_rate_max > 44 THEN 9 WHEN resp_rate_max > 30 THEN 6 WHEN resp_rate_max > 22 THEN 1 WHEN resp_rate_min < 13 THEN 1 ELSE 0 END AS resp_rate_score,
       CASE WHEN temperature_max > 39.88 THEN 6 WHEN temperature_min < 33.22 THEN 3 WHEN temperature_min > 35.93 AND temperature_min <= 36.39 THEN 2 WHEN temperature_max >= 36.89 AND temperature_max <= 39.88 THEN 2 WHEN temperature_min >= 33.22 AND temperature_min <= 35.93 THEN 4 WHEN temperature_max >= 33.22 AND temperature_max <= 35.93 THEN 4 ELSE 0 END AS temp_score,
       CASE WHEN urineoutput < 671.09 THEN 10 WHEN urineoutput > 6896.80 THEN 8 WHEN urineoutput >= 671.09 AND urineoutput <= 1426.99 THEN 5 WHEN urineoutput >= 1427.00 AND urineoutput <= 2544.14 THEN 1 ELSE 0 END AS urineoutput_score,
       CASE WHEN mechvent = 1 THEN 9 ELSE 0 END AS mechvent_score,
       CASE WHEN electivesurgery = 1 THEN 0 ELSE 6 END AS electivesurgery_score
   FROM cohort_oasis co
),
final_oasis_scores AS (
   SELECT s.stay_id,
       (COALESCE(age_score, 0) + COALESCE(preiculos_score, 0) + COALESCE(gcs_score, 0) + COALESCE(heart_rate_score, 0)
       + COALESCE(mbp_score, 0) + COALESCE(resp_rate_score, 0) + COALESCE(temp_score, 0)
       + COALESCE(urineoutput_score, 0) + COALESCE(mechvent_score, 0) + COALESCE(electivesurgery_score, 0)
       ) AS oasis
   FROM scorecomp_oasis s
),
cohort_with_intime_lods AS (
   SELECT
       arf.subject_id, arf.hadm_id, arf.stay_id,
       icu.intime
   FROM Core_Cohort arf
   LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON arf.stay_id = icu.stay_id
),
ventilation_events_lods AS (
   SELECT
       stay_id, charttime
   FROM `physionet-data.mimiciv_3_1_icu.chartevents`
   WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
cpap_lods AS (
   SELECT
       c.stay_id,
       MIN(DATETIME_SUB(ce.charttime, INTERVAL '1' HOUR)) AS starttime,
       MAX(DATETIME_ADD(ce.charttime, INTERVAL '4' HOUR)) AS endtime
   FROM cohort_with_intime_lods c
   INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce
       ON c.stay_id = ce.stay_id
       AND ce.charttime >= c.intime
       AND ce.charttime <= DATETIME_ADD(c.intime, INTERVAL '1' DAY)
   WHERE ce.itemid = 226732 AND (LOWER(ce.value) LIKE '%cpap%' OR LOWER(ce.value) LIKE '%bipap mask%')
   GROUP BY c.stay_id
),
pafi1_lods AS (
   SELECT c.stay_id, bg.charttime, pao2fio2ratio,
       CASE WHEN vd.stay_id IS NOT NULL THEN 1 ELSE 0 END AS vent,
       CASE WHEN cp.stay_id IS NOT NULL THEN 1 ELSE 0 END AS cpap
   FROM `physionet-data.mimiciv_3_1_derived.bg` bg
   INNER JOIN cohort_with_intime_lods c ON bg.hadm_id = c.hadm_id
   LEFT JOIN ventilation_events_lods vd ON c.stay_id = vd.stay_id AND bg.charttime = vd.charttime
   LEFT JOIN cpap_lods cp ON c.stay_id = cp.stay_id AND bg.charttime >= cp.starttime AND bg.charttime <= cp.endtime
   WHERE bg.charttime >= c.intime AND bg.charttime <= DATETIME_ADD(c.intime, INTERVAL '1' DAY)
),
pafi2_lods AS (
   SELECT stay_id, MIN(pao2fio2ratio) AS pao2fio2_vent_min
   FROM pafi1_lods
   WHERE vent = 1 OR cpap = 1
   GROUP BY stay_id
),
cohort_lods AS (
   SELECT ie.stay_id,
       gcs.gcs_min, vital.heart_rate_max, vital.heart_rate_min, vital.sbp_max, vital.sbp_min,
       pf.pao2fio2_vent_min,
       labs.bun_max, labs.bun_min, labs.wbc_max, labs.wbc_min,
       labs.bilirubin_total_max AS bilirubin_max, labs.creatinine_max,
       labs.pt_min, labs.pt_max, labs.platelets_min AS platelet_min,
       uo.urineoutput
   FROM cohort_with_intime_lods ie
   LEFT JOIN pafi2_lods pf ON ie.stay_id = pf.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
   LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
),
final_lods_scores AS (
   SELECT
       stay_id,
       CASE WHEN gcs_min < 3 THEN NULL WHEN gcs_min <= 5 THEN 5 WHEN gcs_min <= 8 THEN 3 WHEN gcs_min <= 13 THEN 1 ELSE 0 END AS neurologic,
       CASE WHEN heart_rate_min < 30 THEN 5 WHEN sbp_min < 40 THEN 5 WHEN sbp_min < 70 THEN 3 WHEN sbp_max >= 270 THEN 3 WHEN heart_rate_max >= 140 THEN 1 WHEN sbp_max >= 240 THEN 1 WHEN sbp_min < 90 THEN 1 ELSE 0 END AS cardiovascular,
       CASE WHEN urineoutput < 500.0 THEN 5 WHEN bun_max >= 56.0 THEN 5 WHEN creatinine_max >= 1.60 THEN 3 WHEN urineoutput < 750.0 THEN 3 WHEN bun_max >= 28.0 THEN 3 WHEN urineoutput >= 10000.0 THEN 3 WHEN creatinine_max >= 1.20 THEN 1 WHEN bun_max >= 17.0 THEN 1 WHEN bun_max >= 7.50 THEN 1 ELSE 0 END AS renal,
       CASE WHEN pao2fio2_vent_min IS NULL THEN 0 WHEN pao2fio2_vent_min >= 150 THEN 1 WHEN pao2fio2_vent_min < 150 THEN 3 END AS pulmonary,
       CASE WHEN wbc_min < 1.0 THEN 3 WHEN wbc_min < 2.5 THEN 1 WHEN platelet_min < 50.0 THEN 1 WHEN wbc_max >= 50.0 THEN 1 ELSE 0 END AS hematologic,
       CASE WHEN bilirubin_max >= 2.0 THEN 1 WHEN pt_max > (12 + 3) THEN 1 WHEN pt_min < (12 * 0.25) THEN 1 ELSE 0 END AS hepatic
   FROM cohort_lods
),

niv_start_times_rox AS (
  SELECT stay_id, MIN(charttime) AS niv_starttime
  FROM (
       SELECT stay_id, starttime AS charttime FROM `physionet-data.mimiciv_3_1_icu.procedureevents` WHERE itemid = 225794 AND stay_id IN (SELECT stay_id FROM Core_Cohort)
       UNION ALL
       SELECT stay_id, charttime FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 226732 AND value IN ('Bipap mask', 'CPAP mask') AND stay_id IN (SELECT stay_id FROM Core_Cohort)
       UNION ALL
       SELECT stay_id, charttime FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 229314 AND value IN ('DuoPaP', 'NIV', 'NIV-ST') AND stay_id IN (SELECT stay_id FROM Core_Cohort)
  )
  GROUP BY stay_id
),
sbt_start_times_rox AS (
  SELECT stay_id, MIN(charttime) AS sbt_starttime
  FROM `physionet-data.mimiciv_3_1_icu.chartevents`
  WHERE itemid = 224715 AND value = 'Yes' AND stay_id IN (SELECT stay_id FROM Core_Cohort)
  GROUP BY stay_id
),
anchor_times_rox AS (
   SELECT
       b.stay_id,
       n.niv_starttime,
       s.sbt_starttime
   FROM Core_Cohort b
   LEFT JOIN niv_start_times_rox n ON b.stay_id = n.stay_id
   LEFT JOIN sbt_start_times_rox s ON b.stay_id = s.stay_id
),
raw_rox_data_rox AS (
   SELECT
       c.stay_id, c.niv_starttime, c.sbt_starttime, ce.charttime,
       CASE
           WHEN ce.itemid = 220277 THEN 'spo2'
           WHEN ce.itemid = 223835 THEN 'fio2'
           WHEN ce.itemid IN (220210, 224690) THEN 'resp_rate'
           WHEN ce.itemid IN (229671, 224691, 228154) THEN 'flow_rate'
       END AS item_group,
       CASE WHEN ce.itemid = 223835 AND ce.valuenum > 1.0 THEN ce.valuenum / 100.0 ELSE ce.valuenum END AS valuenum
   FROM anchor_times_rox c
   JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON c.stay_id = ce.stay_id
   WHERE ce.itemid IN (220277, 223835, 220210, 224690, 229671, 224691, 228154)
),

values_in_windows_ranked_rox AS (
   SELECT stay_id, item_group, valuenum, time_window
   FROM (
       SELECT *,
           ROW_NUMBER() OVER (PARTITION BY stay_id, item_group, time_window ORDER BY charttime ASC) as rn
       FROM (
           SELECT stay_id, item_group, valuenum, charttime, 'niv_t0_t6' as time_window
           FROM raw_rox_data_rox
           WHERE charttime BETWEEN niv_starttime AND DATETIME_ADD(niv_starttime, INTERVAL 6 HOUR)

           UNION ALL

           SELECT stay_id, item_group, valuenum, charttime, 'niv_t6_t12' as time_window
           FROM raw_rox_data_rox
           WHERE charttime BETWEEN DATETIME_ADD(niv_starttime, INTERVAL 6 HOUR) AND DATETIME_ADD(niv_starttime, INTERVAL 12 HOUR)

           UNION ALL

           SELECT stay_id, item_group, valuenum, charttime, 'niv_t12_t24' as time_window
           FROM raw_rox_data_rox
           WHERE charttime BETWEEN DATETIME_ADD(niv_starttime, INTERVAL 12 HOUR) AND DATETIME_ADD(niv_starttime, INTERVAL 24 HOUR)

           UNION ALL

           SELECT stay_id, item_group, valuenum, charttime, 'sbt_t0_t1' as time_window FROM raw_rox_data_rox WHERE charttime BETWEEN sbt_starttime AND DATETIME_ADD(sbt_starttime, INTERVAL 1 HOUR)
           UNION ALL
           SELECT stay_id, item_group, valuenum, charttime, 'sbt_t0_t3' as time_window FROM raw_rox_data_rox WHERE charttime BETWEEN sbt_starttime AND DATETIME_ADD(sbt_starttime, INTERVAL 3 HOUR)
       )
   )
   WHERE rn = 1
),

pivoted_values_rox AS (
   SELECT
       stay_id,
       MAX(CASE WHEN time_window = 'niv_t0_t6' AND item_group = 'spo2' THEN valuenum END) AS spo2_niv_t6,
       MAX(CASE WHEN time_window = 'niv_t0_t6' AND item_group = 'fio2' THEN valuenum END) AS fio2_niv_t6,
       MAX(CASE WHEN time_window = 'niv_t0_t6' AND item_group = 'flow_rate' THEN valuenum END) AS flow_rate_niv_t6,
       MAX(CASE WHEN time_window = 'niv_t0_t6' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_niv_t6,

       MAX(CASE WHEN time_window = 'niv_t6_t12' AND item_group = 'spo2' THEN valuenum END) AS spo2_niv_t6_t12,
       MAX(CASE WHEN time_window = 'niv_t6_t12' AND item_group = 'fio2' THEN valuenum END) AS fio2_niv_t6_t12,
       MAX(CASE WHEN time_window = 'niv_t6_t12' AND item_group = 'flow_rate' THEN valuenum END) AS flow_rate_niv_t6_t12,
       MAX(CASE WHEN time_window = 'niv_t6_t12' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_niv_t6_t12,

       MAX(CASE WHEN time_window = 'niv_t12_t24' AND item_group = 'spo2' THEN valuenum END) AS spo2_niv_t12_t24,
       MAX(CASE WHEN time_window = 'niv_t12_t24' AND item_group = 'fio2' THEN valuenum END) AS fio2_niv_t12_t24,
       MAX(CASE WHEN time_window = 'niv_t12_t24' AND item_group = 'flow_rate' THEN valuenum END) AS flow_rate_niv_t12_t24,
       MAX(CASE WHEN time_window = 'niv_t12_t24' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_niv_t12_t24,

       MAX(CASE WHEN time_window = 'sbt_t0_t1' AND item_group = 'spo2' THEN valuenum END) AS spo2_sbt_t1,
       MAX(CASE WHEN time_window = 'sbt_t0_t1' AND item_group = 'fio2' THEN valuenum END) AS fio2_sbt_t1,
       MAX(CASE WHEN time_window = 'sbt_t0_t1' AND item_group = 'flow_rate' THEN valuenum END) AS flow_rate_sbt_t1,
       MAX(CASE WHEN time_window = 'sbt_t0_t1' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_sbt_t1,
       MAX(CASE WHEN time_window = 'sbt_t0_t3' AND item_group = 'spo2' THEN valuenum END) AS spo2_sbt_t3,
       MAX(CASE WHEN time_window = 'sbt_t0_t3' AND item_group = 'fio2' THEN valuenum END) AS fio2_sbt_t3,
       MAX(CASE WHEN time_window = 'sbt_t0_t3' AND item_group = 'flow_rate' THEN valuenum END) AS flow_rate_sbt_t3,
       MAX(CASE WHEN time_window = 'sbt_t0_t3' AND item_group = 'resp_rate' THEN valuenum END) AS resp_rate_sbt_t3
   FROM values_in_windows_ranked_rox
   GROUP BY stay_id
),

final_rox_scores AS (
    SELECT
        p.stay_id,
        (p.spo2_niv_t6 / NULLIF(COALESCE(p.fio2_niv_t6, 0.21 + (p.flow_rate_niv_t6 * 0.03)), 0)) / NULLIF(p.resp_rate_niv_t6, 0) AS rox_index_niv_t0_t6,

        CASE
            WHEN flags.intubated_within_6h_of_niv = 1 THEN NULL
            ELSE (p.spo2_niv_t6_t12 / NULLIF(COALESCE(p.fio2_niv_t6_t12, 0.21 + (p.flow_rate_niv_t6_t12 * 0.03)), 0)) / NULLIF(p.resp_rate_niv_t6_t12, 0)
        END AS rox_index_niv_t6_t12,

        CASE
            WHEN flags.intubated_within_12h_of_niv = 1 THEN NULL
            ELSE (p.spo2_niv_t12_t24 / NULLIF(COALESCE(p.fio2_niv_t12_t24, 0.21 + (p.flow_rate_niv_t12_t24 * 0.03)), 0)) / NULLIF(p.resp_rate_niv_t12_t24, 0)
        END AS rox_index_niv_t12_t24,

        (p.spo2_sbt_t1 / NULLIF(COALESCE(p.fio2_sbt_t1, 0.21 + (p.flow_rate_sbt_t1 * 0.03)), 0)) / NULLIF(p.resp_rate_sbt_t1, 0) AS rox_index_sbt_t1,
        (p.spo2_sbt_t3 / NULLIF(COALESCE(p.fio2_sbt_t3, 0.21 + (p.flow_rate_sbt_t3 * 0.03)), 0)) / NULLIF(p.resp_rate_sbt_t3, 0) AS rox_index_sbt_t3
    FROM pivoted_values_rox p
    LEFT JOIN NIV_Intubation_Flags flags ON p.stay_id = flags.stay_id
),

First_Vent_Type AS (
    WITH all_vents AS (
        SELECT stay_id, event_time, 'NIV' as vent_type FROM All_NIV_Events
        UNION ALL
        SELECT stay_id, event_time, 'IMV' as vent_type FROM All_IMV_Events
    ),
    ranked_vents AS (
        SELECT
            stay_id,
            vent_type,
            ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY event_time ASC) as rn
        FROM all_vents
    )
    SELECT
        stay_id,
        vent_type as first_vent_type
    FROM ranked_vents
    WHERE rn = 1
),

ICU_Readmission_Flag AS (
  SELECT
    stay_id,
    CASE
      WHEN next_hadm_id = hadm_id
           AND DATETIME_DIFF(next_icu_intime, outtime, DAY) > 1
           AND DATETIME_DIFF(next_icu_intime, outtime, DAY) <= 30
      THEN 1
      ELSE 0
    END AS icu_readmission_30_day
  FROM (
    SELECT
      stay_id,
      subject_id,
      hadm_id,
      intime,
      outtime,
      LEAD(hadm_id, 1) OVER (PARTITION BY subject_id ORDER BY intime ASC) as next_hadm_id,
      LEAD(intime, 1) OVER (PARTITION BY subject_id ORDER BY intime ASC) as next_icu_intime
    FROM `physionet-data.mimiciv_3_1_icu.icustays`
    WHERE subject_id IN (SELECT subject_id FROM Core_Cohort)
  ) AS ranked_stays
  WHERE stay_id IN (SELECT stay_id FROM Core_Cohort)
),

Supplemental_Oxygen_Flag AS (
  SELECT
      i.hadm_id,
      1 AS has_supplemental_oxygen
  FROM `physionet-data.mimiciv_3_1_icu.chartevents` AS c
  JOIN `physionet-data.mimiciv_3_1_icu.icustays` AS i ON c.stay_id = i.stay_id
  WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort)
    AND c.itemid = 226732
    AND c.value IN (
        'Non-rebreather', 'Face tent', 'Aerosol-cool', 'Venti mask',
        'Medium conc mask', 'Ultrasonic neb', 'Vapomist', 'Oxymizer',
        'High flow neb', 'Nasal cannula'
    )
  GROUP BY i.hadm_id
),

NIV_Flag AS (
  SELECT
      hadm_id,
      1 AS had_niv
  FROM (
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.procedureevents` pe JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON pe.stay_id = i.stay_id WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort) AND pe.itemid = 225794
      UNION DISTINCT
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.chartevents` c JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
      WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort)
        AND (
            (c.itemid = 226732 AND c.value IN ('Bipap mask', 'CPAP mask'))
            OR (c.itemid = 229314 AND c.value IN ('DuoPaP', 'NIV', 'NIV-ST'))
        )
  )
  GROUP BY hadm_id
),

IMV_Flag AS (
  SELECT
      hadm_id,
      1 AS had_imv
  FROM (
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.procedureevents` pe JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON pe.stay_id = i.stay_id WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort) AND pe.itemid = 225792
      UNION DISTINCT
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.chartevents` c JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
      WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort)
        AND (
            (c.itemid = 226732 AND c.value IN ('Endotracheal tube', 'Trach mask'))
            OR (c.itemid = 223849 AND c.value IN ('CMV', 'APRV', 'APRV/Biphasic+ApnPress', 'APRV/Biphasic+ApnVol', 'APV (cmv)', 'Ambient', 'Apnea Ventilation', 'CMV/ASSIST', 'CMV/ASSIST/AutoFlow', 'CMV/AutoFlow', 'CPAP/PPS', 'CPAP/PSV', 'CPAP/PSV+Apn TCPL', 'CPAP/PSV+ApnPres', 'CPAP/PSV+ApnVol', 'MMV', 'MMV/AutoFlow', 'MMV/PSV', 'MMV/PSV/AutoFlow', 'P-CMV', 'PCV+', 'PCV+/PSV', 'PCV+Assist', 'PRES/AC', 'PRVC/AC', 'PRVC/SIMV', 'PSV/SBT', 'SIMV', 'SIMV/AutoFlow', 'SIMV/PRES', 'SIMV/PSV', 'SIMV/PSV/AutoFlow', 'SIMV/VOL', 'SYNCHRON MASTER', 'SYNCHRON SLAVE', 'VOL/AC'))
            OR (c.itemid = 229314 AND c.value IN ('APRV', 'APV (cmv)', 'Ambient', '(S) CMV', 'P-CMV', 'SIMV', 'APV (simv)', 'P-SIMV', 'VS', 'ASV'))
        )
  )
  GROUP BY hadm_id
),

RRT_Flag AS (
  SELECT
      hadm_id,
      1 AS had_rrt
  FROM `physionet-data.mimiciv_3_1_hosp.procedures_icd`
  WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
    AND (
      (icd_version = 9 AND icd_code IN ('3995', '3927', '3943', '3895', '3942', '5498'))
      OR
      (icd_version = 10 AND icd_code IN ('5A1D70Z', '5A1D80Z', '5A1D90Z'))
    )
  GROUP BY hadm_id
),

First_Invasive_Vent_Time AS (
    SELECT
        stay_id,
        MIN(event_time) as first_invasive_time
    FROM Ventilation_Events
    WHERE vent_type = 'Invasive'
    GROUP BY stay_id
),

NIV_Procedure_Durations AS (
    SELECT
        stay_id,
        starttime,
        endtime
    FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
    WHERE itemid = 225794
      AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),

NIVF_Durations AS (
    SELECT
        niv.stay_id,
        DATETIME_DIFF(
            LEAST(niv.endtime, IFNULL(fiv.first_invasive_time, niv.endtime)),
            niv.starttime,
            MINUTE
        ) AS duration_minutes
    FROM NIV_Procedure_Durations AS niv
    LEFT JOIN First_Invasive_Vent_Time AS fiv ON niv.stay_id = fiv.stay_id
    WHERE niv.starttime < IFNULL(fiv.first_invasive_time, DATETIME '9999-12-31 23:59:59')
),

NIVF_Total_Duration AS (
    SELECT
        stay_id,
        SUM(duration_minutes) AS duration_NIVF
    FROM NIVF_Durations
    GROUP BY stay_id
),

Charlson_Comorbidity_Index AS (
    WITH
    diag AS (
        SELECT
            hadm_id,
            CASE WHEN icd_version = 9 THEN icd_code ELSE NULL END AS icd9_code,
            CASE WHEN icd_version = 10 THEN icd_code ELSE NULL END AS icd10_code
        FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
        WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
    ),
    com AS (
        SELECT
            hadm_id,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('410', '412') OR SUBSTR(icd10_code, 1, 3) IN ('I21', 'I22') OR SUBSTR(icd10_code, 1, 4) = 'I252' THEN 1 ELSE 0 END) AS myocardial_infarct,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) = '428' OR SUBSTR(icd9_code, 1, 5) IN ('39891', '40201', '40211', '40291', '40401', '40403', '40411', '40413', '40491', '40493') OR SUBSTR(icd9_code, 1, 4) BETWEEN '4254' AND '4259' OR SUBSTR(icd10_code, 1, 3) IN ('I43', 'I50') OR SUBSTR(icd10_code, 1, 4) IN ('I099', 'I110', 'I130', 'I132', 'I255', 'I420', 'I425', 'I426', 'I427', 'I428', 'I429', 'P290') THEN 1 ELSE 0 END) AS congestive_heart_failure,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('440', '441') OR SUBSTR(icd9_code, 1, 4) IN ('0930', '4373', '4471', '5571', '5579', 'V434') OR SUBSTR(icd9_code, 1, 4) BETWEEN '4431' AND '4439' OR SUBSTR(icd10_code, 1, 3) IN ('I70', 'I71') OR SUBSTR(icd10_code, 1, 4) IN ('I731', 'I738', 'I739', 'I771', 'I790', 'I792', 'K551', 'K558', 'K559', 'Z958', 'Z959') THEN 1 ELSE 0 END) AS peripheral_vascular_disease,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) BETWEEN '430' AND '438' OR SUBSTR(icd9_code, 1, 5) = '36234' OR SUBSTR(icd10_code, 1, 3) IN ('G45', 'G46') OR SUBSTR(icd10_code, 1, 3) BETWEEN 'I60' AND 'I69' OR SUBSTR(icd10_code, 1, 4) = 'H340' THEN 1 ELSE 0 END) AS cerebrovascular_disease,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) = '290' OR SUBSTR(icd9_code, 1, 4) IN ('2941', '3312') OR SUBSTR(icd10_code, 1, 3) IN ('F00', 'F01', 'F02', 'F03', 'G30') OR SUBSTR(icd10_code, 1, 4) IN ('F051', 'G311') THEN 1 ELSE 0 END) AS dementia,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) BETWEEN '490' AND '505' OR SUBSTR(icd9_code, 1, 4) IN ('4168', '4169', '5064', '5081', '5088') OR SUBSTR(icd10_code, 1, 3) BETWEEN 'J40' AND 'J47' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'J60' AND 'J67' OR SUBSTR(icd10_code, 1, 4) IN ('I278', 'I279', 'J684', 'J701', 'J703') THEN 1 ELSE 0 END) AS chronic_pulmonary_disease,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) = '725' OR SUBSTR(icd9_code, 1, 4) IN ('4465', '7100', '7101', '7102', '7103', '7104', '7140', '7141', '7142', '7148') OR SUBSTR(icd10_code, 1, 3) IN ('M05', 'M06', 'M32', 'M33', 'M34') OR SUBSTR(icd10_code, 1, 4) IN ('M315', 'M351', 'M353', 'M360') THEN 1 ELSE 0 END) AS rheumatic_disease,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('531', '532', '533', '534') OR SUBSTR(icd10_code, 1, 3) IN ('K25', 'K26', 'K27', 'K28') THEN 1 ELSE 0 END) AS peptic_ulcer_disease,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('570', '571') OR SUBSTR(icd9_code, 1, 4) IN ('0706', '0709', '5733', '5734', '5738', '5739', 'V427') OR SUBSTR(icd9_code, 1, 5) IN ('07022', '07023', '07032', '07033', '07044', '07054') OR SUBSTR(icd10_code, 1, 3) IN ('B18', 'K73', 'K74') OR SUBSTR(icd10_code, 1, 4) IN ('K700', 'K701', 'K702', 'K703', 'K709', 'K713', 'K714', 'K715', 'K717', 'K760', 'K762', 'K763', 'K764', 'K768', 'K769', 'Z944') THEN 1 ELSE 0 END) AS mild_liver_disease,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('2500', '2501', '2502', '2503', '2508', '2509') OR SUBSTR(icd10_code, 1, 4) IN ('E100', 'E101', 'E106', 'E108', 'E109', 'E110', 'E111', 'E116', 'E118', 'E119', 'E120', 'E121', 'E126', 'E128', 'E129', 'E130', 'E131', 'E136', 'E138', 'E139', 'E140', 'E141', 'E146', 'E148', 'E149') THEN 1 ELSE 0 END) AS diabetes_without_cc,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('2504', '2505', '2506', '2507') OR SUBSTR(icd10_code, 1, 4) IN ('E102', 'E103', 'E104', 'E105', 'E107', 'E112', 'E113', 'E114', 'E115', 'E117', 'E122', 'E123', 'E124', 'E125', 'E127', 'E132', 'E133', 'E134', 'E135', 'E137', 'E142', 'E143', 'E144', 'E145', 'E147') THEN 1 ELSE 0 END) AS diabetes_with_cc,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('342', '343') OR SUBSTR(icd9_code, 1, 4) IN ('3341', '3440', '3441', '3442', '3443', '3444', '3445', '3446', '3449') OR SUBSTR(icd10_code, 1, 3) IN ('G81', 'G82') OR SUBSTR(icd10_code, 1, 4) IN ('G041', 'G114', 'G801', 'G802', 'G830', 'G831', 'G832', 'G833', 'G834', 'G839') THEN 1 ELSE 0 END) AS paraplegia,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('582', '585', '586', 'V56') OR SUBSTR(icd9_code, 1, 4) IN ('5880', 'V420', 'V451') OR SUBSTR(icd9_code, 1, 4) BETWEEN '5830' AND '5837' OR SUBSTR(icd9_code, 1, 5) IN ('40301', '40311', '40391', '40402', '40403', '40412', '40413', '40492', '40493') OR SUBSTR(icd10_code, 1, 3) IN ('N18', 'N19') OR SUBSTR(icd10_code, 1, 4) IN ('I120', 'I131', 'N032', 'N033', 'N034', 'N035', 'N036', 'N037', 'N052', 'N053', 'N054', 'N055', 'N056', 'N057', 'N250', 'Z490', 'Z491', 'Z492', 'Z940', 'Z992') THEN 1 ELSE 0 END) AS renal_disease,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) BETWEEN '140' AND '172' OR SUBSTR(icd9_code, 1, 4) BETWEEN '1740' AND '1958' OR SUBSTR(icd9_code, 1, 3) BETWEEN '200' AND '208' OR SUBSTR(icd9_code, 1, 4) = '2386' OR SUBSTR(icd10_code, 1, 3) IN ('C43', 'C88') OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C00' AND 'C26' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C30' AND 'C34' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C37' AND 'C41' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C45' AND 'C58' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C60' AND 'C76' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C81' AND 'C85' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C90' AND 'C97' THEN 1 ELSE 0 END) AS malignant_cancer,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('4560', '4561', '4562') OR SUBSTR(icd9_code, 1, 4) BETWEEN '5722' AND '5728' OR SUBSTR(icd10_code, 1, 4) IN ('I850', 'I859', 'I864', 'I982', 'K704', 'K711', 'K721', 'K729', 'K765', 'K766', 'K767') THEN 1 ELSE 0 END) AS severe_liver_disease,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('196', '197', '198', '199') OR SUBSTR(icd10_code, 1, 3) IN ('C77', 'C78', 'C79', 'C80') THEN 1 ELSE 0 END) AS metastatic_solid_tumor,
            MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('042', '043', '044') OR SUBSTR(icd10_code, 1, 3) IN ('B20', 'B21', 'B22', 'B24') THEN 1 ELSE 0 END) AS aids
        FROM diag
        GROUP BY hadm_id
    ),
    ag AS (
        SELECT
            hadm_id,
            age,
            CASE WHEN age <= 50 THEN 0 WHEN age <= 60 THEN 1 WHEN age <= 70 THEN 2 WHEN age <= 80 THEN 3 ELSE 4 END AS age_score
        FROM `physionet-data.mimiciv_3_1_derived.age`
        WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
    )
    SELECT
        com.hadm_id,
        myocardial_infarct AS charlson_myocardial_infarct,
        congestive_heart_failure AS charlson_congestive_heart_failure,
        peripheral_vascular_disease AS charlson_peripheral_vascular_disease,
        cerebrovascular_disease AS charlson_cerebrovascular_disease,
        dementia AS charlson_dementia,
        chronic_pulmonary_disease AS charlson_chronic_pulmonary_disease,
        rheumatic_disease AS charlson_rheumatic_disease,
        peptic_ulcer_disease AS charlson_peptic_ulcer_disease,
        mild_liver_disease AS charlson_mild_liver_disease,
        diabetes_without_cc AS charlson_diabetes_without_cc,
        diabetes_with_cc AS charlson_diabetes_with_cc,
        paraplegia AS charlson_paraplegia,
        renal_disease AS charlson_renal_disease,
        malignant_cancer AS charlson_malignant_cancer,
        severe_liver_disease AS charlson_severe_liver_disease,
        metastatic_solid_tumor AS charlson_metastatic_solid_tumor,
        aids AS charlson_aids,
        (
            COALESCE(ag.age_score, 0) + myocardial_infarct + congestive_heart_failure + peripheral_vascular_disease +
            cerebrovascular_disease + dementia + chronic_pulmonary_disease + rheumatic_disease + peptic_ulcer_disease +
            GREATEST(mild_liver_disease, 3 * severe_liver_disease) +
            GREATEST(2 * diabetes_with_cc, diabetes_without_cc) +
            GREATEST(2 * malignant_cancer, 6 * metastatic_solid_tumor) +
            2 * paraplegia + 2 * renal_disease + 6 * aids
        ) AS charlson_comorbidity_index
    FROM com
    LEFT JOIN ag ON com.hadm_id = ag.hadm_id
)


SELECT
    cohort.subject_id, cohort.hadm_id, cohort.stay_id,
    pat.gender, pat.anchor_age, pat.anchor_year, pat.anchor_year_group,
    so.death_datetime,
    adm.admittime, adm.dischtime, adm.race,
    so.in_hospital_mortality_flag AS hospital_expire_flag,
    arf_codes.arf_icd_code,
    preg.pregnancy_icd_codes, cong.congenital_anomaly_icd_codes, hem_mal.hematolymphoid_icd_codes, mal.malignancy_icd_codes, aids.aids_icd_codes,
    CASE WHEN dnr_events.dni_dnr_status IS NOT NULL AND dnr_icd.dni_dnr_icd IS NOT NULL THEN CONCAT(dnr_events.dni_dnr_status, '; ', dnr_icd.dni_dnr_icd) ELSE IFNULL(dnr_events.dni_dnr_status, dnr_icd.dni_dnr_icd) END AS dni_dnr_combined_status,
    IFNULL(com.has_t1d, 0) AS has_t1d,
    IFNULL(com.has_t2d, 0) AS has_t2d,
    IFNULL(com.has_hypertension, 0) AS has_hypertension,
    IFNULL(com.has_dyslipidemia, 0) AS has_dyslipidemia,
    IFNULL(com.has_ischemic_heart_disease, 0) AS has_ischemic_heart_disease,
    IFNULL(com.has_angina, 0) AS has_angina,
    IFNULL(com.has_coronary_artery_disease, 0) AS has_coronary_artery_disease,
    IFNULL(com.has_myocardial_infarction, 0) AS has_myocardial_infarction,
    IFNULL(com.has_heart_failure, 0) AS has_heart_failure,
    IFNULL(com.has_cardiac_arrest, 0) AS has_cardiac_arrest,
    IFNULL(com.has_shock, 0) AS has_shock,
    IFNULL(com.has_cardiogenic_shock, 0) AS has_cardiogenic_shock,
    IFNULL(com.has_atrial_fibrillation, 0) AS has_atrial_fibrillation,
    IFNULL(com.has_congenital_heart_defect, 0) AS has_congenital_heart_defect,
    IFNULL(com.has_infective_endocarditis, 0) AS has_infective_endocarditis,
    IFNULL(com.has_cerebrovascular_disease, 0) AS has_cerebrovascular_disease,
    IFNULL(com.has_dvt, 0) AS has_dvt,
    IFNULL(com.has_pneumonia, 0) AS has_pneumonia,
    IFNULL(com.has_bacterial_pneumonia, 0) AS has_bacterial_pneumonia,
    IFNULL(com.has_viral_pneumonia, 0) AS has_viral_pneumonia,
    IFNULL(com.has_copd, 0) AS has_copd,
    IFNULL(com.has_chronic_bronchitis, 0) AS has_chronic_bronchitis,
    IFNULL(com.has_emphysema, 0) AS has_emphysema,
    IFNULL(com.has_asthma, 0) AS has_asthma,
    IFNULL(com.has_bronchiectasis, 0) AS has_bronchiectasis,
    IFNULL(com.has_ild, 0) AS has_ild,
    IFNULL(com.has_post_inflammatory_pulmonary_fibrosis, 0) AS has_post_inflammatory_pulmonary_fibrosis,
    IFNULL(com.has_pulmonary_alveolar_proteinosis, 0) AS has_pulmonary_alveolar_proteinosis,
    IFNULL(com.has_idiopathic_pulmonary_hemosiderosis, 0) AS has_idiopathic_pulmonary_hemosiderosis,
    IFNULL(com.has_pulmonary_alveolar_microlithiasis, 0) AS has_pulmonary_alveolar_microlithiasis,
    IFNULL(com.has_idiopathic_interstitial_pneumonia_nos, 0) AS has_idiopathic_interstitial_pneumonia_nos,
    IFNULL(com.has_idiopathic_pulmonary_fibrosis, 0) AS has_idiopathic_pulmonary_fibrosis,
    IFNULL(com.has_idiopathic_non_specific_inter_pneumo, 0) AS has_idiopathic_non_specific_inter_pneumo,
    IFNULL(com.has_acute_interstitial_pneumonitis, 0) AS has_acute_interstitial_pneumonitis,
    IFNULL(com.has_respiratory_bronchiolitis_inter_lung_dis, 0) AS has_respiratory_bronchiolitis_inter_lung_dis,
    IFNULL(com.has_idiopathic_lymphoid_inter_pneumonia, 0) AS has_idiopathic_lymphoid_inter_pneumonia,
    IFNULL(com.has_cryptogenic_organizing_pneumonia, 0) AS has_cryptogenic_organizing_pneumonia,
    IFNULL(com.has_desquamative_interstitial_pneumonia, 0) AS has_desquamative_interstitial_pneumonia,
    IFNULL(com.has_lymphangioleiomyomatosis, 0) AS has_lymphangioleiomyomatosis,
    IFNULL(com.has_adult_pulm_langerhans_cell_histiocytos, 0) AS has_adult_pulm_langerhans_cell_histiocytos,
    IFNULL(com.has_neuroendocrine_cell_hyperpi_of_infancy, 0) AS has_neuroendocrine_cell_hyperpi_of_infancy,
    IFNULL(com.has_pulmonary_interstitial_glycogenosis, 0) AS has_pulmonary_interstitial_glycogenosis,
    IFNULL(com.has_surfactant_mutatons_of_the_lung, 0) AS has_surfactant_mutatons_of_the_lung,
    IFNULL(com.has_alveol_cap_dysplasia_w_vein_misalign, 0) AS has_alveol_cap_dysplasia_w_vein_misalign,
    IFNULL(com.has_other_interstital_lung_dis_of_childhood, 0) AS has_other_interstital_lung_dis_of_childhood,
    IFNULL(com.has_other_nonspec_alveol_parietoalveol_pneumopathies, 0) AS has_other_nonspec_alveol_parietoalveol_pneumopathies,
    IFNULL(com.has_sarcoidosis, 0) AS has_sarcoidosis,
    IFNULL(com.has_lung_involvement_in_other_lung_diseases, 0) AS has_lung_involvement_in_other_lung_diseases,
    IFNULL(com.has_systemic_sclerosis_w_lung_involve, 0) AS has_systemic_sclerosis_w_lung_involve,
    IFNULL(com.has_sicca_syndrome_w_lung_involve, 0) AS has_sicca_syndrome_w_lung_involve,
    IFNULL(com.has_dermatomyositis_with_lung_involve, 0) AS has_dermatomyositis_with_lung_involve,
    IFNULL(com.has_polymyositis_w_lung_involvement, 0) AS has_polymyositis_w_lung_involvement,
    IFNULL(com.has_rheumatoid_lung_disease, 0) AS has_rheumatoid_lung_disease,
    IFNULL(com.has_pulmonary_embolism, 0) AS has_pulmonary_embolism,
    IFNULL(com.has_pneumothorax, 0) AS has_pneumothorax,
    IFNULL(com.has_pulmonary_hypertension, 0) AS has_pulmonary_hypertension,
    IFNULL(com.has_ards, 0) AS has_ards,
    IFNULL(com.has_respiratory_failure, 0) AS has_respiratory_failure,
    IFNULL(com.has_arf_t1, 0) AS has_arf_t1,
    IFNULL(com.has_arf_t2, 0) AS has_arf_t2,
    IFNULL(com.has_sepsis, 0) AS has_sepsis,
    IFNULL(com.has_cirrhosis, 0) AS has_cirrhosis,
    IFNULL(com.has_hepatitis, 0) AS has_hepatitis,
    IFNULL(com.has_peptic_ulcer_disease, 0) AS has_peptic_ulcer_disease,
    IFNULL(com.has_ckd, 0) AS has_ckd,
    IFNULL(com.has_aki_comorbidity, 0) AS has_aki_comorbidity,
    IFNULL(com.has_hemiplegia, 0) AS has_hemiplegia,
    IFNULL(com.has_dementia, 0) AS has_dementia,
    IFNULL(com.has_leukemia, 0) AS has_leukemia,
    IFNULL(com.has_lymphoma, 0) AS has_lymphoma,
    IFNULL(com.has_aids, 0) AS has_aids,
    IFNULL(com.has_connective_tissue_disease, 0) AS has_connective_tissue_disease,
    IFNULL(com.has_rheumatoid_arthritis, 0) AS has_rheumatoid_arthritis,
    IFNULL(so2_flag.has_supplemental_oxygen, 0) AS has_supplemental_oxygen,
    IFNULL(com.has_dnr_status, 0) AS has_dnr_status,
    IFNULL(com.has_lung_transplant, 0) AS has_lung_transplant,
    IFNULL(niv_flag.had_niv, 0) AS had_niv,
    IFNULL(imv_flag.had_imv, 0) AS had_imv,
    IFNULL(rrt_flag.had_rrt, 0) AS had_rrt,
    bf.* EXCEPT (subject_id, hadm_id, stay_id),
    meds.* EXCEPT(hadm_id),

    so.icu_los_days,
    so.hospital_los_days,
    so.in_hospital_mortality_flag,
    so.in_hospital_mortality_duration,
    so.overall_mortality_flag,
    so.overall_mortality_duration,
    CASE WHEN so.in_hospital_mortality_flag = 1 AND DATETIME_DIFF(so.death_datetime, icu.intime, DAY) <= 28 THEN 1 ELSE 0 END AS icu_mortality_28d,
    CASE WHEN so.in_hospital_mortality_flag = 1 AND DATETIME_DIFF(so.death_datetime, adm.admittime, DAY) < 29 THEN 1 ELSE 0 END AS hospital_mortality_28d,
    CASE WHEN so.death_datetime IS NOT NULL AND DATETIME_DIFF(so.death_datetime, adm.admittime, DAY) <= 91 THEN 1 ELSE 0 END AS mortality_3_mo,
    CASE WHEN so.death_datetime IS NOT NULL AND DATETIME_DIFF(so.death_datetime, adm.admittime, DAY) <= 182 THEN 1 ELSE 0 END AS mortality_6_mo,
    CASE WHEN so.death_datetime IS NOT NULL AND DATETIME_DIFF(so.death_datetime, adm.admittime, DAY) <= 273 THEN 1 ELSE 0 END AS mortality_9_mo,
    CASE WHEN so.death_datetime IS NOT NULL AND DATETIME_DIFF(so.death_datetime, adm.admittime, DAY) <= 365 THEN 1 ELSE 0 END AS mortality_12_mo,
    IFNULL(hosp_readmit.readmission_30_day, 0) as hospital_readmission_30_day,
    IFNULL(icu_readmit.icu_readmission_30_day, 0) AS icu_readmission_30_day,

    peo.duration_mask_ventilation,
    peo.duration_invasive_vent,
    IFNULL(peo.extubation_flag, 0) as extubation_flag,
    nivf.duration_NIVF,
    IFNULL(oflags.weaning_deferred_flag, 0) as weaning_deferred_flag,
    IFNULL(oflags.ecmo_flag, 0) as ecmo_flag,
    IFNULL(oflags.lung_transplant_current, 0) as lung_transplant_current,

    IFNULL(w_outcome.weaning_failure, 0) as weaning_failure,
    IFNULL(w_outcome.weaning_success, 0) as weaning_success,
    IFNULL(w_outcome.weaning_indeterminate, 0) as weaning_indeterminate,
    IFNULL(w_outcome.weaning_outcome_status, 0) as weaning_outcome_status,

    nfc.niv_failure,
    nfc.niv_failure_no_trachmask,
    aps.apsiii,
    1 / (1 + EXP(- (-4.4360 + 0.04726 * (aps.apsiii)))) AS apsiii_prob,

    hacor.sbt_starttime,
    hacor.hr_t1, hacor.hacor_score_hr_t1,
    hacor.ph_t1, hacor.hacor_score_ph_t1,
    hacor.gcs_total_t1, hacor.hacor_score_gcs_t1,
    hacor.pf_ratio_calc_t1, hacor.hacor_score_pf_t1,
    hacor.resp_rate_t1, hacor.hacor_score_rr_t1,
    (hacor.hacor_score_hr_t1 + hacor.hacor_score_ph_t1 + hacor.hacor_score_gcs_t1 + hacor.hacor_score_pf_t1 + hacor.hacor_score_rr_t1) AS total_hacor_score_t1,
    hacor.hr_t3, hacor.hacor_score_hr_t3,
    hacor.ph_t3, hacor.hacor_score_ph_t3,
    hacor.gcs_total_t3, hacor.hacor_score_gcs_t3,
    hacor.pf_ratio_calc_t3, hacor.hacor_score_pf_t3,
    hacor.resp_rate_t3, hacor.hacor_score_rr_t3,
    (hacor.hacor_score_hr_t3 + hacor.hacor_score_ph_t3 + hacor.hacor_score_gcs_t3 + hacor.hacor_score_pf_t3 + hacor.hacor_score_rr_t3) AS total_hacor_score_t3,

    niv_hacor.niv_starttime,
    niv_hacor.niv_hr_t0_t6,
    niv_hacor.niv_ph_t0_t6,
    niv_hacor.niv_gcs_t0_t6,
    niv_hacor.niv_pf_ratio_t0_t6,
    niv_hacor.niv_rr_t0_t6,
    niv_hacor.niv_hacor_hr_t0_t6,
    niv_hacor.niv_hacor_ph_t0_t6,
    niv_hacor.niv_hacor_gcs_t0_t6,
    niv_hacor.niv_hacor_pf_t0_t6,
    niv_hacor.niv_hacor_rr_t0_t6,
    (niv_hacor.niv_hacor_hr_t0_t6 + niv_hacor.niv_hacor_ph_t0_t6 + niv_hacor.niv_hacor_gcs_t0_t6 + niv_hacor.niv_hacor_pf_t0_t6 + niv_hacor.niv_hacor_rr_t0_t6) AS total_niv_hacor_score_t0_t6,
    niv_hacor.niv_hr_t6_t12,
    niv_hacor.niv_ph_t6_t12,
    niv_hacor.niv_gcs_t6_t12,
    niv_hacor.niv_pf_ratio_t6_t12,
    niv_hacor.niv_rr_t6_t12,
    niv_hacor.niv_hacor_hr_t6_t12,
    niv_hacor.niv_hacor_ph_t6_t12,
    niv_hacor.niv_hacor_gcs_t6_t12,
    niv_hacor.niv_hacor_pf_t6_t12,
    niv_hacor.niv_hacor_rr_t6_t12,
    (niv_hacor.niv_hacor_hr_t6_t12 + niv_hacor.niv_hacor_ph_t6_t12 + niv_hacor.niv_hacor_gcs_t6_t12 + niv_hacor.niv_hacor_pf_t6_t12 + niv_hacor.niv_hacor_rr_t6_t12) AS total_niv_hacor_score_t6_t12,
    niv_hacor.niv_hr_t12_t24,
    niv_hacor.niv_ph_t12_t24,
    niv_hacor.niv_gcs_t12_t24,
    niv_hacor.niv_pf_ratio_t12_t24,
    niv_hacor.niv_rr_t12_t24,
    niv_hacor.niv_hacor_hr_t12_t24,
    niv_hacor.niv_hacor_ph_t12_t24,
    niv_hacor.niv_hacor_gcs_t12_t24,
    niv_hacor.niv_hacor_pf_t12_t24,
    niv_hacor.niv_hacor_rr_t12_t24,
    (niv_hacor.niv_hacor_hr_t12_t24 + niv_hacor.niv_hacor_ph_t12_t24 + niv_hacor.niv_hacor_gcs_t12_t24 + niv_hacor.niv_hacor_pf_t12_t24 + niv_hacor.niv_hacor_rr_t12_t24) AS total_niv_hacor_score_t12_t24,
    niv_hacor.intubated_within_6h_of_niv,
    niv_hacor.intubated_within_12h_of_niv,

    sofa.respiration_sofa + sofa.coagulation_sofa + sofa.liver_sofa + sofa.cardiovascular_sofa + sofa.cns_sofa + sofa.renal_sofa AS first_day_sofa_score,
    sofa.respiration_sofa,
    sofa.coagulation_sofa,
    sofa.liver_sofa,
    sofa.cardiovascular_sofa,
    sofa.cns_sofa,
    sofa.renal_sofa,
    COALESCE(sirs.temp_score, 0) + COALESCE(sirs.heart_rate_score, 0) + COALESCE(sirs.resp_score, 0) + COALESCE(sirs.wbc_score, 0) AS first_day_sirs_score,
    sirs.temp_score AS sirs_temp_score,
    sirs.heart_rate_score AS sirs_hr_score,
    sirs.resp_score AS sirs_resp_score,
    sirs.wbc_score AS sirs_wbc_score,
    sapsii.sapsii,
    1 / (1 + EXP(- (-7.7631 + 0.0737 * (sapsii.sapsii) + 0.9971 * (LN(sapsii.sapsii + 1))))) AS sapsii_prob,
    sapsii.age_score, sapsii.hr_score, sapsii.sysbp_score, sapsii.temp_score, sapsii.pao2fio2_score, sapsii.uo_score,
    sapsii.bun_score, sapsii.wbc_score, sapsii.potassium_score, sapsii.sodium_score, sapsii.bicarbonate_score,
    sapsii.bilirubin_score, sapsii.gcs_score, sapsii.comorbidity_score, sapsii.admissiontype_score,
    oasis.oasis,
    1 / (1 + EXP(- (-6.1746 + 0.1275 * (oasis.oasis)))) AS oasis_prob,
    COALESCE(lods.neurologic, 0) + COALESCE(lods.cardiovascular, 0) + COALESCE(lods.renal, 0) + COALESCE(lods.pulmonary, 0) + COALESCE(lods.hematologic, 0) + COALESCE(lods.hepatic, 0) AS first_day_lods_score,
    lods.neurologic AS lods_neurologic,
    lods.cardiovascular AS lods_cardiovascular,
    lods.renal AS lods_renal,
    lods.pulmonary AS lods_pulmonary,
    lods.hematologic AS lods_hematologic,
    lods.hepatic AS lods_hepatic,
    rox.rox_index_niv_t0_t6,
    rox.rox_index_niv_t6_t12,
    rox.rox_index_niv_t12_t24,
    rox.rox_index_sbt_t1,
    rox.rox_index_sbt_t3,

    COALESCE(
        CASE
            WHEN fvt.first_vent_type = 'IMV' THEN 'IMV First'
            WHEN fvt.first_vent_type = 'NIV' THEN 'NIV First'
        END,
        'No Ventilation'
    ) AS ventilation_status,

    cci.charlson_comorbidity_index,
    cci.charlson_myocardial_infarct,
    cci.charlson_congestive_heart_failure,
    cci.charlson_peripheral_vascular_disease,
    cci.charlson_cerebrovascular_disease,
    cci.charlson_dementia,
    cci.charlson_chronic_pulmonary_disease,
    cci.charlson_rheumatic_disease,
    cci.charlson_peptic_ulcer_disease,
    cci.charlson_mild_liver_disease,
    cci.charlson_diabetes_without_cc,
    cci.charlson_diabetes_with_cc,
    cci.charlson_paraplegia,
    cci.charlson_renal_disease,
    cci.charlson_malignant_cancer,
    cci.charlson_severe_liver_disease,
    cci.charlson_metastatic_solid_tumor,
    cci.charlson_aids

FROM Core_Cohort AS cohort
LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` AS adm ON cohort.hadm_id = adm.hadm_id
LEFT JOIN `physionet-data.mimiciv_3_1_hosp.patients` AS pat ON cohort.subject_id = pat.subject_id
LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` AS icu ON cohort.stay_id = icu.stay_id
LEFT JOIN ARF_Inclusion_Codes AS arf_codes ON cohort.hadm_id = arf_codes.hadm_id
LEFT JOIN Pregnancy_Codes AS preg ON cohort.hadm_id = preg.hadm_id
LEFT JOIN Congenital_Codes AS cong ON cohort.hadm_id = cong.hadm_id
LEFT JOIN Hematolymphoid_Malignancy_Codes AS hem_mal ON cohort.hadm_id = hem_mal.hadm_id
LEFT JOIN Malignancy_Codes AS mal ON cohort.hadm_id = mal.hadm_id
LEFT JOIN AIDS_Codes AS aids ON cohort.hadm_id = aids.hadm_id
LEFT JOIN NIV_Flag AS niv_flag ON cohort.hadm_id = niv_flag.hadm_id
LEFT JOIN IMV_Flag AS imv_flag ON cohort.hadm_id = imv_flag.hadm_id
LEFT JOIN DNI_DNR_From_Events AS dnr_events ON cohort.stay_id = dnr_events.stay_id
LEFT JOIN DNI_DNR_From_ICD AS dnr_icd ON cohort.hadm_id = dnr_icd.hadm_id
LEFT JOIN Comorbidities_Flags AS com ON cohort.hadm_id = com.hadm_id
LEFT JOIN Baseline_Features AS bf ON cohort.stay_id = bf.stay_id
LEFT JOIN Comprehensive_Outcomes so ON cohort.stay_id = so.stay_id
LEFT JOIN Proc_Events_Outcomes peo ON cohort.stay_id = peo.stay_id
LEFT JOIN Other_Flags oflags ON cohort.hadm_id = oflags.hadm_id
LEFT JOIN ICU_Readmission_Flag AS icu_readmit ON cohort.stay_id = icu_readmit.stay_id
LEFT JOIN Hospital_Readmission hosp_readmit ON cohort.hadm_id = hosp_readmit.hadm_id
LEFT JOIN Weaning_Status_Final w_outcome ON cohort.stay_id = w_outcome.stay_id
LEFT JOIN NIV_Failure_Calculation nfc ON cohort.stay_id = nfc.stay_id
LEFT JOIN apsiii_scores aps ON cohort.stay_id = aps.stay_id
LEFT JOIN hacor_scores hacor ON cohort.stay_id = hacor.stay_id
LEFT JOIN niv_hacor_scores niv_hacor ON cohort.stay_id = niv_hacor.stay_id
LEFT JOIN final_sofa_scores sofa ON cohort.stay_id = sofa.stay_id
LEFT JOIN sirs_scorecalc sirs ON cohort.stay_id = sirs.stay_id
LEFT JOIN sapsii_scores sapsii ON cohort.stay_id = sapsii.stay_id
LEFT JOIN final_oasis_scores oasis ON cohort.stay_id = oasis.stay_id
LEFT JOIN final_lods_scores lods ON cohort.stay_id = lods.stay_id
LEFT JOIN final_rox_scores rox ON cohort.stay_id = rox.stay_id
LEFT JOIN First_Vent_Type AS fvt ON cohort.stay_id = fvt.stay_id
LEFT JOIN Supplemental_Oxygen_Flag AS so2_flag ON cohort.hadm_id = so2_flag.hadm_id
LEFT JOIN RRT_Flag AS rrt_flag ON cohort.hadm_id = rrt_flag.hadm_id
LEFT JOIN NIVF_Total_Duration AS nivf ON cohort.stay_id = nivf.stay_id
LEFT JOIN Charlson_Comorbidity_Index AS cci ON cohort.hadm_id = cci.hadm_id
LEFT JOIN Medication_Flags AS meds ON cohort.hadm_id = meds.hadm_id
ORDER BY
   cohort.subject_id
);