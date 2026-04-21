CREATE OR REPLACE TABLE `my-mimic-research.my_results.ami_cohort_with_all_features` AS
(
WITH
Base_Cohort AS (
   SELECT
       subject_id,
       hadm_id,
       admittime,
       stay_id
   FROM (
       SELECT
           icu.subject_id,
           adm.hadm_id,
           adm.admittime,
           icu.stay_id,
           ROW_NUMBER() OVER (PARTITION BY adm.subject_id ORDER BY icu.intime ASC) as icu_stay_rank
       FROM `physionet-data.mimiciv_3_1_icu.icustays` AS icu
       INNER JOIN `physionet-data.mimiciv_3_1_hosp.admissions` AS adm ON icu.hadm_id = adm.hadm_id
       INNER JOIN `physionet-data.mimiciv_3_1_hosp.patients` AS pat ON adm.subject_id = pat.subject_id
       WHERE
           (EXTRACT(YEAR FROM adm.admittime) - pat.anchor_year + pat.anchor_age) >= 18
           AND EXISTS (
               SELECT 1 FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` d
               WHERE d.hadm_id = adm.hadm_id
                 AND (
                     (d.icd_version = 9 AND (d.icd_code LIKE '410%' OR d.icd_code = '4111'))
                     OR (d.icd_version = 10 AND (d.icd_code LIKE 'I21%' OR d.icd_code = 'I20.0' OR d.icd_code = 'I249'))
                 )
           )
   ) AS ranked_stays
   WHERE icu_stay_rank = 1
),

Patient_Prior_History AS (
    WITH all_diagnoses AS (
        SELECT
            p.subject_id,
            d.icd_code,
            d.icd_version,
            a.admittime
        FROM `physionet-data.mimiciv_3_1_hosp.patients` p
        JOIN `physionet-data.mimiciv_3_1_hosp.admissions` a ON p.subject_id = a.subject_id
        JOIN `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` d ON a.hadm_id = d.hadm_id
        WHERE p.subject_id IN (SELECT subject_id FROM Base_Cohort)
    )
    SELECT
        bc.subject_id,
        MAX(CASE WHEN ad.admittime < bc.admittime AND ad.icd_code IN ('Z9861', 'Z955', 'V4582') THEN 1 ELSE 0 END) AS prior_pci,
        MAX(CASE WHEN ad.admittime < bc.admittime AND ad.icd_code IN ('Z951', 'V4581') THEN 1 ELSE 0 END) AS prior_cabg,
        MAX(CASE WHEN ad.admittime < bc.admittime AND ((ad.icd_version = 9 AND (ad.icd_code LIKE '433%' OR ad.icd_code LIKE '434%' OR ad.icd_code LIKE '435%')) OR (ad.icd_version = 10 AND (ad.icd_code LIKE 'I63%' OR ad.icd_code LIKE 'I65%' OR ad.icd_code LIKE 'I66%'))) THEN 1 ELSE 0 END) AS prior_stroke
    FROM Base_Cohort bc
    LEFT JOIN all_diagnoses ad ON bc.subject_id = ad.subject_id
    GROUP BY bc.subject_id, bc.admittime
),
CICU_Info AS (
  SELECT stay_id, first_careunit AS careunit
  FROM `physionet-data.mimiciv_3_1_icu.icustays`
  WHERE first_careunit IN ('Coronary Care Unit (CCU)', 'Cardiac Vascular Intensive Care Unit (CVICU)')
    AND stay_id IN (SELECT stay_id FROM Base_Cohort)
),
ACS_Codes AS (
  SELECT hadm_id, STRING_AGG(icd_code, ', ') AS acs_icd_codes
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    AND ((icd_version = 9 AND (icd_code LIKE '410%' OR icd_code = '4111')) OR (icd_version = 10 AND (icd_code LIKE 'I21%' OR icd_code = 'I20.0' OR icd_code = 'I249')))
  GROUP BY hadm_id
),
Pregnancy_Codes AS (
    SELECT hadm_id, STRING_AGG(icd_code, ', ') AS pregnancy_icd_codes
    FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
    WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
      AND ((icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^Z3[3469]')) OR (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^V2[234]')))
    GROUP BY hadm_id
),
Congenital_Codes AS (
    SELECT hadm_id, STRING_AGG(icd_code, ', ') AS congenital_anomaly_icd_codes
    FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
    WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
      AND ((icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(74|75)')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^Q')))
    GROUP BY hadm_id
),
Hematolymphoid_Malignancy_Codes AS (
    SELECT
        hadm_id,
        STRING_AGG(icd_code, ', ' ORDER BY icd_code) AS hematolymphoid_icd_codes
    FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
    WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
      AND (
        (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(20[0-8]|1985|2384|2387[2-6])'))
        OR
        (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^(C8[1-68]|C9[0-5]|D4[567]|C7952)'))
      )
    GROUP BY hadm_id
),
Malignancy_Codes AS (
    SELECT hadm_id, STRING_AGG(icd_code, ', ') AS malignancy_icd_codes
    FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
    WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
      AND ((icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(1[4-9]|20)')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C')))
    GROUP BY hadm_id
),
AIDS_Codes AS (
  SELECT hadm_id, STRING_AGG(icd_code, ', ') AS aids_icd_codes
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    AND (
         icd_code IN ('B20', 'V08', '042')
         OR (icd_version = 10 AND icd_code LIKE 'O987%')
    )
  GROUP BY hadm_id
),
Confounder_Flags AS (
    SELECT hadm_id,
        MAX(CASE WHEN (icd_version = 10 AND icd_code LIKE 'E10%') OR (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^250\d[13]')) THEN 1 ELSE 0 END) AS has_t1d,
        MAX(CASE WHEN (icd_version = 10 AND icd_code LIKE 'E11%') OR (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^250\d[02]')) THEN 1 ELSE 0 END) AS has_t2d,
        MAX(CASE WHEN icd_code IN ('4010', '4011', '4019') OR (icd_version = 10 AND icd_code LIKE 'I10%') THEN 1 ELSE 0 END) AS has_hypertension_outcome,
        MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '272%') OR (icd_version = 10 AND icd_code LIKE 'E78%') THEN 1 ELSE 0 END) AS has_dyslipidemia,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^41[0-4]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I2[0-5]')) THEN 1 ELSE 0 END) AS has_ischemic_heart_disease,
        MAX(CASE WHEN (icd_version = 10 AND icd_code IN ('I210', 'I211', 'I212', 'I213')) OR (icd_version = 9 AND icd_code LIKE '410%' AND icd_code != '4107') THEN 1 ELSE 0 END) AS has_ischemic_heart_disease_STEMI,
        MAX(CASE WHEN (icd_version = 10 AND icd_code = 'I214') OR (icd_version = 9 AND icd_code = '4107') THEN 1 ELSE 0 END) AS has_ischemic_heart_disease_NSTEMI,
        MAX(CASE WHEN (icd_version = 10 AND icd_code = 'I200') OR (icd_version = 9 AND icd_code = '4111') THEN 1 ELSE 0 END) AS has_ischemic_heart_disease_UA,
        MAX(CASE WHEN (icd_version = 10 AND icd_code IN ('I219', 'I21A1', 'I21A9', 'I21B')) THEN 1 ELSE 0 END) AS has_ischemic_heart_disease_USp,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '413%' OR icd_code = '4111')) OR (icd_version = 10 AND icd_code LIKE 'I20%') THEN 1 ELSE 0 END) AS has_angina,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '4140%' OR icd_code = '41181')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I21[0-3B]|^I240|^I251|^I257|^I258[1-4]|^I2481')) THEN 1 ELSE 0 END) AS has_coronary_artery_disease,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '410%' OR icd_code LIKE '412%')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I2[123]|^I25')) THEN 1 ELSE 0 END) AS has_myocardial_infarction,
        MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '428%') OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I50|^I110|^I13|^I9713|^I0981')) THEN 1 ELSE 0 END) AS has_heart_failure,
        MAX(CASE WHEN (icd_version = 9 AND icd_code = '4275') OR (icd_version = 10 AND (icd_code LIKE 'I46%' OR icd_code IN ('I97710', 'I9712', 'I9771'))) THEN 1 ELSE 0 END) AS has_cardiac_arrest,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '7855%' OR REGEXP_CONTAINS(icd_code, r'^9980[0-29]'))) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^R57|^T78[02]|^T886|^T805|^T811')) THEN 1 ELSE 0 END) AS has_shock,
        MAX(CASE WHEN icd_code IN ('78551', 'R570') THEN 1 ELSE 0 END) AS has_cardiogenic_shock,
        MAX(CASE WHEN (icd_version = 9 AND icd_code = '42731') OR (icd_version = 10 AND icd_code IN ('I480', 'I481', 'I482', 'I4891')) THEN 1 ELSE 0 END) AS has_atrial_fibrillation,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^74[56]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^Q2[0-4]')) THEN 1 ELSE 0 END) AS has_congenital_heart_defects,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '421%' OR icd_code IN ('42490', '42491', '07422', '09884', '11504', '11514', '11594', '03642', '11281'))) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I33|^I38|^B376|^A5483|^M3211|^A3282|^A3951|^M0531|^A5203|^A1884|^A0102')) THEN 1 ELSE 0 END) AS has_infective_endocarditis,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^43[0-8]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I6')) THEN 1 ELSE 0 END) AS has_cerebrovascular_disease,
        MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('45340', '45342', '45350', '45352')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^(I8240|I8243|I8244|I8245|I8246|I8249|I824Z|I8250|I8253|I8254|I8255|I8256|I8259|I825Z)')) THEN 1 ELSE 0 END) AS has_dvt,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^48[0-8]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^J1[2-8]|^J95851')) THEN 1 ELSE 0 END) AS has_pneumonia,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^48[12]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^J1[3-5]')) THEN 1 ELSE 0 END) AS has_bacterial_pneumonia,
        MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '480%') OR (icd_version = 10 AND icd_code LIKE 'J12%') THEN 1 ELSE 0 END) AS has_viral_pneumonia,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '491%' OR icd_code LIKE '492%' OR icd_code LIKE '496%')) OR (icd_version = 10 AND (icd_code LIKE 'J41%' OR icd_code LIKE 'J42%' OR icd_code LIKE 'J43%' OR icd_code LIKE 'J44%')) THEN 1 ELSE 0 END) AS has_copd,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code = 'V1255' OR icd_code LIKE '4151%' OR icd_code LIKE '4162%')) OR (icd_version = 10 AND (icd_code LIKE 'I26%' OR icd_code LIKE 'I2782%' OR icd_code IN ('T790', 'T791', 'T800', 'T817', 'T828'))) THEN 1 ELSE 0 END) AS has_pulmonary_embolism,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '512%' OR icd_code IN ('0117', '860', 'A15'))) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^J93|^J86|^J95811|^S270')) THEN 1 ELSE 0 END) AS has_pneumothorax,
        MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '4160%') OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I270|^I272')) THEN 1 ELSE 0 END) AS has_pulmonary_hypertension,
        MAX(CASE WHEN icd_code = 'J80' THEN 1 ELSE 0 END) AS has_ards,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'5185[13]|5188[1-4]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^J9582[12]|^J96')) THEN 1 ELSE 0 END) AS has_respiratory_failure,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '038%' OR icd_code IN ('0202', '0223'))) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^A4[01]|^O85|^A427|^B377|^A267|^A282|^A5486|^A327|^A39[2-4]|^A207|^A217|^A483|^A227')) THEN 1 ELSE 0 END) AS has_sepsis,
        MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('5715', '5712', '5716')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^K702|^K703|^K704|^K721|^K74')) THEN 1 ELSE 0 END) AS has_cirrhosis,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '070%' OR icd_code IN ('5731', '5732', '5733'))) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^B1[5-9]|^K701|^K73|^K754|^K7581')) THEN 1 ELSE 0 END) AS has_hepatitis,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^53[1-4]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^K2[5-8]')) THEN 1 ELSE 0 END) AS has_peptic_ulcer_disease,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'585[1-69]|28521|40[34]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^E1[013]2|^I1[23]|^I15[01]|^N1[89]|^N1[12456]|^N0[0-8]|^M321[45]|^M350[4A]')) THEN 1 ELSE 0 END) AS has_ckd,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '584%' OR icd_code = '586')) OR (icd_version = 10 AND (icd_code LIKE 'N17%' OR icd_code = 'N19')) THEN 1 ELSE 0 END) AS has_aki,
        MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '342%') OR (icd_version = 10 AND icd_code LIKE 'G81%') THEN 1 ELSE 0 END) AS has_hemiplegia,
        MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '290%') OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^F0[1-4]')) THEN 1 ELSE 0 END) AS has_dementia,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^20[4-8]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C9[1-5]')) THEN 1 ELSE 0 END) AS has_leukemia,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^20[0-2]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C8[1-68]')) THEN 1 ELSE 0 END) AS has_lymphoma,
        MAX(CASE WHEN icd_code IN ('B20', 'V08', '042') OR (icd_version = 10 AND icd_code LIKE 'O987%') THEN 1 ELSE 0 END) AS has_aids,
        MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '710%') OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^M3[0-6]')) THEN 1 ELSE 0 END) AS has_connective_tissue_disease,
        MAX(CASE WHEN (icd_version = 10 AND (icd_code LIKE 'M05%' OR icd_code LIKE 'M06%')) OR (icd_version = 9 AND icd_code LIKE '714%') THEN 1 ELSE 0 END) AS has_rheumatoid_arthritis
    FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
    WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    GROUP BY hadm_id
),
DNR_Flag AS (
    SELECT hadm_id, 1 AS has_dnr_status
    FROM (
        SELECT hadm_id FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` WHERE icd_code IN ('V4986', 'Z66') AND hadm_id IN (SELECT hadm_id FROM Base_Cohort)
        UNION DISTINCT
        SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.chartevents` c JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
        WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort) AND c.itemid IN (223758, 228687) AND REGEXP_CONTAINS(c.value, 'DNI|DNR')
    )
    GROUP BY hadm_id
),
Procedure_Flags AS (
    SELECT
        hadm_id,
        MAX(CASE WHEN event_type = 'ANGIO' THEN 1 ELSE 0 END) as had_coronary_angiography,
        MAX(CASE WHEN event_type = 'PCI' THEN 1 ELSE 0 END) as had_pci,
        MAX(CASE WHEN event_type = 'CABG' THEN 1 ELSE 0 END) as had_cabg
    FROM (
        SELECT i.hadm_id, 'ANGIO' as event_type
        FROM `physionet-data.mimiciv_3_1_icu.procedureevents` pe
        JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON pe.stay_id = i.stay_id
        WHERE pe.itemid = 225427
        UNION ALL
        SELECT hadm_id, 'PCI' as event_type FROM `physionet-data.mimiciv_3_1_hosp.procedures_icd`
        WHERE (icd_version = 9 AND icd_code IN ('00.66', '36.06', '36.07')) OR (icd_version = 10 AND icd_code LIKE '027%')
        UNION ALL
        SELECT hadm_id, 'CABG' as event_type FROM `physionet-data.mimiciv_3_1_hosp.procedures_icd`
        WHERE (icd_version = 9 AND icd_code LIKE '36.1%') OR (icd_version = 10 AND icd_code LIKE '021%')
    ) AS proc_events
    WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    GROUP BY hadm_id
),
Medication_Flags AS (
    SELECT
        i.hadm_id,
        MAX(CASE WHEN ie.itemid = 221906 THEN 1 ELSE 0 END) AS had_norepinephrine,
        MAX(CASE WHEN ie.itemid = 221289 THEN 1 ELSE 0 END) AS had_epinephrine,
        MAX(CASE WHEN ie.itemid = 221662 THEN 1 ELSE 0 END) AS had_dopamine,
        MAX(CASE WHEN ie.itemid = 222315 THEN 1 ELSE 0 END) AS had_vasopressin,
        MAX(CASE WHEN ie.itemid = 221653 THEN 1 ELSE 0 END) AS had_dobutamine,
        MAX(CASE WHEN ie.itemid = 221749 THEN 1 ELSE 0 END) AS had_phenylephrine,
        MAX(CASE WHEN ie.itemid = 221986 THEN 1 ELSE 0 END) AS had_milrinone
    FROM `physionet-data.mimiciv_3_1_icu.inputevents` ie
    JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON ie.stay_id = i.stay_id
    WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
      AND ie.itemid IN (
          221906,
          221289,
          221662,
          222315,
          221653,
          221749,
          221986
      )
    GROUP BY i.hadm_id
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
      WHERE stay_id IN (SELECT stay_id FROM Base_Cohort)
    ),
    AllEvents AS (
        SELECT i.stay_id, le.charttime, le.itemid, le.valuenum, le.comments AS comment_val
        FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
        JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON le.hadm_id = i.hadm_id
        WHERE i.stay_id IN (SELECT stay_id FROM Base_Cohort)
        UNION ALL
        SELECT i.stay_id, ce.charttime, ce.itemid, ce.valuenum, ce.value AS comment_val
        FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
        JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON ce.stay_id = i.stay_id
        WHERE i.stay_id IN (SELECT stay_id FROM Base_Cohort)
        UNION ALL
        SELECT i.stay_id, oe.charttime, oe.itemid, oe.value as valuenum, CAST(NULL as STRING) as comment_val
        FROM `physionet-data.mimiciv_3_1_icu.outputevents` oe
        JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON oe.stay_id = i.stay_id
        WHERE i.stay_id IN (SELECT stay_id FROM Base_Cohort)
    ),
    RankedEvents AS (
        SELECT
            AllEvents.stay_id,
            valuenum,
            comment_val,
            CASE
                WHEN itemid IN (226707) THEN 'height_in' WHEN itemid IN (226730) THEN 'height_cm' WHEN itemid IN (226531) THEN 'weight_lbs' WHEN itemid IN (226512) THEN 'weight_kg'
                WHEN itemid IN (229770) THEN 'resting_pulse' WHEN itemid IN (220045) THEN 'heart_rate' WHEN itemid IN (220210, 224690) THEN 'resp_rate'
                WHEN itemid IN (220050, 220179, 225309) THEN 'sbp' WHEN itemid IN (220051, 220180, 225310) THEN 'dbp' WHEN itemid IN (220052, 220181, 225312) THEN 'mbp'
                WHEN itemid IN (223761) THEN 'temp_f' WHEN itemid IN (223762, 50825) THEN 'temp_c' WHEN itemid IN (226755, 227013) THEN 'gcs' WHEN itemid IN (223901) THEN 'gcs_motor' WHEN itemid IN (223900) THEN 'gcs_verbal' WHEN itemid IN (220739) THEN 'gcs_eye'
                WHEN itemid IN (50811, 51640, 51222) THEN 'hb' WHEN itemid IN (51279, 52170) THEN 'rbc' WHEN itemid IN (50810) THEN 'hematocrit_calc' WHEN itemid IN (52028, 51638, 51639, 51221) THEN 'hematocrit'
                WHEN itemid IN (51300, 51301, 51755, 220546) THEN 'wbc' WHEN itemid IN (51248) THEN 'mch' WHEN itemid IN (51249) THEN 'mchc' WHEN itemid IN (51691, 51250) THEN 'mcv'
                WHEN itemid IN (52172) THEN 'rdw_sd' WHEN itemid IN (51277) THEN 'rdw_cv' WHEN itemid IN (51704, 51265) THEN 'platelet_count' WHEN itemid IN (51240) THEN 'p_lcc'
                WHEN itemid IN (51256, 225643) THEN 'neutrophils_pct' WHEN itemid IN (52075) THEN 'abs_neutrophil_lab' WHEN itemid IN (229355) THEN 'abs_neutrophil_chart'
                WHEN itemid IN (51244, 51245, 51690, 225641) THEN 'lymphocytes_pct' WHEN itemid IN (52769, 51133, 53132) THEN 'abs_lymphocyte_lab' WHEN itemid IN (229358) THEN 'abs_lymphocyte_chart'
                WHEN itemid IN (51254, 225642) THEN 'monocyte_pct' WHEN itemid IN (51253, 52074) THEN 'abs_monocyte_lab' WHEN itemid IN (229359) THEN 'abs_monocyte_chart'
                WHEN itemid IN (51146, 225639) THEN 'basophil_pct' WHEN itemid IN (229361) THEN 'abs_basophil_chart' WHEN itemid IN (52069) THEN 'abs_basophil_lab'
                WHEN itemid IN (51200, 225640) THEN 'eosinophil_pct' WHEN itemid IN (51199) THEN 'eosinophil_count' WHEN itemid IN (52073) THEN 'abs_eosinophil_lab' WHEN itemid IN (229360) THEN 'abs_eosinophil_chart'
                WHEN itemid IN (50889) THEN 'crp_lab' WHEN itemid IN (227444) THEN 'crp_chart' WHEN itemid IN (51288) THEN 'esr'
                WHEN itemid IN (52921, 51274) THEN 'pt' WHEN itemid IN (51275, 52923) THEN 'aptt' WHEN itemid IN (51675, 51237) THEN 'inr'
                WHEN itemid IN (50931, 50809, 52569, 52027) THEN 'glucose' WHEN itemid IN (50854) THEN 'abs_a1c' WHEN itemid IN (51631) THEN 'glycated_hb' WHEN itemid IN (50852) THEN 'hba1c_pct'
                WHEN itemid IN (50907, 50906) THEN 'total_cholesterol' WHEN itemid IN (50905, 50906) THEN 'ldl' WHEN itemid IN (50904) THEN 'hdl' WHEN itemid IN (51000) THEN 'triglycerides'
                WHEN itemid IN (50885, 53089) THEN 'bilirubin_total' WHEN itemid IN (50883, 51592) THEN 'bilirubin_direct' WHEN itemid IN (50884, 51751) THEN 'bilirubin_indirect'
                WHEN itemid IN (50878) THEN 'ast' WHEN itemid IN (50861) THEN 'alt' WHEN itemid IN (53086, 50863) THEN 'alp' WHEN itemid IN (50927, 53093) THEN 'ggt'
                WHEN itemid IN (50912, 52546) THEN 'creatinine' WHEN itemid IN (52024) THEN 'creatinine_wb' WHEN itemid IN (51842, 52647, 51006) THEN 'bun' WHEN itemid IN (50920, 52026, 51770) THEN 'egfr'
                WHEN itemid IN (50983, 52623) THEN 'sodium' WHEN itemid IN (50824, 52455) THEN 'sodium_wb' WHEN itemid IN (50971, 50833, 52610) THEN 'potassium' WHEN itemid IN (50822, 52452) THEN 'potassium_wb'
                WHEN itemid IN (50902, 52535) THEN 'chloride' WHEN itemid IN (50806, 52434) THEN 'chloride_wb' WHEN itemid IN (50882) THEN 'bicarbonate' WHEN itemid IN (50803) THEN 'bicarb_calc_wb' WHEN itemid IN (52039) THEN 'bicarb_calc'
                WHEN itemid IN (50808, 51624) THEN 'calcium_free' WHEN itemid IN (50893, 52035, 52034) THEN 'calcium_total' WHEN itemid IN (50970) THEN 'phosphate'
                WHEN itemid IN (50862, 53085, 52022, 53138) THEN 'albumin'
                WHEN itemid IN (50821, 220224) THEN 'pao2' WHEN itemid IN (220277) THEN 'spo2' WHEN itemid IN (229407, 229393, 229405) THEN 'pf_ratio' WHEN itemid IN (223835) THEN 'fio2'
                WHEN itemid IN (50818) THEN 'paco2' WHEN itemid IN (50820, 223830) THEN 'ph_arterial'
                WHEN itemid IN (227519) THEN 'urine_output' WHEN itemid IN (51994, 51498) THEN 'urine_spec_gravity' WHEN itemid IN (52045) THEN 'urine_ph' WHEN itemid IN (52044, 51093) THEN 'urine_osmolality'
                WHEN itemid IN (51102) THEN 'urine_protein' WHEN itemid IN (51069, 52703) THEN 'urine_albumin' WHEN itemid IN (51084, 51981, 51478) THEN 'urine_glucose'
                WHEN itemid IN (51106, 52000, 51082) THEN 'urine_creatinine' WHEN itemid IN (51984, 51484) THEN 'urine_ketone'
                WHEN itemid = 224700 THEN 'peep_total' WHEN itemid = 220339 THEN 'peep_set'
                WHEN itemid IN (227580, 227581, 227579, 227578, 227577, 227582) THEN 'bipap_ipap'
                WHEN itemid = 227579 THEN 'bipap_epap' WHEN itemid = 227583 THEN 'cpap'
                WHEN itemid IN (52642, 51002) THEN 'trop_i' WHEN itemid IN (51003) THEN 'trop_t' WHEN itemid IN (51580) THEN 'ckmb' WHEN itemid IN (50954) THEN 'ldh' WHEN itemid IN (50963) THEN 'nt_pro_bnp'
                WHEN itemid IN (227008) THEN 'lvef'
                WHEN itemid = 226732 THEN 'o2_device'
            END as concept,
            ROW_NUMBER() OVER(PARTITION BY AllEvents.stay_id,
                CASE
                    WHEN itemid IN (226707) THEN 'height_in' WHEN itemid IN (226730) THEN 'height_cm' WHEN itemid IN (226531) THEN 'weight_lbs' WHEN itemid IN (226512) THEN 'weight_kg'
                    WHEN itemid IN (229770) THEN 'resting_pulse' WHEN itemid IN (220045) THEN 'heart_rate' WHEN itemid IN (220210, 224690) THEN 'resp_rate'
                    WHEN itemid IN (220050, 220179, 225309) THEN 'sbp' WHEN itemid IN (220051, 220180, 225310) THEN 'dbp' WHEN itemid IN (220052, 220181, 225312) THEN 'mbp'
                    WHEN itemid IN (223761) THEN 'temp_f' WHEN itemid IN (223762, 50825) THEN 'temp_c' WHEN itemid IN (226755, 227013) THEN 'gcs' WHEN itemid IN (223901) THEN 'gcs_motor' WHEN itemid IN (223900) THEN 'gcs_verbal' WHEN itemid IN (220739) THEN 'gcs_eye'
                    WHEN itemid IN (50811, 51640, 51222) THEN 'hb' WHEN itemid IN (51279, 52170) THEN 'rbc' WHEN itemid IN (50810) THEN 'hematocrit_calc' WHEN itemid IN (52028, 51638, 51639, 51221) THEN 'hematocrit'
                    WHEN itemid IN (51300, 51301, 51755, 220546) THEN 'wbc' WHEN itemid IN (51248) THEN 'mch' WHEN itemid IN (51249) THEN 'mchc' WHEN itemid IN (51691, 51250) THEN 'mcv'
                    WHEN itemid IN (52172) THEN 'rdw_sd' WHEN itemid IN (51277) THEN 'rdw_cv' WHEN itemid IN (51704, 51265) THEN 'platelet_count' WHEN itemid IN (51240) THEN 'p_lcc'
                    WHEN itemid IN (51256, 225643) THEN 'neutrophils_pct' WHEN itemid IN (52075) THEN 'abs_neutrophil_lab' WHEN itemid IN (229355) THEN 'abs_neutrophil_chart'
                    WHEN itemid IN (51244, 51245, 51690, 225641) THEN 'lymphocytes_pct' WHEN itemid IN (52769, 51133, 53132) THEN 'abs_lymphocyte_lab' WHEN itemid IN (229358) THEN 'abs_lymphocyte_chart'
                    WHEN itemid IN (51254, 225642) THEN 'monocyte_pct' WHEN itemid IN (51253, 52074) THEN 'abs_monocyte_lab' WHEN itemid IN (229359) THEN 'abs_monocyte_chart'
                    WHEN itemid IN (51146, 225639) THEN 'basophil_pct' WHEN itemid IN (229361) THEN 'abs_basophil_chart' WHEN itemid IN (52069) THEN 'abs_basophil_lab'
                    WHEN itemid IN (51200, 225640) THEN 'eosinophil_pct' WHEN itemid IN (51199) THEN 'eosinophil_count' WHEN itemid IN (52073) THEN 'abs_eosinophil_lab' WHEN itemid IN (229360) THEN 'abs_eosinophil_chart'
                    WHEN itemid IN (50889) THEN 'crp_lab' WHEN itemid IN (227444) THEN 'crp_chart' WHEN itemid IN (51288) THEN 'esr'
                    WHEN itemid IN (52921, 51274) THEN 'pt' WHEN itemid IN (51275, 52923) THEN 'aptt' WHEN itemid IN (51675, 51237) THEN 'inr'
                    WHEN itemid IN (50931, 50809, 52569, 52027) THEN 'glucose' WHEN itemid IN (50854) THEN 'abs_a1c' WHEN itemid IN (51631) THEN 'glycated_hb' WHEN itemid IN (50852) THEN 'hba1c_pct'
                    WHEN itemid IN (50907, 50906) THEN 'total_cholesterol' WHEN itemid IN (50905, 50906) THEN 'ldl' WHEN itemid IN (50904) THEN 'hdl' WHEN itemid IN (51000) THEN 'triglycerides'
                    WHEN itemid IN (50885, 53089) THEN 'bilirubin_total' WHEN itemid IN (50883, 51592) THEN 'bilirubin_direct' WHEN itemid IN (50884, 51751) THEN 'bilirubin_indirect'
                    WHEN itemid IN (50878) THEN 'ast' WHEN itemid IN (50861) THEN 'alt' WHEN itemid IN (53086, 50863) THEN 'alp' WHEN itemid IN (50927, 53093) THEN 'ggt'
                    WHEN itemid IN (50912, 52546) THEN 'creatinine' WHEN itemid IN (52024) THEN 'creatinine_wb' WHEN itemid IN (51842, 52647, 51006) THEN 'bun' WHEN itemid IN (50920, 52026, 51770) THEN 'egfr'
                    WHEN itemid IN (50983, 52623) THEN 'sodium' WHEN itemid IN (50824, 52455) THEN 'sodium_wb' WHEN itemid IN (50971, 50833, 52610) THEN 'potassium' WHEN itemid IN (50822, 52452) THEN 'potassium_wb'
                    WHEN itemid IN (50902, 52535) THEN 'chloride' WHEN itemid IN (50806, 52434) THEN 'chloride_wb' WHEN itemid IN (50882) THEN 'bicarbonate' WHEN itemid IN (50803) THEN 'bicarb_calc_wb' WHEN itemid IN (52039) THEN 'bicarb_calc'
                    WHEN itemid IN (50808, 51624) THEN 'calcium_free' WHEN itemid IN (50893, 52035, 52034) THEN 'calcium_total' WHEN itemid IN (50970) THEN 'phosphate'
                    WHEN itemid IN (50862, 53085, 52022, 53138) THEN 'albumin'
                    WHEN itemid IN (50821, 220224) THEN 'pao2' WHEN itemid IN (220277) THEN 'spo2' WHEN itemid IN (229407, 229393, 229405) THEN 'pf_ratio' WHEN itemid IN (223835) THEN 'fio2'
                    WHEN itemid IN (50818) THEN 'paco2' WHEN itemid IN (50820, 223830) THEN 'ph_arterial'
                    WHEN itemid IN (227519) THEN 'urine_output' WHEN itemid IN (51994, 51498) THEN 'urine_spec_gravity' WHEN itemid IN (52045) THEN 'urine_ph' WHEN itemid IN (52044, 51093) THEN 'urine_osmolality'
                    WHEN itemid IN (51102) THEN 'urine_protein' WHEN itemid IN (51069, 52703) THEN 'urine_albumin' WHEN itemid IN (51084, 51981, 51478) THEN 'urine_glucose'
                    WHEN itemid IN (51106, 52000, 51082) THEN 'urine_creatinine' WHEN itemid IN (51984, 51484) THEN 'urine_ketone'
                    WHEN itemid = 224700 THEN 'peep_total' WHEN itemid = 220339 THEN 'peep_set'
                    WHEN itemid IN (227580, 227581, 227579, 227578, 227577, 227582) THEN 'bipap_ipap'
                    WHEN itemid = 227579 THEN 'bipap_epap' WHEN itemid = 227583 THEN 'cpap'
                    WHEN itemid IN (52642, 51002) THEN 'trop_i' WHEN itemid IN (51003) THEN 'trop_t' WHEN itemid IN (51580) THEN 'ckmb' WHEN itemid IN (50954) THEN 'ldh' WHEN itemid IN (50963) THEN 'nt_pro_bnp'
                    WHEN itemid IN (227008) THEN 'lvef'
                    WHEN itemid = 226732 THEN 'o2_device'
                END
            ORDER BY charttime ASC) as rn
        FROM AllEvents
        JOIN Time_Window tw ON AllEvents.stay_id = tw.stay_id
        WHERE AllEvents.charttime BETWEEN tw.window_start AND tw.window_end
    )
    SELECT stay_id,
        MAX(CASE WHEN concept = 'height_in' THEN valuenum END) AS first_height_in, MAX(CASE WHEN concept = 'height_cm' THEN valuenum END) AS first_height_cm, MAX(CASE WHEN concept = 'weight_lbs' THEN valuenum END) AS first_weight_lbs, MAX(CASE WHEN concept = 'weight_kg' THEN valuenum END) AS first_weight_kg,
        MAX(CASE WHEN concept = 'resting_pulse' THEN valuenum END) AS first_resting_pulse, MAX(CASE WHEN concept = 'heart_rate' THEN valuenum END) AS first_heart_rate, MAX(CASE WHEN concept = 'resp_rate' THEN valuenum END) AS first_resp_rate,
        MAX(CASE WHEN concept = 'sbp' THEN valuenum END) AS first_sbp, MAX(CASE WHEN concept = 'dbp' THEN valuenum END) AS first_dbp, MAX(CASE WHEN concept = 'mbp' THEN valuenum END) AS first_mbp,
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
        MAX(CASE WHEN concept = 'glucose' THEN valuenum END) AS first_glucose, MAX(CASE WHEN concept = 'abs_a1c' THEN valuenum END) AS first_abs_a1c, MAX(CASE WHEN concept = 'glycated_hb' THEN valuenum END) AS first_glycated_hb, MAX(CASE WHEN concept = 'hba1c_pct' THEN valuenum END) AS first_hba1c_pct,
        MAX(CASE WHEN concept = 'total_cholesterol' THEN valuenum END) AS first_total_cholesterol, MAX(CASE WHEN concept = 'ldl' THEN valuenum END) AS first_ldl, MAX(CASE WHEN concept = 'hdl' THEN valuenum END) AS first_hdl, MAX(CASE WHEN concept = 'triglycerides' THEN valuenum END) AS first_triglycerides,
        MAX(CASE WHEN concept = 'bilirubin_total' THEN valuenum END) AS first_bilirubin_total, MAX(CASE WHEN concept = 'bilirubin_direct' THEN valuenum END) AS first_bilirubin_direct, MAX(CASE WHEN concept = 'bilirubin_indirect' THEN valuenum END) AS first_bilirubin_indirect,
        MAX(CASE WHEN concept = 'ast' THEN valuenum END) AS first_ast, MAX(CASE WHEN concept = 'alt' THEN valuenum END) AS first_alt, MAX(CASE WHEN concept = 'alp' THEN valuenum END) AS first_alp, MAX(CASE WHEN concept = 'ggt' THEN valuenum END) AS first_ggt,
        MAX(CASE WHEN concept = 'creatinine' THEN valuenum END) AS first_creatinine, MAX(CASE WHEN concept = 'creatinine_wb' THEN valuenum END) AS first_creatinine_wb, MAX(CASE WHEN concept = 'bun' THEN valuenum END) AS first_bun, MAX(CASE WHEN concept = 'egfr' THEN comment_val END) AS first_egfr_comment,
        MAX(CASE WHEN concept = 'sodium' THEN valuenum END) AS first_sodium, MAX(CASE WHEN concept = 'sodium_wb' THEN valuenum END) AS first_sodium_wb, MAX(CASE WHEN concept = 'potassium' THEN valuenum END) AS first_potassium, MAX(CASE WHEN concept = 'potassium_wb' THEN valuenum END) AS first_potassium_wb,
        MAX(CASE WHEN concept = 'chloride' THEN valuenum END) AS first_chloride, MAX(CASE WHEN concept = 'chloride_wb' THEN valuenum END) AS first_chloride_wb, MAX(CASE WHEN concept = 'bicarbonate' THEN valuenum END) AS first_bicarbonate,
        MAX(CASE WHEN concept = 'bicarb_calc_wb' THEN valuenum END) AS first_bicarb_calc_wb, MAX(CASE WHEN concept = 'bicarb_calc' THEN valuenum END) AS first_bicarb_calc,
        MAX(CASE WHEN concept = 'calcium_free' THEN valuenum END) AS first_calcium_free, MAX(CASE WHEN concept = 'calcium_total' THEN valuenum END) AS first_calcium_total, MAX(CASE WHEN concept = 'phosphate' THEN valuenum END) AS first_phosphate,
        MAX(CASE WHEN concept = 'albumin' THEN valuenum END) AS first_albumin,
        MAX(CASE WHEN concept = 'pao2' THEN valuenum END) AS first_pao2, MAX(CASE WHEN concept = 'spo2' THEN valuenum END) AS first_spo2, MAX(CASE WHEN concept = 'pf_ratio' THEN valuenum END) AS first_pf_ratio, MAX(CASE WHEN concept = 'fio2' THEN valuenum END) AS first_fio2,
        MAX(CASE WHEN concept = 'paco2' THEN valuenum END) AS first_paco2, MAX(CASE WHEN concept = 'ph_arterial' THEN valuenum END) AS first_ph_arterial,
        MAX(CASE WHEN concept = 'urine_output' THEN valuenum END) AS first_urine_output, MAX(CASE WHEN concept = 'urine_spec_gravity' THEN valuenum END) AS first_urine_spec_gravity, MAX(CASE WHEN concept = 'urine_ph' THEN valuenum END) AS first_urine_ph,
        MAX(CASE WHEN concept = 'urine_osmolality' THEN valuenum END) AS first_urine_osmolality, MAX(CASE WHEN concept = 'urine_protein' THEN valuenum END) AS first_urine_protein, MAX(CASE WHEN concept = 'urine_albumin' THEN valuenum END) AS first_urine_albumin,
        MAX(CASE WHEN concept = 'urine_glucose' THEN valuenum END) AS first_urine_glucose, MAX(CASE WHEN concept = 'urine_creatinine' THEN valuenum END) AS first_urine_creatinine, MAX(CASE WHEN concept = 'urine_ketone' THEN valuenum END) AS first_urine_ketone,
        MAX(CASE WHEN concept = 'peep_total' THEN valuenum END) AS first_peep_total, MAX(CASE WHEN concept = 'peep_set' THEN valuenum END) AS first_peep_set,
        MAX(CASE WHEN concept = 'bipap_ipap' THEN valuenum END) AS first_bipap_ipap, MAX(CASE WHEN concept = 'bipap_epap' THEN valuenum END) AS first_bipap_epap, MAX(CASE WHEN concept = 'cpap' THEN valuenum END) AS first_cpap,
        MAX(CASE WHEN concept = 'trop_i' THEN valuenum END) AS first_trop_i, MAX(CASE WHEN concept = 'trop_t' THEN valuenum END) AS first_trop_t, MAX(CASE WHEN concept = 'ckmb' THEN valuenum END) AS first_ckmb,
        MAX(CASE WHEN concept = 'ldh' THEN valuenum END) AS first_ldh, MAX(CASE WHEN concept = 'nt_pro_bnp' THEN valuenum END) AS first_nt_pro_bnp,
        MAX(CASE WHEN concept = 'lvef' THEN valuenum END) AS first_lvef,
        MAX(CASE WHEN concept = 'o2_device' AND comment_val = 'High Flow Nasal Cannula' THEN 1 ELSE 0 END) AS hfnc_flag,
        MAX(CASE WHEN concept = 'o2_device' AND comment_val IN ('Non-rebreather', 'Face tent', 'Aerosol-cool', 'Venti mask', 'Medium conc mask', 'Ultrasonic neb', 'Vapomist', 'Oxymizer', 'High flow neb', 'Nasal cannula') THEN 1 ELSE 0 END) AS supplemental_oxygen_flag
    FROM RankedEvents WHERE rn = 1
    GROUP BY stay_id
),
Min_Heart_Rate_First_24h AS (
    SELECT
        i.stay_id,
        MIN(c.valuenum) as min_heart_rate_first_24h
    FROM `physionet-data.mimiciv_3_1_icu.icustays` i
    JOIN `physionet-data.mimiciv_3_1_icu.chartevents` c ON c.stay_id = i.stay_id
    WHERE i.stay_id IN (SELECT stay_id FROM Base_Cohort)
      AND c.itemid = 220045
      AND c.charttime BETWEEN i.intime AND DATETIME_ADD(i.intime, INTERVAL 24 HOUR)
    GROUP BY i.stay_id
),
Braden_Scores AS (
    SELECT
        i.hadm_id,
        SUM(CASE WHEN c.itemid = 224059 THEN c.valuenum END) AS sum_braden_skin_score,
        SUM(CASE WHEN c.itemid = 224055 THEN c.valuenum END) AS sum_braden_moisture,
        SUM(CASE WHEN c.itemid = 224057 THEN c.valuenum END) AS sum_braden_mobility,
        SUM(CASE WHEN c.itemid = 224056 THEN c.valuenum END) AS sum_braden_activity,
        SUM(CASE WHEN c.itemid = 224058 THEN c.valuenum END) AS sum_braden_nutrition,
        SUM(CASE WHEN c.itemid = 224054 THEN c.valuenum END) AS sum_braden_perception
    FROM `physionet-data.mimiciv_3_1_icu.chartevents` c
    JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
    WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
      AND c.itemid IN (224059, 224055, 224057, 224056, 224058, 224054)
    GROUP BY i.hadm_id
),
Comprehensive_Outcomes AS (
  SELECT
      bc.hadm_id,
      bc.stay_id,
      COALESCE(
          adm.deathtime,
          IF(adm.hospital_expire_flag = 1, adm.dischtime, NULL),
          CAST(pat.dod AS DATETIME)
      CASE
          WHEN pat.dod IS NOT NULL OR adm.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN 1
          ELSE 0
      END AS overall_mortality_flag,
      CASE
          WHEN adm.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN 1
          ELSE 0
      END AS in_hospital_mortality_flag,
      DATETIME_DIFF(adm.dischtime, adm.admittime, HOUR) / 24.0 AS hospital_los_days,
      LEAST(icu.los, DATETIME_DIFF(adm.dischtime, adm.admittime, HOUR) / 24.0) AS icu_los_days,
      CASE WHEN icu.los < 1 THEN 1 ELSE 0 END AS icu_los_less_than_1_day_flag,
      CASE
          WHEN adm.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN DATETIME_DIFF(COALESCE(adm.deathtime, adm.dischtime), adm.admittime, HOUR) / 24.0
          ELSE NULL
      END AS in_hospital_mortality_duration,
      CASE
          WHEN pat.dod IS NOT NULL OR adm.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN DATETIME_DIFF(COALESCE(adm.deathtime, CAST(pat.dod AS DATETIME), adm.dischtime), adm.admittime, HOUR) / 24.0
          ELSE NULL
      END AS overall_mortality_duration
  FROM Base_Cohort bc
  JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON bc.hadm_id = adm.hadm_id
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` pat ON bc.subject_id = pat.subject_id
  JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON bc.stay_id = icu.stay_id
),
Readmission_Flag AS (
    SELECT
        b.hadm_id,
        MAX(CASE WHEN a.admittime > b.dischtime AND a.admittime <= DATETIME_ADD(b.dischtime, INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS readmission_30_day
    FROM `physionet-data.mimiciv_3_1_hosp.admissions` a
    JOIN `physionet-data.mimiciv_3_1_hosp.admissions` b
      ON a.subject_id = b.subject_id AND a.hadm_id != b.hadm_id
    WHERE b.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    GROUP BY b.hadm_id
),
Outcome_Flags_ICD AS (
   SELECT
       hadm_id,
       MAX(CASE WHEN icd_code = 'I22' THEN 1 ELSE 0 END) AS outcome_mace_ami_subsequent,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '433%' OR icd_code LIKE '434%' OR icd_code LIKE '435%')) OR (icd_version = 10 AND (icd_code LIKE 'I63%' OR icd_code LIKE 'I65%' OR icd_code LIKE 'I66%')) THEN 1 ELSE 0 END) AS has_current_stroke,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '428%') OR (icd_version = 10 AND icd_code LIKE 'I50%') THEN 1 ELSE 0 END) AS outcome_mace_heart_failure,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('00.66', '36.01', '36.02', '36.05')) OR (icd_version = 10 AND (icd_code LIKE '0270%' OR icd_code LIKE '0271%' OR icd_code LIKE '0272%' OR icd_code LIKE '0273%')) THEN 1 ELSE 0 END) AS outcome_pci_current_stay,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('36.17', '36.16', '36.15')) OR (icd_version = 10 AND (icd_code LIKE '0210%' OR icd_code LIKE '0211%' OR icd_code LIKE '0212%' OR icd_code LIKE '0213%')) THEN 1 ELSE 0 END) AS outcome_cabg_current_stay
   FROM (
       SELECT hadm_id, icd_code, icd_version FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
       UNION ALL
       SELECT hadm_id, icd_code, icd_version FROM `physionet-data.mimiciv_3_1_hosp.procedures_icd` WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
   )
   GROUP BY hadm_id
),
First_Scores AS (
    SELECT
        i.hadm_id,
        MAX(CASE WHEN c.itemid = 227073 THEN c.valuenum END) AS first_anion_gap,
        MAX(CASE WHEN c.itemid = 224828 THEN c.valuenum END) AS first_base_excess,
        MAX(CASE WHEN c.itemid = 226743 THEN c.valuenum END) AS first_apache_ii,
        MAX(CASE WHEN c.itemid = 226991 THEN c.valuenum END) AS first_apache_iii
    FROM (
        SELECT
            stay_id,
            itemid,
            valuenum,
            ROW_NUMBER() OVER(PARTITION BY stay_id, itemid ORDER BY charttime ASC) as rn
        FROM `physionet-data.mimiciv_3_1_icu.chartevents`
        WHERE itemid IN (
            227073,
            224828,
            226743,
            226991
        )
        AND stay_id IN (SELECT stay_id FROM Base_Cohort)
    ) c
    JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
    WHERE c.rn = 1
    GROUP BY i.hadm_id
),
Charlson_Index AS (
    WITH diag AS (
        SELECT
            hadm_id,
            CASE WHEN icd_version = 9 THEN icd_code ELSE NULL END AS icd9_code,
            CASE WHEN icd_version = 10 THEN icd_code ELSE NULL END AS icd10_code
        FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
        WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
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
        WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    )
    SELECT
        com.hadm_id,
        myocardial_infarct, congestive_heart_failure, peripheral_vascular_disease, cerebrovascular_disease,
        dementia, chronic_pulmonary_disease, rheumatic_disease, peptic_ulcer_disease, mild_liver_disease,
        diabetes_without_cc, diabetes_with_cc, paraplegia, renal_disease, malignant_cancer,
        severe_liver_disease, metastatic_solid_tumor, aids,
        ag.age_score
        + myocardial_infarct + congestive_heart_failure + peripheral_vascular_disease + cerebrovascular_disease
        + dementia + chronic_pulmonary_disease + rheumatic_disease + peptic_ulcer_disease
        + GREATEST(mild_liver_disease, 3 * severe_liver_disease)
        + GREATEST(2 * diabetes_with_cc, diabetes_without_cc)
        + GREATEST(2 * malignant_cancer, 6 * metastatic_solid_tumor)
        + 2 * paraplegia + 2 * renal_disease + 6 * aids
        AS charlson_comorbidity_index
    FROM com
    LEFT JOIN ag ON com.hadm_id = ag.hadm_id
),

First_Day_SOFA AS (
    WITH ventilation_events AS (
       SELECT stay_id, charttime
       FROM `physionet-data.mimiciv_3_1_icu.chartevents`
       WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Base_Cohort)
    ),
    vasoactive_agent AS (
       SELECT ie.stay_id, ie.starttime, ie.itemid, ie.rate AS vaso_rate
       FROM `physionet-data.mimiciv_3_1_icu.inputevents` ie
       JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
       WHERE ie.itemid IN (221906, 221289, 221662, 221653)
         AND ie.stay_id IN (SELECT stay_id FROM Base_Cohort)
         AND ie.starttime BETWEEN icu.intime AND DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
    ),
    pafi AS (
       SELECT ie.stay_id,
           MIN(CASE WHEN vd.stay_id IS NULL THEN pao2fio2ratio END) AS pao2fio2ratio_novent,
           MIN(CASE WHEN vd.stay_id IS NOT NULL THEN pao2fio2ratio END) AS pao2fio2ratio_vent
       FROM Base_Cohort ie
       INNER JOIN `physionet-data.mimiciv_3_1_derived.bg` bg ON ie.hadm_id = bg.hadm_id
       LEFT JOIN ventilation_events vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime
       LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
       WHERE bg.specimen = 'ART.' AND bg.charttime BETWEEN icu.intime AND DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
       GROUP BY ie.stay_id
    ),
    vasopressors AS (
       SELECT
           stay_id,
           MAX(CASE WHEN itemid = 221906 THEN vaso_rate END) AS rate_norepinephrine,
           MAX(CASE WHEN itemid = 221289 THEN vaso_rate END) AS rate_epinephrine,
           MAX(CASE WHEN itemid = 221662 THEN vaso_rate END) AS rate_dopamine,
           MAX(CASE WHEN itemid = 221653 THEN vaso_rate END) AS rate_dobutamine
       FROM vasoactive_agent
       GROUP BY stay_id
    ),
    scorecomp AS (
       SELECT
           ie.stay_id,
           pf.pao2fio2ratio_novent, pf.pao2fio2ratio_vent,
           labs.platelets_min, labs.bilirubin_total_max AS bilirubin_max,
           vital.mbp_min, vaso.rate_norepinephrine, vaso.rate_epinephrine, vaso.rate_dopamine, vaso.rate_dobutamine,
           gcs.gcs_min, labs.creatinine_max, uo.urineoutput AS uo_24hr
       FROM Base_Cohort ie
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
       LEFT JOIN pafi pf ON ie.stay_id = pf.stay_id
       LEFT JOIN vasopressors vaso ON ie.stay_id = vaso.stay_id
    ),
    scorecalc AS (
       SELECT stay_id,
           CASE WHEN pao2fio2ratio_vent < 100 THEN 4 WHEN pao2fio2ratio_vent < 200 THEN 3 WHEN pao2fio2ratio_novent < 300 THEN 2 WHEN pao2fio2ratio_vent < 300 THEN 2 WHEN pao2fio2ratio_novent < 400 THEN 1 WHEN pao2fio2ratio_vent < 400 THEN 1 ELSE 0 END AS respiration_sofa,
           CASE WHEN platelets_min < 20 THEN 4 WHEN platelets_min < 50 THEN 3 WHEN platelets_min < 100 THEN 2 WHEN platelets_min < 150 THEN 1 ELSE 0 END AS coagulation_sofa,
           CASE WHEN bilirubin_max >= 12.0 THEN 4 WHEN bilirubin_max >= 6.0 THEN 3 WHEN bilirubin_max >= 2.0 THEN 2 WHEN bilirubin_max >= 1.2 THEN 1 ELSE 0 END AS liver_sofa,
           CASE WHEN rate_dopamine > 15 OR rate_epinephrine > 0.1 OR rate_norepinephrine > 0.1 THEN 4 WHEN rate_dopamine > 5 OR rate_epinephrine <= 0.1 OR rate_norepinephrine <= 0.1 THEN 3 WHEN rate_dopamine > 0 OR rate_dobutamine > 0 THEN 2 WHEN mbp_min < 70 THEN 1 ELSE 0 END AS cardiovascular_sofa,
           CASE WHEN (gcs_min >= 13 AND gcs_min <= 14) THEN 1 WHEN (gcs_min >= 10 AND gcs_min <= 12) THEN 2 WHEN (gcs_min >= 6 AND gcs_min <= 9) THEN 3 WHEN gcs_min < 6 THEN 4 ELSE 0 END AS cns_sofa,
           CASE WHEN (creatinine_max >= 5.0) THEN 4 WHEN uo_24hr < 200 THEN 4 WHEN (creatinine_max >= 3.5 AND creatinine_max < 5.0) THEN 3 WHEN uo_24hr < 500 THEN 3 WHEN (creatinine_max >= 2.0 AND creatinine_max < 3.5) THEN 2 WHEN (creatinine_max >= 1.2 AND creatinine_max < 2.0) THEN 1 ELSE 0 END AS renal_sofa
       FROM scorecomp
    )
    SELECT
       stay_id,
       COALESCE(s.respiration_sofa, 0) + COALESCE(s.coagulation_sofa, 0) + COALESCE(s.liver_sofa, 0) +
       COALESCE(s.cardiovascular_sofa, 0) + COALESCE(s.cns_sofa, 0) + COALESCE(s.renal_sofa, 0)
       AS sofa_score,
       s.respiration_sofa, s.coagulation_sofa, s.liver_sofa, s.cardiovascular_sofa, s.cns_sofa, s.renal_sofa
    FROM scorecalc s
),
APSIII_Score AS (
    WITH ventilation_events AS (
       SELECT stay_id, charttime
       FROM `physionet-data.mimiciv_3_1_icu.chartevents`
       WHERE itemid = 223849 AND value IS NOT NULL
         AND stay_id IN (SELECT stay_id FROM Base_Cohort)
    ),
    pa AS (
       SELECT ie.stay_id, bg.po2,
           ROW_NUMBER() OVER (PARTITION BY ie.stay_id ORDER BY bg.po2 DESC) AS rn
       FROM `physionet-data.mimiciv_3_1_derived.bg` bg
       INNER JOIN Base_Cohort ie ON bg.hadm_id = ie.hadm_id
       LEFT JOIN ventilation_events vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime
       WHERE vd.stay_id IS NULL AND COALESCE(fio2, fio2_chartevents, 21) < 50
         AND bg.po2 IS NOT NULL AND bg.specimen = 'ART.'
    ),
    aa AS (
       SELECT ie.stay_id, bg.aado2,
           ROW_NUMBER() OVER (PARTITION BY ie.stay_id ORDER BY bg.aado2 DESC) AS rn
       FROM `physionet-data.mimiciv_3_1_derived.bg` bg
       INNER JOIN Base_Cohort ie ON bg.hadm_id = ie.hadm_id
       INNER JOIN ventilation_events vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime
       WHERE vd.stay_id IS NOT NULL AND COALESCE(fio2, fio2_chartevents) >= 50
         AND bg.aado2 IS NOT NULL AND bg.specimen = 'ART.'
    ),
    acidbase AS (
       SELECT ie.stay_id,
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
       INNER JOIN Base_Cohort ie ON bg.hadm_id = ie.hadm_id
       WHERE ph IS NOT NULL AND pco2 IS NOT NULL AND bg.specimen = 'ART.'
    ),
    acidbase_max AS (
       SELECT stay_id, acidbase_score,
           ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY acidbase_score DESC) AS acidbase_rn
       FROM acidbase
    ),
    arf AS (
       SELECT ie.stay_id,
           CASE WHEN labs.creatinine_max >= 1.5 AND uo.urineoutput < 410 AND icd.ckd = 0 THEN 1 ELSE 0 END AS arf
       FROM Base_Cohort ie
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
       LEFT JOIN (SELECT hadm_id, MAX(CASE WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 4) IN ('5854', '5855', '5856') THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 4) IN ('N184', 'N185', 'N186') THEN 1 ELSE 0 END) AS ckd FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` GROUP BY hadm_id) icd ON ie.hadm_id = icd.hadm_id
    ),
    vent AS (
       SELECT ie.stay_id, MAX(CASE WHEN v.stay_id IS NOT NULL THEN 1 ELSE 0 END) AS vent
       FROM Base_Cohort ie
       LEFT JOIN ventilation_events v ON ie.stay_id = v.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
       WHERE DATETIME_DIFF(v.charttime, icu.intime, HOUR) < 24
       GROUP BY ie.stay_id
    ),
    cohort AS (
       SELECT ie.stay_id,
           vital.heart_rate_min, vital.heart_rate_max, vital.mbp_min, vital.mbp_max, vital.temperature_min,
           vital.temperature_max, vital.resp_rate_min, vital.resp_rate_max,
           pa.po2, aa.aado2, ab.acidbase_score,
           labs.hematocrit_min, labs.hematocrit_max, labs.wbc_min, labs.wbc_max, labs.creatinine_min,
           labs.creatinine_max, labs.bun_min, labs.bun_max, labs.sodium_min, labs.sodium_max,
           labs.albumin_min, labs.albumin_max, labs.bilirubin_total_min AS bilirubin_min,
           labs.bilirubin_total_max AS bilirubin_max,
           GREATEST(labs.glucose_max, vital.glucose_max) AS glucose_max,
           LEAST(labs.glucose_min, vital.glucose_min) AS glucose_min,
           vent.vent, uo.urineoutput,
           gcs.gcs_min, gcs.gcs_motor, gcs.gcs_verbal, gcs.gcs_eyes, gcs.gcs_unable,
           arf.arf
       FROM Base_Cohort ie
       LEFT JOIN pa ON ie.stay_id = pa.stay_id AND pa.rn = 1
       LEFT JOIN aa ON ie.stay_id = aa.stay_id AND aa.rn = 1
       LEFT JOIN acidbase_max ab ON ie.stay_id = ab.stay_id AND ab.acidbase_rn = 1
       LEFT JOIN arf ON ie.stay_id = arf.stay_id
       LEFT JOIN vent ON ie.stay_id = vent.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
    ),
    score_min AS (
       SELECT c.stay_id,
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
       FROM cohort c
    ),
    score_max AS (
       SELECT c.stay_id,
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
       FROM cohort c
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
           CASE WHEN po2 IS NOT NULL THEN CASE WHEN po2 < 50 THEN 15 WHEN po2 < 70 THEN 5 WHEN po2 < 80 THEN 2 ELSE 0 END WHEN aado2 IS NOT NULL THEN CASE WHEN aado2 < 100 THEN 0 WHEN aado2 < 250 THEN 7 WHEN aado2 < 350 THEN 9 WHEN aado2 < 500 THEN 11 WHEN aado2 >= 500 THEN 14 ELSE 0 END END AS pao2_aado2_score
       FROM cohort co
       LEFT JOIN score_min smin ON co.stay_id = smin.stay_id
       LEFT JOIN score_max smax ON co.stay_id = smax.stay_id
    ),
    score AS (
       SELECT s.*,
           (COALESCE(hr_score, 0) + COALESCE(mbp_score, 0) + COALESCE(temp_score, 0) + COALESCE(resp_rate_score, 0)
           + COALESCE(pao2_aado2_score, 0) + COALESCE(hematocrit_score, 0) + COALESCE(wbc_score, 0)
           + COALESCE(creatinine_score, 0) + COALESCE(uo_score, 0) + COALESCE(bun_score, 0) + COALESCE(sodium_score, 0)
           + COALESCE(albumin_score, 0) + COALESCE(bilirubin_score, 0) + COALESCE(glucose_score, 0)
           + COALESCE(acidbase_score, 0) + COALESCE(gcs_score, 0)) AS apsiii
       FROM scorecomp s
    )
    SELECT
       s.stay_id,
       s.apsiii,
       1 / (1 + EXP(- (-4.4360 + 0.04726 * (s.apsiii)))) AS apsiii_prob,
       s.hr_score, s.mbp_score, s.temp_score, s.resp_rate_score, s.pao2_aado2_score, s.hematocrit_score,
       s.wbc_score, s.creatinine_score, s.uo_score, s.bun_score, s.sodium_score, s.albumin_score,
       s.bilirubin_score, s.glucose_score, s.acidbase_score, s.gcs_score
    FROM score s
),
LODS_Scores AS (
    WITH
    cohort_with_intime AS (
       SELECT
           arf.subject_id, arf.hadm_id, arf.stay_id,
           icu.intime
       FROM Base_Cohort arf
       LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON arf.stay_id = icu.stay_id
    ),
    ventilation_events AS (
       SELECT
           stay_id, charttime,
           CASE WHEN itemid = 223849 THEN 'InvasiveVent' END AS ventilation_status
       FROM `physionet-data.mimiciv_3_1_icu.chartevents`
       WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM cohort_with_intime)
    ),
    cpap AS (
       SELECT
           c.stay_id,
           MIN(DATETIME_SUB(ce.charttime, INTERVAL '1' HOUR)) AS starttime,
           MAX(DATETIME_ADD(ce.charttime, INTERVAL '4' HOUR)) AS endtime
       FROM cohort_with_intime c
       INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce
           ON c.stay_id = ce.stay_id
           AND ce.charttime >= c.intime
           AND ce.charttime <= DATETIME_ADD(c.intime, INTERVAL '1' DAY)
       WHERE ce.itemid = 226732 AND (LOWER(ce.value) LIKE '%cpap%' OR LOWER(ce.value) LIKE '%bipap mask%')
       GROUP BY c.stay_id
    ),
    pafi1 AS (
       SELECT c.stay_id, bg.charttime, pao2fio2ratio,
           CASE WHEN vd.stay_id IS NOT NULL THEN 1 ELSE 0 END AS vent,
           CASE WHEN cp.stay_id IS NOT NULL THEN 1 ELSE 0 END AS cpap
       FROM `physionet-data.mimiciv_3_1_derived.bg` bg
       INNER JOIN cohort_with_intime c ON bg.hadm_id = c.hadm_id
       LEFT JOIN ventilation_events vd ON c.stay_id = vd.stay_id AND bg.charttime = vd.charttime
       LEFT JOIN cpap cp ON c.stay_id = cp.stay_id AND bg.charttime >= cp.starttime AND bg.charttime <= cp.endtime
       WHERE bg.charttime >= c.intime AND bg.charttime <= DATETIME_ADD(c.intime, INTERVAL '1' DAY)
    ),
    pafi2 AS (
       SELECT stay_id, MIN(pao2fio2ratio) AS pao2fio2_vent_min
       FROM pafi1
       WHERE vent = 1 OR cpap = 1
       GROUP BY stay_id
    ),
    cohort AS (
       SELECT ie.subject_id, ie.hadm_id, ie.stay_id,
           gcs.gcs_min, vital.heart_rate_max, vital.heart_rate_min, vital.sbp_max, vital.sbp_min,
           pf.pao2fio2_vent_min,
           labs.bun_max, labs.bun_min, labs.wbc_max, labs.wbc_min,
           labs.bilirubin_total_max AS bilirubin_max, labs.creatinine_max,
           labs.pt_min, labs.pt_max, labs.platelets_min AS platelet_min,
           uo.urineoutput
       FROM cohort_with_intime ie
       LEFT JOIN pafi2 pf ON ie.stay_id = pf.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
    ),
    scorecomp AS (
       SELECT
           cohort.*,
           CASE WHEN gcs_min < 3 THEN NULL WHEN gcs_min <= 5 THEN 5 WHEN gcs_min <= 8 THEN 3 WHEN gcs_min <= 13 THEN 1 ELSE 0 END AS neurologic,
           CASE WHEN heart_rate_min < 30 THEN 5 WHEN sbp_min < 40 THEN 5 WHEN sbp_min < 70 THEN 3 WHEN sbp_max >= 270 THEN 3 WHEN heart_rate_max >= 140 THEN 1 WHEN sbp_max >= 240 THEN 1 WHEN sbp_min < 90 THEN 1 ELSE 0 END AS cardiovascular,
           CASE WHEN urineoutput < 500.0 THEN 5 WHEN bun_max >= 56.0 THEN 5 WHEN creatinine_max >= 1.60 THEN 3 WHEN urineoutput < 750.0 THEN 3 WHEN bun_max >= 28.0 THEN 3 WHEN urineoutput >= 10000.0 THEN 3 WHEN creatinine_max >= 1.20 THEN 1 WHEN bun_max >= 17.0 THEN 1 WHEN bun_max >= 7.50 THEN 1 ELSE 0 END AS renal,
           CASE WHEN pao2fio2_vent_min IS NULL THEN 0 WHEN pao2fio2_vent_min >= 150 THEN 1 WHEN pao2fio2_vent_min < 150 THEN 3 END AS pulmonary,
           CASE WHEN wbc_min < 1.0 THEN 3 WHEN wbc_min < 2.5 THEN 1 WHEN platelet_min < 50.0 THEN 1 WHEN wbc_max >= 50.0 THEN 1 ELSE 0 END AS hematologic,
           CASE WHEN bilirubin_max >= 2.0 THEN 1 WHEN pt_max > (12 + 3) THEN 1 WHEN pt_min < (12 * 0.25) THEN 1 ELSE 0 END AS hepatic
       FROM cohort
    )
    SELECT
       s.subject_id, s.hadm_id, s.stay_id,
       COALESCE(s.neurologic, 0) + COALESCE(s.cardiovascular, 0) + COALESCE(s.renal, 0) + COALESCE(s.pulmonary, 0) + COALESCE(s.hematologic, 0) + COALESCE(s.hepatic, 0) AS lods,
       s.neurologic, s.cardiovascular, s.renal, s.pulmonary, s.hematologic, s.hepatic
    FROM scorecomp s
),
SAPSII_Score AS (
    WITH ventilation_events AS (
       SELECT stay_id, charttime
       FROM `physionet-data.mimiciv_3_1_icu.chartevents`
       WHERE itemid = 223849 AND value IS NOT NULL
         AND stay_id IN (SELECT stay_id FROM Base_Cohort)
    ),
    cpap AS (
       SELECT co.stay_id,
           GREATEST(MIN(DATETIME_SUB(charttime, INTERVAL '1' HOUR)), co.starttime) AS starttime,
           LEAST(MAX(DATETIME_ADD(charttime, INTERVAL '4' HOUR)), co.endtime) AS endtime
       FROM (
           SELECT ie.stay_id, icu.intime AS starttime, DATETIME_ADD(icu.intime, INTERVAL '24' HOUR) AS endtime
           FROM Base_Cohort ie
           INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
       ) co
       INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON co.stay_id = ce.stay_id
           AND ce.charttime > co.starttime AND ce.charttime <= co.endtime
       WHERE ce.itemid = 226732 AND REGEXP_CONTAINS(LOWER(ce.value), '(cpap mask|bipap)')
       GROUP BY co.stay_id, co.starttime, co.endtime
    ),
    surgflag AS (
       SELECT adm.hadm_id,
           CASE WHEN LOWER(curr_service) LIKE '%surg%' THEN 1 ELSE 0 END AS surgical,
           ROW_NUMBER() OVER (PARTITION BY adm.hadm_id ORDER BY transfertime) AS serviceorder
       FROM `physionet-data.mimiciv_3_1_hosp.admissions` adm
       LEFT JOIN `physionet-data.mimiciv_3_1_hosp.services` se ON adm.hadm_id = se.hadm_id
       WHERE adm.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    ),
    comorb AS (
       SELECT hadm_id,
           MAX(CASE WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 3) BETWEEN '042' AND '044' THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'B20' AND 'B22' THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) = 'B24' THEN 1 ELSE 0 END) AS aids,
           MAX(CASE WHEN icd_version = 9 AND (SUBSTR(icd_code, 1, 5) BETWEEN '20000' AND '20302' OR SUBSTR(icd_code, 1, 5) BETWEEN '20310' AND '20312' OR SUBSTR(icd_code, 1, 5) BETWEEN '20302' AND '20382' OR SUBSTR(icd_code, 1, 5) BETWEEN '20400' AND '20892' OR SUBSTR(icd_code, 1, 4) IN ('2386', '2733')) THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'C81' AND 'C96' THEN 1 ELSE 0 END) AS hem,
           MAX(CASE WHEN icd_version = 9 AND (SUBSTR(icd_code, 1, 4) BETWEEN '1960' AND '1991' OR SUBSTR(icd_code, 1, 5) BETWEEN '20970' AND '20975' OR SUBSTR(icd_code, 1, 5) IN ('20979', '78951')) THEN 1 WHEN icd_version = 10 AND (SUBSTR(icd_code, 1, 3) BETWEEN 'C77' AND 'C79' OR SUBSTR(icd_code, 1, 4) = 'C800') THEN 1 ELSE 0 END) AS mets
       FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
       WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
       GROUP BY hadm_id
    ),
    pafi1 AS (
       SELECT co.stay_id, bg.charttime, pao2fio2ratio AS pao2fio2,
           CASE WHEN vd.stay_id IS NOT NULL THEN 1 ELSE 0 END AS vent,
           CASE WHEN cp.stay_id IS NOT NULL THEN 1 ELSE 0 END AS cpap
       FROM Base_Cohort co
       JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON co.stay_id = icu.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.bg` bg ON co.subject_id = bg.subject_id AND bg.specimen = 'ART.' AND bg.charttime > icu.intime AND bg.charttime <= DATETIME_ADD(icu.intime, INTERVAL '24' HOUR)
       LEFT JOIN ventilation_events vd ON co.stay_id = vd.stay_id AND bg.charttime = vd.charttime
       LEFT JOIN cpap cp ON co.stay_id = cp.stay_id AND bg.charttime > cp.starttime AND bg.charttime <= cp.endtime
    ),
    pafi2 AS (
       SELECT stay_id, MIN(pao2fio2) AS pao2fio2_vent_min
       FROM pafi1
       WHERE vent = 1 OR cpap = 1
       GROUP BY stay_id
    ),
    cohort AS (
       SELECT
           ie.stay_id,
           va.age,
           vital.heart_rate_max, vital.heart_rate_min,
           vital.sbp_max, vital.sbp_min,
           vital.temperature_max, vital.temperature_min,
           pf.pao2fio2_vent_min,
           uo.urineoutput,
           labs.bun_min, labs.bun_max,
           labs.wbc_min, labs.wbc_max,
           labs.potassium_min, labs.potassium_max,
           labs.sodium_min, labs.sodium_max,
           labs.bicarbonate_min, labs.bicarbonate_max,
           labs.bilirubin_total_min, labs.bilirubin_total_max,
           gcs.gcs_min,
           comorb.aids, comorb.hem, comorb.mets,
           CASE
               WHEN adm.admission_type = 'ELECTIVE' AND sf.surgical = 1 THEN 'ScheduledSurgical'
               WHEN adm.admission_type != 'ELECTIVE' AND sf.surgical = 1 THEN 'UnscheduledSurgical'
               ELSE 'Medical'
           END AS admissiontype
       FROM Base_Cohort ie
       INNER JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON ie.hadm_id = adm.hadm_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.age` va ON ie.hadm_id = va.hadm_id
       LEFT JOIN pafi2 pf ON ie.stay_id = pf.stay_id
       LEFT JOIN surgflag sf ON adm.hadm_id = sf.hadm_id AND sf.serviceorder = 1
       LEFT JOIN comorb ON ie.hadm_id = comorb.hadm_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id
    ),
    scorecomp AS (
       SELECT
           cohort.*,
           CASE WHEN age < 40 THEN 0 WHEN age < 60 THEN 7 WHEN age < 70 THEN 12 WHEN age < 75 THEN 15 WHEN age < 80 THEN 16 WHEN age >= 80 THEN 18 END AS age_score,
           CASE WHEN heart_rate_min < 40 THEN 11 WHEN heart_rate_max >= 160 THEN 7 WHEN heart_rate_max >= 120 THEN 4 WHEN heart_rate_min < 70 THEN 2 ELSE 0 END AS hr_score,
           CASE WHEN sbp_min < 70 THEN 13 WHEN sbp_min < 100 THEN 5 WHEN sbp_max >= 200 THEN 2 ELSE 0 END AS sysbp_score,
           CASE WHEN temperature_max >= 39.0 THEN 3 ELSE 0 END AS temp_score,
           CASE WHEN pao2fio2_vent_min < 100 THEN 11 WHEN pao2fio2_vent_min < 200 THEN 9 WHEN pao2fio2_vent_min >= 200 THEN 6 END AS pao2fio2_score,
           CASE WHEN urineoutput < 500.0 THEN 11 WHEN urineoutput < 1000.0 THEN 4 ELSE 0 END AS uo_score,
           CASE WHEN bun_max < 28.0 THEN 0 WHEN bun_max < 84.0 THEN 6 WHEN bun_max >= 84.0 THEN 10 END AS bun_score,
           CASE WHEN wbc_min < 1.0 THEN 12 WHEN wbc_max >= 20.0 THEN 3 ELSE 0 END AS wbc_score,
           CASE WHEN potassium_min < 3.0 THEN 3 WHEN potassium_max >= 5.0 THEN 3 ELSE 0 END AS potassium_score,
           CASE WHEN sodium_min < 125 THEN 5 WHEN sodium_max >= 145 THEN 1 ELSE 0 END AS sodium_score,
           CASE WHEN bicarbonate_min < 15.0 THEN 6 WHEN bicarbonate_min < 20.0 THEN 3 ELSE 0 END AS bicarbonate_score,
           CASE WHEN bilirubin_total_max < 4.0 THEN 0 WHEN bilirubin_total_max < 6.0 THEN 4 WHEN bilirubin_total_max >= 6.0 THEN 9 END AS bilirubin_score,
           CASE WHEN gcs_min < 3 THEN NULL WHEN gcs_min < 6 THEN 26 WHEN gcs_min < 9 THEN 13 WHEN gcs_min < 11 THEN 7 WHEN gcs_min < 14 THEN 5 WHEN gcs_min >= 14 THEN 0 END AS gcs_score,
           CASE WHEN aids = 1 THEN 17 WHEN hem = 1 THEN 10 WHEN mets = 1 THEN 9 ELSE 0 END AS comorbidity_score,
           CASE WHEN admissiontype = 'ScheduledSurgical' THEN 0 WHEN admissiontype = 'Medical' THEN 6 WHEN admissiontype = 'UnscheduledSurgical' THEN 8 END AS admissiontype_score
       FROM cohort
    ),
    score AS (
       SELECT s.*,
           (COALESCE(age_score, 0) + COALESCE(hr_score, 0) + COALESCE(sysbp_score, 0) + COALESCE(temp_score, 0)
           + COALESCE(pao2fio2_score, 0) + COALESCE(uo_score, 0) + COALESCE(bun_score, 0) + COALESCE(wbc_score, 0)
           + COALESCE(potassium_score, 0) + COALESCE(sodium_score, 0) + COALESCE(bicarbonate_score, 0)
           + COALESCE(bilirubin_score, 0) + COALESCE(gcs_score, 0) + COALESCE(comorbidity_score, 0) + COALESCE(admissiontype_score, 0)
           ) AS sapsii
       FROM scorecomp s
    )
    SELECT
       s.stay_id,
       s.sapsii,
       1 / (1 + EXP(- (-7.7631 + 0.0737 * (s.sapsii) + 0.9971 * (LN(s.sapsii + 1))))) AS sapsii_prob,
       age_score, hr_score, sysbp_score, temp_score, pao2fio2_score, uo_score,
       bun_score, wbc_score, potassium_score, sodium_score, bicarbonate_score,
       bilirubin_score, gcs_score, comorbidity_score, admissiontype_score
    FROM score s
),
OASIS_Score AS (
    WITH ventilation_events AS (
       SELECT stay_id, charttime
       FROM `physionet-data.mimiciv_3_1_icu.chartevents`
       WHERE itemid = 223849 AND value IS NOT NULL
         AND stay_id IN (SELECT stay_id FROM Base_Cohort)
    ),
    surgflag AS (
       SELECT ie.stay_id,
           MAX(CASE WHEN LOWER(curr_service) LIKE '%surg%' THEN 1 WHEN curr_service = 'ORTHO' THEN 1 ELSE 0 END) AS surgical
       FROM Base_Cohort ie
       LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_hosp.services` se
           ON ie.hadm_id = se.hadm_id AND se.transfertime < DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
       GROUP BY ie.stay_id
    ),
    vent AS (
       SELECT ie.stay_id,
           MAX(CASE WHEN v.stay_id IS NOT NULL THEN 1 ELSE 0 END) AS vent
       FROM Base_Cohort ie
       LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
       LEFT JOIN ventilation_events v
           ON ie.stay_id = v.stay_id AND v.charttime >= icu.intime AND v.charttime <= DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
       GROUP BY ie.stay_id
    ),
    cohort AS (
       SELECT ie.stay_id,
           DATETIME_DIFF(icu.intime, adm.admittime, MINUTE) AS preiculos,
           ag.age, gcs.gcs_min,
           vital.heart_rate_max, vital.heart_rate_min,
           vital.mbp_max, vital.mbp_min,
           vital.resp_rate_max, vital.resp_rate_min,
           vital.temperature_max, vital.temperature_min,
           vent.vent AS mechvent, uo.urineoutput,
           CASE WHEN adm.admission_type = 'ELECTIVE' AND sf.surgical = 1 THEN 1 WHEN adm.admission_type IS NULL OR sf.surgical IS NULL THEN null ELSE 0 END AS electivesurgery
       FROM Base_Cohort ie
       INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id
       INNER JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON ie.hadm_id = adm.hadm_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.age` ag ON ie.hadm_id = ag.hadm_id
       LEFT JOIN surgflag sf ON ie.stay_id = sf.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id
       LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id
       LEFT JOIN vent ON ie.stay_id = vent.stay_id
    ),
    scorecomp AS (
       SELECT co.stay_id,
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
       FROM cohort co
    ),
    score AS (
       SELECT s.*,
           (COALESCE(age_score, 0) + COALESCE(preiculos_score, 0) + COALESCE(gcs_score, 0) + COALESCE(heart_rate_score, 0)
           + COALESCE(mbp_score, 0) + COALESCE(resp_rate_score, 0) + COALESCE(temp_score, 0)
           + COALESCE(urineoutput_score, 0) + COALESCE(mechvent_score, 0) + COALESCE(electivesurgery_score, 0)
           ) AS oasis
       FROM scorecomp s
    )
    SELECT
       stay_id, oasis, 1 / (1 + EXP(- (-6.1746 + 0.1275 * (oasis)))) AS oasis_prob,
       age_score, preiculos_score, gcs_score, heart_rate_score, mbp_score, resp_rate_score, temp_score,
       urineoutput_score, mechvent_score, electivesurgery_score
    FROM score
),
First_Braden_Assessment AS (
   WITH
   All_Braden_Events AS (
       SELECT
           i.hadm_id,
           c.charttime,
           c.itemid,
           c.valuenum
       FROM `physionet-data.mimiciv_3_1_icu.chartevents` c
       JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
       WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
         AND c.itemid IN (
             224054,
             224055,
             224056,
             224057,
             224058,
             224059
         )
         AND c.valuenum IS NOT NULL
   ),
   Grouped_Assessments AS (
       SELECT
           hadm_id,
           charttime,

           MAX(CASE WHEN itemid = 224054 THEN valuenum END) AS perception,
           MAX(CASE WHEN itemid = 224055 THEN valuenum END) AS moisture,
           MAX(CASE WHEN itemid = 224056 THEN valuenum END) AS activity,
           MAX(CASE WHEN itemid = 224057 THEN valuenum END) AS mobility,
           MAX(CASE WHEN itemid = 224058 THEN valuenum END) AS nutrition,
           MAX(CASE WHEN itemid = 224059 THEN valuenum END) AS friction_shear
       FROM All_Braden_Events
       GROUP BY hadm_id, charttime

       HAVING COUNT(DISTINCT itemid) = 6
   ),
   Ranked_Assessments AS (

       SELECT
           hadm_id,
           perception,
           moisture,
           activity,
           mobility,
           nutrition,
           friction_shear,

           (perception + moisture + activity + mobility + nutrition + friction_shear) AS total_braden_score,
           ROW_NUMBER() OVER(PARTITION BY hadm_id ORDER BY charttime ASC) as rn
       FROM Grouped_Assessments
   )
   SELECT
       hadm_id,
       perception AS first_braden_perception,
       moisture AS first_braden_moisture,
       activity AS first_braden_activity,
       mobility AS first_braden_mobility,
       nutrition AS first_braden_nutrition,
       friction_shear AS first_braden_friction_shear,
       total_braden_score
   FROM Ranked_Assessments
   WHERE rn = 1
),
CHA2DS2_VASc_Components AS (
    SELECT
        hadm_id,
        MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '428%') OR (icd_version = 10 AND icd_code LIKE 'I50%') THEN 1 ELSE 0 END) AS has_chf,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^40[1-5]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^I1[0-5A]')) THEN 1 ELSE 0 END) AS has_hypertension,
        MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(249|250)')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^E(08|09|10|11|13)')) THEN 1 ELSE 0 END) AS has_diabetes,
        MAX(CASE WHEN (icd_version = 9 AND (REGEXP_CONTAINS(icd_code, r'^43[3-5]') OR icd_code IN ('V1254', 'V1251'))) OR (icd_version = 10 AND (REGEXP_CONTAINS(icd_code, r'^I6[356]') OR icd_code IN ('Z8673', 'Z8671'))) THEN 1 ELSE 0 END) AS has_stroke_tia_thrombo,
        MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '410%' OR icd_code = '4439')) OR (icd_version = 10 AND (REGEXP_CONTAINS(icd_code, r'^I2[1-3]') OR icd_code = 'I739')) THEN 1 ELSE 0 END) AS has_vascular_disease
    FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
    WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    GROUP BY hadm_id
),
First_Braden_Scores AS (
    WITH RankedBraden AS (
        SELECT
            i.hadm_id,
            c.itemid,
            c.valuenum,
            ROW_NUMBER() OVER(PARTITION BY i.hadm_id, c.itemid ORDER BY c.charttime ASC) as rn
        FROM `physionet-data.mimiciv_3_1_icu.chartevents` c
        JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
        WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
          AND c.itemid IN (224055, 224057, 224056, 224058, 224054)
    )
    SELECT
        hadm_id,
        MAX(CASE WHEN itemid = 224055 THEN valuenum END) AS first_braden_moisture,
        MAX(CASE WHEN itemid = 224057 THEN valuenum END) AS first_braden_mobility,
        MAX(CASE WHEN itemid = 224056 THEN valuenum END) AS first_braden_activity,
        MAX(CASE WHEN itemid = 224058 THEN valuenum END) AS first_braden_nutrition,
        MAX(CASE WHEN itemid = 224054 THEN valuenum END) AS first_braden_perception
    FROM RankedBraden
    WHERE rn = 1
    GROUP BY hadm_id
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
    WHERE subject_id IN (SELECT subject_id FROM Base_Cohort)
  ) AS ranked_stays
  WHERE stay_id IN (SELECT stay_id FROM Base_Cohort)
),
Supplemental_Oxygen_Flag AS (
  SELECT
      i.hadm_id,
      1 AS has_supplemental_oxygen
  FROM `physionet-data.mimiciv_3_1_icu.chartevents` AS c
  JOIN `physionet-data.mimiciv_3_1_icu.icustays` AS i ON c.stay_id = i.stay_id
  WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    AND c.itemid = 226732
    AND c.value IN (
        'Non-rebreather', 'Face tent', 'Aerosol-cool', 'Venti mask',
        'Medium conc mask', 'Ultrasonic neb', 'Vapomist', 'Oxymizer',
        'High flow neb', 'Nasal cannula'
    )
  GROUP BY i.hadm_id
)
,
NIV_Flag AS (
  SELECT
      hadm_id,
      1 AS had_niv
  FROM (
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.procedureevents` pe JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON pe.stay_id = i.stay_id WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort) AND pe.itemid = 225794
      UNION DISTINCT
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.chartevents` c JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
      WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
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
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.procedureevents` pe JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON pe.stay_id = i.stay_id WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort) AND pe.itemid = 225792
      UNION DISTINCT
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.chartevents` c JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
      WHERE i.hadm_id IN (SELECT hadm_id FROM Base_Cohort)
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
  WHERE hadm_id IN (SELECT hadm_id FROM Base_Cohort)
    AND (
      (icd_version = 9 AND icd_code IN ('3995', '3927', '3943', '3895', '3942', '5498'))
      OR
      (icd_version = 10 AND icd_code IN ('5A1D70Z', '5A1D80Z', '5A1D90Z'))
    )
  GROUP BY hadm_id
),
MCARS_Score AS (
    SELECT
        bc.stay_id,
        IFNULL(CASE WHEN bf.first_bun > 23 THEN 1 ELSE 0 END, 0) +
        IFNULL(CASE WHEN fs.first_anion_gap > 14 THEN 1 ELSE 0 END, 0) +
        IFNULL(CASE
            WHEN fba.total_braden_score <= 12 THEN 2
            WHEN fba.total_braden_score BETWEEN 13 AND 15 THEN 1
            ELSE 0
        END, 0) +
        IFNULL(CASE WHEN bf.first_rdw_cv > 14.3 THEN 1 ELSE 0 END, 0) +
        IFNULL(CASE WHEN flags.has_cardiac_arrest = 1 THEN 2 ELSE 0 END, 0) +
        IFNULL(CASE WHEN flags.has_shock = 1 THEN 2 ELSE 0 END, 0) +
        IFNULL(CASE WHEN flags.has_respiratory_failure = 1 THEN 1 ELSE 0 END, 0)
        AS mcars_score
    FROM Base_Cohort bc
    LEFT JOIN Baseline_Features bf ON bc.stay_id = bf.stay_id
    LEFT JOIN First_Scores fs ON bc.hadm_id = fs.hadm_id
    LEFT JOIN First_Braden_Assessment fba ON bc.hadm_id = fba.hadm_id
    LEFT JOIN Confounder_Flags flags ON bc.hadm_id = flags.hadm_id
)
SELECT
   bc.subject_id, bc.hadm_id, bc.stay_id,
   CASE WHEN cicu.careunit IS NOT NULL THEN 1 ELSE 0 END AS is_cicu_stay,
   cicu.careunit, acs.acs_icd_codes,
   preg.pregnancy_icd_codes, con.congenital_anomaly_icd_codes, hem_mal.hematolymphoid_icd_codes, mal.malignancy_icd_codes, aids.aids_icd_codes,
   pat.gender, pat.anchor_age, pat.anchor_year, pat.anchor_year_group,
   adm.admittime, adm.dischtime,
   outcomes.death_datetime,
   adm.race,
   IFNULL(flags.has_t1d, 0) AS has_t1d, IFNULL(flags.has_t2d, 0) AS has_t2d,
   IFNULL(flags.has_hypertension_outcome, 0) AS has_hypertension_outcome,
   IFNULL(flags.has_dyslipidemia, 0) AS has_dyslipidemia,
   IFNULL(flags.has_ischemic_heart_disease, 0) AS has_ischemic_heart_disease, IFNULL(flags.has_angina, 0) AS has_angina,
   IFNULL(flags.has_coronary_artery_disease, 0) AS has_coronary_artery_disease, IFNULL(flags.has_myocardial_infarction, 0) AS has_myocardial_infarction,
   IFNULL(flags.has_heart_failure, 0) AS has_heart_failure, IFNULL(flags.has_cardiac_arrest, 0) AS has_cardiac_arrest,
   IFNULL(flags.has_shock, 0) AS has_shock, IFNULL(flags.has_cardiogenic_shock, 0) AS has_cardiogenic_shock,
   IFNULL(flags.has_atrial_fibrillation, 0) AS has_atrial_fibrillation, IFNULL(flags.has_congenital_heart_defects, 0) AS has_congenital_heart_defects,
   IFNULL(flags.has_infective_endocarditis, 0) AS has_infective_endocarditis, IFNULL(flags.has_cerebrovascular_disease, 0) AS has_cerebrovascular_disease,
   IFNULL(flags.has_dvt, 0) AS has_dvt, IFNULL(flags.has_pneumonia, 0) AS has_pneumonia, IFNULL(flags.has_bacterial_pneumonia, 0) AS has_bacterial_pneumonia,
   IFNULL(flags.has_viral_pneumonia, 0) AS has_viral_pneumonia, IFNULL(flags.has_copd, 0) AS has_copd,
   IFNULL(flags.has_pulmonary_embolism, 0) AS has_pulmonary_embolism, IFNULL(flags.has_pneumothorax, 0) AS has_pneumothorax,
   IFNULL(flags.has_pulmonary_hypertension, 0) AS has_pulmonary_hypertension, IFNULL(flags.has_ards, 0) AS has_ards,
   IFNULL(flags.has_respiratory_failure, 0) AS has_respiratory_failure, IFNULL(flags.has_sepsis, 0) AS has_sepsis,
   IFNULL(flags.has_cirrhosis, 0) AS has_cirrhosis, IFNULL(flags.has_hepatitis, 0) AS has_hepatitis,
   IFNULL(flags.has_peptic_ulcer_disease, 0) AS has_peptic_ulcer_disease, IFNULL(flags.has_ckd, 0) AS has_ckd,
   IFNULL(flags.has_aki, 0) AS has_aki, IFNULL(flags.has_hemiplegia, 0) AS has_hemiplegia, IFNULL(flags.has_dementia, 0) AS has_dementia,
   IFNULL(flags.has_leukemia, 0) AS has_leukemia, IFNULL(flags.has_lymphoma, 0) AS has_lymphoma,
   IFNULL(flags.has_aids, 0) AS has_aids,
   IFNULL(flags.has_connective_tissue_disease, 0) AS has_connective_tissue_disease, IFNULL(flags.has_rheumatoid_arthritis, 0) AS has_rheumatoid_arthritis,
   IFNULL(so2_flag.has_supplemental_oxygen, 0) AS has_supplemental_oxygen,
   IFNULL(dnr.has_dnr_status, 0) AS has_dnr_status,
   IFNULL(proc.had_coronary_angiography, 0) AS had_coronary_angiography,
   IFNULL(proc.had_pci, 0) AS had_pci,
   IFNULL(proc.had_cabg, 0) AS had_cabg,
   IFNULL(niv_flag.had_niv, 0) AS had_niv,
   IFNULL(imv_flag.had_imv, 0) AS had_imv,
   IFNULL(rrt_flag.had_rrt, 0) AS had_rrt,

   IFNULL(meds.had_norepinephrine, 0) AS had_norepinephrine,
   IFNULL(meds.had_epinephrine, 0) AS had_epinephrine,
   IFNULL(meds.had_dopamine, 0) AS had_dopamine,
   IFNULL(meds.had_vasopressin, 0) AS had_vasopressin,
   IFNULL(meds.had_dobutamine, 0) AS had_dobutamine,
   IFNULL(meds.had_phenylephrine, 0) AS had_phenylephrine,
   IFNULL(meds.had_milrinone, 0) AS had_milrinone,

   bf.* EXCEPT(stay_id),
   mhr.min_heart_rate_first_24h,
   bs.sum_braden_skin_score, bs.sum_braden_moisture, bs.sum_braden_mobility, bs.sum_braden_activity, bs.sum_braden_nutrition, bs.sum_braden_perception,

   outcomes.icu_los_days,
   outcomes.icu_los_less_than_1_day_flag,
   outcomes.hospital_los_days,
   outcomes.in_hospital_mortality_flag AS hospital_expire_flag,
   outcomes.in_hospital_mortality_duration,
   outcomes.overall_mortality_flag,
   outcomes.overall_mortality_duration,
   CASE WHEN outcomes.in_hospital_mortality_flag = 1 AND DATETIME_DIFF(outcomes.death_datetime, icu.intime, DAY) <= 28 THEN 1 ELSE 0 END AS icu_mortality_28_day,
   CASE WHEN outcomes.in_hospital_mortality_flag = 1 AND DATETIME_DIFF(outcomes.death_datetime, adm.admittime, DAY) < 29 THEN 1 ELSE 0 END AS hospital_mortality_28_day,
   CASE WHEN outcomes.death_datetime IS NOT NULL AND DATETIME_DIFF(outcomes.death_datetime, adm.admittime, DAY) <= 91 THEN 1 ELSE 0 END AS mortality_3_month,
   CASE WHEN outcomes.death_datetime IS NOT NULL AND DATETIME_DIFF(outcomes.death_datetime, adm.admittime, DAY) <= 182 THEN 1 ELSE 0 END AS mortality_6_month,
   CASE WHEN outcomes.death_datetime IS NOT NULL AND DATETIME_DIFF(outcomes.death_datetime, adm.admittime, DAY) <= 273 THEN 1 ELSE 0 END AS mortality_9_month,
   CASE WHEN outcomes.death_datetime IS NOT NULL AND DATETIME_DIFF(outcomes.death_datetime, adm.admittime, DAY) <= 365 THEN 1 ELSE 0 END AS mortality_12_month,
   IFNULL(rf.readmission_30_day, 0) AS readmission_30_day,
   IFNULL(icu_readmit.icu_readmission_30_day, 0) AS icu_readmission_30_day,
   outcomes.in_hospital_mortality_flag AS outcome_mace_death_in_hospital,
    IFNULL(oicd.outcome_mace_ami_subsequent, 0) AS outcome_mace_ami_subsequent,
   CASE WHEN IFNULL(oicd.has_current_stroke, 0) = 1 AND IFNULL(hist.prior_stroke, 0) = 0 THEN 1 ELSE 0 END AS outcome_mace_stroke_new,
   IFNULL(oicd.outcome_mace_heart_failure, 0) AS outcome_mace_heart_failure,
   IFNULL(oicd.outcome_pci_current_stay, 0) AS outcome_pci_current_stay,
   IFNULL(oicd.outcome_cabg_current_stay, 0) AS outcome_cabg_current_stay,
   fs.first_anion_gap,
   fs.first_base_excess,
   fs.first_apache_ii,
   fs.first_apache_iii,
   lods.lods AS lods_score,
  lods.neurologic AS lods_neurologic,
  lods.cardiovascular AS lods_cardiovascular,
  lods.renal AS lods_renal,
  lods.pulmonary AS lods_pulmonary,
  lods.hematologic AS lods_hematologic,
  lods.hepatic AS lods_hepatic,
   IFNULL(cci.charlson_comorbidity_index, 0) AS charlson_comorbidity_index,
   IFNULL(cci.myocardial_infarct, 0) AS charlson_myocardial_infarct,
   IFNULL(cci.congestive_heart_failure, 0) AS charlson_congestive_heart_failure,
   IFNULL(cci.peripheral_vascular_disease, 0) AS charlson_peripheral_vascular_disease,
   IFNULL(cci.cerebrovascular_disease, 0) AS charlson_cerebrovascular_disease,
   IFNULL(cci.dementia, 0) AS charlson_dementia,
   IFNULL(cci.chronic_pulmonary_disease, 0) AS charlson_chronic_pulmonary_disease,
   IFNULL(cci.rheumatic_disease, 0) AS charlson_rheumatic_disease,
   IFNULL(cci.peptic_ulcer_disease, 0) AS charlson_peptic_ulcer_disease,
   IFNULL(cci.mild_liver_disease, 0) AS charlson_mild_liver_disease,
   IFNULL(cci.diabetes_without_cc, 0) AS charlson_diabetes_without_cc,
   IFNULL(cci.diabetes_with_cc, 0) AS charlson_diabetes_with_cc,
   IFNULL(cci.paraplegia, 0) AS charlson_paraplegia,
   IFNULL(cci.renal_disease, 0) AS charlson_renal_disease,
   IFNULL(cci.malignant_cancer, 0) AS charlson_malignant_cancer,
   IFNULL(cci.severe_liver_disease, 0) AS charlson_severe_liver_disease,
   IFNULL(cci.metastatic_solid_tumor, 0) AS charlson_metastatic_solid_tumor,
   IFNULL(cci.aids, 0) AS charlson_aids,
   IFNULL(sofa.sofa_score, 0) AS sofa_score,
   IFNULL(sofa.respiration_sofa, 0) AS sofa_respiration,
   IFNULL(sofa.coagulation_sofa, 0) AS sofa_coagulation,
   IFNULL(sofa.liver_sofa, 0) AS sofa_liver,
   IFNULL(sofa.cardiovascular_sofa, 0) AS sofa_cardiovascular,
   IFNULL(sofa.cns_sofa, 0) AS sofa_cns,
   IFNULL(sofa.renal_sofa, 0) AS sofa_renal,
   IFNULL(aps.apsiii, 0) AS apsiii,
   aps.apsiii_prob,
   IFNULL(aps.hr_score, 0) AS apsiii_hr_score,
   IFNULL(aps.mbp_score, 0) AS apsiii_mbp_score,
   IFNULL(aps.temp_score, 0) AS apsiii_temp_score,
   IFNULL(aps.resp_rate_score, 0) AS apsiii_resp_rate_score,
   IFNULL(aps.pao2_aado2_score, 0) AS apsiii_pao2_aado2_score,
   IFNULL(aps.hematocrit_score, 0) AS apsiii_hematocrit_score,
   IFNULL(aps.wbc_score, 0) AS apsiii_wbc_score,
   IFNULL(aps.creatinine_score, 0) AS apsiii_creatinine_score,
   IFNULL(aps.uo_score, 0) AS apsiii_uo_score,
   IFNULL(aps.bun_score, 0) AS apsiii_bun_score,
   IFNULL(aps.sodium_score, 0) AS apsiii_sodium_score,
   IFNULL(aps.albumin_score, 0) AS apsiii_albumin_score,
   IFNULL(aps.bilirubin_score, 0) AS apsiii_bilirubin_score,
   IFNULL(aps.glucose_score, 0) AS apsiii_glucose_score,
   IFNULL(aps.acidbase_score, 0) AS apsiii_acidbase_score,
   IFNULL(aps.gcs_score, 0) AS apsiii_gcs_score,
   IFNULL(saps.sapsii, 0) AS sapsii,
   saps.sapsii_prob,
   IFNULL(saps.age_score, 0) AS sapsii_age_score,
   IFNULL(saps.hr_score, 0) AS sapsii_hr_score,
   IFNULL(saps.sysbp_score, 0) AS sapsii_sysbp_score,
   IFNULL(saps.temp_score, 0) AS sapsii_temp_score,
   IFNULL(saps.pao2fio2_score, 0) AS sapsii_pao2fio2_score,
   IFNULL(saps.uo_score, 0) AS sapsii_uo_score,
   IFNULL(saps.bun_score, 0) AS sapsii_bun_score,
   IFNULL(saps.wbc_score, 0) AS sapsii_wbc_score,
   IFNULL(saps.potassium_score, 0) AS sapsii_potassium_score,
   IFNULL(saps.sodium_score, 0) AS sapsii_sodium_score,
   IFNULL(saps.bicarbonate_score, 0) AS sapsii_bicarbonate_score,
   IFNULL(saps.bilirubin_score, 0) AS sapsii_bilirubin_score,
   IFNULL(saps.gcs_score, 0) AS sapsii_gcs_score,
   IFNULL(saps.comorbidity_score, 0) AS sapsii_comorbidity_score,
   IFNULL(saps.admissiontype_score, 0) AS sapsii_admissiontype_score,
   IFNULL(oasis.oasis, 0) AS oasis,
   oasis.oasis_prob,
   IFNULL(oasis.age_score, 0) AS oasis_age_score,
   IFNULL(oasis.preiculos_score, 0) AS oasis_preiculos_score,
   IFNULL(oasis.gcs_score, 0) AS oasis_gcs_score,
   IFNULL(oasis.heart_rate_score, 0) AS oasis_heart_rate_score,
   IFNULL(oasis.mbp_score, 0) AS oasis_mbp_score,
   IFNULL(oasis.resp_rate_score, 0) AS oasis_resp_rate_score,
   IFNULL(oasis.temp_score, 0) AS oasis_temp_score,
   IFNULL(oasis.urineoutput_score, 0) AS oasis_urineoutput_score,
   IFNULL(oasis.mechvent_score, 0) AS oasis_mechvent_score,
   IFNULL(oasis.electivesurgery_score, 0) AS oasis_electivesurgery_score,
   IFNULL(cha.has_chf, 0) AS has_chf,
   IFNULL(cha.has_hypertension, 0) AS has_hypertension,
   IFNULL(cha.has_diabetes, 0) AS has_diabetes,
   IFNULL(cha.has_stroke_tia_thrombo, 0) AS has_stroke_tia_thrombo,
   IFNULL(cha.has_vascular_disease, 0) AS has_vascular_disease,
   mcars.mcars_score

FROM Base_Cohort bc
INNER JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON bc.hadm_id = adm.hadm_id
INNER JOIN `physionet-data.mimiciv_3_1_hosp.patients` pat ON bc.subject_id = pat.subject_id
INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` AS icu ON bc.stay_id = icu.stay_id
LEFT JOIN CICU_Info cicu ON bc.stay_id = cicu.stay_id
LEFT JOIN ACS_Codes acs ON bc.hadm_id = acs.hadm_id
LEFT JOIN Pregnancy_Codes AS preg ON bc.hadm_id = preg.hadm_id
LEFT JOIN Congenital_Codes AS con ON bc.hadm_id = con.hadm_id
LEFT JOIN Hematolymphoid_Malignancy_Codes AS hem_mal ON bc.hadm_id = hem_mal.hadm_id
LEFT JOIN Malignancy_Codes AS mal ON bc.hadm_id = mal.hadm_id
LEFT JOIN AIDS_Codes AS aids ON bc.hadm_id = aids.hadm_id
LEFT JOIN Confounder_Flags flags ON bc.hadm_id = flags.hadm_id
LEFT JOIN DNR_Flag dnr ON bc.hadm_id = dnr.hadm_id
LEFT JOIN Procedure_Flags proc ON bc.hadm_id = proc.hadm_id
LEFT JOIN Medication_Flags meds ON bc.hadm_id = meds.hadm_id
LEFT JOIN Baseline_Features AS bf ON bc.stay_id = bf.stay_id
LEFT JOIN Braden_Scores bs ON bc.hadm_id = bs.hadm_id
LEFT JOIN Comprehensive_Outcomes outcomes ON bc.stay_id = outcomes.stay_id
LEFT JOIN Outcome_Flags_ICD oicd ON bc.hadm_id = oicd.hadm_id
LEFT JOIN Readmission_Flag rf ON bc.hadm_id = rf.hadm_id
LEFT JOIN First_Scores fs ON bc.hadm_id = fs.hadm_id
LEFT JOIN Charlson_Index cci ON bc.hadm_id = cci.hadm_id
LEFT JOIN First_Day_SOFA sofa ON bc.stay_id = sofa.stay_id
LEFT JOIN APSIII_Score aps ON bc.stay_id = aps.stay_id
LEFT JOIN SAPSII_Score saps ON bc.stay_id = saps.stay_id
LEFT JOIN OASIS_Score oasis ON bc.stay_id = oasis.stay_id
LEFT JOIN CHA2DS2_VASc_Components cha ON bc.hadm_id = cha.hadm_id
LEFT JOIN ICU_Readmission_Flag AS icu_readmit ON bc.stay_id = icu_readmit.stay_id
LEFT JOIN Supplemental_Oxygen_Flag AS so2_flag ON bc.hadm_id = so2_flag.hadm_id
LEFT JOIN NIV_Flag AS niv_flag ON bc.hadm_id = niv_flag.hadm_id
LEFT JOIN IMV_Flag AS imv_flag ON bc.hadm_id = imv_flag.hadm_id
LEFT JOIN RRT_Flag AS rrt_flag ON bc.hadm_id = rrt_flag.hadm_id
LEFT JOIN LODS_Scores AS lods ON icu.stay_id = lods.stay_id
LEFT JOIN MCARS_Score mcars ON bc.stay_id = mcars.stay_id
LEFT JOIN Min_Heart_Rate_First_24h mhr ON bc.stay_id = mhr.stay_id
LEFT JOIN Patient_Prior_History hist ON bc.subject_id = hist.subject_id
ORDER BY bc.subject_id
);