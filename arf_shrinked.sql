CREATE OR REPLACE TABLE `my-mimic-research.my_results.arf_cohort_subset_corrected` AS
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
    SELECT hadm_id, STRING_AGG(icd_code, ', ' ORDER BY icd_code) AS hematolymphoid_icd_codes
    FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
    WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
      AND ((icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(20[0-8]|1985|2384|2387[2-6])')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^(C8[1-68]|C9[0-5]|D4[567]|C7952)')))
    GROUP BY hadm_id
),
Malignancy_Codes AS (
   SELECT hadm_id, STRING_AGG(icd_code, ', ') AS malignancy_icd_codes FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
   WHERE (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^(1[4-9]|20)')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C')) GROUP BY hadm_id
),
AIDS_Codes AS (
  SELECT hadm_id, STRING_AGG(icd_code, ', ') AS aids_icd_codes FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort) AND (icd_code IN ('B20', 'V08', '042') OR (icd_version = 10 AND icd_code LIKE 'O987%'))
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
  WHERE icd_code IN ('V4986', 'Z66') GROUP BY hadm_id
),
Comorbidities_Flags AS (
   SELECT hadm_id,
       MAX(CASE WHEN (icd_version = 10 AND icd_code LIKE 'E10%') OR (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^250\d[13]')) THEN 1 ELSE 0 END) AS has_t1d,
       MAX(CASE WHEN (icd_version = 10 AND icd_code LIKE 'E11%') OR (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^250\d[02]')) THEN 1 ELSE 0 END) AS has_t2d,
       MAX(CASE WHEN icd_code IN ('4010', '4011', '4019') OR (icd_version = 10 AND icd_code LIKE 'I10%') THEN 1 ELSE 0 END) AS has_hypertension,
       MAX(CASE WHEN (icd_version = 9 AND (icd_code LIKE '491%' OR icd_code LIKE '492%' OR icd_code LIKE '496%')) OR (icd_version = 10 AND (icd_code LIKE 'J41%' OR icd_code LIKE 'J42%' OR icd_code LIKE 'J43%' OR icd_code LIKE 'J44%')) THEN 1 ELSE 0 END) AS has_copd,
       MAX(CASE WHEN (icd_version = 9 AND icd_code IN ('515', '5160', '5161', '5162', '51630', '51631', '51632', '51633', '51634', '51635', '51636', '51637', '5164', '5165', '51661', '51662', '51663', '51664', '51669', '5169', '135', '5178', '7100', '7101', '7102', '7103', '7104', '71481')) OR (icd_version = 10 AND icd_code IN ('J8410', 'J8489', 'J8401', 'J8403', 'J8402', 'J84111', 'J84112', 'J84113', 'J84114', 'J84115', 'J842', 'J84116', 'J84117', 'J8481', 'J8482', 'J84841', 'J84842', 'J8483', 'J84843', 'J84848', 'J849', 'D860', 'D861', 'D862', 'J99', 'M3213', 'M3401', 'M3301', 'M3502', 'M3311', 'M3391', 'M3321', 'M0501', 'M0511', 'M0517', 'M0519')) THEN 1 ELSE 0 END) AS has_ild,
       MAX(CASE WHEN (icd_version = 9 AND icd_code LIKE '428%') OR (icd_version = 10 AND icd_code IN ('I110', 'I13', 'I9713', 'I0981') OR icd_code LIKE 'I50%') THEN 1 ELSE 0 END) AS has_heart_failure,
       MAX(CASE WHEN icd_code IN ('K702', 'K704', 'K740', 'K741', 'K742', 'K743', 'K744', 'K745', 'K746', '5715', '5712', '5716') OR (icd_version = 10 AND icd_code LIKE 'K703%') OR (icd_version = 10 AND icd_code LIKE 'K721%') THEN 1 ELSE 0 END) AS has_cirrhosis,
       MAX(CASE WHEN icd_code IN ('I150', 'I151','N19', 'N11', 'N12', 'N14', 'N15', 'N16', 'N00', 'N01', 'N02', 'N06', 'N07', 'N08', '5851', '5852', '5853', '5854', '5855', '5856', '5859', '28521', '403', '404', 'M3214', 'M3215', 'M3504', 'M350A') OR (icd_version = 10 AND (icd_code LIKE 'E102%' OR icd_code LIKE 'E112%' OR icd_code LIKE 'E132%' OR icd_code LIKE 'I12%' OR icd_code LIKE 'I13%' OR icd_code LIKE 'N18%' OR icd_code LIKE 'N03%' OR icd_code LIKE 'N04%' OR icd_code LIKE 'N05%')) THEN 1 ELSE 0 END) AS has_ckd,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^20[4-8]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C9[1-5]')) THEN 1 ELSE 0 END) AS has_leukemia,
       MAX(CASE WHEN (icd_version = 9 AND REGEXP_CONTAINS(icd_code, r'^20[0-2]')) OR (icd_version = 10 AND REGEXP_CONTAINS(icd_code, r'^C8[1-68]')) THEN 1 ELSE 0 END) AS has_lymphoma
   FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
   GROUP BY hadm_id
),
NIV_Flag AS (
  SELECT hadm_id, 1 AS had_niv
  FROM (
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.procedureevents` pe JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON pe.stay_id = i.stay_id WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort) AND pe.itemid = 225794
      UNION DISTINCT
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.chartevents` c JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
      WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort) AND ((c.itemid = 226732 AND c.value IN ('Bipap mask', 'CPAP mask')) OR (c.itemid = 229314 AND c.value IN ('DuoPaP', 'NIV', 'NIV-ST')))
  ) GROUP BY hadm_id
),
IMV_Flag AS (
  SELECT hadm_id, 1 AS had_imv
  FROM (
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.procedureevents` pe JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON pe.stay_id = i.stay_id WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort) AND pe.itemid = 225792
      UNION DISTINCT
      SELECT i.hadm_id FROM `physionet-data.mimiciv_3_1_icu.chartevents` c JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON c.stay_id = i.stay_id
      WHERE i.hadm_id IN (SELECT hadm_id FROM Core_Cohort)
        AND ((c.itemid = 226732 AND c.value IN ('Endotracheal tube', 'Trach mask')) OR (c.itemid = 223849 AND c.value IN ('CMV', 'APRV', 'APRV/Biphasic+ApnPress', 'APRV/Biphasic+ApnVol', 'APV (cmv)', 'Ambient', 'Apnea Ventilation', 'CMV/ASSIST', 'CMV/ASSIST/AutoFlow', 'CMV/AutoFlow', 'CPAP/PPS', 'CPAP/PSV', 'CPAP/PSV+Apn TCPL', 'CPAP/PSV+ApnPres', 'CPAP/PSV+ApnVol', 'MMV', 'MMV/AutoFlow', 'MMV/PSV', 'MMV/PSV/AutoFlow', 'P-CMV', 'PCV+', 'PCV+/PSV', 'PCV+Assist', 'PRES/AC', 'PRVC/AC', 'PRVC/SIMV', 'PSV/SBT', 'SIMV', 'SIMV/AutoFlow', 'SIMV/PRES', 'SIMV/PSV', 'SIMV/PSV/AutoFlow', 'SIMV/VOL', 'SYNCHRON MASTER', 'SYNCHRON SLAVE', 'VOL/AC')) OR (c.itemid = 229314 AND c.value IN ('APRV', 'APV (cmv)', 'Ambient', '(S) CMV', 'P-CMV', 'SIMV', 'APV (simv)', 'P-SIMV', 'VS', 'ASV')))
  ) GROUP BY hadm_id
),

Baseline_Features AS (
    WITH
    Time_Window AS (
      SELECT stay_id, intime, DATETIME_SUB(intime, INTERVAL 24 HOUR) as window_start, DATETIME_ADD(intime, INTERVAL 24 HOUR) as window_end
      FROM `physionet-data.mimiciv_3_1_icu.icustays` WHERE stay_id IN (SELECT stay_id FROM Core_Cohort)
    ),
    all_events_unioned AS (

        SELECT tw.stay_id, cc.hadm_id, ev.charttime, ev.itemid, CASE WHEN ev.itemid IN (229355, 229359, 229361, 229360) THEN ev.valuenum * 1000 ELSE ev.valuenum END AS valuenum
        FROM Time_Window tw INNER JOIN Core_Cohort cc ON tw.stay_id = cc.stay_id INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ev ON tw.stay_id = ev.stay_id
        WHERE ev.charttime BETWEEN tw.window_start AND tw.window_end
        UNION ALL

        SELECT tw.stay_id, cc.hadm_id, ev.charttime, ev.itemid,
            CASE

                WHEN ev.itemid IN (51199, 52769, 53132, 51253) THEN ev.valuenum / 1000

                ELSE ev.valuenum
            END AS valuenum
        FROM Time_Window tw INNER JOIN Core_Cohort cc ON tw.stay_id = cc.stay_id INNER JOIN `physionet-data.mimiciv_3_1_hosp.labevents` ev ON cc.hadm_id = ev.hadm_id
        WHERE ev.charttime BETWEEN tw.window_start AND tw.window_end
    ),
    events_ranked AS (
        SELECT stay_id, valuenum,
            CASE WHEN itemid=220045 THEN 'heart_rate' WHEN itemid IN (220052, 220181, 225312) THEN 'mbp' WHEN itemid=223761 THEN 'temp_f' WHEN itemid=223901 THEN 'gcs_motor' WHEN itemid=223900 THEN 'gcs_verbal' WHEN itemid=220739 THEN 'gcs_eye' WHEN itemid IN (50811, 51640, 51222) THEN 'hb' WHEN itemid IN (51300, 51301, 51755, 220546) THEN 'wbc' WHEN itemid=51248 THEN 'mch' WHEN itemid IN (51691, 51250) THEN 'mcv' WHEN itemid=51277 THEN 'rdw_cv' WHEN itemid IN (51704, 51265) THEN 'platelet_count' WHEN itemid IN (52075, 53133) THEN 'abs_neutrophil_lab' WHEN itemid IN (52769, 51133, 53132) THEN 'abs_lymphocyte_lab' WHEN itemid IN (51253, 52074) THEN 'abs_monocyte_lab' WHEN itemid IN (50931, 50809, 52569, 52027) THEN 'glucose_lab' WHEN itemid IN (50912, 52546) THEN 'creatinine_lab' WHEN itemid=50882 THEN 'bicarbonate_lab' WHEN itemid IN (50821, 220224) THEN 'pao2' WHEN itemid=220277 THEN 'spo2' WHEN itemid=223835 THEN 'fio2' WHEN itemid=50818 THEN 'paco2' WHEN itemid IN (50820, 223830) THEN 'ph_arterial' END AS concept,
            ROW_NUMBER() OVER (PARTITION BY stay_id, CASE WHEN itemid=220045 THEN 'heart_rate' WHEN itemid IN (220052, 220181, 225312) THEN 'mbp' WHEN itemid=223761 THEN 'temp_f' WHEN itemid=223901 THEN 'gcs_motor' WHEN itemid=223900 THEN 'gcs_verbal' WHEN itemid=220739 THEN 'gcs_eye' WHEN itemid IN (50811, 51640, 51222) THEN 'hb' WHEN itemid IN (51300, 51301, 51755, 220546) THEN 'wbc' WHEN itemid=51248 THEN 'mch' WHEN itemid IN (51691, 51250) THEN 'mcv' WHEN itemid=51277 THEN 'rdw_cv' WHEN itemid IN (51704, 51265) THEN 'platelet_count' WHEN itemid IN (52075, 53133) THEN 'abs_neutrophil_lab' WHEN itemid IN (52769, 51133, 53132) THEN 'abs_lymphocyte_lab' WHEN itemid IN (51253, 52074) THEN 'abs_monocyte_lab' WHEN itemid IN (50931, 50809, 52569, 52027) THEN 'glucose_lab' WHEN itemid IN (50912, 52546) THEN 'creatinine_lab' WHEN itemid=50882 THEN 'bicarbonate_lab' WHEN itemid IN (50821, 220224) THEN 'pao2' WHEN itemid=220277 THEN 'spo2' WHEN itemid=223835 THEN 'fio2' WHEN itemid=50818 THEN 'paco2' WHEN itemid IN (50820, 223830) THEN 'ph_arterial' END ORDER BY charttime ASC) AS rn
        FROM all_events_unioned
    ),
    First_Day_Values AS (
        SELECT stay_id,
            MAX(CASE WHEN concept = 'heart_rate' THEN valuenum END) AS first_heart_rate, MAX(CASE WHEN concept = 'mbp' THEN valuenum END) AS first_mbp, MAX(CASE WHEN concept = 'temp_f' THEN valuenum END) AS first_temp_f, MAX(CASE WHEN concept = 'gcs_motor' THEN valuenum END) AS first_gcs_motor, MAX(CASE WHEN concept = 'gcs_verbal' THEN valuenum END) AS first_gcs_verbal, MAX(CASE WHEN concept = 'gcs_eye' THEN valuenum END) AS first_gcs_eye,
            MAX(CASE WHEN concept = 'hb' THEN valuenum END) AS first_hb, MAX(CASE WHEN concept = 'wbc' THEN valuenum END) AS first_wbc, MAX(CASE WHEN concept = 'mch' THEN valuenum END) AS first_mch, MAX(CASE WHEN concept = 'mcv' THEN valuenum END) AS first_mcv,
            MAX(CASE WHEN concept = 'rdw_cv' THEN valuenum END) AS first_rdw_cv, MAX(CASE WHEN concept = 'platelet_count' THEN valuenum END) AS first_platelet_count, MAX(CASE WHEN concept = 'abs_neutrophil_lab' THEN valuenum END) AS first_abs_neutrophil_lab, MAX(CASE WHEN concept = 'abs_lymphocyte_lab' THEN valuenum END) AS first_abs_lymphocyte_lab, MAX(CASE WHEN concept = 'abs_monocyte_lab' THEN valuenum END) AS first_abs_monocyte_lab,
            MAX(CASE WHEN concept = 'glucose_lab' THEN valuenum END) AS first_glucose_lab, MAX(CASE WHEN concept = 'creatinine_lab' THEN valuenum END) AS first_creatinine_lab, MAX(CASE WHEN concept = 'bicarbonate_lab' THEN valuenum END) AS first_bicarbonate_lab,
            MAX(CASE WHEN concept = 'pao2' THEN valuenum END) AS first_pao2, MAX(CASE WHEN concept = 'spo2' THEN valuenum END) AS first_spo2, MAX(CASE WHEN concept = 'fio2' THEN valuenum END) AS first_fio2,
            MAX(CASE WHEN concept = 'paco2' THEN valuenum END) AS first_paco2, MAX(CASE WHEN concept = 'ph_arterial' THEN valuenum END) AS first_ph_arterial
        FROM events_ranked WHERE rn = 1
        GROUP BY stay_id
    )
    SELECT cohort.subject_id, cohort.hadm_id, cohort.stay_id, fdv.* EXCEPT(stay_id)
    FROM Core_Cohort AS cohort LEFT JOIN First_Day_Values as fdv ON cohort.stay_id = fdv.stay_id
),


Outcomes_Base AS (
  SELECT core.subject_id, core.hadm_id, core.stay_id, icu.intime, adm.admittime, adm.dischtime, adm.deathtime, pat.dod
  FROM Core_Cohort core
  LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON core.stay_id = icu.stay_id
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON core.hadm_id = adm.hadm_id
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.patients` pat ON core.subject_id = pat.subject_id
),
Comprehensive_Outcomes AS (
  SELECT ob.stay_id, COALESCE(ob.deathtime, IF(adm.hospital_expire_flag = 1, ob.dischtime, NULL), CAST(ob.dod AS DATETIME)) AS death_datetime,
      CASE WHEN ob.dod IS NOT NULL OR ob.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN 1 ELSE 0 END AS overall_mortality_flag,
      CASE WHEN ob.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN 1 ELSE 0 END AS in_hospital_mortality_flag,
      DATETIME_DIFF(ob.dischtime, ob.admittime, HOUR) / 24.0 AS hospital_los_days, LEAST(icu.los, DATETIME_DIFF(ob.dischtime, ob.admittime, HOUR) / 24.0) AS icu_los_days,
      CASE WHEN ob.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN DATETIME_DIFF(COALESCE(ob.deathtime, ob.dischtime), ob.admittime, HOUR) / 24.0 ELSE NULL END AS in_hospital_mortality_duration,
      CASE WHEN ob.dod IS NOT NULL OR ob.deathtime IS NOT NULL OR adm.hospital_expire_flag = 1 THEN DATETIME_DIFF(COALESCE(ob.deathtime, CAST(ob.dod AS DATETIME), ob.dischtime), ob.admittime, HOUR) / 24.0 ELSE NULL END AS overall_mortality_duration
  FROM Outcomes_Base ob
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON ob.hadm_id = adm.hadm_id
  LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ob.stay_id = icu.stay_id
),
Proc_Events_Outcomes AS (
   SELECT stay_id, SUM(CASE WHEN itemid = 225792 THEN value END) AS duration_invasive_vent, MAX(CASE WHEN itemid = 227194 THEN 1 ELSE 0 END) AS extubation_flag
   FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
   WHERE stay_id IN (SELECT stay_id FROM Outcomes_Base) GROUP BY stay_id
),
Hospital_Readmission AS (
  WITH next_admission AS (SELECT hadm_id, subject_id, dischtime, LEAD(admittime, 1) OVER (PARTITION BY subject_id ORDER BY admittime) as next_admittime FROM `physionet-data.mimiciv_3_1_hosp.admissions` WHERE subject_id IN (SELECT subject_id FROM Core_Cohort))
  SELECT hadm_id, CASE WHEN next_admittime IS NOT NULL AND DATETIME_DIFF(next_admittime, dischtime, DAY) <= 30 THEN 1 ELSE 0 END AS readmission_30_day
  FROM next_admission WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)
),
ICU_Readmission_Flag AS (
  SELECT stay_id, CASE WHEN next_hadm_id = hadm_id AND DATETIME_DIFF(next_icu_intime, outtime, DAY) > 1 AND DATETIME_DIFF(next_icu_intime, outtime, DAY) <= 30 THEN 1 ELSE 0 END AS icu_readmission_30_day
  FROM (SELECT stay_id, subject_id, hadm_id, intime, outtime, LEAD(hadm_id, 1) OVER (PARTITION BY subject_id ORDER BY intime ASC) as next_hadm_id, LEAD(intime, 1) OVER (PARTITION BY subject_id ORDER BY intime ASC) as next_icu_intime
    FROM `physionet-data.mimiciv_3_1_icu.icustays` WHERE subject_id IN (SELECT subject_id FROM Core_Cohort)) AS ranked_stays
  WHERE stay_id IN (SELECT stay_id FROM Core_Cohort)
),
All_IMV_Events AS (
    SELECT stay_id, starttime AS event_time FROM `physionet-data.mimiciv_3_1_icu.procedureevents` WHERE itemid = 225792 AND stay_id IN (SELECT stay_id FROM Core_Cohort)
    UNION ALL SELECT stay_id, charttime AS event_time FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 226732 AND value IN ('Endotracheal tube', 'Trach mask') AND stay_id IN (SELECT stay_id FROM Core_Cohort)
    UNION ALL SELECT stay_id, charttime AS event_time FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IN ('CMV', 'APRV', 'APRV/Biphasic+ApnPress', 'APRV/Biphasic+ApnVol', 'APV (cmv)', 'Ambient', 'Apnea Ventilation', 'CMV/ASSIST', 'CMV/ASSIST/AutoFlow', 'CMV/AutoFlow', 'CPAP/PPS', 'CPAP/PSV', 'CPAP/PSV+Apn TCPL', 'CPAP/PSV+ApnPres', 'CPAP/PSV+ApnVol', 'MMV', 'MMV/AutoFlow', 'MMV/PSV', 'MMV/PSV/AutoFlow', 'P-CMV', 'PCV+', 'PCV+/PSV', 'PCV+Assist', 'PRES/AC', 'PRVC/AC', 'PRVC/SIMV', 'PSV/SBT', 'SIMV', 'SIMV/AutoFlow', 'SIMV/PRES', 'SIMV/PSV', 'SIMV/PSV/AutoFlow', 'SIMV/VOL', 'SYNCHRON MASTER', 'SYNCHRON SLAVE', 'VOL/AC') AND stay_id IN (SELECT stay_id FROM Core_Cohort)
    UNION ALL SELECT stay_id, charttime AS event_time FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 229314 AND value IN ('APRV', 'APV (cmv)', 'Ambient', '(S) CMV', 'P-CMV', 'SIMV', 'APV (simv)', 'P-SIMV', 'VS', 'ASV') AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
First_SBT_Time AS (
    SELECT stay_id, MIN(charttime) AS sbt_time FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid = 224715 AND value = 'Yes' AND stay_id IN (SELECT stay_id FROM Core_Cohort) GROUP BY stay_id
),
First_Extubation_Time AS (
    SELECT stay_id, MIN(starttime) AS extubation_time FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
    WHERE itemid = 227194 AND stay_id IN (SELECT stay_id FROM Core_Cohort) GROUP BY stay_id
),
First_IMV_Time AS (SELECT stay_id, MIN(event_time) AS first_imv_time FROM All_IMV_Events GROUP BY stay_id),
Weaning_Attempt AS (
    SELECT imv.stay_id, ext.extubation_time FROM First_IMV_Time imv
    INNER JOIN First_SBT_Time sbt ON imv.stay_id = sbt.stay_id INNER JOIN First_Extubation_Time ext ON imv.stay_id = ext.stay_id
    WHERE imv.first_imv_time < ext.extubation_time AND sbt.sbt_time <= ext.extubation_time
),
Post_Extubation_Reintubation AS (
    SELECT wa.stay_id, wa.extubation_time, MIN(imv.event_time) as reintubation_time
    FROM Weaning_Attempt wa LEFT JOIN All_IMV_Events imv ON wa.stay_id = imv.stay_id AND imv.event_time > wa.extubation_time
    GROUP BY wa.stay_id, wa.extubation_time
),
Weaning_Outcomes_Calculation AS (
    SELECT att.stay_id,
        CASE WHEN (re.reintubation_time IS NOT NULL AND DATETIME_DIFF(re.reintubation_time, att.extubation_time, HOUR) <= 48) OR (ob.deathtime IS NOT NULL AND DATETIME_DIFF(ob.deathtime, att.extubation_time, HOUR) <= 48) THEN 1 ELSE 0 END AS weaning_failure,
        CASE WHEN (re.reintubation_time IS NOT NULL AND DATETIME_DIFF(re.reintubation_time, att.extubation_time, HOUR) > 48 AND DATETIME_DIFF(re.reintubation_time, att.extubation_time, DAY) <= 7) OR (ob.deathtime IS NOT NULL AND DATETIME_DIFF(ob.deathtime, att.extubation_time, HOUR) > 48 AND DATETIME_DIFF(ob.deathtime, att.extubation_time, DAY) <= 7) THEN 1 ELSE 0 END AS weaning_indeterminate,
        CASE WHEN (re.reintubation_time IS NULL OR DATETIME_DIFF(re.reintubation_time, att.extubation_time, DAY) > 7) AND (ob.deathtime IS NULL OR DATETIME_DIFF(ob.deathtime, att.extubation_time, DAY) > 7) THEN 1 ELSE 0 END AS weaning_success
    FROM Weaning_Attempt att LEFT JOIN Post_Extubation_Reintubation re ON att.stay_id = re.stay_id LEFT JOIN Outcomes_Base ob ON att.stay_id = ob.stay_id
),
Weaning_Status_Final AS (
    SELECT stay_id, weaning_failure, CASE WHEN weaning_failure = 1 THEN 0 ELSE weaning_indeterminate END as weaning_indeterminate, CASE WHEN weaning_failure = 1 OR weaning_indeterminate = 1 THEN 0 ELSE weaning_success END as weaning_success,
        CASE WHEN weaning_failure = 1 THEN 1 WHEN weaning_indeterminate = 1 AND weaning_failure = 0 THEN 2 WHEN weaning_success = 1 AND weaning_failure = 0 AND weaning_indeterminate = 0 THEN 3 ELSE 0 END AS weaning_outcome_status
    FROM Weaning_Outcomes_Calculation
),
All_NIV_Events AS (
    SELECT stay_id, starttime AS event_time FROM `physionet-data.mimiciv_3_1_icu.procedureevents` WHERE itemid = 225794 AND stay_id IN (SELECT stay_id FROM Core_Cohort)
    UNION ALL SELECT stay_id, charttime AS event_time FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 226732 AND value IN ('Bipap mask', 'CPAP mask') AND stay_id IN (SELECT stay_id FROM Core_Cohort)
    UNION ALL SELECT stay_id, charttime AS event_time FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 229314 AND value IN ('DuoPaP', 'NIV', 'NIV-ST') AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
First_Vent_Times AS (
    SELECT stay_id, MIN(CASE WHEN vent_type = 'NIV' THEN event_time END) as first_niv_time, MIN(CASE WHEN vent_type = 'IMV' THEN event_time END) as first_imv_time
    FROM (SELECT stay_id, event_time, 'NIV' as vent_type FROM All_NIV_Events UNION ALL SELECT stay_id, event_time, 'IMV' as vent_type FROM All_IMV_Events) AS all_vents
    GROUP BY stay_id
),
NIV_Failure_Calculation AS (
    SELECT c.stay_id,
        CASE WHEN fvt.first_niv_time IS NULL AND fvt.first_imv_time IS NULL THEN 'N/A'
             WHEN fvt.first_imv_time IS NOT NULL AND (fvt.first_niv_time IS NULL OR fvt.first_imv_time <= fvt.first_niv_time) THEN 'IMV_FIRST'
             WHEN fvt.first_imv_time IS NOT NULL AND fvt.first_imv_time > fvt.first_niv_time THEN 'Yes'
             WHEN ob.deathtime IS NOT NULL AND ob.deathtime > fvt.first_niv_time AND fvt.first_imv_time IS NULL THEN 'Yes'
             WHEN fvt.first_niv_time IS NOT NULL THEN 'No'
             ELSE 'N/A'
        END AS niv_failure
    FROM Core_Cohort c LEFT JOIN First_Vent_Times fvt ON c.stay_id = fvt.stay_id LEFT JOIN Outcomes_Base ob ON c.stay_id = ob.stay_id
),
Ventilation_Events AS (
   SELECT stay_id, starttime as event_time, 'Invasive' as vent_type FROM `physionet-data.mimiciv_3_1_icu.procedureevents` WHERE itemid = 225792 AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL SELECT stay_id, charttime as event_time, 'Invasive' as vent_type FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 226732 AND value IN ('Endotracheal tube', 'Trach mask') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL SELECT stay_id, charttime as event_time, 'Invasive' as vent_type FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IN ('CMV', 'APRV', 'APRV/Biphasic+ApnPress', 'APRV/Biphasic+ApnVol', 'APV (cmv)', 'Ambient', 'Apnea Ventilation', 'CMV/ASSIST', 'CMV/ASSIST/AutoFlow', 'CMV/AutoFlow', 'P-CMV', 'PCV+', 'PCV+/PSV', 'PCV+Assist', 'PRES/AC', 'PRVC/AC', 'PRVC/SIMV', 'SIMV', 'SIMV/AutoFlow', 'SIMV/PRES', 'SIMV/PSV', 'SIMV/PSV/AutoFlow', 'SIMV/VOL', 'SYNCHRON MASTER', 'SYNCHRON SLAVE', 'VOL/AC') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
   UNION ALL SELECT stay_id, charttime as event_time, 'Invasive' as vent_type FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 229314 AND value IN ('APRV', 'APV (cmv)', 'Ambient', '(S) CMV', 'P-CMV', 'SIMV', 'APV (simv)', 'P-SIMV', 'VS', 'ASV') AND stay_id IN (SELECT stay_id FROM Outcomes_Base)
),
First_Invasive_Vent_Time AS (
    SELECT stay_id, MIN(event_time) as first_invasive_time FROM Ventilation_Events WHERE vent_type = 'Invasive' GROUP BY stay_id
),
NIV_Procedure_Durations AS (
    SELECT stay_id, starttime, endtime FROM `physionet-data.mimiciv_3_1_icu.procedureevents`
    WHERE itemid = 225794 AND stay_id IN (SELECT stay_id FROM Core_Cohort)
),
NIVF_Durations AS (
    SELECT niv.stay_id, DATETIME_DIFF(LEAST(niv.endtime, IFNULL(fiv.first_invasive_time, niv.endtime)), niv.starttime, MINUTE) AS duration_minutes
    FROM NIV_Procedure_Durations AS niv LEFT JOIN First_Invasive_Vent_Time AS fiv ON niv.stay_id = fiv.stay_id
    WHERE niv.starttime < IFNULL(fiv.first_invasive_time, DATETIME '9999-12-31 23:59:59')
),
NIVF_Total_Duration AS (SELECT stay_id, SUM(duration_minutes) AS duration_NIVF FROM NIVF_Durations GROUP BY stay_id),
sbt_start_times AS (
  SELECT stay_id, MIN(charttime) AS sbt_starttime
  FROM `physionet-data.mimiciv_3_1_icu.chartevents`
  WHERE itemid = 224715 AND value = 'Yes' AND stay_id IN (SELECT stay_id FROM Core_Cohort) GROUP BY stay_id
),
ventilation_events_sofa AS (SELECT stay_id, charttime FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Core_Cohort)),
vasoactive_agent_sofa AS (SELECT stay_id, itemid, rate AS vaso_rate FROM `physionet-data.mimiciv_3_1_icu.inputevents` WHERE itemid IN (221906, 221289, 221662, 221653) AND stay_id IN (SELECT stay_id FROM Core_Cohort)),
pafi_sofa AS (SELECT ie.stay_id, MIN(CASE WHEN vd.stay_id IS NULL THEN pao2fio2ratio END) AS pao2fio2ratio_novent, MIN(CASE WHEN vd.stay_id IS NOT NULL THEN pao2fio2ratio END) AS pao2fio2ratio_vent FROM Core_Cohort ie INNER JOIN `physionet-data.mimiciv_3_1_derived.bg` bg ON ie.hadm_id = bg.hadm_id LEFT JOIN ventilation_events_sofa vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id WHERE bg.specimen = 'ART.' AND bg.charttime BETWEEN icu.intime AND DATETIME_ADD(icu.intime, INTERVAL '1' DAY) GROUP BY ie.stay_id),
vasopressors_sofa AS (SELECT stay_id, MAX(CASE WHEN itemid = 221906 THEN vaso_rate END) AS rate_norepinephrine, MAX(CASE WHEN itemid = 221289 THEN vaso_rate END) AS rate_epinephrine, MAX(CASE WHEN itemid = 221662 THEN vaso_rate END) AS rate_dopamine, MAX(CASE WHEN itemid = 221653 THEN vaso_rate END) AS rate_dobutamine FROM vasoactive_agent_sofa GROUP BY stay_id),
sofa_scorecomp AS (SELECT ie.stay_id, pf.pao2fio2ratio_novent, pf.pao2fio2ratio_vent, labs.platelets_min, labs.bilirubin_total_max AS bilirubin_max, vital.mbp_min, vaso.rate_norepinephrine, vaso.rate_epinephrine, vaso.rate_dopamine, vaso.rate_dobutamine, gcs.gcs_min, labs.creatinine_max, uo.urineoutput AS uo_24hr FROM Core_Cohort ie LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id LEFT JOIN pafi_sofa pf ON ie.stay_id = pf.stay_id LEFT JOIN vasopressors_sofa vaso ON ie.stay_id = vaso.stay_id),
final_sofa_scores AS (
   SELECT stay_id,
       CASE WHEN pao2fio2ratio_vent < 100 THEN 4 WHEN pao2fio2ratio_vent < 200 THEN 3 WHEN pao2fio2ratio_novent < 300 THEN 2 WHEN pao2fio2ratio_vent < 300 THEN 2 WHEN pao2fio2ratio_novent < 400 THEN 1 WHEN pao2fio2ratio_vent < 400 THEN 1 ELSE 0 END AS respiration_sofa,
       CASE WHEN platelets_min < 20 THEN 4 WHEN platelets_min < 50 THEN 3 WHEN platelets_min < 100 THEN 2 WHEN platelets_min < 150 THEN 1 ELSE 0 END AS coagulation_sofa,
       CASE WHEN bilirubin_max >= 12.0 THEN 4 WHEN bilirubin_max >= 6.0 THEN 3 WHEN bilirubin_max >= 2.0 THEN 2 WHEN bilirubin_max >= 1.2 THEN 1 ELSE 0 END AS liver_sofa,
       CASE WHEN rate_dopamine > 15 OR rate_epinephrine > 0.1 OR rate_norepinephrine > 0.1 THEN 4 WHEN rate_dopamine > 5 OR rate_epinephrine <= 0.1 OR rate_norepinephrine <= 0.1 THEN 3 WHEN rate_dopamine > 0 OR rate_dobutamine > 0 THEN 2 WHEN mbp_min < 70 THEN 1 ELSE 0 END AS cardiovascular_sofa,
       CASE WHEN (gcs_min >= 13 AND gcs_min <= 14) THEN 1 WHEN (gcs_min >= 10 AND gcs_min <= 12) THEN 2 WHEN (gcs_min >= 6 AND gcs_min <= 9) THEN 3 WHEN gcs_min < 6 THEN 4 ELSE 0 END AS cns_sofa,
       CASE WHEN (creatinine_max >= 5.0) THEN 4 WHEN uo_24hr < 200 THEN 4 WHEN (creatinine_max >= 3.5 AND creatinine_max < 5.0) THEN 3 WHEN uo_24hr < 500 THEN 3 WHEN (creatinine_max >= 2.0 AND creatinine_max < 3.5) THEN 2 WHEN (creatinine_max >= 1.2 AND creatinine_max < 2.0) THEN 1 ELSE 0 END AS renal_sofa
   FROM sofa_scorecomp
),
sirs_scorecomp AS (SELECT ie.stay_id, v.temperature_min, v.temperature_max, v.heart_rate_max, v.resp_rate_max, bg.pco2_min AS paco2_min, l.wbc_min, l.wbc_max, l.bands_max FROM Core_Cohort ie LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_bg_art` bg ON ie.stay_id = bg.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` v ON ie.stay_id = v.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` l ON ie.stay_id = l.stay_id),
sirs_scorecalc AS (SELECT stay_id, CASE WHEN temperature_min < 36.0 THEN 1 WHEN temperature_max > 38.0 THEN 1 ELSE 0 END AS temp_score, CASE WHEN heart_rate_max > 90.0 THEN 1 ELSE 0 END AS heart_rate_score, CASE WHEN resp_rate_max > 20.0 THEN 1 WHEN paco2_min < 32.0 THEN 1 ELSE 0 END AS resp_score, CASE WHEN wbc_min < 4.0 THEN 1 WHEN wbc_max > 12.0 THEN 1 WHEN bands_max > 10 THEN 1 ELSE 0 END AS wbc_score FROM sirs_scorecomp),
co_sapsii AS (SELECT ie.subject_id, ie.hadm_id, ie.stay_id, icu.intime AS starttime, DATETIME_ADD(icu.intime, INTERVAL '24' HOUR) AS endtime FROM Core_Cohort ie INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id),
ventilation_events_sapsii AS (SELECT stay_id, charttime, CASE WHEN itemid = 223849 THEN 'InvasiveVent' END AS ventilation_status FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Core_Cohort)),
cpap_sapsii AS (SELECT co.stay_id, GREATEST(MIN(DATETIME_SUB(charttime, INTERVAL '1' HOUR)), co.starttime) AS starttime, LEAST(MAX(DATETIME_ADD(charttime, INTERVAL '4' HOUR)), co.endtime) AS endtime FROM co_sapsii co INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON co.stay_id = ce.stay_id AND ce.charttime > co.starttime AND ce.charttime <= co.endtime WHERE ce.itemid = 226732 AND REGEXP_CONTAINS(LOWER(ce.value), '(cpap mask|bipap)') GROUP BY co.stay_id, co.starttime, co.endtime),
surgflag_sapsii AS (SELECT adm.hadm_id, CASE WHEN LOWER(curr_service) LIKE '%surg%' THEN 1 ELSE 0 END AS surgical, ROW_NUMBER() OVER (PARTITION BY adm.hadm_id ORDER BY transfertime) AS serviceorder FROM `physionet-data.mimiciv_3_1_hosp.admissions` adm LEFT JOIN `physionet-data.mimiciv_3_1_hosp.services` se ON adm.hadm_id = se.hadm_id WHERE adm.hadm_id IN (SELECT hadm_id FROM Core_Cohort)),
comorb_sapsii AS (SELECT hadm_id, MAX(CASE WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 3) BETWEEN '042' AND '044' THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'B20' AND 'B22' THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) = 'B24' THEN 1 ELSE 0 END) AS aids, MAX(CASE WHEN icd_version = 9 AND (SUBSTR(icd_code, 1, 5) BETWEEN '20000' AND '20302' OR SUBSTR(icd_code, 1, 5) BETWEEN '20310' AND '20312' OR SUBSTR(icd_code, 1, 5) BETWEEN '20302' AND '20382' OR SUBSTR(icd_code, 1, 5) BETWEEN '20400' AND '20892' OR SUBSTR(icd_code, 1, 4) IN ('2386', '2733')) THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'C81' AND 'C96' THEN 1 ELSE 0 END) AS hem, MAX(CASE WHEN icd_version = 9 AND (SUBSTR(icd_code, 1, 4) BETWEEN '1960' AND '1991' OR SUBSTR(icd_code, 1, 5) BETWEEN '20970' AND '20975' OR SUBSTR(icd_code, 1, 5) IN ('20979', '78951')) THEN 1 WHEN icd_version = 10 AND (SUBSTR(icd_code, 1, 3) BETWEEN 'C77' AND 'C79' OR SUBSTR(icd_code, 1, 4) = 'C800') THEN 1 ELSE 0 END) AS mets FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort) GROUP BY hadm_id),
pafi1_sapsii AS (SELECT co.stay_id, bg.charttime, pao2fio2ratio AS pao2fio2, CASE WHEN vd.stay_id IS NOT NULL THEN 1 ELSE 0 END AS vent, CASE WHEN cp.stay_id IS NOT NULL THEN 1 ELSE 0 END AS cpap FROM co_sapsii co LEFT JOIN `physionet-data.mimiciv_3_1_derived.bg` bg ON co.subject_id = bg.subject_id AND bg.specimen = 'ART.' AND bg.charttime > co.starttime AND bg.charttime <= co.endtime LEFT JOIN ventilation_events_sapsii vd ON co.stay_id = vd.stay_id AND bg.charttime = vd.charttime LEFT JOIN cpap_sapsii cp ON co.stay_id = cp.stay_id AND bg.charttime > cp.starttime AND bg.charttime <= cp.endtime),
pafi2_sapsii AS (SELECT stay_id, MIN(pao2fio2) AS pao2fio2_vent_min FROM pafi1_sapsii WHERE vent = 1 OR cpap = 1 GROUP BY stay_id),
sapsii_cohort AS (SELECT ie.subject_id, ie.hadm_id, ie.stay_id, va.age, vital.heart_rate_max, vital.heart_rate_min, vital.sbp_max, vital.sbp_min, vital.temperature_max AS tempc_max, vital.temperature_min AS tempc_min, pf.pao2fio2_vent_min, uo.urineoutput, labs.bun_min, labs.bun_max, labs.wbc_min, labs.wbc_max, labs.potassium_min, labs.potassium_max, labs.sodium_min, labs.sodium_max, labs.bicarbonate_min, labs.bicarbonate_max, labs.bilirubin_total_min AS bilirubin_min, labs.bilirubin_total_max AS bilirubin_max, gcs.gcs_min AS mingcs, comorb.aids, comorb.hem, comorb.mets, CASE WHEN adm.admission_type = 'ELECTIVE' AND sf.surgical = 1 THEN 'ScheduledSurgical' WHEN adm.admission_type != 'ELECTIVE' AND sf.surgical = 1 THEN 'UnscheduledSurgical' ELSE 'Medical' END AS admissiontype FROM Core_Cohort ie INNER JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON ie.hadm_id = adm.hadm_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.age` va ON ie.hadm_id = va.hadm_id LEFT JOIN pafi2_sapsii pf ON ie.stay_id = pf.stay_id LEFT JOIN surgflag_sapsii sf ON adm.hadm_id = sf.hadm_id AND sf.serviceorder = 1 LEFT JOIN comorb_sapsii comorb ON ie.hadm_id = comorb.hadm_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id),
sapsii_scorecomp AS (SELECT cohort.*, CASE WHEN age < 40 THEN 0 WHEN age < 60 THEN 7 WHEN age < 70 THEN 12 WHEN age < 75 THEN 15 WHEN age < 80 THEN 16 WHEN age >= 80 THEN 18 END AS age_score, CASE WHEN heart_rate_min < 40 THEN 11 WHEN heart_rate_max >= 160 THEN 7 WHEN heart_rate_max >= 120 THEN 4 WHEN heart_rate_min < 70 THEN 2 ELSE 0 END AS hr_score, CASE WHEN sbp_min < 70 THEN 13 WHEN sbp_min < 100 THEN 5 WHEN sbp_max >= 200 THEN 2 ELSE 0 END AS sysbp_score, CASE WHEN tempc_max >= 39.0 THEN 3 ELSE 0 END AS temp_score, CASE WHEN pao2fio2_vent_min < 100 THEN 11 WHEN pao2fio2_vent_min < 200 THEN 9 WHEN pao2fio2_vent_min >= 200 THEN 6 END AS pao2fio2_score, CASE WHEN urineoutput < 500.0 THEN 11 WHEN urineoutput < 1000.0 THEN 4 ELSE 0 END AS uo_score, CASE WHEN bun_max < 28.0 THEN 0 WHEN bun_max < 84.0 THEN 6 WHEN bun_max >= 84.0 THEN 10 END AS bun_score, CASE WHEN wbc_min < 1.0 THEN 12 WHEN wbc_max >= 20.0 THEN 3 ELSE 0 END AS wbc_score, CASE WHEN potassium_min < 3.0 THEN 3 WHEN potassium_max >= 5.0 THEN 3 ELSE 0 END AS potassium_score, CASE WHEN sodium_min < 125 THEN 5 WHEN sodium_max >= 145 THEN 1 ELSE 0 END AS sodium_score, CASE WHEN bicarbonate_min < 15.0 THEN 6 WHEN bicarbonate_min < 20.0 THEN 3 ELSE 0 END AS bicarbonate_score, CASE WHEN bilirubin_max < 4.0 THEN 0 WHEN bilirubin_max < 6.0 THEN 4 WHEN bilirubin_max >= 6.0 THEN 9 END AS bilirubin_score, CASE WHEN mingcs < 3 THEN NULL WHEN mingcs < 6 THEN 26 WHEN mingcs < 9 THEN 13 WHEN mingcs < 11 THEN 7 WHEN mingcs < 14 THEN 5 WHEN mingcs >= 14 THEN 0 END AS gcs_score, CASE WHEN aids = 1 THEN 17 WHEN hem = 1 THEN 10 WHEN mets = 1 THEN 9 ELSE 0 END AS comorbidity_score, CASE WHEN admissiontype = 'ScheduledSurgical' THEN 0 WHEN admissiontype = 'Medical' THEN 6 WHEN admissiontype = 'UnscheduledSurgical' THEN 8 END AS admissiontype_score FROM sapsii_cohort cohort),
sapsii_scores AS (SELECT s.stay_id, (COALESCE(age_score, 0) + COALESCE(hr_score, 0) + COALESCE(sysbp_score, 0) + COALESCE(temp_score, 0) + COALESCE(pao2fio2_score, 0) + COALESCE(uo_score, 0) + COALESCE(bun_score, 0) + COALESCE(wbc_score, 0) + COALESCE(potassium_score, 0) + COALESCE(sodium_score, 0) + COALESCE(bicarbonate_score, 0) + COALESCE(bilirubin_score, 0) + COALESCE(gcs_score, 0) + COALESCE(comorbidity_score, 0) + COALESCE(admissiontype_score, 0)) AS sapsii FROM sapsii_scorecomp s),
aps_ventilation_events AS (SELECT stay_id, charttime FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Core_Cohort)),
pa AS (SELECT ie.stay_id, bg.charttime, po2 AS pao2, ROW_NUMBER() OVER (PARTITION BY ie.stay_id ORDER BY bg.po2 DESC) AS rn FROM `physionet-data.mimiciv_3_1_derived.bg` bg INNER JOIN Core_Cohort ie ON bg.hadm_id = ie.hadm_id LEFT JOIN aps_ventilation_events vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime WHERE vd.stay_id IS NULL AND COALESCE(fio2, fio2_chartevents, 21) < 50 AND bg.po2 IS NOT NULL AND bg.specimen = 'ART.'),
aa AS (SELECT ie.stay_id, bg.charttime, bg.aado2, ROW_NUMBER() OVER (PARTITION BY ie.stay_id ORDER BY bg.aado2 DESC) AS rn FROM `physionet-data.mimiciv_3_1_derived.bg` bg INNER JOIN Core_Cohort ie ON bg.hadm_id = ie.hadm_id INNER JOIN aps_ventilation_events vd ON ie.stay_id = vd.stay_id AND bg.charttime = vd.charttime WHERE vd.stay_id IS NOT NULL AND COALESCE(fio2, fio2_chartevents) >= 50 AND bg.aado2 IS NOT NULL AND bg.specimen = 'ART.'),
acidbase AS (SELECT ie.stay_id, ph, pco2 AS paco2, CASE WHEN ph < 7.20 THEN CASE WHEN pco2 < 50 THEN 12 ELSE 4 END WHEN ph < 7.30 THEN CASE WHEN pco2 < 30 THEN 9 WHEN pco2 < 40 THEN 6 WHEN pco2 < 50 THEN 3 ELSE 2 END WHEN ph < 7.35 THEN CASE WHEN pco2 < 30 THEN 9 WHEN pco2 < 45 THEN 0 ELSE 1 END WHEN ph < 7.45 THEN CASE WHEN pco2 < 30 THEN 5 WHEN pco2 < 45 THEN 0 ELSE 1 END WHEN ph < 7.50 THEN CASE WHEN pco2 < 30 THEN 5 WHEN pco2 < 35 THEN 0 WHEN pco2 < 45 THEN 2 ELSE 12 END WHEN ph < 7.60 THEN CASE WHEN pco2 < 40 THEN 3 ELSE 12 END ELSE CASE WHEN pco2 < 25 THEN 0 WHEN pco2 < 40 THEN 3 ELSE 12 END END AS acidbase_score FROM `physionet-data.mimiciv_3_1_derived.bg` bg INNER JOIN Core_Cohort ie ON bg.hadm_id = ie.hadm_id WHERE ph IS NOT NULL AND pco2 IS NOT NULL AND bg.specimen = 'ART.'),
acidbase_max AS (SELECT stay_id, acidbase_score, ph, paco2, ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY acidbase_score DESC) AS acidbase_rn FROM acidbase),
arf_aps AS (SELECT ie.stay_id, CASE WHEN labs.creatinine_max >= 1.5 AND uo.urineoutput < 410 AND icd.ckd = 0 THEN 1 ELSE 0 END AS arf FROM Core_Cohort ie LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id LEFT JOIN (SELECT hadm_id, MAX(CASE WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 4) IN ('5854', '5855', '5856') THEN 1 WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 4) IN ('N184', 'N185', 'N186') THEN 1 ELSE 0 END) AS ckd FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` GROUP BY hadm_id) icd ON ie.hadm_id = icd.hadm_id),
vent AS (SELECT ie.stay_id, MAX(CASE WHEN v.stay_id IS NOT NULL THEN 1 ELSE 0 END) AS vent FROM Core_Cohort ie LEFT JOIN aps_ventilation_events v ON ie.stay_id = v.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id WHERE DATETIME_DIFF(v.charttime, icu.intime, HOUR) < 24 GROUP BY ie.stay_id),
apsiii_cohort AS (SELECT ie.subject_id, ie.hadm_id, ie.stay_id, vital.heart_rate_min, vital.heart_rate_max, vital.mbp_min, vital.mbp_max, vital.temperature_min, vital.temperature_max, vital.resp_rate_min, vital.resp_rate_max, pa.pao2, aa.aado2, ab.ph, ab.paco2, ab.acidbase_score, labs.hematocrit_min, labs.hematocrit_max, labs.wbc_min, labs.wbc_max, labs.creatinine_min, labs.creatinine_max, labs.bun_min, labs.bun_max, labs.sodium_min, labs.sodium_max, labs.albumin_min, labs.albumin_max, labs.bilirubin_total_min AS bilirubin_min, labs.bilirubin_total_max AS bilirubin_max, GREATEST(labs.glucose_max, vital.glucose_max) AS glucose_max, LEAST(labs.glucose_min, vital.glucose_min) AS glucose_min, vent.vent, uo.urineoutput, gcs.gcs_min AS mingcs, gcs.gcs_motor, gcs.gcs_verbal, gcs.gcs_eyes, gcs.gcs_unable, arf_aps.arf AS arf FROM Core_Cohort ie LEFT JOIN pa ON ie.stay_id = pa.stay_id AND pa.rn = 1 LEFT JOIN aa ON ie.stay_id = aa.stay_id AND aa.rn = 1 LEFT JOIN acidbase_max ab ON ie.stay_id = ab.stay_id AND ab.acidbase_rn = 1 LEFT JOIN arf_aps ON ie.stay_id = arf_aps.stay_id LEFT JOIN vent ON ie.stay_id = vent.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id),
score_min AS (SELECT c.subject_id, c.hadm_id, c.stay_id, CASE WHEN heart_rate_min < 40 THEN 8 WHEN heart_rate_min < 50 THEN 5 WHEN heart_rate_min < 100 THEN 0 WHEN heart_rate_min < 110 THEN 1 WHEN heart_rate_min < 120 THEN 5 WHEN heart_rate_min < 140 THEN 7 WHEN heart_rate_min < 155 THEN 13 WHEN heart_rate_min >= 155 THEN 17 END AS hr_score, CASE WHEN mbp_min < 40 THEN 23 WHEN mbp_min < 60 THEN 15 WHEN mbp_min < 70 THEN 7 WHEN mbp_min < 80 THEN 6 WHEN mbp_min < 100 THEN 0 WHEN mbp_min < 120 THEN 4 WHEN mbp_min < 130 THEN 7 WHEN mbp_min < 140 THEN 9 WHEN mbp_min >= 140 THEN 10 END AS mbp_score, CASE WHEN temperature_min < 33.0 THEN 20 WHEN temperature_min < 33.5 THEN 16 WHEN temperature_min < 34.0 THEN 13 WHEN temperature_min < 35.0 THEN 8 WHEN temperature_min < 36.0 THEN 2 WHEN temperature_min < 40.0 THEN 0 WHEN temperature_min >= 40.0 THEN 4 END AS temp_score, CASE WHEN vent = 1 AND resp_rate_min < 14 THEN 0 WHEN resp_rate_min < 6 THEN 17 WHEN resp_rate_min < 12 THEN 8 WHEN resp_rate_min < 14 THEN 7 WHEN resp_rate_min < 25 THEN 0 WHEN resp_rate_min < 35 THEN 6 WHEN resp_rate_min < 40 THEN 9 WHEN resp_rate_min < 50 THEN 11 WHEN resp_rate_min >= 50 THEN 18 END AS resp_rate_score, CASE WHEN hematocrit_min < 41.0 THEN 3 WHEN hematocrit_min < 50.0 THEN 0 WHEN hematocrit_min >= 50.0 THEN 3 END AS hematocrit_score, CASE WHEN wbc_min < 1.0 THEN 19 WHEN wbc_min < 3.0 THEN 5 WHEN wbc_min < 20.0 THEN 0 WHEN wbc_min < 25.0 THEN 1 WHEN wbc_min >= 25.0 THEN 5 END AS wbc_score, CASE WHEN arf = 1 AND creatinine_min >= 1.5 THEN 10 WHEN arf = 1 THEN 0 WHEN creatinine_min < 0.5 THEN 3 WHEN creatinine_min < 1.5 THEN 0 WHEN creatinine_min < 1.95 THEN 4 WHEN creatinine_min >= 1.95 THEN 7 END AS creatinine_score, CASE WHEN bun_min < 17.0 THEN 0 WHEN bun_min < 20.0 THEN 2 WHEN bun_min < 40.0 THEN 7 WHEN bun_min < 80.0 THEN 11 WHEN bun_min >= 80.0 THEN 12 END AS bun_score, CASE WHEN sodium_min < 120 THEN 3 WHEN sodium_min < 135 THEN 2 WHEN sodium_min < 155 THEN 0 WHEN sodium_min >= 155 THEN 4 END AS sodium_score, CASE WHEN albumin_min < 2.0 THEN 11 WHEN albumin_min < 2.5 THEN 6 WHEN albumin_min < 4.5 THEN 0 WHEN albumin_min >= 4.5 THEN 4 END AS albumin_score, CASE WHEN bilirubin_min < 2.0 THEN 0 WHEN bilirubin_min < 3.0 THEN 5 WHEN bilirubin_min < 5.0 THEN 6 WHEN bilirubin_min < 8.0 THEN 8 WHEN bilirubin_min >= 8.0 THEN 16 END AS bilirubin_score, CASE WHEN glucose_min < 40 THEN 8 WHEN glucose_min < 60 THEN 9 WHEN glucose_min < 200 THEN 0 WHEN glucose_min < 350 THEN 3 WHEN glucose_min >= 350 THEN 5 END AS glucose_score FROM apsiii_cohort c),
score_max AS (SELECT c.subject_id, c.hadm_id, c.stay_id, CASE WHEN heart_rate_max < 40 THEN 8 WHEN heart_rate_max < 50 THEN 5 WHEN heart_rate_max < 100 THEN 0 WHEN heart_rate_max < 110 THEN 1 WHEN heart_rate_max < 120 THEN 5 WHEN heart_rate_max < 140 THEN 7 WHEN heart_rate_max < 155 THEN 13 WHEN heart_rate_max >= 155 THEN 17 END AS hr_score, CASE WHEN mbp_max < 40 THEN 23 WHEN mbp_max < 60 THEN 15 WHEN mbp_max < 70 THEN 7 WHEN mbp_max < 80 THEN 6 WHEN mbp_max < 100 THEN 0 WHEN mbp_max < 120 THEN 4 WHEN mbp_max < 130 THEN 7 WHEN mbp_max < 140 THEN 9 WHEN mbp_max >= 140 THEN 10 END AS mbp_score, CASE WHEN temperature_max < 33.0 THEN 20 WHEN temperature_max < 33.5 THEN 16 WHEN temperature_max < 34.0 THEN 13 WHEN temperature_max < 35.0 THEN 8 WHEN temperature_max < 36.0 THEN 2 WHEN temperature_max < 40.0 THEN 0 WHEN temperature_max >= 40.0 THEN 4 END AS temp_score, CASE WHEN vent = 1 AND resp_rate_max < 14 THEN 0 WHEN resp_rate_max < 6 THEN 17 WHEN resp_rate_max < 12 THEN 8 WHEN resp_rate_max < 14 THEN 7 WHEN resp_rate_max < 25 THEN 0 WHEN resp_rate_max < 35 THEN 6 WHEN resp_rate_max < 40 THEN 9 WHEN resp_rate_max < 50 THEN 11 WHEN resp_rate_max >= 50 THEN 18 END AS resp_rate_score, CASE WHEN hematocrit_max < 41.0 THEN 3 WHEN hematocrit_max < 50.0 THEN 0 WHEN hematocrit_max >= 50.0 THEN 3 END AS hematocrit_score, CASE WHEN wbc_max < 1.0 THEN 19 WHEN wbc_max < 3.0 THEN 5 WHEN wbc_max < 20.0 THEN 0 WHEN wbc_max < 25.0 THEN 1 WHEN wbc_max >= 25.0 THEN 5 END AS wbc_score, CASE WHEN arf = 1 AND creatinine_max >= 1.5 THEN 10 WHEN arf = 1 THEN 0 WHEN creatinine_max < 0.5 THEN 3 WHEN creatinine_max < 1.5 THEN 0 WHEN creatinine_max < 1.95 THEN 4 WHEN creatinine_max >= 1.95 THEN 7 END AS creatinine_score, CASE WHEN bun_max < 17.0 THEN 0 WHEN bun_max < 20.0 THEN 2 WHEN bun_max < 40.0 THEN 7 WHEN bun_max < 80.0 THEN 11 WHEN bun_max >= 80.0 THEN 12 END AS bun_score, CASE WHEN sodium_max < 120 THEN 3 WHEN sodium_max < 135 THEN 2 WHEN sodium_max < 155 THEN 0 WHEN sodium_max >= 155 THEN 4 END AS sodium_score, CASE WHEN albumin_max < 2.0 THEN 11 WHEN albumin_max < 2.5 THEN 6 WHEN albumin_max < 4.5 THEN 0 WHEN albumin_max >= 4.5 THEN 4 END AS albumin_score, CASE WHEN bilirubin_max < 2.0 THEN 0 WHEN bilirubin_max < 3.0 THEN 5 WHEN bilirubin_max < 5.0 THEN 6 WHEN bilirubin_max < 8.0 THEN 8 WHEN bilirubin_max >= 8.0 THEN 16 END AS bilirubin_score, CASE WHEN glucose_max < 40 THEN 8 WHEN glucose_max < 60 THEN 9 WHEN glucose_max < 200 THEN 0 WHEN glucose_max < 350 THEN 3 WHEN glucose_max >= 350 THEN 5 END AS glucose_score FROM apsiii_cohort c),
scorecomp AS (SELECT co.*, CASE WHEN ABS(heart_rate_max - 75) > ABS(heart_rate_min - 75) THEN smax.hr_score WHEN ABS(heart_rate_max - 75) < ABS(heart_rate_min - 75) THEN smin.hr_score WHEN smax.hr_score >= smin.hr_score THEN smax.hr_score ELSE smin.hr_score END AS hr_score, CASE WHEN ABS(mbp_max - 90) > ABS(mbp_min - 90) THEN smax.mbp_score WHEN ABS(mbp_max - 90) < ABS(mbp_min - 90) THEN smin.mbp_score WHEN smax.mbp_score >= smin.mbp_score THEN smax.mbp_score ELSE smin.mbp_score END AS mbp_score, CASE WHEN ABS(temperature_max - 38) > ABS(temperature_min - 38) THEN smax.temp_score WHEN ABS(temperature_max - 38) < ABS(temperature_min - 38) THEN smin.temp_score WHEN smax.temp_score >= smin.temp_score THEN smax.temp_score ELSE smin.temp_score END AS temp_score, CASE WHEN ABS(resp_rate_max - 19) > ABS(resp_rate_min - 19) THEN smax.resp_rate_score WHEN ABS(resp_rate_max - 19) < ABS(resp_rate_min - 19) THEN smin.resp_rate_score WHEN smax.resp_rate_score >= smin.resp_rate_score THEN smax.resp_rate_score ELSE smin.resp_rate_score END AS resp_rate_score, CASE WHEN ABS(hematocrit_max - 45.5) > ABS(hematocrit_min - 45.5) THEN smax.hematocrit_score WHEN ABS(hematocrit_max - 45.5) < ABS(hematocrit_min - 45.5) THEN smin.hematocrit_score WHEN smax.hematocrit_score >= smin.hematocrit_score THEN smax.hematocrit_score ELSE smin.hematocrit_score END AS hematocrit_score, CASE WHEN ABS(wbc_max - 11.5) > ABS(wbc_min - 11.5) THEN smax.wbc_score WHEN ABS(wbc_max - 11.5) < ABS(wbc_min - 11.5) THEN smin.wbc_score WHEN smax.wbc_score >= smin.wbc_score THEN smax.wbc_score ELSE smin.wbc_score END AS wbc_score, CASE WHEN arf = 1 THEN smax.creatinine_score WHEN ABS(creatinine_max - 1) > ABS(creatinine_min - 1) THEN smax.creatinine_score WHEN ABS(creatinine_max - 1) < ABS(creatinine_min - 1) THEN smin.creatinine_score WHEN smax.creatinine_score >= smin.creatinine_score THEN smax.creatinine_score ELSE smin.creatinine_score END AS creatinine_score, smax.bun_score AS bun_score, CASE WHEN ABS(sodium_max - 145.5) > ABS(sodium_min - 145.5) THEN smax.sodium_score WHEN ABS(sodium_max - 145.5) < ABS(sodium_min - 145.5) THEN smin.sodium_score WHEN smax.sodium_score >= smin.sodium_score THEN smax.sodium_score ELSE smin.sodium_score END AS sodium_score, CASE WHEN ABS(albumin_max - 3.5) > ABS(albumin_min - 3.5) THEN smax.albumin_score WHEN ABS(albumin_max - 3.5) < ABS(albumin_min - 3.5) THEN smin.albumin_score WHEN smax.albumin_score >= smin.albumin_score THEN smax.albumin_score ELSE smin.albumin_score END AS albumin_score, smax.bilirubin_score AS bilirubin_score, CASE WHEN ABS(glucose_max - 130) > ABS(glucose_min - 130) THEN smax.glucose_score WHEN ABS(glucose_max - 130) < ABS(glucose_min - 130) THEN smin.glucose_score WHEN smax.glucose_score >= smin.glucose_score THEN smax.glucose_score ELSE smin.glucose_score END AS glucose_score, CASE WHEN urineoutput < 400 THEN 15 WHEN urineoutput < 600 THEN 8 WHEN urineoutput < 900 THEN 7 WHEN urineoutput < 1500 THEN 5 WHEN urineoutput < 2000 THEN 4 WHEN urineoutput < 4000 THEN 0 WHEN urineoutput >= 4000 THEN 1 END AS uo_score, CASE WHEN gcs_unable = 1 THEN 0 WHEN gcs_eyes = 1 THEN CASE WHEN gcs_verbal = 1 AND gcs_motor IN (1, 2) THEN 48 WHEN gcs_verbal = 1 AND gcs_motor IN (3, 4) THEN 33 WHEN gcs_verbal = 1 AND gcs_motor IN (5, 6) THEN 16 WHEN gcs_verbal IN (2, 3) AND gcs_motor IN (1, 2) THEN 29 WHEN gcs_verbal IN (2, 3) AND gcs_motor IN (3, 4) THEN 24 END WHEN gcs_eyes > 1 THEN CASE WHEN gcs_verbal = 1 AND gcs_motor IN (1, 2) THEN 29 WHEN gcs_verbal = 1 AND gcs_motor IN (3, 4) THEN 24 WHEN gcs_verbal = 1 AND gcs_motor IN (5, 6) THEN 15 WHEN gcs_verbal IN (2, 3) AND gcs_motor IN (1, 2) THEN 29 WHEN gcs_verbal IN (2, 3) AND gcs_motor IN (3, 4) THEN 24 WHEN gcs_verbal IN (2, 3) AND gcs_motor = 5 THEN 13 WHEN gcs_verbal IN (2, 3) AND gcs_motor = 6 THEN 10 WHEN gcs_verbal = 4 AND gcs_motor IN (1, 2, 3, 4) THEN 13 WHEN gcs_verbal = 4 AND gcs_motor = 5 THEN 8 WHEN gcs_verbal = 4 AND gcs_motor = 6 THEN 3 WHEN gcs_verbal = 5 AND gcs_motor IN (1, 2, 3, 4, 5) THEN 3 WHEN gcs_verbal = 5 AND gcs_motor = 6 THEN 0 END END AS gcs_score, CASE WHEN pao2 IS NOT NULL THEN CASE WHEN pao2 < 50 THEN 15 WHEN pao2 < 70 THEN 5 WHEN pao2 < 80 THEN 2 ELSE 0 END WHEN aado2 IS NOT NULL THEN CASE WHEN aado2 < 100 THEN 0 WHEN aado2 < 250 THEN 7 WHEN aado2 < 350 THEN 9 WHEN aado2 < 500 THEN 11 WHEN aado2 >= 500 THEN 14 ELSE 0 END END AS pao2_aado2_score FROM apsiii_cohort co LEFT JOIN score_min smin ON co.stay_id = smin.stay_id LEFT JOIN score_max smax ON co.stay_id = smax.stay_id),
apsiii_scores AS (SELECT s.stay_id, (COALESCE(hr_score, 0) + COALESCE(mbp_score, 0) + COALESCE(temp_score, 0) + COALESCE(resp_rate_score, 0) + COALESCE(pao2_aado2_score, 0) + COALESCE(hematocrit_score, 0) + COALESCE(wbc_score, 0) + COALESCE(creatinine_score, 0) + COALESCE(uo_score, 0) + COALESCE(bun_score, 0) + COALESCE(sodium_score, 0) + COALESCE(albumin_score, 0) + COALESCE(bilirubin_score, 0) + COALESCE(glucose_score, 0) + COALESCE(acidbase_score, 0) + COALESCE(gcs_score, 0)) AS apsiii FROM scorecomp s),
ventilation_events_oasis AS (SELECT stay_id, charttime FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Core_Cohort)),
surgflag_oasis AS (SELECT ie.stay_id, MAX(CASE WHEN LOWER(curr_service) LIKE '%surg%' THEN 1 WHEN curr_service = 'ORTHO' THEN 1 ELSE 0 END) AS surgical FROM Core_Cohort ie LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_hosp.services` se ON ie.hadm_id = se.hadm_id AND se.transfertime < DATETIME_ADD(icu.intime, INTERVAL '1' DAY) GROUP BY ie.stay_id),
vent_oasis AS (SELECT ie.stay_id, MAX(CASE WHEN v.stay_id IS NOT NULL THEN 1 ELSE 0 END) AS vent FROM Core_Cohort ie LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON ie.stay_id = icu.stay_id LEFT JOIN ventilation_events_oasis v ON ie.stay_id = v.stay_id AND v.charttime >= icu.intime AND v.charttime <= DATETIME_ADD(icu.intime, INTERVAL '1' DAY) GROUP BY ie.stay_id),
cohort_oasis AS (SELECT ie.subject_id, ie.hadm_id, ie.stay_id, ie.intime, ie.outtime, adm.deathtime, DATETIME_DIFF(ie.intime, adm.admittime, MINUTE) AS preiculos, ag.age, gcs.gcs_min, vital.heart_rate_max, vital.heart_rate_min, vital.mbp_max, vital.mbp_min, vital.resp_rate_max, vital.resp_rate_min, vital.temperature_max, vital.temperature_min, vent.vent AS mechvent, uo.urineoutput, CASE WHEN adm.admission_type = 'ELECTIVE' AND sf.surgical = 1 THEN 1 ELSE 0 END AS electivesurgery, adm.hospital_expire_flag FROM `physionet-data.mimiciv_3_1_icu.icustays` ie INNER JOIN Core_Cohort bc ON ie.stay_id = bc.stay_id INNER JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm ON ie.hadm_id = adm.hadm_id INNER JOIN `physionet-data.mimiciv_3_1_hosp.patients` pat ON ie.subject_id = pat.subject_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.age` ag ON ie.hadm_id = ag.hadm_id LEFT JOIN surgflag_oasis sf ON ie.stay_id = sf.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id LEFT JOIN vent_oasis vent ON ie.stay_id = vent.stay_id),
scorecomp_oasis AS (SELECT co.subject_id, co.hadm_id, co.stay_id, CASE WHEN preiculos < 10.2 THEN 5 WHEN preiculos < 297 THEN 3 WHEN preiculos < 1440 THEN 0 WHEN preiculos < 18708 THEN 2 ELSE 1 END AS preiculos_score, CASE WHEN age < 24 THEN 0 WHEN age <= 53 THEN 3 WHEN age <= 77 THEN 6 WHEN age <= 89 THEN 9 WHEN age >= 90 THEN 7 ELSE 0 END AS age_score, CASE WHEN gcs_min <= 7 THEN 10 WHEN gcs_min < 14 THEN 4 WHEN gcs_min = 14 THEN 3 ELSE 0 END AS gcs_score, CASE WHEN heart_rate_max > 125 THEN 6 WHEN heart_rate_min < 33 THEN 4 WHEN heart_rate_max >= 107 AND heart_rate_max <= 125 THEN 3 WHEN heart_rate_max >= 89 AND heart_rate_max <= 106 THEN 1 ELSE 0 END AS heart_rate_score, CASE WHEN mbp_min < 20.65 THEN 4 WHEN mbp_min < 51 THEN 3 WHEN mbp_max > 143.44 THEN 3 WHEN mbp_min >= 51 AND mbp_min < 61.33 THEN 2 ELSE 0 END AS mbp_score, CASE WHEN resp_rate_min < 6 THEN 10 WHEN resp_rate_max > 44 THEN 9 WHEN resp_rate_max > 30 THEN 6 WHEN resp_rate_max > 22 THEN 1 WHEN resp_rate_min < 13 THEN 1 ELSE 0 END AS resp_rate_score, CASE WHEN temperature_max > 39.88 THEN 6 WHEN temperature_min < 33.22 THEN 3 WHEN temperature_min > 35.93 AND temperature_min <= 36.39 THEN 2 WHEN temperature_max >= 36.89 AND temperature_max <= 39.88 THEN 2 WHEN temperature_min >= 33.22 AND temperature_min <= 35.93 THEN 4 WHEN temperature_max >= 33.22 AND temperature_max <= 35.93 THEN 4 ELSE 0 END AS temp_score, CASE WHEN urineoutput < 671.09 THEN 10 WHEN urineoutput > 6896.80 THEN 8 WHEN urineoutput >= 671.09 AND urineoutput <= 1426.99 THEN 5 WHEN urineoutput >= 1427.00 AND urineoutput <= 2544.14 THEN 1 ELSE 0 END AS urineoutput_score, CASE WHEN mechvent = 1 THEN 9 ELSE 0 END AS mechvent_score, CASE WHEN electivesurgery = 1 THEN 0 ELSE 6 END AS electivesurgery_score FROM cohort_oasis co),
final_oasis_scores AS (SELECT s.stay_id, (COALESCE(age_score, 0) + COALESCE(preiculos_score, 0) + COALESCE(gcs_score, 0) + COALESCE(heart_rate_score, 0) + COALESCE(mbp_score, 0) + COALESCE(resp_rate_score, 0) + COALESCE(temp_score, 0) + COALESCE(urineoutput_score, 0) + COALESCE(mechvent_score, 0) + COALESCE(electivesurgery_score, 0)) AS oasis FROM scorecomp_oasis s),
cohort_with_intime_lods AS (SELECT arf.subject_id, arf.hadm_id, arf.stay_id, icu.intime FROM Core_Cohort arf LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON arf.stay_id = icu.stay_id),
ventilation_events_lods AS (SELECT stay_id, charttime FROM `physionet-data.mimiciv_3_1_icu.chartevents` WHERE itemid = 223849 AND value IS NOT NULL AND stay_id IN (SELECT stay_id FROM Core_Cohort)),
cpap_lods AS (SELECT c.stay_id, MIN(DATETIME_SUB(ce.charttime, INTERVAL '1' HOUR)) AS starttime, MAX(DATETIME_ADD(ce.charttime, INTERVAL '4' HOUR)) AS endtime FROM cohort_with_intime_lods c INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON c.stay_id = ce.stay_id AND ce.charttime >= c.intime AND ce.charttime <= DATETIME_ADD(c.intime, INTERVAL '1' DAY) WHERE ce.itemid = 226732 AND (LOWER(ce.value) LIKE '%cpap%' OR LOWER(ce.value) LIKE '%bipap mask%') GROUP BY c.stay_id),
pafi1_lods AS (SELECT c.stay_id, bg.charttime, pao2fio2ratio, CASE WHEN vd.stay_id IS NOT NULL THEN 1 ELSE 0 END AS vent, CASE WHEN cp.stay_id IS NOT NULL THEN 1 ELSE 0 END AS cpap FROM `physionet-data.mimiciv_3_1_derived.bg` bg INNER JOIN cohort_with_intime_lods c ON bg.hadm_id = c.hadm_id LEFT JOIN ventilation_events_lods vd ON c.stay_id = vd.stay_id AND bg.charttime = vd.charttime LEFT JOIN cpap_lods cp ON c.stay_id = cp.stay_id AND bg.charttime >= cp.starttime AND bg.charttime <= cp.endtime WHERE bg.charttime >= c.intime AND bg.charttime <= DATETIME_ADD(c.intime, INTERVAL '1' DAY)),
pafi2_lods AS (SELECT stay_id, MIN(pao2fio2ratio) AS pao2fio2_vent_min FROM pafi1_lods WHERE vent = 1 OR cpap = 1 GROUP BY stay_id),
cohort_lods AS (SELECT ie.stay_id, gcs.gcs_min, vital.heart_rate_max, vital.heart_rate_min, vital.sbp_max, vital.sbp_min, pf.pao2fio2_vent_min, labs.bun_max, labs.bun_min, labs.wbc_max, labs.wbc_min, labs.bilirubin_total_max AS bilirubin_max, labs.creatinine_max, labs.pt_min, labs.pt_max, labs.platelets_min AS platelet_min, uo.urineoutput FROM cohort_with_intime_lods ie LEFT JOIN pafi2_lods pf ON ie.stay_id = pf.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_gcs` gcs ON ie.stay_id = gcs.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_vitalsign` vital ON ie.stay_id = vital.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_urine_output` uo ON ie.stay_id = uo.stay_id LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` labs ON ie.stay_id = labs.stay_id),
final_lods_scores AS (SELECT stay_id, CASE WHEN gcs_min < 3 THEN NULL WHEN gcs_min <= 5 THEN 5 WHEN gcs_min <= 8 THEN 3 WHEN gcs_min <= 13 THEN 1 ELSE 0 END AS neurologic, CASE WHEN heart_rate_min < 30 THEN 5 WHEN sbp_min < 40 THEN 5 WHEN sbp_min < 70 THEN 3 WHEN sbp_max >= 270 THEN 3 WHEN heart_rate_max >= 140 THEN 1 WHEN sbp_max >= 240 THEN 1 WHEN sbp_min < 90 THEN 1 ELSE 0 END AS cardiovascular, CASE WHEN urineoutput < 500.0 THEN 5 WHEN bun_max >= 56.0 THEN 5 WHEN creatinine_max >= 1.60 THEN 3 WHEN urineoutput < 750.0 THEN 3 WHEN bun_max >= 28.0 THEN 3 WHEN urineoutput >= 10000.0 THEN 3 WHEN creatinine_max >= 1.20 THEN 1 WHEN bun_max >= 17.0 THEN 1 WHEN bun_max >= 7.50 THEN 1 ELSE 0 END AS renal, CASE WHEN pao2fio2_vent_min IS NULL THEN 0 WHEN pao2fio2_vent_min >= 150 THEN 1 WHEN pao2fio2_vent_min < 150 THEN 3 END AS pulmonary, CASE WHEN wbc_min < 1.0 THEN 3 WHEN wbc_min < 2.5 THEN 1 WHEN platelet_min < 50.0 THEN 1 WHEN wbc_max >= 50.0 THEN 1 ELSE 0 END AS hematologic, CASE WHEN bilirubin_max >= 2.0 THEN 1 WHEN pt_max > (12 + 3) THEN 1 WHEN pt_min < (12 * 0.25) THEN 1 ELSE 0 END AS hepatic FROM cohort_lods),
First_Vent_Type AS (
    WITH all_vents AS (SELECT stay_id, event_time, 'NIV' as vent_type FROM All_NIV_Events UNION ALL SELECT stay_id, event_time, 'IMV' as vent_type FROM All_IMV_Events),
    ranked_vents AS (SELECT stay_id, vent_type, ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY event_time ASC) as rn FROM all_vents)
    SELECT stay_id, vent_type as first_vent_type FROM ranked_vents WHERE rn = 1
),
Charlson_Comorbidity_Index AS (
    WITH diag AS (SELECT hadm_id, CASE WHEN icd_version = 9 THEN icd_code ELSE NULL END AS icd9_code, CASE WHEN icd_version = 10 THEN icd_code ELSE NULL END AS icd10_code FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort)),
    com AS (SELECT hadm_id, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('410', '412') OR SUBSTR(icd10_code, 1, 3) IN ('I21', 'I22') OR SUBSTR(icd10_code, 1, 4) = 'I252' THEN 1 ELSE 0 END) AS myocardial_infarct, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) = '428' OR SUBSTR(icd9_code, 1, 5) IN ('39891', '40201', '40211', '40291', '40401', '40403', '40411', '40413', '40491', '40493') OR SUBSTR(icd9_code, 1, 4) BETWEEN '4254' AND '4259' OR SUBSTR(icd10_code, 1, 3) IN ('I43', 'I50') OR SUBSTR(icd10_code, 1, 4) IN ('I099', 'I110', 'I130', 'I132', 'I255', 'I420', 'I425', 'I426', 'I427', 'I428', 'I429', 'P290') THEN 1 ELSE 0 END) AS congestive_heart_failure, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('440', '441') OR SUBSTR(icd9_code, 1, 4) IN ('0930', '4373', '4471', '5571', '5579', 'V434') OR SUBSTR(icd9_code, 1, 4) BETWEEN '4431' AND '4439' OR SUBSTR(icd10_code, 1, 3) IN ('I70', 'I71') OR SUBSTR(icd10_code, 1, 4) IN ('I731', 'I738', 'I739', 'I771', 'I790', 'I792', 'K551', 'K558', 'K559', 'Z958', 'Z959') THEN 1 ELSE 0 END) AS peripheral_vascular_disease, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) BETWEEN '430' AND '438' OR SUBSTR(icd9_code, 1, 5) = '36234' OR SUBSTR(icd10_code, 1, 3) IN ('G45', 'G46') OR SUBSTR(icd10_code, 1, 3) BETWEEN 'I60' AND 'I69' OR SUBSTR(icd10_code, 1, 4) = 'H340' THEN 1 ELSE 0 END) AS cerebrovascular_disease, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) = '290' OR SUBSTR(icd9_code, 1, 4) IN ('2941', '3312') OR SUBSTR(icd10_code, 1, 3) IN ('F00', 'F01', 'F02', 'F03', 'G30') OR SUBSTR(icd10_code, 1, 4) IN ('F051', 'G311') THEN 1 ELSE 0 END) AS dementia, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) BETWEEN '490' AND '505' OR SUBSTR(icd9_code, 1, 4) IN ('4168', '4169', '5064', '5081', '5088') OR SUBSTR(icd10_code, 1, 3) BETWEEN 'J40' AND 'J47' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'J60' AND 'J67' OR SUBSTR(icd10_code, 1, 4) IN ('I278', 'I279', 'J684', 'J701', 'J703') THEN 1 ELSE 0 END) AS chronic_pulmonary_disease, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) = '725' OR SUBSTR(icd9_code, 1, 4) IN ('4465', '7100', '7101', '7102', '7103', '7104', '7140', '7141', '7142', '7148') OR SUBSTR(icd10_code, 1, 3) IN ('M05', 'M06', 'M32', 'M33', 'M34') OR SUBSTR(icd10_code, 1, 4) IN ('M315', 'M351', 'M353', 'M360') THEN 1 ELSE 0 END) AS rheumatic_disease, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('531', '532', '533', '534') OR SUBSTR(icd10_code, 1, 3) IN ('K25', 'K26', 'K27', 'K28') THEN 1 ELSE 0 END) AS peptic_ulcer_disease, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('570', '571') OR SUBSTR(icd9_code, 1, 4) IN ('0706', '0709', '5733', '5734', '5738', '5739', 'V427') OR SUBSTR(icd9_code, 1, 5) IN ('07022', '07023', '07032', '07033', '07044', '07054') OR SUBSTR(icd10_code, 1, 3) IN ('B18', 'K73', 'K74') OR SUBSTR(icd10_code, 1, 4) IN ('K700', 'K701', 'K702', 'K703', 'K709', 'K713', 'K714', 'K715', 'K717', 'K760', 'K762', 'K763', 'K764', 'K768', 'K769', 'Z944') THEN 1 ELSE 0 END) AS mild_liver_disease, MAX(CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('2500', '2501', '2502', '2503', '2508', '2509') OR SUBSTR(icd10_code, 1, 4) IN ('E100', 'E101', 'E106', 'E108', 'E109', 'E110', 'E111', 'E116', 'E118', 'E119', 'E120', 'E121', 'E126', 'E128', 'E129', 'E130', 'E131', 'E136', 'E138', 'E139', 'E140', 'E141', 'E146', 'E148', 'E149') THEN 1 ELSE 0 END) AS diabetes_without_cc, MAX(CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('2504', '2505', '2506', '2507') OR SUBSTR(icd10_code, 1, 4) IN ('E102', 'E103', 'E104', 'E105', 'E107', 'E112', 'E113', 'E114', 'E115', 'E117', 'E122', 'E123', 'E124', 'E125', 'E127', 'E132', 'E133', 'E134', 'E135', 'E137', 'E142', 'E143', 'E144', 'E145', 'E147') THEN 1 ELSE 0 END) AS diabetes_with_cc, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('342', '343') OR SUBSTR(icd9_code, 1, 4) IN ('3341', '3440', '3441', '3442', '3443', '3444', '3445', '3446', '3449') OR SUBSTR(icd10_code, 1, 3) IN ('G81', 'G82') OR SUBSTR(icd10_code, 1, 4) IN ('G041', 'G114', 'G801', 'G802', 'G830', 'G831', 'G832', 'G833', 'G834', 'G839') THEN 1 ELSE 0 END) AS paraplegia, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('582', '585', '586', 'V56') OR SUBSTR(icd9_code, 1, 4) IN ('5880', 'V420', 'V451') OR SUBSTR(icd9_code, 1, 4) BETWEEN '5830' AND '5837' OR SUBSTR(icd9_code, 1, 5) IN ('40301', '40311', '40391', '40402', '40403', '40412', '40413', '40492', '40493') OR SUBSTR(icd10_code, 1, 3) IN ('N18', 'N19') OR SUBSTR(icd10_code, 1, 4) IN ('I120', 'I131', 'N032', 'N033', 'N034', 'N035', 'N036', 'N037', 'N052', 'N053', 'N054', 'N055', 'N056', 'N057', 'N250', 'Z490', 'Z491', 'Z492', 'Z940', 'Z992') THEN 1 ELSE 0 END) AS renal_disease, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) BETWEEN '140' AND '172' OR SUBSTR(icd9_code, 1, 4) BETWEEN '1740' AND '1958' OR SUBSTR(icd9_code, 1, 3) BETWEEN '200' AND '208' OR SUBSTR(icd9_code, 1, 4) = '2386' OR SUBSTR(icd10_code, 1, 3) IN ('C43', 'C88') OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C00' AND 'C26' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C30' AND 'C34' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C37' AND 'C41' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C45' AND 'C58' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C60' AND 'C76' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C81' AND 'C85' OR SUBSTR(icd10_code, 1, 3) BETWEEN 'C90' AND 'C97' THEN 1 ELSE 0 END) AS malignant_cancer, MAX(CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('4560', '4561', '4562') OR SUBSTR(icd9_code, 1, 4) BETWEEN '5722' AND '5728' OR SUBSTR(icd10_code, 1, 4) IN ('I850', 'I859', 'I864', 'I982', 'K704', 'K711', 'K721', 'K729', 'K765', 'K766', 'K767') THEN 1 ELSE 0 END) AS severe_liver_disease, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('196', '197', '198', '199') OR SUBSTR(icd10_code, 1, 3) IN ('C77', 'C78', 'C79', 'C80') THEN 1 ELSE 0 END) AS metastatic_solid_tumor, MAX(CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('042', '043', '044') OR SUBSTR(icd10_code, 1, 3) IN ('B20', 'B21', 'B22', 'B24') THEN 1 ELSE 0 END) AS aids FROM diag GROUP BY hadm_id),
    ag AS (SELECT hadm_id, age, CASE WHEN age <= 50 THEN 0 WHEN age <= 60 THEN 1 WHEN age <= 70 THEN 2 WHEN age <= 80 THEN 3 ELSE 4 END AS age_score FROM `physionet-data.mimiciv_3_1_derived.age` WHERE hadm_id IN (SELECT hadm_id FROM Core_Cohort))
    SELECT com.hadm_id, (COALESCE(ag.age_score, 0) + myocardial_infarct + congestive_heart_failure + peripheral_vascular_disease + cerebrovascular_disease + dementia + chronic_pulmonary_disease + rheumatic_disease + peptic_ulcer_disease + GREATEST(mild_liver_disease, 3 * severe_liver_disease) + GREATEST(2 * diabetes_with_cc, diabetes_without_cc) + GREATEST(2 * malignant_cancer, 6 * metastatic_solid_tumor) + 2 * paraplegia + 2 * renal_disease + 6 * aids) AS charlson_comorbidity_index
    FROM com LEFT JOIN ag ON com.hadm_id = ag.hadm_id
),

SBT_Start_After_IMV AS (
    SELECT
        ce.stay_id,
        MIN(ce.charttime) as first_sbt_charttime
    FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
    INNER JOIN First_IMV_Time fimv ON ce.stay_id = fimv.stay_id
    WHERE ce.itemid = 224715
      AND ce.value IN ('Yes', '1')
      AND ce.charttime > fimv.first_imv_time
    GROUP BY ce.stay_id
),


SBT_Outcome_After_Start AS (
    SELECT
        ce.stay_id,
        ce.itemid,
        ce.value,
        ce.charttime,
        ROW_NUMBER() OVER(PARTITION BY ce.stay_id ORDER BY ce.charttime ASC) as rn
    FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
    INNER JOIN SBT_Start_After_IMV sbt ON ce.stay_id = sbt.stay_id
    WHERE ce.itemid IN (224717, 224716)
      AND ce.charttime > sbt.first_sbt_charttime
),

New_SBT_Columns AS (
    SELECT
        cc.stay_id,
        icu.intime AS first_icu_intime,

        CASE WHEN fimv.first_imv_time IS NOT NULL THEN 1 ELSE 0 END AS first_imv_flag,

        CASE WHEN sbt.first_sbt_charttime IS NOT NULL THEN 1 ELSE 0 END AS first_sbt_flag,
        sbt.first_sbt_charttime,

        CASE
            WHEN outc.itemid = 224717 THEN outc.value
            ELSE NULL
        END AS sbt_succ_flag,

        CASE
            WHEN outc.itemid = 224717 THEN outc.charttime
            ELSE NULL
        END AS sbt_success_charttime,

        CASE
            WHEN outc.itemid = 224716 THEN outc.value
            ELSE NULL
        END AS sbt_stop_reason,
        CASE
            WHEN outc.itemid = 224716 THEN outc.charttime
            ELSE NULL
        END AS sbt_stop_charttime

    FROM Core_Cohort cc
    INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON cc.stay_id = icu.stay_id
    LEFT JOIN First_IMV_Time fimv ON cc.stay_id = fimv.stay_id
    LEFT JOIN SBT_Start_After_IMV sbt ON cc.stay_id = sbt.stay_id
    LEFT JOIN SBT_Outcome_After_Start outc ON cc.stay_id = outc.stay_id AND outc.rn = 1
),

First_RR_After_Intime AS (
    SELECT
        cc.stay_id,
        ce.valuenum as first_respiratory_rate,
        ROW_NUMBER() OVER (PARTITION BY cc.stay_id ORDER BY ce.charttime ASC) as rn
    FROM Core_Cohort cc
    INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON cc.stay_id = icu.stay_id
    INNER JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON cc.stay_id = ce.stay_id
    WHERE ce.itemid IN (220210, 224690)
      AND ce.valuenum IS NOT NULL
      AND ce.charttime >= icu.intime
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cc.stay_id ORDER BY ce.charttime ASC) = 1
),

All_Vent_Categories AS (
    SELECT cc.stay_id, charttime AS event_time, 'IMV' AS category
    FROM Core_Cohort cc JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON cc.stay_id = ce.stay_id
    WHERE ce.itemid = 223849
       OR (ce.itemid = 226732 AND ce.value IN ('Endotracheal tube', 'Trach mask'))
    UNION DISTINCT
    SELECT cc.stay_id, starttime AS event_time, 'IMV' AS category
    FROM Core_Cohort cc JOIN `physionet-data.mimiciv_3_1_icu.procedureevents` pe ON cc.stay_id = pe.stay_id
    WHERE pe.itemid = 225792

    UNION DISTINCT
    SELECT cc.stay_id, charttime AS event_time, 'NIV' AS category
    FROM Core_Cohort cc JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON cc.stay_id = ce.stay_id
    WHERE ce.itemid = 229314
       OR (ce.itemid = 226732 AND ce.value IN ('Bipap mask', 'CPAP mask'))
    UNION DISTINCT
    SELECT cc.stay_id, starttime AS event_time, 'NIV' AS category
    FROM Core_Cohort cc JOIN `physionet-data.mimiciv_3_1_icu.procedureevents` pe ON cc.stay_id = pe.stay_id
    WHERE pe.itemid = 225794

    UNION DISTINCT
    SELECT cc.stay_id, charttime AS event_time, 'HFNC' AS category
    FROM Core_Cohort cc JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON cc.stay_id = ce.stay_id
    WHERE ce.itemid = 226732 AND ce.value IN ('High flow nasal cannula', 'High flow neb')

    UNION DISTINCT
    SELECT cc.stay_id, charttime AS event_time, 'Supplemental O2' AS category
    FROM Core_Cohort cc JOIN `physionet-data.mimiciv_3_1_icu.chartevents` ce ON cc.stay_id = ce.stay_id
    WHERE ce.itemid = 226732 AND ce.value IN ('Nasal cannula', 'Face tent', 'Aerosol-cool', 'Non-rebreather', 'Venti mask', 'Medium conc mask')
),

First_Vent_Status_24h AS (
    SELECT
        avc.stay_id,
        avc.category,
        ROW_NUMBER() OVER (PARTITION BY avc.stay_id ORDER BY avc.event_time ASC) as rn
    FROM All_Vent_Categories avc
    INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON avc.stay_id = icu.stay_id
    WHERE avc.event_time >= icu.intime
      AND avc.event_time <= DATETIME_ADD(icu.intime, INTERVAL 24 HOUR)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY avc.stay_id ORDER BY avc.event_time ASC) = 1
),

HACOR_ROX_Anchors AS (
    SELECT
        cc.stay_id,
        icu.intime AS time_admit,
        fimv.first_imv_time AS time_imv,
        nsbt.first_sbt_charttime AS time_sbt
    FROM Core_Cohort cc
    INNER JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu ON cc.stay_id = icu.stay_id
    LEFT JOIN First_IMV_Time fimv ON cc.stay_id = fimv.stay_id
    LEFT JOIN New_SBT_Columns nsbt ON cc.stay_id = nsbt.stay_id
),

Raw_HACOR_ROX_Events AS (
    SELECT stay_id, charttime,
        CASE
            WHEN itemid = 220045 THEN 'hr'
            WHEN itemid IN (220210, 224690) THEN 'rr'
            WHEN itemid = 220277 THEN 'spo2'
            WHEN itemid = 223835 THEN 'fio2'
            WHEN itemid = 223901 THEN 'gcs_motor'
            WHEN itemid = 223900 THEN 'gcs_verbal'
            WHEN itemid = 220739 THEN 'gcs_eye'
        END AS variable,
        valuenum
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid IN (220045, 220210, 224690, 220277, 223835, 223901, 223900, 220739)
      AND valuenum IS NOT NULL
      AND stay_id IN (SELECT stay_id FROM Core_Cohort)

    UNION ALL

    SELECT stay_id, charttime,
        CASE
            WHEN itemid IN (50820, 223830) THEN 'ph'
            WHEN itemid IN (50821, 220224) THEN 'pao2'
        END AS variable,
        valuenum
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid IN (50820, 223830, 50821, 220224)
      AND valuenum IS NOT NULL
      AND stay_id IN (SELECT stay_id FROM Core_Cohort)

    UNION ALL

    SELECT cc.stay_id, le.charttime,
        CASE
            WHEN itemid = 50820 THEN 'ph'
            WHEN itemid = 50821 THEN 'pao2'
        END AS variable,
        le.valuenum
    FROM Core_Cohort cc
    JOIN `physionet-data.mimiciv_3_1_hosp.labevents` le ON cc.hadm_id = le.hadm_id
    WHERE le.itemid IN (50820, 50821)
      AND le.valuenum IS NOT NULL
),

Vals_Admit AS (
    SELECT
        anc.stay_id,
        ARRAY_AGG(CASE WHEN r.variable = 'hr' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS hr,
        ARRAY_AGG(CASE WHEN r.variable = 'rr' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS rr,
        ARRAY_AGG(CASE WHEN r.variable = 'ph' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS ph,
        ARRAY_AGG(CASE WHEN r.variable = 'pao2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS pao2,
        ARRAY_AGG(CASE WHEN r.variable = 'spo2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS spo2,
        ARRAY_AGG(CASE WHEN r.variable = 'fio2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS fio2,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_motor' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_motor,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_verbal' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_verbal,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_eye' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_eye
    FROM HACOR_ROX_Anchors anc
    JOIN Raw_HACOR_ROX_Events r ON anc.stay_id = r.stay_id
    WHERE r.charttime >= anc.time_admit
    GROUP BY anc.stay_id
),

Vals_IMV AS (
    SELECT
        anc.stay_id,
        ARRAY_AGG(CASE WHEN r.variable = 'hr' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS hr,
        ARRAY_AGG(CASE WHEN r.variable = 'rr' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS rr,
        ARRAY_AGG(CASE WHEN r.variable = 'ph' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS ph,
        ARRAY_AGG(CASE WHEN r.variable = 'pao2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS pao2,
        ARRAY_AGG(CASE WHEN r.variable = 'spo2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS spo2,
        ARRAY_AGG(CASE WHEN r.variable = 'fio2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS fio2,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_motor' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_motor,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_verbal' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_verbal,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_eye' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_eye
    FROM HACOR_ROX_Anchors anc
    JOIN Raw_HACOR_ROX_Events r ON anc.stay_id = r.stay_id
    WHERE anc.time_imv IS NOT NULL AND r.charttime >= anc.time_imv
    GROUP BY anc.stay_id
),

Vals_SBT AS (
    SELECT
        anc.stay_id,
        ARRAY_AGG(CASE WHEN r.variable = 'hr' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS hr,
        ARRAY_AGG(CASE WHEN r.variable = 'rr' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS rr,
        ARRAY_AGG(CASE WHEN r.variable = 'ph' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS ph,
        ARRAY_AGG(CASE WHEN r.variable = 'pao2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS pao2,
        ARRAY_AGG(CASE WHEN r.variable = 'spo2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS spo2,
        ARRAY_AGG(CASE WHEN r.variable = 'fio2' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS fio2,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_motor' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_motor,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_verbal' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_verbal,
        ARRAY_AGG(CASE WHEN r.variable = 'gcs_eye' THEN r.valuenum END IGNORE NULLS ORDER BY r.charttime ASC LIMIT 1)[SAFE_OFFSET(0)] AS gcs_eye
    FROM HACOR_ROX_Anchors anc
    JOIN Raw_HACOR_ROX_Events r ON anc.stay_id = r.stay_id
    WHERE anc.time_sbt IS NOT NULL AND r.charttime >= anc.time_sbt
    GROUP BY anc.stay_id
),

Final_New_Scores AS (
    SELECT
        cc.stay_id,

        va.hr AS raw_hr_admit,
        va.ph AS raw_ph_admit,
        va.rr AS raw_rr_admit,
        va.spo2 AS raw_spo2_admit,
        va.fio2 AS raw_fio2_admit,
        va.pao2 AS raw_pao2_admit,
        (COALESCE(va.gcs_motor,6) + COALESCE(va.gcs_verbal,5) + COALESCE(va.gcs_eye,4)) AS admit_gcs_total,
        (va.pao2 / NULLIF(CASE WHEN va.fio2 > 1 THEN va.fio2/100 ELSE va.fio2 END, 0)) AS admit_pf_ratio,

        CASE WHEN va.hr > 120 THEN 1 ELSE 0 END AS hacor_admit_hr_score,
        CASE WHEN va.ph < 7.25 THEN 4 WHEN va.ph < 7.30 THEN 3 WHEN va.ph < 7.35 THEN 2 ELSE 0 END AS hacor_admit_ph_score,
        CASE WHEN (COALESCE(va.gcs_motor,6)+COALESCE(va.gcs_verbal,5)+COALESCE(va.gcs_eye,4)) <= 10 THEN 10 WHEN (COALESCE(va.gcs_motor,6)+COALESCE(va.gcs_verbal,5)+COALESCE(va.gcs_eye,4)) <= 12 THEN 5 WHEN (COALESCE(va.gcs_motor,6)+COALESCE(va.gcs_verbal,5)+COALESCE(va.gcs_eye,4)) <= 14 THEN 2 ELSE 0 END AS hacor_admit_gcs_score,
        CASE WHEN (va.pao2 / NULLIF(CASE WHEN va.fio2 > 1 THEN va.fio2/100 ELSE va.fio2 END, 0)) < 101 THEN 6 WHEN (va.pao2 / NULLIF(CASE WHEN va.fio2 > 1 THEN va.fio2/100 ELSE va.fio2 END, 0)) < 126 THEN 5 WHEN (va.pao2 / NULLIF(CASE WHEN va.fio2 > 1 THEN va.fio2/100 ELSE va.fio2 END, 0)) < 151 THEN 4 WHEN (va.pao2 / NULLIF(CASE WHEN va.fio2 > 1 THEN va.fio2/100 ELSE va.fio2 END, 0)) < 176 THEN 3 WHEN (va.pao2 / NULLIF(CASE WHEN va.fio2 > 1 THEN va.fio2/100 ELSE va.fio2 END, 0)) < 201 THEN 2 ELSE 0 END AS hacor_admit_pf_score,
        CASE WHEN va.rr > 45 THEN 4 WHEN va.rr > 40 THEN 3 WHEN va.rr > 35 THEN 2 WHEN va.rr > 30 THEN 1 ELSE 0 END AS hacor_admit_rr_score,
        (va.spo2 / NULLIF(CASE WHEN va.fio2 > 1 THEN va.fio2/100 ELSE COALESCE(va.fio2, 0.21) END, 0)) / NULLIF(va.rr, 0) AS first_rox_admit,

        vi.hr AS raw_hr_imv,
        vi.ph AS raw_ph_imv,
        vi.rr AS raw_rr_imv,
        vi.spo2 AS raw_spo2_imv,
        vi.fio2 AS raw_fio2_imv,
        vi.pao2 AS raw_pao2_imv,
        (COALESCE(vi.gcs_motor,6) + COALESCE(vi.gcs_verbal,5) + COALESCE(vi.gcs_eye,4)) AS imv_gcs_total,
        (vi.pao2 / NULLIF(CASE WHEN vi.fio2 > 1 THEN vi.fio2/100 ELSE vi.fio2 END, 0)) AS imv_pf_ratio,

        CASE WHEN vi.hr > 120 THEN 1 ELSE 0 END AS hacor_imv_hr_score,
        CASE WHEN vi.ph < 7.25 THEN 4 WHEN vi.ph < 7.30 THEN 3 WHEN vi.ph < 7.35 THEN 2 ELSE 0 END AS hacor_imv_ph_score,
        CASE WHEN (COALESCE(vi.gcs_motor,6)+COALESCE(vi.gcs_verbal,5)+COALESCE(vi.gcs_eye,4)) <= 10 THEN 10 WHEN (COALESCE(vi.gcs_motor,6)+COALESCE(vi.gcs_verbal,5)+COALESCE(vi.gcs_eye,4)) <= 12 THEN 5 WHEN (COALESCE(vi.gcs_motor,6)+COALESCE(vi.gcs_verbal,5)+COALESCE(vi.gcs_eye,4)) <= 14 THEN 2 ELSE 0 END AS hacor_imv_gcs_score,
        CASE WHEN (vi.pao2 / NULLIF(CASE WHEN vi.fio2 > 1 THEN vi.fio2/100 ELSE vi.fio2 END, 0)) < 101 THEN 6 WHEN (vi.pao2 / NULLIF(CASE WHEN vi.fio2 > 1 THEN vi.fio2/100 ELSE vi.fio2 END, 0)) < 126 THEN 5 WHEN (vi.pao2 / NULLIF(CASE WHEN vi.fio2 > 1 THEN vi.fio2/100 ELSE vi.fio2 END, 0)) < 151 THEN 4 WHEN (vi.pao2 / NULLIF(CASE WHEN vi.fio2 > 1 THEN vi.fio2/100 ELSE vi.fio2 END, 0)) < 176 THEN 3 WHEN (vi.pao2 / NULLIF(CASE WHEN vi.fio2 > 1 THEN vi.fio2/100 ELSE vi.fio2 END, 0)) < 201 THEN 2 ELSE 0 END AS hacor_imv_pf_score,
        CASE WHEN vi.rr > 45 THEN 4 WHEN vi.rr > 40 THEN 3 WHEN vi.rr > 35 THEN 2 WHEN vi.rr > 30 THEN 1 ELSE 0 END AS hacor_imv_rr_score,
        (vi.spo2 / NULLIF(CASE WHEN vi.fio2 > 1 THEN vi.fio2/100 ELSE COALESCE(vi.fio2, 0.21) END, 0)) / NULLIF(vi.rr, 0) AS first_rox_imv,

        vs.hr AS raw_hr_sbt,
        vs.ph AS raw_ph_sbt,
        vs.rr AS raw_rr_sbt,
        vs.spo2 AS raw_spo2_sbt,
        vs.fio2 AS raw_fio2_sbt,
        vs.pao2 AS raw_pao2_sbt,
        (COALESCE(vs.gcs_motor,6) + COALESCE(vs.gcs_verbal,5) + COALESCE(vs.gcs_eye,4)) AS sbt_gcs_total,
        (vs.pao2 / NULLIF(CASE WHEN vs.fio2 > 1 THEN vs.fio2/100 ELSE vs.fio2 END, 0)) AS sbt_pf_ratio,

        CASE WHEN vs.hr > 120 THEN 1 ELSE 0 END AS hacor_sbt_hr_score,
        CASE WHEN vs.ph < 7.25 THEN 4 WHEN vs.ph < 7.30 THEN 3 WHEN vs.ph < 7.35 THEN 2 ELSE 0 END AS hacor_sbt_ph_score,
        CASE WHEN (COALESCE(vs.gcs_motor,6)+COALESCE(vs.gcs_verbal,5)+COALESCE(vs.gcs_eye,4)) <= 10 THEN 10 WHEN (COALESCE(vs.gcs_motor,6)+COALESCE(vs.gcs_verbal,5)+COALESCE(vs.gcs_eye,4)) <= 12 THEN 5 WHEN (COALESCE(vs.gcs_motor,6)+COALESCE(vs.gcs_verbal,5)+COALESCE(vs.gcs_eye,4)) <= 14 THEN 2 ELSE 0 END AS hacor_sbt_gcs_score,
        CASE WHEN (vs.pao2 / NULLIF(CASE WHEN vs.fio2 > 1 THEN vs.fio2/100 ELSE vs.fio2 END, 0)) < 101 THEN 6 WHEN (vs.pao2 / NULLIF(CASE WHEN vs.fio2 > 1 THEN vs.fio2/100 ELSE vs.fio2 END, 0)) < 126 THEN 5 WHEN (vs.pao2 / NULLIF(CASE WHEN vs.fio2 > 1 THEN vs.fio2/100 ELSE vs.fio2 END, 0)) < 151 THEN 4 WHEN (vs.pao2 / NULLIF(CASE WHEN vs.fio2 > 1 THEN vs.fio2/100 ELSE vs.fio2 END, 0)) < 176 THEN 3 WHEN (vs.pao2 / NULLIF(CASE WHEN vs.fio2 > 1 THEN vs.fio2/100 ELSE vs.fio2 END, 0)) < 201 THEN 2 ELSE 0 END AS hacor_sbt_pf_score,
        CASE WHEN vs.rr > 45 THEN 4 WHEN vs.rr > 40 THEN 3 WHEN vs.rr > 35 THEN 2 WHEN vs.rr > 30 THEN 1 ELSE 0 END AS hacor_sbt_rr_score,
        (vs.spo2 / NULLIF(CASE WHEN vs.fio2 > 1 THEN vs.fio2/100 ELSE COALESCE(vs.fio2, 0.21) END, 0)) / NULLIF(vs.rr, 0) AS first_rox_sbt

    FROM Core_Cohort cc
    LEFT JOIN Vals_Admit va ON cc.stay_id = va.stay_id
    LEFT JOIN Vals_IMV vi ON cc.stay_id = vi.stay_id
    LEFT JOIN Vals_SBT vs ON cc.stay_id = vs.stay_id
)

SELECT

    cohort.subject_id,
    cohort.hadm_id,
    cohort.stay_id,
    pat.gender,
    pat.anchor_age,
    adm.admittime,
    adm.dischtime,


    so.death_datetime,
    so.in_hospital_mortality_flag AS hospital_expire_flag,
    so.icu_los_days,
    so.hospital_los_days,
    so.in_hospital_mortality_flag,
    so.in_hospital_mortality_duration,
    so.overall_mortality_flag,
    so.overall_mortality_duration,
    IFNULL(hosp_readmit.readmission_30_day, 0) as hospital_readmission_30_day,
    IFNULL(icu_readmit.icu_readmission_30_day, 0) AS icu_readmission_30_day,


    arf_codes.arf_icd_code,
    preg.pregnancy_icd_codes,
    cong.congenital_anomaly_icd_codes,
    hem_mal.hematolymphoid_icd_codes,
    mal.malignancy_icd_codes,
    aids.aids_icd_codes,
    CASE WHEN dnr_events.dni_dnr_status IS NOT NULL AND dnr_icd.dni_dnr_icd IS NOT NULL THEN CONCAT(dnr_events.dni_dnr_status, '; ', dnr_icd.dni_dnr_icd) ELSE IFNULL(dnr_events.dni_dnr_status, dnr_icd.dni_dnr_icd) END AS dni_dnr_combined_status,


    IFNULL(com.has_leukemia, 0) AS has_leukemia,
    IFNULL(com.has_lymphoma, 0) AS has_lymphoma,
    IFNULL(com.has_copd, 0) AS has_copd,
    IFNULL(com.has_ild, 0) AS has_ild,
    IFNULL(com.has_t1d, 0) AS has_t1d,
    IFNULL(com.has_t2d, 0) AS has_t2d,
    IFNULL(com.has_hypertension, 0) AS has_hypertension,
    IFNULL(com.has_heart_failure, 0) AS has_heart_failure,
    IFNULL(com.has_cirrhosis, 0) AS has_cirrhosis,
    IFNULL(com.has_ckd, 0) AS has_ckd,


    IFNULL(niv_flag.had_niv, 0) AS had_niv,
    IFNULL(imv_flag.had_imv, 0) AS had_imv,
    peo.duration_invasive_vent,
    nivf.duration_NIVF,


    bf.first_heart_rate,
    bf.first_mbp,
    bf.first_temp_f,
    bf.first_gcs_motor,
    bf.first_gcs_verbal,
    bf.first_gcs_eye,
    bf.first_hb,
    bf.first_wbc,
    bf.first_mch,
    bf.first_mcv,
    bf.first_rdw_cv,
    bf.first_platelet_count,
    bf.first_abs_neutrophil_lab,
    bf.first_abs_lymphocyte_lab,
    bf.first_abs_monocyte_lab,
    bf.first_glucose_lab,
    bf.first_creatinine_lab,
    bf.first_bicarbonate_lab,
    bf.first_pao2,
    bf.first_spo2,
    bf.first_fio2,
    bf.first_paco2,
    bf.first_ph_arterial,

    fns.raw_hr_admit, fns.raw_ph_admit, fns.raw_rr_admit, fns.raw_spo2_admit, fns.raw_fio2_admit, fns.raw_pao2_admit, fns.admit_gcs_total, fns.admit_pf_ratio,
    fns.hacor_admit_hr_score,
    fns.hacor_admit_ph_score,
    fns.hacor_admit_gcs_score,
    fns.hacor_admit_pf_score,
    fns.hacor_admit_rr_score,
    (COALESCE(fns.hacor_admit_hr_score,0) + COALESCE(fns.hacor_admit_ph_score,0) + COALESCE(fns.hacor_admit_gcs_score,0) + COALESCE(fns.hacor_admit_pf_score,0) + COALESCE(fns.hacor_admit_rr_score,0)) AS first_hacor_admit,
    fns.first_rox_admit,

    fns.raw_hr_imv, fns.raw_ph_imv, fns.raw_rr_imv, fns.raw_spo2_imv, fns.raw_fio2_imv, fns.raw_pao2_imv, fns.imv_gcs_total, fns.imv_pf_ratio,
    fns.hacor_imv_hr_score,
    fns.hacor_imv_ph_score,
    fns.hacor_imv_gcs_score,
    fns.hacor_imv_pf_score,
    fns.hacor_imv_rr_score,
    (COALESCE(fns.hacor_imv_hr_score,0) + COALESCE(fns.hacor_imv_ph_score,0) + COALESCE(fns.hacor_imv_gcs_score,0) + COALESCE(fns.hacor_imv_pf_score,0) + COALESCE(fns.hacor_imv_rr_score,0)) AS first_hacor_imv,
    fns.first_rox_imv,

    fns.raw_hr_sbt, fns.raw_ph_sbt, fns.raw_rr_sbt, fns.raw_spo2_sbt, fns.raw_fio2_sbt, fns.raw_pao2_sbt, fns.sbt_gcs_total, fns.sbt_pf_ratio,
    fns.hacor_sbt_hr_score,
    fns.hacor_sbt_ph_score,
    fns.hacor_sbt_gcs_score,
    fns.hacor_sbt_pf_score,
    fns.hacor_sbt_rr_score,
    (COALESCE(fns.hacor_sbt_hr_score,0) + COALESCE(fns.hacor_sbt_ph_score,0) + COALESCE(fns.hacor_sbt_gcs_score,0) + COALESCE(fns.hacor_sbt_pf_score,0) + COALESCE(fns.hacor_sbt_rr_score,0)) AS first_hacor_sbt,
    fns.first_rox_sbt,

    sofa.respiration_sofa + sofa.coagulation_sofa + sofa.liver_sofa + sofa.cardiovascular_sofa + sofa.cns_sofa + sofa.renal_sofa AS first_day_sofa_score,
    sapsii.sapsii,
    aps.apsiii,
    oasis.oasis,
    COALESCE(lods.neurologic, 0) + COALESCE(lods.cardiovascular, 0) + COALESCE(lods.renal, 0) + COALESCE(lods.pulmonary, 0) + COALESCE(lods.hematologic, 0) + COALESCE(lods.hepatic, 0) AS first_day_lods_score,
    COALESCE(sirs.temp_score, 0) + COALESCE(sirs.heart_rate_score, 0) + COALESCE(sirs.resp_score, 0) + COALESCE(sirs.wbc_score, 0) AS first_day_sirs_score,
    cci.charlson_comorbidity_index,

    COALESCE(CASE WHEN fvt.first_vent_type = 'IMV' THEN 'IMV First' WHEN fvt.first_vent_type = 'NIV' THEN 'NIV First' END, 'No Ventilation') AS ventilation_status,
    nfc.niv_failure,
    IFNULL(peo.extubation_flag, 0) as extubation_flag,
    IFNULL(w_outcome.weaning_failure, 0) as weaning_failure,
    IFNULL(w_outcome.weaning_success, 0) as weaning_success,
    IFNULL(w_outcome.weaning_indeterminate, 0) as weaning_indeterminate,
    IFNULL(w_outcome.weaning_outcome_status, 0) as weaning_outcome_status,
    nsbt.first_icu_intime,
    nsbt.first_imv_flag,
    nsbt.first_sbt_flag,
    nsbt.first_sbt_charttime,
    nsbt.sbt_succ_flag,
    nsbt.sbt_success_charttime,
    nsbt.sbt_stop_reason,
    nsbt.sbt_stop_charttime,
    new_rr.first_respiratory_rate,
    COALESCE(new_vent.category, 'No Data') AS first_ventilation_status,

FROM Core_Cohort AS cohort
LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` AS adm ON cohort.hadm_id = adm.hadm_id
LEFT JOIN `physionet-data.mimiciv_3_1_hosp.patients` AS pat ON cohort.subject_id = pat.subject_id
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
LEFT JOIN Hospital_Readmission hosp_readmit ON cohort.hadm_id = hosp_readmit.hadm_id
LEFT JOIN ICU_Readmission_Flag AS icu_readmit ON cohort.stay_id = icu_readmit.stay_id
LEFT JOIN Weaning_Status_Final w_outcome ON cohort.stay_id = w_outcome.stay_id
LEFT JOIN NIV_Failure_Calculation nfc ON cohort.stay_id = nfc.stay_id
LEFT JOIN apsiii_scores aps ON cohort.stay_id = aps.stay_id
LEFT JOIN final_sofa_scores sofa ON cohort.stay_id = sofa.stay_id
LEFT JOIN sirs_scorecalc sirs ON cohort.stay_id = sirs.stay_id
LEFT JOIN sapsii_scores sapsii ON cohort.stay_id = sapsii.stay_id
LEFT JOIN final_oasis_scores oasis ON cohort.stay_id = oasis.stay_id
LEFT JOIN final_lods_scores lods ON cohort.stay_id = lods.stay_id
LEFT JOIN First_Vent_Type AS fvt ON cohort.stay_id = fvt.stay_id
LEFT JOIN NIVF_Total_Duration AS nivf ON cohort.stay_id = nivf.stay_id
LEFT JOIN Charlson_Comorbidity_Index AS cci ON cohort.hadm_id = cci.hadm_id
LEFT JOIN New_SBT_Columns AS nsbt ON cohort.stay_id = nsbt.stay_id
LEFT JOIN First_RR_After_Intime AS new_rr ON cohort.stay_id = new_rr.stay_id
LEFT JOIN First_Vent_Status_24h AS new_vent ON cohort.stay_id = new_vent.stay_id
LEFT JOIN Final_New_Scores AS fns ON cohort.stay_id = fns.stay_id
ORDER BY
   cohort.subject_id
);