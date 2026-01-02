SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..##cp_cohorts') IS NOT NULL DROP TABLE ##cp_cohorts;
WITH
    cohorts AS (
        SELECT PATIENT_FYI_FLAGS.PATIENT_ID AS PAT_ID,
               MAX(CASE WHEN ZC_BPA_TRIGGER_FYI.name = 'SA64 Pharmacist - HTN' THEN 'HTN Cohort'
                       WHEN ZC_BPA_TRIGGER_FYI.name = 'SA64 Pharmacist: Pre-DM' THEN 'Pre-DM Cohort'
                       WHEN ZC_BPA_TRIGGER_FYI.name = 'SA64 Pharmacist - Anticoagulation' THEN 'Anticoagulation Cohort'
                       WHEN ZC_BPA_TRIGGER_FYI.name = 'SA64 Pharmacist - DM' THEN 'Diabetes Cohort'
                       WHEN ZC_BPA_TRIGGER_FYI.name = 'SA64 Pharmacist - Tobacco' THEN 'Tobacco Cohort'
                       WHEN ZC_BPA_TRIGGER_FYI.name = 'SA64 Pharmacist - Miscellaneous' THEN 'Miscellaneous Cohort'
                       WHEN ZC_BPA_TRIGGER_FYI.name = 'SA64 PHARMACIST- PrEP' THEN 'PrEP Cohort'
                       WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640025' THEN 'Dietitian Care Cohort'
                       ELSE 'Not in CP Cohort'
                   END) AS CP_COHORT
        FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS
            INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI AS ZC_BPA_TRIGGER_FYI ON PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = ZC_BPA_TRIGGER_FYI.BPA_TRIGGER_FYI_C
        WHERE PATIENT_FYI_FLAGS.ACTIVE_C = 1
        GROUP BY PATIENT_FYI_FLAGS.PATIENT_ID
    )
SELECT * INTO ##cp_cohorts FROM cohorts;


/* Grab the most recent office visit */
IF OBJECT_ID('tempdb..##most_recent_visit') IS NOT NULL DROP TABLE ##most_recent_visit;
SELECT PAT_ENC.PAT_ID,
       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS VISIT_DATE,
       CLARITY_DEP.DEPARTMENT_NAME AS VISIT_DEPARTMENT_NAME,
       CLARITY_DEP.STATE,
       CLARITY_DEP.CITY,
       CLARITY_DEP.SERVICE_TYPE,
       CLARITY_DEP.SERVICE_LINE,
       CLARITY_DEP.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE DESC) AS ROW_NUM_DESC,
       PAT_ENC.ENC_TYPE_C
INTO ##most_recent_visit
FROM Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
WHERE PAT_ENC.CONTACT_DATE <= CURRENT_TIMESTAMP
      AND PAT_ENC.ENC_TYPE_C = 101
      AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'MD';


IF OBJECT_ID('tempdb..##next_medical_appointment') IS NOT NULL DROP TABLE ##next_medical_appointment;
SELECT PAT_ENC.PAT_ID,
       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_MEDICAL_APPOINTMENT,
       CLARITY_SER.PROV_NAME AS NEXT_APPT_PROVIDER,
       ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS RN_ASC
INTO ##next_medical_appointment
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
WHERE PAT_ENC.APPT_STATUS_C = 1 -- Scheduled
      AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND PAT_ENC.CONTACT_DATE > CURRENT_TIMESTAMP;


/* 
FLAG_NAME	FLAG_TYPE_C
PrEP		640005
Fasle Pos	640017
PEP			9800035
STI			640008
AODA HIV-	640007
Other HIV-  9800065
MPX as STI  640034 
*/
IF OBJECT_ID('tempdb..##flag_type') IS NOT NULL DROP TABLE ##flag_type;
SELECT flag.PATIENT_ID,
       MIN(CASE WHEN flag.PAT_FLAG_TYPE_C = '640005'
                     AND flag.ACTIVE_C = 1 THEN 'PrEP'
               WHEN flag.PAT_FLAG_TYPE_C IN ( '640008', '640034' )
                    AND flag.ACTIVE_C = 1 THEN 'STI'
               WHEN flag.PAT_FLAG_TYPE_C = '6400017'
                    AND flag.ACTIVE_C = 1 THEN 'False Positive HIV Test'
               WHEN flag.PAT_FLAG_TYPE_C = '9800035'
                    AND flag.ACTIVE_C = 1 THEN 'PEP'
               WHEN flag.PAT_FLAG_TYPE_C = '640007'
                    AND flag.ACTIVE_C = 1 THEN 'AODA HIV-'
               ELSE 'Other'
           END) AS PATIENT_TYPE
INTO ##flag_type
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
WHERE flag.PAT_FLAG_TYPE_C IN ( '640005', '640017', '9800035', '640008', '640007', '9800065', '640034' )
      AND flag.ACTIVE_C = 1
GROUP BY flag.PATIENT_ID;


IF OBJECT_ID('tempdb..##active_hiv_patients') IS NOT NULL DROP TABLE ##active_hiv_patients;
SELECT DISTINCT plv.PAT_ID,
                'HIV+' AS PATIENT_TYPE
INTO ##active_hiv_patients
FROM Clarity.dbo.PROBLEM_LIST_VIEW plv
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
WHERE icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1; --Active Dx


SELECT TOP 10 * FROM CLARITY.dbo.PATIENT_VIEW;
