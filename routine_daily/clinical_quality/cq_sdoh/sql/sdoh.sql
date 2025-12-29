/*
Targets:
| FLO_MEAS_ID | FLO_MEAS_NAME                                     | DISP_NAME                                                                                                     |
|:-----------:|:-------------------------------------------------:|:-------------------------------------------------------------------------------------------------------------:|
| 3447        | R HOW DO YOU LEARN BEST                           | How do you learn best?                                                                                        | 
| 3434        | R FRS SEVERITY                                    | How hard is it for you to pay for the very basics like food, housing, heating, medical care, and medications? |
| 5693        | R SDH HOUSING LIVING SITUATION                    | What is your living situation today?                                                                          |
| 3494        | R SDH FOOD WORRY RUNNING OUT                      | Within the past 12 months, you worried that your food would run out before you got money to buy more.         |
| 9388        | R SDH C3 FOOD WORRY RUNNING OUT                   | Within the past 12 months, you worried that your food would run out before you got money to buy more.         |
| 6569        | R BHN SDH FOOD WORRY RUNNING OUT                  | Within the past 12 months, you worried whether your food would run out before you got money to buy more?      |
| 9150        | R SDH C3 TRANSPORTATION                           | In the past 12 months, has lack of transportation kept you from medical appointments, meetings, work or from  |
|             |                                                   | getting things needed for daily living? (Check all that apply)                                                |
| 3511        | R SDH SOCIAL CONNECTIONS/ISOLATION ACCESS TO HELP | Do you have someone you could call if you needed help?                                                        |
| 5700        | R SDH SAFETY QUESTION 1 (HITTS ADAPTED)           | How often does anyone, including family and friends, physically hurt you?                                     |
| 5701        | R SDH SAFETY QUESTION 2 (HITTS ADAPTED)           | How often does anyone, including family and friends, insult or talk down to you?                              |
| 5702        | R SDH SAFETY QUESTION 3 (HITTS ADAPTED)           | How often does anyone, including family and friends, threaten you with harm?                                  |
| 5703        | R SDH SAFETY QUESTION 4 (HITTS ADAPTED)           | How often does anyone, including family and friends, scream or curse at you?                                  |
*/

SET ANSI_WARNINGS OFF;
SET NOCOUNT ON;

/* Capture attribution to department LOs */
IF OBJECT_ID('tempdb..#pat_enc_dep_los') IS NOT NULL DROP TABLE #pat_enc_dep_los;
SELECT pev.PAT_ID,
       CAST(pev.CONTACT_DATE AS DATE) AS LAST_OFFICE_VISIT,
       SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2) AS STATE,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 5, 2)
           WHEN 'MK' THEN 'MILWAUKEE'
           WHEN 'KN' THEN 'KENOSHA'
           WHEN 'GB' THEN 'GREEN BAY'
           WHEN 'WS' THEN 'WAUSAU'
           WHEN 'AP' THEN 'APPLETON'
           WHEN 'EC' THEN 'EAU CLAIRE'
           WHEN 'LC' THEN 'LACROSSE'
           WHEN 'MD' THEN 'MADISON'
           WHEN 'BL' THEN 'BELOIT'
           WHEN 'BI' THEN 'BILLING'
           WHEN 'SL' THEN 'ST LOUIS'
           WHEN 'KC' THEN 'KANSAS CITY'
           WHEN 'DN' THEN 'DENVER'
           WHEN 'AS' THEN 'AUSTIN'
           ELSE 'ERROR'
       END AS CITY,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 7, 2)
           WHEN 'MN' THEN 'MAIN LOCATION'
           WHEN 'DR' THEN 'D&R'
           WHEN 'KE' THEN 'KEENEN'
           WHEN 'UC' THEN 'UNIVERSITY OF COLORADO'
           WHEN 'ON' THEN 'AUSTIN MAIN'
           WHEN 'TW' THEN 'AUSTIN OTHER'
           ELSE 'ERROR'
       END AS 'SITE',
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2)
           WHEN 'MD' THEN 'MEDICAL'
           WHEN 'DT' THEN 'DENTAL'
           WHEN 'CM' THEN 'CASE MANAGEMENT'
           WHEN 'RX' THEN 'PHARMACY'
           WHEN 'AD' THEN 'BEHAVIORAL'
           WHEN 'PY' THEN 'BEHAVIORAL'
           WHEN 'BH' THEN 'BEHAVIORAL'
           WHEN 'MH' THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS 'LOS',
       CLARITY_DEP.DEPARTMENT_NAME,
       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
INTO #pat_enc_dep_los
FROM CLARITY.dbo.PAT_ENC_VIEW pev
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.APPT_STATUS_C IN ( 2, 6 )
      AND DATEDIFF(MONTH, pev.CONTACT_DATE, GETDATE()) <= 12;


IF OBJECT_ID('tempdb..#medical_patients') IS NOT NULL DROP TABLE #medical_patients;
WITH
    target_service_line AS (
        SELECT #pat_enc_dep_los.PAT_ID,
               #pat_enc_dep_los.STATE,
               #pat_enc_dep_los.CITY,
               #pat_enc_dep_los.SITE,
               #pat_enc_dep_los.LOS,
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
               #pat_enc_dep_los.DEPARTMENT_NAME
        FROM #pat_enc_dep_los
        WHERE #pat_enc_dep_los.LOS = 'MEDICAL'
              AND #pat_enc_dep_los.ROW_NUM_DESC = 1
    )
SELECT target_service_line.PAT_ID,
       target_service_line.LOS,
       target_service_line.CITY,
       target_service_line.STATE,
       target_service_line.SITE,
       target_service_line.DEPARTMENT_NAME,
       target_service_line.LAST_OFFICE_VISIT
INTO #medical_patients
FROM target_service_line;


IF OBJECT_ID('tempdb..#active_hiv_patients') IS NOT NULL DROP TABLE #active_hiv_patients;
SELECT DISTINCT pev.PAT_ID,
                'HIV+' AS PATIENT_TYPE
INTO #active_hiv_patients
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
WHERE ser.SERV_AREA_ID = 64
      AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
      AND pev.CONTACT_DATE > DATEADD(MM, -12, CURRENT_TIMESTAMP) --Visit in past year
      AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                     8053, 8054, 8055, 8056 ) -- Office Visits
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
      AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
      AND p4.PAT_LIVING_STAT_C = 1;


IF OBJECT_ID('tempdb..#flag_type') IS NOT NULL DROP TABLE #flag_type;
SELECT PATIENT_FYI_FLAGS.PATIENT_ID,
       MIN(CASE WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640005'
                     AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'PrEP'
               WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IN ( '640008', '640034' )
                    AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'STI'
               WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '6400017'
                    AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'False Positive HIV Test'
               WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '9800035'
                    AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'PEP'
               WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640007'
                    AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'AODA HIV-'
               ELSE 'Other'
           END) AS PATIENT_TYPE
INTO #flag_type
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS
WHERE PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IN ( /*PrEP*/ '640005', /*Fasle Pos*/ '640017', /*PEP*/ '9800035', /*STI*/ '640008', /*AODA HIV-*/
                                                      '640007', /*Other HIV-*/ '9800065', /*MPX as STI*/ '640034' )
      AND PATIENT_FYI_FLAGS.ACTIVE_C = 1
GROUP BY PATIENT_FYI_FLAGS.PATIENT_ID;


IF OBJECT_ID('tempdb..#sdoh_patient_cohort') IS NOT NULL DROP TABLE #sdoh_patient_cohort;
SELECT IP_FLWSHT_REC.PAT_ID,
       IP_FLWSHT_MEAS.FLO_MEAS_ID,
       IP_FLO_GP_DATA.DISP_NAME,
       IP_FLWSHT_MEAS.MEAS_VALUE,
       CLARITY_EMP.NAME AS ENTERED_BY,
       CAST(COALESCE(IP_FLWSHT_MEAS.RECORDED_TIME, IP_FLWSHT_MEAS.ENTRY_TIME) AS DATE) AS ENTRY_TIME,
       ROW_NUMBER() OVER (PARTITION BY IP_FLWSHT_REC.PAT_ID,
                                       IP_FLO_GP_DATA.FLO_MEAS_ID
                          ORDER BY COALESCE(IP_FLWSHT_MEAS.RECORDED_TIME, IP_FLWSHT_MEAS.ENTRY_TIME) DESC) AS ROW_NUM_DESC,
       IP_FLWSHT_REC.INPATIENT_DATA_ID
INTO #sdoh_patient_cohort
FROM clarity.dbo.IP_FLWSHT_MEAS_VIEW AS IP_FLWSHT_MEAS
    INNER JOIN clarity.dbo.IP_FLWSHT_REC_VIEW AS IP_FLWSHT_REC ON IP_FLWSHT_MEAS.FSD_ID = IP_FLWSHT_REC.FSD_ID
    INNER JOIN clarity.dbo.IP_FLO_GP_DATA ON IP_FLWSHT_MEAS.FLO_MEAS_ID = IP_FLO_GP_DATA.FLO_MEAS_ID
    INNER JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON IP_FLWSHT_MEAS.ENTRY_USER_ID = CLARITY_EMP.USER_ID
WHERE IP_FLWSHT_MEAS.FLO_MEAS_ID IN ( '3447', '3434', '5693', '3494', '9388', '6569', '5692', '3511', '5700', '5701', '5702', '5703', '2373' )
      AND DATEDIFF(MI, IP_FLWSHT_MEAS.RECORDED_TIME, IP_FLWSHT_MEAS.ENTRY_TIME) < 1440;


WITH
    sdoh_pat_encs AS (
        SELECT #sdoh_patient_cohort.PAT_ID,
               #sdoh_patient_cohort.FLO_MEAS_ID,
               #sdoh_patient_cohort.DISP_NAME,
               #sdoh_patient_cohort.MEAS_VALUE,
               #sdoh_patient_cohort.ENTRY_TIME,
               #sdoh_patient_cohort.ENTERED_BY
        FROM #sdoh_patient_cohort
            INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON #sdoh_patient_cohort.INPATIENT_DATA_ID = PAT_ENC.INPATIENT_DATA_ID
        WHERE ROW_NUM_DESC = 1
    ),
    sdoh_patients AS (
        SELECT sdoh_pat_encs.PAT_ID,
               sdoh_pat_encs.FLO_MEAS_ID,
               sdoh_pat_encs.DISP_NAME,
               sdoh_pat_encs.MEAS_VALUE,
               sdoh_pat_encs.ENTERED_BY,
               sdoh_pat_encs.ENTRY_TIME
        FROM sdoh_pat_encs
    ),
    sdoh_questions_completed AS (
        SELECT sdoh_pat_encs.PAT_ID,
               COUNT(sdoh_pat_encs.PAT_ID) AS QUESTIONS_COMPLETED
        FROM sdoh_pat_encs
        GROUP BY sdoh_pat_encs.PAT_ID
    ),
    next_visit_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_ANY_APPOINTMENT,
               CLARITY_SER.PROV_NAME AS NEXT_APPT_PROVIDER,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS RN_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1 -- Scheduled
    ),
    latest_visit_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS LATEST_APPOINTMENT,
               CLARITY_SER.PROV_NAME AS LATEST_VISIT_PROVIDER,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE DESC) AS RN_DESC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
              AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'MD'
    )
SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       PATIENT.PAT_NAME,
       (DATEDIFF(MONTH, PATIENT.BIRTH_DATE, GETDATE()) / 12) AGE,
       COALESCE(ZC_SEX.NAME, 'Unknown') AS SEX,
       COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE,
       COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY,
       #medical_patients.CITY,
       #medical_patients.STATE,
       CASE WHEN #active_hiv_patients.PATIENT_TYPE IS NOT NULL THEN #active_hiv_patients.PATIENT_TYPE
           WHEN #flag_type.PATIENT_TYPE IS NOT NULL THEN #flag_type.PATIENT_TYPE
           ELSE 'Other'
       END AS PATIENT_TYPE,
       COALESCE(sdoh_questions_completed.QUESTIONS_COMPLETED, 0) AS QUESTIONS_COMPLETED,
       CASE WHEN sdoh_patients.FLO_MEAS_ID = '3434' THEN '2. Financial Resource Strain'
           WHEN sdoh_patients.FLO_MEAS_ID = '3447' THEN '1. Health Literacy'
           WHEN sdoh_patients.FLO_MEAS_ID = '5693' THEN '3. Living Situation'
           WHEN sdoh_patients.FLO_MEAS_ID IN ( '3494', '9388', '6569' ) THEN '4. Food'
           WHEN sdoh_patients.FLO_MEAS_ID IN ( '5692', '2373' ) THEN '5. Transportation'
           WHEN sdoh_patients.FLO_MEAS_ID = '3511' THEN '6. Social Isolation'
           WHEN sdoh_patients.FLO_MEAS_ID IN ( '5700', '5701', '5702', '5703' ) THEN '7. Relationship Safety'
           ELSE '8. No Questions Asked'
       END AS DOMAIN,
       COALESCE(sdoh_patients.DISP_NAME, 'No Questions Asked') AS SDOH_QUESTION,
       COALESCE(sdoh_patients.MEAS_VALUE, 'No Answer Provided.') AS SDOH_ANSWER,
       COALESCE(sdoh_patients.ENTRY_TIME, '1900-01-01') AS SDOH_ENTRY_TIME,
       COALESCE(sdoh_patients.ENTERED_BY, 'None') AS ENTERED_BY,
       CASE WHEN sdoh_patients.PAT_ID IS NOT NULL THEN 'Y'
           ELSE 'N'
       END AS SDOH_COMPLETED_YN,
       CASE WHEN sdoh_patients.PAT_ID IS NOT NULL
                 AND DATEDIFF(MONTH, sdoh_patients.ENTRY_TIME, CURRENT_TIMESTAMP) >= 12 THEN 'N'
           WHEN sdoh_patients.PAT_ID IS NULL THEN 'N'
           ELSE 'Y'
       END AS SDOH_ASKED_WITHIN_12_MONTHS_YN,
       latest_visit_info.LATEST_APPOINTMENT AS LAST_MEDICAL_APPOINTMENT,
       latest_visit_info.LATEST_VISIT_PROVIDER AS LATEST_MEDICAL_VISIT_PROVIDER,
       COALESCE(next_visit_info.NEXT_ANY_APPOINTMENT, '1900-01-01') AS NEXT_ANY_APPOINTMENT,
       COALESCE(next_visit_info.NEXT_APPT_PROVIDER, 'None') AS NEXT_APPT_PROVIDER
FROM #medical_patients
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON #medical_patients.PAT_ID = PATIENT.PAT_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON PATIENT.PAT_ID = IDENTITY_ID.PAT_ID
    LEFT JOIN sdoh_patients ON #medical_patients.PAT_ID = sdoh_patients.PAT_ID
    LEFT JOIN sdoh_questions_completed ON #medical_patients.PAT_ID = sdoh_questions_completed.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_SEX AS ZC_SEX ON PATIENT.SEX_C = ZC_SEX.RCPT_MEM_SEX_C
    LEFT JOIN next_visit_info ON #medical_patients.PAT_ID = next_visit_info.PAT_ID
                                 AND next_visit_info.RN_ASC = 1
    LEFT JOIN latest_visit_info ON #medical_patients.PAT_ID = latest_visit_info.PAT_ID
                                   AND latest_visit_info.RN_DESC = 1
    LEFT JOIN #active_hiv_patients ON #medical_patients.PAT_ID = #active_hiv_patients.PAT_ID
    LEFT JOIN #flag_type ON #medical_patients.PAT_ID = #flag_type.PATIENT_ID
    LEFT JOIN ##patient_race_ethnicity ON #medical_patients.PAT_ID = ##patient_race_ethnicity.PAT_ID;
