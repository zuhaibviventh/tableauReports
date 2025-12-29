/* 
    To get the first date of the smartphrase use, to use an the enrollment date 
    878323  = SA64RNCMRTCHK
    1475049 = SA64CAREPLANNURSECASEMANAGEMENT

    BPA_TRIGGER_FYI_C NAME                  TITLE                 ABBR          INTERNAL_ID
    640040            SA64 RN-Case Managed  SA64 RN-CASE MANAGED  SA64 RN-Case  640040
*/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#fyi_pop') IS NOT NULL DROP TABLE #fyi_pop;
WITH
    fyi_pop AS (
        SELECT PATIENT_FYI_FLAGS.PATIENT_ID,
               CLARITY_EMP.NAME AS FLAGGED_BY_NAME,
               CLARITY_SER.PROV_TYPE AS FLAGGED_BY_ROLE,
               ZC_BPA_TRIGGER_FYI.NAME AS FYI_FLAG_NAME,
               PATIENT_FYI_FLAGS.ACCT_NOTE_INSTANT AS ENROLLMENT_DATE
        FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS
            INNER JOIN Clarity.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON PATIENT_FYI_FLAGS.ENTRY_PERSON_ID = CLARITY_EMP.USER_ID
            INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI AS ZC_BPA_TRIGGER_FYI ON PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = ZC_BPA_TRIGGER_FYI.BPA_TRIGGER_FYI_C
            LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON clarity_emp.PROV_ID = CLARITY_SER.PROV_ID
        WHERE PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640040'
              AND PATIENT_FYI_FLAGS.ACTIVE_C = 1
    )
SELECT PATIENT.PAT_ID,
       IDENTITY_ID.IDENTITY_ID AS PAT_MRN,
       patient.PAT_NAME,
       ZC_SEX.NAME AS LEGAL_SEX,
       fyi_pop.FLAGGED_BY_NAME,
       fyi_pop.FLAGGED_BY_ROLE,
       fyi_pop.FYI_FLAG_NAME,
       fyi_pop.ENROLLMENT_DATE,
       CASE WHEN ORDER_RESULTS.ORD_NUM_VALUE <> 9999999 THEN ORDER_RESULTS.ORD_NUM_VALUE
           WHEN ORDER_RESULTS.ORD_NUM_VALUE LIKE '>%' THEN 10000000
           ELSE 0
       END AS HIV_VIRAL_LOAD,
       ORDER_RESULTS.RESULT_DATE AS LAB_DATE,
       ROW_NUMBER() OVER (PARTITION BY ORDER_PROC.PAT_ID
ORDER BY ORDER_RESULTS.RESULT_DATE DESC) AS ROW_NUM_DESC,
       ROW_NUMBER() OVER (PARTITION BY ORDER_PROC.PAT_ID
ORDER BY ORDER_RESULTS.RESULT_DATE ASC) AS ROW_NUM_ASC
INTO #fyi_pop
FROM CLARITY.dbo.PATIENT_VIEW AS PATIENT
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
    INNER JOIN fyi_pop ON fyi_pop.PATIENT_ID = PATIENT.PAT_ID
    INNER JOIN Clarity.dbo.ORDER_PROC_VIEW AS ORDER_PROC ON ORDER_PROC.PAT_ID = PATIENT.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_SEX AS ZC_SEX ON PATIENT.SEX_C = ZC_SEX.RCPT_MEM_SEX_C
    INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW AS ORDER_RESULTS ON ORDER_RESULTS.ORDER_PROC_ID = ORDER_PROC.ORDER_PROC_ID
    INNER JOIN Clarity.dbo.CLARITY_COMPONENT AS CLARITY_COMPONENT ON CLARITY_COMPONENT.COMPONENT_ID = ORDER_RESULTS.COMPONENT_ID
WHERE CLARITY_COMPONENT.COMMON_NAME = 'HIV VIRAL LOAD'
      AND ORDER_RESULTS.ORD_VALUE NOT IN ( 'Delete', 'See comment' );


IF OBJECT_ID('tempdb..#pat_viral_load_info') IS NOT NULL DROP TABLE #pat_viral_load_info;
WITH
    first_viral_load_info AS (
        SELECT #fyi_pop.PAT_ID,
               #fyi_pop.HIV_VIRAL_LOAD AS FIRST_VIRAL_LOAD,
               CAST(#fyi_pop.LAB_DATE AS DATE) AS FIRST_VIRAL_LOAD_DATE
        FROM #fyi_pop
        WHERE #fyi_pop.ROW_NUM_ASC = 1
    ),
    last_viral_load_info AS (
        SELECT #fyi_pop.PAT_ID,
               #fyi_pop.HIV_VIRAL_LOAD AS LAST_VIRAL_LOAD,
               CAST(#fyi_pop.LAB_DATE AS DATE) AS LAST_VIRAL_LOAD_DATE
        FROM #fyi_pop
        WHERE #fyi_pop.ROW_NUM_DESC = 1
    )
SELECT #fyi_pop.PAT_ID,
       #fyi_pop.PAT_MRN,
       #fyi_pop.PAT_NAME,
       #fyi_pop.LEGAL_SEX,
       #fyi_pop.FLAGGED_BY_NAME,
       #fyi_pop.FLAGGED_BY_ROLE,
       #fyi_pop.FYI_FLAG_NAME,
       #fyi_pop.LAB_DATE,
       #fyi_pop.ENROLLMENT_DATE,
       first_viral_load_info.FIRST_VIRAL_LOAD,
       first_viral_load_info.FIRST_VIRAL_LOAD_DATE,
       last_viral_load_info.LAST_VIRAL_LOAD,
       last_viral_load_info.LAST_VIRAL_LOAD_DATE
INTO #pat_viral_load_info
FROM #fyi_pop
    INNER JOIN first_viral_load_info ON first_viral_load_info.PAT_ID = #fyi_pop.PAT_ID
    INNER JOIN last_viral_load_info ON last_viral_load_info.PAT_ID = #fyi_pop.PAT_ID
GROUP BY #fyi_pop.PAT_ID,
         PAT_MRN,
         PAT_NAME,
         LEGAL_SEX,
         first_viral_load_info.FIRST_VIRAL_LOAD,
         first_viral_load_info.FIRST_VIRAL_LOAD_DATE,
         last_viral_load_info.LAST_VIRAL_LOAD,
         last_viral_load_info.LAST_VIRAL_LOAD_DATE,
         #fyi_pop.FLAGGED_BY_NAME,
         #fyi_pop.FLAGGED_BY_ROLE,
         #fyi_pop.FYI_FLAG_NAME,
         #fyi_pop.ENROLLMENT_DATE,
         #fyi_pop.LAB_DATE;

WITH
    last_visit_info AS (
        SELECT pat_enc.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS LAST_VISIT_DATE,
               CLARITY_SER.PROV_NAME AS LAST_VISIT_PROVIDER,
               CLARITY_DEP.DEPARTMENT_NAME AS LAST_VISIT_LOCATION,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE DESC) AS RN_DESC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID
            LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
              AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'MD'
              AND PAT_ENC.CONTACT_DATE > DATEADD(MONTH, -24, CURRENT_TIMESTAMP)
    ),
    next_visit_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_VISIT_DATE,
               CLARITY_SER.PROV_NAME AS NEXT_VISIT_PROVIDER,
               CLARITY_DEP.DEPARTMENT_NAME AS NEXT_VISIT_LOCATION,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS RN_ASC
        FROM CLARITY.dbo.pat_enc_view AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID
            LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1
              AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'MD'
    ),
    vls_outcome AS (
        SELECT #pat_viral_load_info.PAT_ID,
               CASE WHEN #pat_viral_load_info.FIRST_VIRAL_LOAD_DATE = #pat_viral_load_info.LAST_VIRAL_LOAD_DATE THEN 'Cannot Evaluate'
                   WHEN #pat_viral_load_info.FIRST_VIRAL_LOAD < 200
                        AND #pat_viral_load_info.LAST_VIRAL_LOAD < 200 THEN 'Maintained Suppression'
                   WHEN #pat_viral_load_info.FIRST_VIRAL_LOAD < 200
                        AND #pat_viral_load_info.LAST_VIRAL_LOAD >= 200 THEN 'Lost Suppression'
                   WHEN #pat_viral_load_info.FIRST_VIRAL_LOAD >= 200
                        AND #pat_viral_load_info.LAST_VIRAL_LOAD < 200 THEN 'Achieved Suppression'
                   WHEN #pat_viral_load_info.FIRST_VIRAL_LOAD >= 200
                        AND #pat_viral_load_info.LAST_VIRAL_LOAD >= 200 THEN 'Did Not Achieve Suppression'
                   ELSE 'ERROR'
               END AS OUTCOME
        FROM #pat_viral_load_info
    ),
    smart_phrase AS (
        SELECT PAT_ENC.PAT_ID,
               PAT_ENC.CONTACT_DATE AS VISIT_DATE,
               CL_SPHR.SMARTPHRASE_NAME AS SMART_PHRASE_NAME,
               MIN(CAST(PAT_ENC.CONTACT_DATE AS DATE)) AS SMART_PHRASE_USE_DATE, -- Get first date of smartphrase use to use as enrollment date
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID,
                                               CL_SPHR.SMARTPHRASE_NAME
                                  ORDER BY PAT_ENC.CONTACT_DATE DESC) AS ROW_NUM_DESC
        FROM Clarity.dbo.PAT_ENC_VIEW PAT_ENC
            INNER JOIN Clarity.dbo.SMARTTOOL_LOGGER_VIEW AS SMARTTOOL_LOGGER ON SMARTTOOL_LOGGER.CSN = PAT_ENC.PAT_ENC_CSN_ID
            INNER JOIN Clarity.dbo.CL_SPHR AS CL_SPHR ON SMARTTOOL_LOGGER.SMARTPHRASE_ID = CL_SPHR.SMARTPHRASE_ID
        WHERE CL_SPHR.SMARTPHRASE_ID IN ( 878323, 1475049 )
        GROUP BY PAT_ENC.PAT_ID,
                 CL_SPHR.SMARTPHRASE_NAME,
                 PAT_ENC.CONTACT_DATE
    )
SELECT #pat_viral_load_info.PAT_MRN,
       #pat_viral_load_info.PAT_NAME,
       PATIENT.CITY,
       ZC_STATE.NAME AS STATE,
       COALESCE(CLARITY_SER.PROV_NAME, 'Unknown') AS PCP,
       COALESCE(ZC_GENDER_IDENTITY.NAME, 'Unknown') AS GENDER_IDENTITY,
       COALESCE(ZC_PATIENT_RACE.NAME, 'Unknown') AS PATIENT_RACE,
       COALESCE(ZC_ETHNIC_GROUP.NAME, 'Unknown') AS ETHNICITY,
       CAST(patient.BIRTH_DATE AS DATE) AS BIRTH_DATE,
       COALESCE(DATEDIFF(MONTH, smart_phrase.SMART_PHRASE_USE_DATE, CURRENT_TIMESTAMP), -1) AS MONTHS_ENROLLED,
       COALESCE(smart_phrase.SMART_PHRASE_NAME, 'None') AS SMART_PHRASE_NAME,
       COALESCE(smart_phrase.SMART_PHRASE_USE_DATE, '2100-01-01') AS SMART_PHRASE_USE_DATE,
       #pat_viral_load_info.FYI_FLAG_NAME,
       #pat_viral_load_info.ENROLLMENT_DATE,
       #pat_viral_load_info.FIRST_VIRAL_LOAD,
       #pat_viral_load_info.FIRST_VIRAL_LOAD_DATE,
       #pat_viral_load_info.LAST_VIRAL_LOAD,
       #pat_viral_load_info.LAST_VIRAL_LOAD_DATE,
       CASE WHEN #pat_viral_load_info.LAST_VIRAL_LOAD < 200 THEN 'Suppressed'
           ELSE 'Unsuppressed'
       END AS CURRENT_SUPPRESSION_STATUS,
       DATEDIFF(MONTH, #pat_viral_load_info.FIRST_VIRAL_LOAD_DATE, #pat_viral_load_info.LAST_VIRAL_LOAD_DATE) AS MONTHS_BETWEEN_LABS,
       COALESCE(last_visit_info.LAST_VISIT_DATE, '1900-01-01') AS LAST_VISIT_DATE,
       COALESCE(last_visit_info.LAST_VISIT_PROVIDER, 'No Last Visit') AS LAST_VISIT_PROVIDER,
       COALESCE(last_visit_info.LAST_VISIT_LOCATION, 'No Last Visit') AS LAST_VISIT_LOCATION,
       COALESCE(next_visit_info.NEXT_VISIT_DATE, '2100-01-01') AS NEXT_VISIT_DATE,
       COALESCE(next_visit_info.NEXT_VISIT_PROVIDER, 'Unscheduled') AS NEXT_VISIT_PROVIDER,
       COALESCE(next_visit_info.NEXT_VISIT_LOCATION, 'Unscheduled') AS NEXT_VISIT_LOCATION,
       vls_outcome.OUTCOME,
       COALESCE(#pat_viral_load_info.FLAGGED_BY_NAME, 'Not in RN Case Management') AS FLAGGED_BY_NAME,
       COALESCE(#pat_viral_load_info.FLAGGED_BY_ROLE, 'Not in RN Case Management') AS FLAGGED_BY_ROLE
FROM #pat_viral_load_info
    INNER JOIN CLARITY.dbo.patient_view AS PATIENT ON PATIENT.PAT_ID = #pat_viral_load_info.PAT_ID
    INNER JOIN vls_outcome ON vls_outcome.PAT_ID = #pat_viral_load_info.PAT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON patient.CUR_PCP_PROV_ID = CLARITY_SER.PROV_ID
    LEFT JOIN CLARITY.dbo.PATIENT_4 AS PATIENT_4 ON #pat_viral_load_info.PAT_ID = PATIENT_4.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_GENDER_IDENTITY AS ZC_GENDER_IDENTITY ON PATIENT_4.GENDER_IDENTITY_C = ZC_GENDER_IDENTITY.GENDER_IDENTITY_C
    LEFT JOIN CLARITY.dbo.PATIENT_RACE AS PATIENT_RACE ON #pat_viral_load_info.PAT_ID = PATIENT_RACE.PAT_ID
                                                          AND PATIENT_RACE.LINE = 1
    LEFT JOIN CLARITY.dbo.ZC_PATIENT_RACE AS ZC_PATIENT_RACE ON PATIENT_RACE.PATIENT_RACE_C = ZC_PATIENT_RACE.PATIENT_RACE_C
    LEFT JOIN CLARITY.dbo.ZC_ETHNIC_GROUP AS ZC_ETHNIC_GROUP ON patient.ETHNIC_GROUP_C = ZC_ETHNIC_GROUP.ETHNIC_GROUP_C
    LEFT JOIN last_visit_info ON #pat_viral_load_info.PAT_ID = last_visit_info.PAT_ID
                                 AND last_visit_info.RN_DESC = 1
    LEFT JOIN next_visit_info ON #pat_viral_load_info.PAT_ID = next_visit_info.PAT_ID
                                 AND next_visit_info.RN_ASC = 1
    LEFT JOIN smart_phrase ON #pat_viral_load_info.PAT_ID = smart_phrase.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_STATE AS ZC_STATE ON PATIENT.STATE_C = ZC_STATE.STATE_C
WHERE smart_phrase.ROW_NUM_DESC = 1
GROUP BY #pat_viral_load_info.PAT_MRN,
         #pat_viral_load_info.PAT_NAME,
         PATIENT.CITY,
         ZC_STATE.NAME,
         CLARITY_SER.PROV_NAME,
         ZC_GENDER_IDENTITY.NAME,
         ZC_PATIENT_RACE.NAME,
         ZC_ETHNIC_GROUP.NAME,
         patient.BIRTH_DATE,
         smart_phrase.SMART_PHRASE_USE_DATE,
         smart_phrase.SMART_PHRASE_NAME,
         #pat_viral_load_info.FYI_FLAG_NAME,
         #pat_viral_load_info.FIRST_VIRAL_LOAD,
         #pat_viral_load_info.FIRST_VIRAL_LOAD_DATE,
         #pat_viral_load_info.LAST_VIRAL_LOAD,
         #pat_viral_load_info.LAST_VIRAL_LOAD_DATE,
         last_visit_info.LAST_VISIT_DATE,
         last_visit_info.LAST_VISIT_PROVIDER,
         last_visit_info.LAST_VISIT_LOCATION,
         next_visit_info.NEXT_VISIT_DATE,
         next_visit_info.NEXT_VISIT_PROVIDER,
         next_visit_info.NEXT_VISIT_LOCATION,
         vls_outcome.OUTCOME,
         #pat_viral_load_info.FLAGGED_BY_NAME,
         #pat_viral_load_info.FLAGGED_BY_ROLE,
         #pat_viral_load_info.ENROLLMENT_DATE
ORDER BY PAT_MRN;
