SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#behavioral_patients') IS NOT NULL DROP TABLE #behavioral_patients;
WITH
    visits_info AS (
        SELECT pev.PAT_ID,
               CAST(pev.CONTACT_DATE AS DATE) AS LAST_OFFICE_VISIT,
               dep.CITY,
               dep.STATE,
               dep.SITE,
               CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
                   ELSE 'ERROR'
               END AS 'LOS',
               dep.SERVICE_TYPE,
               dep.SERVICE_LINE,
               dep.SUB_SERVICE_LINE
        FROM Clarity.dbo.PAT_ENC_VIEW pev
            LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
        WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
              AND pev.APPT_STATUS_C IN ( 2, 6 )
    ),
    behavioral_pats AS (
        SELECT visits_info.*,
               ROW_NUMBER() OVER (PARTITION BY PAT_ID ORDER BY LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM visits_info
        WHERE visits_info.LOS = 'BEHAVIORAL'
    )
SELECT * INTO #behavioral_patients FROM behavioral_pats WHERE behavioral_pats.ROW_NUM_DESC >= 2;


IF OBJECT_ID('tempdb..#mht_patients') IS NOT NULL DROP TABLE #mht_patients;
SELECT PATIENT.PAT_ID,
       PATIENT.PAT_NAME,
       #behavioral_patients.CITY,
       #behavioral_patients.STATE,
       #behavioral_patients.SITE,
       #behavioral_patients.LOS,
       #behavioral_patients.SERVICE_TYPE,
       #behavioral_patients.SERVICE_LINE,
       #behavioral_patients.SUB_SERVICE_LINE,
       COUNT(PAT_ENC.PAT_ENC_CSN_ID) AS VISIT_COUNT_WITHIN_90_DAYS_SINCE_OPEN_EPISODE,
       CAST(EPISODE.START_DATE AS DATE) AS EPISODE_START_DT,
       ROW_NUMBER() OVER (PARTITION BY PATIENT.PAT_ID ORDER BY EPISODE.START_DATE DESC) AS ROW_NUM_DESC
INTO #mht_patients
FROM CLARITY.dbo.PATIENT_VIEW AS PATIENT
    INNER JOIN #behavioral_patients ON PATIENT.PAT_ID = #behavioral_patients.PAT_ID
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON PATIENT.PAT_ID = PAT_ENC.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
    LEFT JOIN CLARITY.dbo.EPISODE_LINK_VIEW AS EPISODE_LINK ON PAT_ENC.PAT_ENC_CSN_ID = EPISODE_LINK.PAT_ENC_CSN_ID
    LEFT JOIN CLARITY.dbo.EPISODE_VIEW AS EPISODE ON EPISODE_LINK.EPISODE_ID = EPISODE.EPISODE_ID
WHERE PAT_ENC.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
      AND EPISODE.SUM_BLK_TYPE_ID = 221
      AND EPISODE.STATUS_C = 1
      AND (PAT_ENC.CONTACT_DATE
      BETWEEN EPISODE.START_DATE AND DATEADD(DAY, 90, EPISODE.START_DATE))
      AND CLARITY_SER.PROVIDER_TYPE_C NOT IN ( '164', '136', '129' )
GROUP BY EPISODE.START_DATE,
         PATIENT.PAT_ID,
         PATIENT.PAT_NAME,
         #behavioral_patients.CITY,
         #behavioral_patients.STATE,
         #behavioral_patients.SITE,              -- ← Add this
         #behavioral_patients.LOS,                -- ← Add this
         #behavioral_patients.SERVICE_TYPE,       -- ← Add this
         #behavioral_patients.SERVICE_LINE,       -- ← Add this
         #behavioral_patients.SUB_SERVICE_LINE;   -- ← Add this


SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       #mht_patients.PAT_NAME,
       #mht_patients.CITY,
       #mht_patients.STATE,
       #mht_patients.SITE,
       #mht_patients.LOS,
        #mht_patients.SERVICE_TYPE,
       #mht_patients.SERVICE_LINE,
      #mht_patients.SUB_SERVICE_LINE,
       #mht_patients.VISIT_COUNT_WITHIN_90_DAYS_SINCE_OPEN_EPISODE,
       #mht_patients.EPISODE_START_DT,
       CASE WHEN #mht_patients.VISIT_COUNT_WITHIN_90_DAYS_SINCE_OPEN_EPISODE >= 4 THEN 'MET'
           ELSE 'NOT MET'
       END AS FOUR_OR_MORE_COMPLETED_VISITS
FROM #mht_patients
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON #mht_patients.PAT_ID = IDENTITY_ID.PAT_ID
WHERE #mht_patients.ROW_NUM_DESC = 1;