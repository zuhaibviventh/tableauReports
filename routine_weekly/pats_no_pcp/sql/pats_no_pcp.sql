/* javelin.ochin.org */

SET NOCOUNT ON
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#active_sti_patients') IS NOT NULL DROP TABLE #active_sti_patients;
SELECT DISTINCT PATIENT_FYI_FLAGS.PATIENT_ID
INTO #active_sti_patients
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS
WHERE PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IN ('640008', '640050')
      AND PATIENT_FYI_FLAGS.ACTIVE_C = 1;


IF OBJECT_ID('tempdb..#a') IS NOT NULL DROP TABLE #a;
WITH
    pcp AS (
        SELECT pcp1.PAT_ID,
               pcp1.TERM_DATE,
               ROW_NUMBER() OVER (PARTITION BY pcp1.PAT_ID ORDER BY pcp1.TERM_DATE DESC) AS ROW_NUM_DESC
        FROM Clarity.dbo.PAT_PCP_VIEW pcp1
            LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pcp1.PCP_PROV_ID = ser.PROV_ID
        WHERE pcp1.PCP_TYPE_C = 1
              AND ser.SERV_AREA_ID = 64
              AND pcp1.TERM_DATE IS NOT NULL
    )
SELECT id.IDENTITY_ID,
       p.PAT_NAME,
       ser.EXTERNAL_NAME VISIT_PROVIDER,
       CAST(pev.CONTACT_DATE AS DATE) AS LAST_VISIT,
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) STATE,
       SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) CITY,
       pcp.TERM_DATE,
       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
INTO #a
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    LEFT JOIN pcp ON pcp.PAT_ID = pev.PAT_ID
                     AND pcp.ROW_NUM_DESC = 1
WHERE pev.APPT_STATUS_C IN ( 2, 6 )
      AND pev.CONTACT_DATE > DATEADD(DAY, -31, GETDATE())
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND p.CUR_PCP_PROV_ID IS NULL
      AND ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs and NPs
      AND pev.PAT_ID NOT IN ( SELECT PATIENT_ID FROM #active_sti_patients );


SELECT a.IDENTITY_ID,
       a.PAT_NAME,
       a.VISIT_PROVIDER,
       a.LAST_VISIT,
       a.STATE,
       a.CITY
FROM #a a
WHERE 1 = 1
      AND (a.TERM_DATE IS NULL --To exclude those who were termed on or after the date of the last visit
           OR a.TERM_DATE < a.LAST_VISIT);


IF OBJECT_ID('tempdb..#a') IS NOT NULL DROP TABLE #a;
IF OBJECT_ID('tempdb..#active_sti_patients') IS NOT NULL DROP TABLE #active_sti_patients;
