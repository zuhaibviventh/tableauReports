SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#flag_type') IS NOT NULL DROP TABLE #flag_type;
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
           END) AS 'PATIENT TYPE'
INTO #flag_type
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
WHERE flag.PAT_FLAG_TYPE_C IN ( /*PrEP*/ '640005', /*Fasle Pos*/ '640017', /*PEP*/ '9800035', /*STI*/ '640008', /*AODA HIV-*/
                                         '640007', /*Other HIV-*/ '9800065', /*MPX as STI*/ '640034' )
      AND flag.ACTIVE_C = 1
GROUP BY flag.PATIENT_ID;

IF OBJECT_ID('tempdb..#hiv_patients') IS NOT NULL DROP TABLE #hiv_patients;
SELECT DISTINCT plv.PAT_ID,
                'HIV+' AS 'PATIENT TYPE'
INTO #hiv_patients
FROM Clarity.dbo.PROBLEM_LIST_VIEW plv
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
WHERE icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1; --Active Dx


WITH
    research_2is_flag AS (
    SELECT DISTINCT PATIENT_FYI_FLAGS.PATIENT_ID AS PAT_ID
    FROM CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS
    WHERE PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640035'
          AND PATIENT_FYI_FLAGS.ACTIVE_C = 1
)
SELECT id.IDENTITY_ID,
       p.PAT_NAME,
       p.BIRTH_DATE,
       (DATEDIFF(m, p.BIRTH_DATE, GETDATE()) / 12) AGE,
       pev.CONTACT_DATE,
       DATENAME(DW, pev.CONTACT_DATE) 'DAY',
       pev.APPT_STATUS_C,
       pev.APPT_TIME APPT_DATETIME,
       LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5) APPOINTMENT_TIME,
       CASE WHEN LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5) LIKE '%:15' THEN LEFT(DATEADD(MINUTE, -15, (CONVERT(TIME(0), LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5)))), 5)
           WHEN LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5) LIKE '%:45' THEN LEFT(DATEADD(MINUTE, -15, (CONVERT(TIME(0), LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5)))), 5)
           WHEN LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5) LIKE '%:40' THEN LEFT(DATEADD(MINUTE, -10, (CONVERT(TIME(0), LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5)))), 5)
           WHEN LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5) LIKE '%:20' THEN LEFT(DATEADD(MINUTE, -20, (CONVERT(TIME(0), LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5)))), 5)
           WHEN LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5) LIKE '%:50' THEN LEFT(DATEADD(MINUTE, +10, (CONVERT(TIME(0), LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5)))), 5)
           WHEN LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5) LIKE '%:10' THEN LEFT(DATEADD(MINUTE, -10, (CONVERT(TIME(0), LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5)))), 5)
           ELSE LEFT(DATEADD(MINUTE, 0, (CONVERT(TIME(0), pev.APPT_TIME))), 5)
       END AS 'APPT_TIME',
       /* No Show (if course), Late Cancel, Patient to Late to be Seen, and Left Without Being Seen. */
       CASE WHEN pev.APPT_STATUS_C IN ( 4, 5, 7, 13 ) THEN 'NO SHOW'
       END AS 'NO SHOW',
       CASE WHEN pev.APPT_STATUS_C NOT IN ( 4, 5, 7, 13 ) THEN 'COMPLETE'
       END AS 'COMPLETE',
       CASE WHEN pev.APPT_STATUS_C IN ( 4, 5, 7, 13 ) THEN 'NO SHOW'
           ELSE 'COMPLETE'
       END AS 'APPOINTMENT STATUS',
       pev.APPT_PRC_ID,
       ser.PROV_NAME,
       CASE WHEN ser.PROV_TYPE = 'Resource' THEN 'Pharmacist'
           ELSE ser.PROV_TYPE
       END AS PROV_TYPE,
       ser.PROVIDER_TYPE_C,
       dep.DEPARTMENT_NAME,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'MH', 'BH', 'PY' ) THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS 'LOS',
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
       zs.NAME SEX,
       zpr.NAME RACE,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWAUKEE'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KN' THEN 'KENOSHA'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'GB' THEN 'GREEN BAY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'WS' THEN 'WAUSAU'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AP' THEN 'APPLETON'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'EC' THEN 'EAU CLAIRE'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'LC' THEN 'LACROSSE'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MD' THEN 'MADISON'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'BL' THEN 'BELOIT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'BI' THEN 'BILLING'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'SL' THEN 'ST LOUIS'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'DN' THEN 'DENVER'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AS' THEN 'AUSTIN'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KC' THEN 'KANSAS CITY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2)
       END AS CITY,
       prc.PRC_NAME 'APPT TYPE',
       CASE WHEN research_2is_flag.PAT_ID IS NOT NULL THEN 'Yes'
           ELSE 'No'
       END [2iS Patient],
       CASE WHEN #hiv_patients.[PATIENT TYPE] IS NOT NULL THEN #hiv_patients.[PATIENT TYPE]
           WHEN #flag_type.[PATIENT TYPE] IS NOT NULL THEN #flag_type.[PATIENT TYPE]
           ELSE 'Other'
       END AS PATIENT_TYPE
FROM Clarity.dbo.PATIENT_VIEW p
    LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
                                              AND pev.APPT_STATUS_C NOT IN ( 1, 3, 8, 10, 12 ) -- Excludes Scheduled, Canceled, Non-encounter, pt not cooperative and Void
                                              AND pev.APPT_PRC_ID NOT IN ( 118, 119, 120, 152, 428, 506, 50 ) --Excludes AODA group and non-important appts
    INNER JOIN Clarity.dbo.CLARITY_PRC prc ON pev.APPT_PRC_ID = prc.PRC_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    LEFT JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.ZC_SEX zs ON p.SEX_C = zs.RCPT_MEM_SEX_C
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON p.PAT_ID = pr.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON pr.PATIENT_RACE_C = zpr.PATIENT_RACE_C
    LEFT JOIN research_2is_flag ON p.PAT_ID = research_2is_flag.PAT_ID
    LEFT JOIN #flag_type ON pev.PAT_ID = #flag_type.PATIENT_ID
    LEFT JOIN #hiv_patients ON pev.PAT_ID = #hiv_patients.PAT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE())
      --AND (ser.PROVIDER_TYPE_C IN ( 117, 113, 129, 1, 136, 117, 9, 6, 110, 119, 10, 134, 164, 108, 102, 173, 185, 177 ) --Only billing-level providers
      --     OR ser.PROV_ID IN ( '640203', '64202' )) -- Clinical Pharm shadow schedules
      --AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) <> 'AD';
