SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#a') IS NOT NULL DROP TABLE #a;
SELECT 'None' AS 'Client ID',
       id.IDENTITY_ID 'Epic MRN',
       p.PAT_LAST_NAME 'Last Name',
       p.PAT_FIRST_NAME 'First Name',
       UPPER(ct.Provider) 'Provider',
       CASE WHEN ct.[Provider Relationship] IS NOT NULL THEN ct.[Provider Relationship]
           WHEN prog.Program = 'Dental' THEN 'Dentist'
           WHEN prog.Program = 'Behavioral' THEN 'Mental Health Provider'
       END AS 'Provider Relationship',
       p.BIRTH_DATE SCPDateOfBirth,
       CONVERT(NVARCHAR(30), p.BIRTH_DATE, 101) AS 'DOB',
       prog.Program,
       DATEADD(YEAR, DATEPART(YEAR, GETDATE()) - DATEPART(YEAR, p.BIRTH_DATE), p.BIRTH_DATE) AS 'Birthday This Year',
       oc.OTHER_COMMUNIC_NUM 'Mobile Phone',
       p.EMAIL_ADDRESS 'Email',
       CONVERT(NVARCHAR(30), svis.[Next Any Appt], 101) AS 'Next Any Appt',
       svis.[Next Appt Prov],
       CONVERT(NVARCHAR(30), spvis.[Next PCP Appt], 101) AS 'Next PCP Appt',
       spvis.[Next PCP Appt Prov],
       CONVERT(NVARCHAR(30), dvis.[Next Dental Appt], 101) AS 'Next Dental Appt',
       dvis.[Next Dental Appt Prov]
INTO #a
FROM Clarity.dbo.IDENTITY_ID_VIEW id
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.OTHER_COMMUNCTN oc ON p.PAT_ID = oc.PAT_ID
                                                AND oc.OTHER_COMMUNIC_C = '1' --mobile
    INNER JOIN (SELECT DISTINCT ev.PAT_LINK_ID PAT_ID,
                                'Dental' AS 'Program'
                FROM Clarity.dbo.EPISODE_VIEW ev
                    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = ev.PAT_LINK_ID
                    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                WHERE ev.STATUS_C = 1 --Active episode
                      AND ev.SUM_BLK_TYPE_ID = 45 --Dental
                      AND pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
                      AND pev.APPT_STATUS_C IN ( 2, 6 )
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'TX'
                UNION
                SELECT DISTINCT ev.PAT_LINK_ID PAT_ID,
                                'Behavioral' AS 'Program'
                FROM Clarity.dbo.EPISODE_VIEW ev
                    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = ev.PAT_LINK_ID
                    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                WHERE ev.STATUS_C = 1 --Active episode
                      AND ev.SUM_BLK_TYPE_ID = 221 --Dental
                      AND pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
                      AND pev.APPT_STATUS_C IN ( 2, 6 )
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'MH', 'BH', 'PY' )
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'TX'
                UNION
                SELECT DISTINCT pev.PAT_ID,
                                'Medical' AS 'Program'
                FROM Clarity.dbo.PATIENT_VIEW p
                    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
                    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
                    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
                    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                WHERE ser.SERV_AREA_ID = 64
                      AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
                      AND pev.CONTACT_DATE > DATEADD(MM, -12, GETDATE()) --Visit in past year
                      AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
                      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049,
                                                     8050, 8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'TX'
                      AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
                      AND plv.RESOLVED_DATE IS NULL --Active Dx
                      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
                      AND p4.PAT_LIVING_STAT_C = 1) prog ON prog.PAT_ID = id.PAT_ID
    LEFT JOIN (SELECT pcp.PAT_ID,
                      ser.PROV_NAME 'Provider',
                      CASE WHEN ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) THEN 'PCP'
                          WHEN ser.PROVIDER_TYPE_C IN ( '164', '136', '129', '134', '117', '178', '177', '175', '10', '171', '110' ) THEN
                              'Mental Health Provider'
                          WHEN ser.PROVIDER_TYPE_C = '108' THEN 'Dentist'
                      END AS 'Provider Relationship'
               FROM Clarity.dbo.PAT_PCP_VIEW pcp
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pcp.PCP_PROV_ID = ser.PROV_ID
               WHERE ser.SERV_AREA_ID = '64'
                     AND pcp.TERM_DATE IS NULL
                     AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' /*Medical*/, '164', '136', '129', '134', '117', '178', '177', '175', '10', '171',
                                                  '110' /*BH*/, '108' /*dental*/ )) ct ON ct.PAT_ID = id.PAT_ID
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next Any Appt',
                      ser.PROV_NAME 'Next Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled

    ) svis ON svis.PAT_ID = id.PAT_ID
              AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next PCP Appt',
                      ser.PROV_NAME 'Next PCP Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND ser.PROV_ID <> '640178' --pulmonologist
                     AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs

    ) spvis ON spvis.PAT_ID = id.PAT_ID
               AND spvis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next Dental Appt',
                      ser.PROV_NAME 'Next Dental Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND ser.PROVIDER_TYPE_C IN ( '108', '119' ) -- Dentist and hygienist

    ) dvis ON dvis.PAT_ID = id.PAT_ID
              AND dvis.ROW_NUM_ASC = 1 -- First scheduled
;


IF OBJECT_ID('tempdb..#b') IS NOT NULL DROP TABLE #b;
SELECT a.[Client ID],
       a.[Epic MRN],
       a.[Last Name],
       a.[First Name],
       a.Provider,
       a.[Provider Relationship],
       a.SCPDateOfBirth,
       a.DOB,
       a.Program,
       a.[Birthday This Year],
       CASE WHEN DATEDIFF(MONTH, GETDATE(), a.[Birthday This Year]) >= 6 THEN DATEADD(MONTH, -6, a.[Birthday This Year]) --For bdays more than 6 months in the future
           WHEN DATEDIFF(MONTH, GETDATE(), a.[Birthday This Year]) < -6 THEN DATEADD(MONTH, 12, a.[Birthday This Year])  --When b-day more than 6 mo ago
           WHEN DATEDIFF(MONTH, GETDATE(), a.[Birthday This Year]) < 0 THEN DATEADD(MONTH, 6, a.[Birthday This Year])    -- When b-day -1 to -6 mo ago
           WHEN DATEDIFF(MONTH, GETDATE(), a.[Birthday This Year]) < 6 THEN a.[Birthday This Year]                       --For bday 0-6 mos from today
       END AS 'Eligibility Due',
       a.[Mobile Phone],
       a.Email,
       a.[Next Any Appt],
       a.[Next Appt Prov],
       a.[Next PCP Appt],
       a.[Next PCP Appt Prov],
       a.[Next Dental Appt],
       a.[Next Dental Appt Prov]
INTO #b
FROM #a a;

SELECT b.[Client ID],
       b.[Epic MRN],
       b.[Last Name],
       b.[First Name],
       b.Provider,
       b.[Provider Relationship],
       b.SCPDateOfBirth,
       b.DOB,
       b.Program,
       b.[Birthday This Year],
       b.[Eligibility Due],
       DATEDIFF(DAY, GETDATE(), b.[Eligibility Due]) 'Days Until Eligibility Due',
       b.[Mobile Phone],
       b.Email,
       CAST(b.[Next Any Appt] AS DATE) AS [Next Any Appt],
       b.[Next Appt Prov],
       CAST(b.[Next PCP Appt] AS DATE) AS [Next PCP Appt],
       b.[Next PCP Appt Prov],
       CAST(b.[Next Dental Appt] AS DATE) AS [Next Dental Appt],
       b.[Next Dental Appt Prov]
FROM #b b;

