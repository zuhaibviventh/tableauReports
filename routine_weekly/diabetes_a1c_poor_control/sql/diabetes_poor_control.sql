SET NOCOUNT ON;

SELECT TOP 100000000 pev.PAT_ID,
                     pev.DEPARTMENT_ID,
                     dep.DEPARTMENT_NAME LAST_VISIT_DEPT,
                     pev.CONTACT_DATE LAST_OFFICE_VISIT,
                     SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
                     CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK'
                              THEN 'MILWAUKEE'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KN'
                         THEN 'KENOSHA'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'GB'
                         THEN 'GREEN BAY'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'WS'
                         THEN 'WAUSAU'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AP'
                         THEN 'APPLETON'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'EC'
                         THEN 'EAU CLAIRE'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'LC'
                         THEN 'LACROSSE'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MD'
                         THEN 'MADISON'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'BL'
                         THEN 'BELOIT'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'BI'
                         THEN 'BILLING'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'SL'
                         THEN 'ST LOUIS'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'DN'
                         THEN 'DENVER'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AS'
                         THEN 'AUSTIN'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KC'
                         THEN 'KANSAS CITY'
					WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
                     ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2)
                     END AS CITY,
                     CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN'
                              THEN 'MAIN LOCATION'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'DR'
                         THEN 'D&R'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'KE'
                         THEN 'KEENEN'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'UC'
                         THEN 'UNIVERSITY OF COLORADO'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'ON'
                         THEN 'AUSTIN MAIN'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'TW'
                         THEN 'AUSTIN OTHER'
                     ELSE 'ERROR'
                     END AS 'SITE',
                     CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                              THEN 'MEDICAL'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
                         THEN 'DENTAL'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM'
                         THEN 'CASE MANAGEMENT'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX'
                         THEN 'PHARMACY'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD'
                         THEN 'BEHAVIORAL'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY'
                         THEN 'BEHAVIORAL'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH'
                         THEN 'BEHAVIORAL'
                     WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH'
                         THEN 'BEHAVIORAL'
                     ELSE 'ERROR'
                     END AS 'LOS'
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT TOP 100000000 a1.PAT_ID,
                     a1.LAST_OFFICE_VISIT,
                     a1.LOS,
                     a1.CITY,
                     a1.STATE,
                     ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';
SELECT TOP 100000000 id.PAT_ID,
                     id.IDENTITY_ID,
                     p.PAT_NAME,
                     ser.EXTERNAL_NAME PCP,
                     att.STATE,
                     att.CITY,
                     CASE WHEN dmr.HBA1C_LAST_DT < DATEADD(
                                                   MONTH, -12, GETDATE())
                              THEN NULL
                     ELSE dmr.HBA1C_LAST
                     END AS LAST_A1c,
                     dmr.HBA1C_LAST_DT RESULT_DATE,
                     CASE WHEN dmr.HBA1C_LAST IS NULL
                              THEN 'Over 7'
                     WHEN dmr.HBA1C_LAST_DT < DATEADD(MONTH, -12, GETDATE())
                         THEN 'Over 7'
                     WHEN dmr.HBA1C_LAST < 7.0
                         THEN 'Under 7'
                     ELSE 'Over 7'
                     END AS 'Controlled_A1C_<_7',
                     CASE WHEN dmr.HBA1C_LAST IS NULL
                              THEN 'Over 9'
                     WHEN dmr.HBA1C_LAST_DT < DATEADD(MONTH, -12, GETDATE())
                         THEN 'Over 9'
                     WHEN dmr.HBA1C_LAST > 9.0
                         THEN 'Over 9'
                     ELSE 'Under 9'
                     END AS 'Poor_A1C_9+',
                     'CLICK HERE FOR PATIENT DETAIL' 'CLICK HERE FOR PATIENT DETAIL'
INTO #a
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.DM_DIABETES_VIEW dmr ON p.PAT_ID = dmr.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN #Attribution2 att ON p.PAT_ID = att.PAT_ID
WHERE att.ROW_NUM_DESC = 1
      AND p.PAT_ID IN ( SELECT DISTINCT pev.PAT_ID
                        FROM Clarity.dbo.PATIENT_VIEW p
                            INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
                            INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
                            INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                            INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
                            INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                            INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                        WHERE ser.SERV_AREA_ID = 64
                              AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6',
                                                           '113' ) -- Physicians and NPs, PAs
                              AND pev.CONTACT_DATE > DATEADD(
                                                     MM, -12, GETDATE()) --Visit in past year
                              AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
                              AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946,
                                                             7947, 7948,
                                                             7949, 7951,
                                                             7952, 7953,
                                                             7954, 7970,
                                                             7971, 7972,
                                                             7973, 7974,
                                                             8047, 8048,
                                                             8049, 8050,
                                                             8051, 8052,
                                                             8053, 8054,
                                                             8055, 8056 ) -- Office Visits
                              AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
                              AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
                              AND plv.RESOLVED_DATE IS NULL --Active Dx
                              AND plv.PROBLEM_STATUS_C = 1 --Active Dx
                              AND p4.PAT_LIVING_STAT_C = 1 );

SELECT TOP 100000000 -- Next ANY Appt
       a.PAT_ID,
       a.IDENTITY_ID MRN,
       a.PAT_NAME,
       a.PCP,
       a.STATE,
       a.CITY,
       a.LAST_A1c,
       a.RESULT_DATE,
       a.[Controlled_A1C_<_7],
       a.[Poor_A1C_9+],
       a.[CLICK HERE FOR PATIENT DETAIL],
       pev2.CONTACT_DATE NEXT_APPT,
       ser2.EXTERNAL_NAME NEXT_APPT_PROV,
       ROW_NUMBER() OVER (PARTITION BY a.PAT_ID ORDER BY pev2.CONTACT_DATE ASC) AS ROW_NUM_ASC
INTO #b
FROM #a a
    LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev2 ON a.PAT_ID = pev2.PAT_ID
                                               AND pev2.APPT_STATUS_C = 1
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser2 ON pev2.VISIT_PROV_ID = ser2.PROV_ID;

SELECT TOP 100000000 --Next PCP Appt
       b.PAT_ID,
       b.MRN,
       b.PAT_NAME AS Patient,
       b.PCP,
       b.STATE,
       b.CITY,
       b.LAST_A1c,
       b.RESULT_DATE,
       b.[Controlled_A1C_<_7],
       b.[Poor_A1C_9+],
       b.NEXT_APPT,
       b.NEXT_APPT_PROV,
       CONVERT(NVARCHAR(30), na.NEXT_PCP_APPT, 101) AS 'NEXT PCP APPT',
       na.EXTERNAL_NAME 'PCP APPT PROVIDER',
       b.[CLICK HERE FOR PATIENT DETAIL],
       MAX(CASE WHEN flag.PAT_FLAG_TYPE_C = '640013' THEN 'YES' ELSE 'NO' END) AS 'IN CLINICAL PHARMACY COHORT',
       MAX(CASE WHEN flag.PAT_FLAG_TYPE_C = '640018' THEN 'YES' ELSE 'NO' END) AS 'IN DIETITIAN CARE',
       zpr.NAME 'RACE',
       CASE WHEN zeg.NAME IS NULL
                THEN 'Unknown'
       WHEN zeg.NAME = ''
           THEN 'Unknown'
       WHEN zeg.NAME = 'Not Collected/Unknown'
           THEN 'Unknown'
       WHEN zeg.NAME = 'Patient Refused'
           THEN 'Unknown'
       ELSE zeg.NAME
       END AS ETHNICITY
FROM #b b
    LEFT JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag ON b.PAT_ID = flag.PATIENT_ID
                                                         AND flag.PAT_FLAG_TYPE_C IN (
'640013'                                              , '640018', '64000011' ) --Diabetic AND pre-DM cohort
                                                         AND flag.ACTIVE_C = 1
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON pr.PAT_ID = b.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON zpr.PATIENT_RACE_C = pr.PATIENT_RACE_C
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = b.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_ETHNIC_GROUP zeg ON zeg.ETHNIC_GROUP_C = p.ETHNIC_GROUP_C
    LEFT JOIN (SELECT pev.PAT_ID,
                      pev.CONTACT_DATE NEXT_PCP_APPT,
                      ser.EXTERNAL_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1
                     AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
                     AND ser.PROV_ID <> '640178' --pulmonologist
    ) na ON b.PAT_ID = na.PAT_ID
            AND na.ROW_NUM_ASC = 1
WHERE b.ROW_NUM_ASC = 1
GROUP BY b.PAT_ID,
         b.MRN,
         zeg.NAME,
         b.PAT_NAME,
         b.PCP,
         b.STATE,
         b.CITY,
         b.LAST_A1c,
         b.RESULT_DATE,
         b.[Controlled_A1C_<_7],
         b.[Poor_A1C_9+],
         b.NEXT_APPT,
         b.NEXT_APPT_PROV,
         na.NEXT_PCP_APPT,
         na.EXTERNAL_NAME,
         b.[CLICK HERE FOR PATIENT DETAIL],
         zpr.NAME;

DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;