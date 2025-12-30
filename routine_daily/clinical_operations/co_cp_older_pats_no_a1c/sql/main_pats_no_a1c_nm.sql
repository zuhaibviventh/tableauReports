/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#Attribution1') IS NOT NULL DROP TABLE #Attribution1;
SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
       dep.STATE,
       dep.CITY,
       dep.SITE,
       dep.LOS
       /*
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN' THEN 'MAIN LOCATION'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'DR' THEN 'D&R'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'KE' THEN 'KEENEN'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'UC' THEN 'UNIVERSITY OF COLORADO'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'ON' THEN 'AUSTIN MAIN'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'TW' THEN 'AUSTIN OTHER'
           ELSE 'ERROR'
       END AS 'SITE',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2)
       END AS 'LOS'
       */
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE DATEDIFF(MONTH, pev.CONTACT_DATE, GETDATE()) <= 12
      AND pev.APPT_STATUS_C IN ( 2, 6 );


IF OBJECT_ID('tempdb..#Attribution2') IS NOT NULL DROP TABLE #Attribution2;
SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';


IF OBJECT_ID('tempdb..#Attribution3') IS NOT NULL DROP TABLE #Attribution3;
SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#hiv_patients') IS NOT NULL DROP TABLE #hiv_patients;
SELECT DISTINCT pev.PAT_ID
INTO #hiv_patients
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE ser.SERV_AREA_ID = 64
      AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
      AND pev.CONTACT_DATE > DATEADD(MM, -12, GETDATE()) --Visit in past year
      AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                     8053, 8054, 8055, 8056 ) -- Office Visits
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
      AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
      AND p4.PAT_LIVING_STAT_C = 1;


IF OBJECT_ID('tempdb..#a') IS NOT NULL DROP TABLE #a;
WITH
    npv AS (
        SELECT pev.PAT_ID,
               pev.CONTACT_DATE 'FIRST MEDICAL VISIT',
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
        FROM Clarity.dbo.PAT_ENC_VIEW pev
            LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
        WHERE pev.APPT_STATUS_C IN ( 2, 6 )
              AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
    ),
    fpl AS (
        SELECT pacv.PAT_ID,
               fplp.FPL_PERCENTAGE,
               ROW_NUMBER() OVER (PARTITION BY pacv.PAT_ID ORDER BY fplp.FPL_EFF_DATE DESC) AS ROW_NUM_DESC
        FROM Clarity.dbo.ACCOUNT_FPL_INFO_VIEW fplp
            INNER JOIN Clarity.dbo.PAT_ACCT_CVG_VIEW pacv ON fplp.ACCOUNT_ID = pacv.ACCOUNT_ID
        WHERE fplp.LINE = 1
    )
SELECT id.IDENTITY_ID,
       id.PAT_ID,
       p.PAT_NAME,
       a3.CITY,
       a3.STATE,
       (DATEDIFF(DAY, P.BIRTH_DATE, GETDATE()) / 365.25) AGE,
       zeg.NAME ETHNICITY,
       zpr.NAME RACE,
       sex.NAME 'Legal Sex',
       fpl.FPL_PERCENTAGE,
       well.BMI_LAST,
       well.SMOKING_USER_YN,
       well.ASCVD_10_YR_SCORE,
       well.HAS_HYPERTENSION_YN,
       DATEDIFF(MONTH, npv.[FIRST MEDICAL VISIT], GETDATE()) 'MONTHS SINCE FIRST MEDICAL VISIT',
       ser.PROV_NAME 'PCP'
INTO #a
FROM Clarity.dbo.IDENTITY_ID_VIEW id
    INNER JOIN #hiv_patients ON id.PAT_ID = #hiv_patients.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_ETHNIC_GROUP zeg ON zeg.ETHNIC_GROUP_C = p.ETHNIC_GROUP_C
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON pr.PAT_ID = id.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON zpr.PATIENT_RACE_C = pr.PATIENT_RACE_C
    LEFT JOIN Clarity.dbo.ZC_SEX sex ON p.SEX_C = sex.RCPT_MEM_SEX_C
    INNER JOIN Clarity.dbo.DM_WLL_ALL_VIEW well ON well.PAT_ID = id.PAT_ID
    LEFT JOIN npv ON npv.PAT_ID = id.PAT_ID
                     AND npv.ROW_NUM_DESC = 1
    LEFT JOIN fpl ON fpl.PAT_ID = id.PAT_ID
                     AND fpl.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#exclusion') IS NOT NULL DROP TABLE #exclusion;
SELECT opv.PAT_ID
INTO #exclusion
FROM Clarity.dbo.ORDER_PROC_VIEW opv
    INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
    INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON cc.COMPONENT_ID = orv.COMPONENT_ID
WHERE opv.ORDER_TYPE_C = 7
      AND opv.ORDERING_DATE > DATEADD(MONTH, -36, GETDATE())
      AND cc.COMMON_NAME = 'HEMOGLOBIN A1C';


SELECT a.IDENTITY_ID,
       a.PAT_NAME,
       a.PCP,
       a.CITY,
       a.STATE,
       CAST(a.AGE AS INT) AS AGE,
       COALESCE(a.ETHNICITY, 'Unknown') 'ETHNICITY',
       COALESCE(a.RACE, 'Unknown') 'RACE',
       a.[Legal Sex] 'LEGAL SEX',
       COALESCE(CAST(a.FPL_PERCENTAGE AS INT), 0) AS 'FPL%',
       a.BMI_LAST 'LAST BMI',
       COALESCE(a.SMOKING_USER_YN, 'Unknown') 'TOBACCO USER',
       a.ASCVD_10_YR_SCORE 'ASCVD 10YR RISK',
       a.HAS_HYPERTENSION_YN 'HTN',
       a.[MONTHS SINCE FIRST MEDICAL VISIT],
       CASE WHEN a.[MONTHS SINCE FIRST MEDICAL VISIT] < 1 THEN 'Y'
           ELSE 'N'
       END AS 'NEW PT IN LAST 30 DAYS'
FROM #a a
WHERE a.AGE > 35.99
      AND a.PAT_ID NOT IN ( SELECT #exclusion.PAT_ID FROM #exclusion );
