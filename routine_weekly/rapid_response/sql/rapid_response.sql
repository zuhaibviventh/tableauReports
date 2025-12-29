/* Uses OCHIN Epic */

SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
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
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2)
       END AS CITY,
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
           ELSE 'ERROR'
       END AS 'LOS'
INTO #Attribution1
FROM CLARITY.dbo.PAT_ENC_VIEW pev
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;

SELECT id.IDENTITY_ID,
       p.PAT_NAME,
       p.PAT_ID,
       zpr.NAME RACE,
       orv.RESULT_DATE,
       orv.ORD_VALUE,
       ser.EXTERNAL_NAME PROV_NAME,
       att.CITY,
       CASE WHEN ISNUMERIC(orv.ORD_VALUE) = 1 THEN orv.ORD_VALUE
           WHEN orv.ORD_VALUE LIKE '>%' THEN 10000000
           ELSE 0.01
       END AS Result_Output,
       ROW_NUMBER() OVER (PARTITION BY opv.PAT_ID ORDER BY orv.RESULT_DATE DESC) ROW_NUM_ASCEND
INTO #a
FROM CLARITY.dbo.PATIENT_VIEW p
    INNER JOIN #Attribution3 att ON att.PAT_ID = p.PAT_ID
    INNER JOIN CLARITY.dbo.ORDER_PROC_VIEW opv ON p.PAT_ID = opv.PAT_ID
    INNER JOIN CLARITY.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    LEFT JOIN CLARITY.dbo.PATIENT_RACE pr ON p.PAT_ID = pr.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_PATIENT_RACE zpr ON pr.PATIENT_RACE_C = zpr.PATIENT_RACE_C
WHERE orv.RESULT_DATE > DATEADD(MM, -14, GETDATE())
      AND orv.COMPONENT_ID IN --(16705, 5543, 64338, 26518, 11688, 63500, 2532, 15737, 85928, 95256, 5037, 61505, 4661, 970107, 20525) Not needed any more since using groupers
          ( SELECT DISTINCT cc.COMPONENT_ID FROM CLARITY.dbo.CLARITY_COMPONENT cc WHERE cc.COMMON_NAME = 'HIV VIRAL LOAD' )
      AND orv.ORD_VALUE NOT IN ( 'Delete', 'See comment' )
      AND p.PAT_ID IN ( SELECT DISTINCT pev.PAT_ID
                        FROM CLARITY.dbo.PATIENT_VIEW p
                            INNER JOIN CLARITY.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
                            INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
                            INNER JOIN CLARITY.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
                            INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                            INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                        WHERE ser.SERV_AREA_ID = 64
                              AND ser.PROVIDER_TYPE_C IN ( 1, 9, 113 ) -- Physicians and NPs
                              AND pev.CONTACT_DATE > DATEADD(MM, -12, CURRENT_TIMESTAMP) --Visit in past year
                              AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
                              AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048,
                                                             8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
                              AND pev.DEPARTMENT_ID IN ( 64001001, 64002001, 64014001, 64015001 ) -- Visit was in a medical department
                              AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
                              AND plv.RESOLVED_DATE IS NULL --Active Dx
                              AND plv.PROBLEM_STATUS_C = 1 --Active Dx
                              AND p4.PAT_LIVING_STAT_C = 1 );

SELECT a.IDENTITY_ID,
       a.PAT_NAME,
       a.PAT_ID,
       a.RACE,
       a.PROV_NAME,
       a.CITY,
       a.ORD_VALUE AS Last_LAB_value,
       a.RESULT_DATE AS Last_LAB_DATE
INTO #b
FROM #a a
WHERE a.Result_Output > 200
      AND a.ROW_NUM_ASCEND = 1
ORDER BY a.IDENTITY_ID;

SELECT a.IDENTITY_ID,
       a.PAT_NAME,
       a.ORD_VALUE AS SecondLast_LAB_value,
       a.RESULT_DATE AS SecondLast_LAB_DATE
INTO #c
FROM #a a
WHERE a.Result_Output < 199
      AND a.ROW_NUM_ASCEND = 2
ORDER BY a.IDENTITY_ID;

SELECT b.IDENTITY_ID,
       b.PAT_NAME,
       b.RACE,
       b.PROV_NAME PCP,
       b.CITY,
       b.Last_LAB_DATE,
       b.Last_LAB_value,
       c.SecondLast_LAB_DATE,
       c.SecondLast_LAB_value,
       DATEDIFF(MONTH, c.SecondLast_LAB_DATE, b.Last_LAB_DATE) MONTHS_BETWEEN,
       pev.CONTACT_DATE,
       ROW_NUMBER() OVER (PARTITION BY b.IDENTITY_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC,
       ser.EXTERNAL_NAME VISIT_PROV
INTO #d
FROM #b b
    INNER JOIN #c c ON b.IDENTITY_ID = c.IDENTITY_ID
    LEFT JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON b.PAT_ID = pev.PAT_ID
                                              AND pev.APPT_STATUS_C = 1
                                              AND pev.CONTACT_DATE > GETDATE()
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID;

SELECT d.IDENTITY_ID,
       d.PAT_NAME,
       d.RACE,
       d.PCP,
       d.CITY,
       CONVERT(NVARCHAR(30), d.Last_LAB_DATE, 101) AS Last_LAB_DATE,
       d.Last_LAB_value,
       CONVERT(NVARCHAR(30), d.SecondLast_LAB_DATE, 101) AS SecondLast_LAB_DATE,
       d.SecondLast_LAB_value,
       d.MONTHS_BETWEEN MONTHS_BETWEEN_LABS,
       CASE WHEN d.CONTACT_DATE IS NULL THEN '(NONE)'
           ELSE CONVERT(NVARCHAR(30), d.CONTACT_DATE, 101)
       END AS NEXT_VISIT_DATE,
       CASE WHEN d.VISIT_PROV IS NULL THEN '(NOBODY)'
           ELSE d.VISIT_PROV
       END AS NEXT_VISIT_PROVIDER
FROM #d d
WHERE d.ROW_NUM_ASC = 1
      AND d.Last_LAB_DATE > DATEADD(d, -8, GETDATE());

DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #c;
DROP TABLE #d;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
