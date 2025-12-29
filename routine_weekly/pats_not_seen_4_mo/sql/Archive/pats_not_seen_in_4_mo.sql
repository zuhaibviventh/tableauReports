set nocount on;

SELECT pev.PAT_ID
      ,pev.DEPARTMENT_ID
      ,dep.DEPARTMENT_NAME LAST_VISIT_DEPT
      ,pev.CONTACT_DATE LAST_OFFICE_VISIT
      ,SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE'
      ,CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK'
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
       ELSE 'ERROR'
       END AS CITY
      ,CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN'
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
       END AS 'SITE'
      ,CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
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
FROM CLARITY.dbo.PAT_ENC_VIEW pev
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE()) --can't do this since looking for pts not seen in a long time
      AND pev.APPT_STATUS_C IN ( 2, 6 );
SELECT a1.PAT_ID
      ,a1.LAST_OFFICE_VISIT
      ,a1.LOS
      ,a1.CITY
      ,a1.STATE
      ,ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT a2.PAT_ID
      ,a2.LAST_OFFICE_VISIT
      ,a2.LOS
      ,a2.CITY
      ,a2.STATE
INTO #Attribution3
FROM #Attribution2 a2
WHERE a2.ROW_NUM_DESC = 1
      AND a2.PAT_ID IN ( SELECT DISTINCT pev.PAT_ID
                         FROM CLARITY.dbo.PATIENT_VIEW p
                             INNER JOIN CLARITY.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
                             INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
                             INNER JOIN CLARITY.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
                             INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                             INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                             INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                         WHERE ser.SERV_AREA_ID = 64
                               AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6'
                                                            ,'113' ) -- Physicians and NPs, PAs
                               --AND pev.CONTACT_DATE > DATEADD (MM,-12, CURRENT_TIMESTAMP) --Can't do since looking for OOC pts
                               AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
                               AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946
                                                             ,7947, 7948
                                                             ,7949, 7951
                                                             ,7952, 7953
                                                             ,7954, 7970
                                                             ,7971, 7972
                                                             ,7973, 7974
                                                             ,8047, 8048
                                                             ,8049, 8050
                                                             ,8051, 8052
                                                             ,8053, 8054
                                                             ,8055, 8056 ) -- Office Visits
                               AND pev.DEPARTMENT_ID IN ( 64001001, 64002001
                                                          ,64003001, 64011001
                                                          ,64012002, 64013001 ) -- Visit was in a medical department
                               AND icd10.CODE IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
                               AND plv.RESOLVED_DATE IS NULL --Active Dx
                               AND plv.PROBLEM_STATUS_C = 1 --Active Dx
                               AND p4.PAT_LIVING_STAT_C = 1 );
----------------------------------------------------------------------------------------------
/*FIRST PASS : Defines Result Output*/
----------------------------------------------------------------------------------------------
SELECT id.IDENTITY_ID AS MRN
      ,a3.LAST_OFFICE_VISIT
      ,a3.CITY
      ,a3.STATE
      ,p.PAT_ID
      ,orv.ORD_VALUE
      ,orv.RESULT_DATE
      ,p.BIRTH_DATE
      ,p.PAT_NAME
      ,pr.PATIENT_RACE_C
      ,p.SEX_C
      ,CASE WHEN ISNUMERIC(orv.ORD_VALUE) = 1
                THEN orv.ORD_VALUE
       WHEN orv.ORD_VALUE LIKE '>%'
           THEN 10000000
       ELSE 0.01
       END AS Result_Output
      ,ROW_NUMBER() OVER (PARTITION BY opv.PAT_ID ORDER BY orv.RESULT_DATE DESC) AS ROW_NUM_DESC
      ,ser.EXTERNAL_NAME PROV_NAME
INTO #First_Temp_Table
FROM CLARITY.dbo.ORDER_PROC_VIEW opv
    INNER JOIN #Attribution3 a3 ON opv.PAT_ID = a3.PAT_ID
    INNER JOIN CLARITY.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = a3.PAT_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
    LEFT JOIN CLARITY.dbo.PATIENT_RACE pr ON pr.PAT_ID = p.PAT_ID
                                             AND pr.LINE = 1
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN CLARITY.dbo.CLARITY_COMPONENT cc ON cc.COMPONENT_ID = orv.COMPONENT_ID
WHERE orv.RESULT_DATE > DATEADD(MONTH, -36, GETDATE()) --Need this to be long ago
      AND cc.COMMON_NAME = 'HIV VIRAL LOAD'
      AND orv.ORD_VALUE NOT IN ( 'Delete', 'See comment' );

-----------------------------------------------------------------------------------------------------------------
--SECOND PASS: Identify MRNs that belong to patients whose earliest lab value was >=200, 
------------------------------------------------------------------------------------------------------------------

SELECT MRN
      ,ftt.PAT_ID
      ,ftt.RESULT_DATE AS MOST_RECENT_RESULT_DATE
      ,ftt.Result_Output AS MOST_RECENT_Result_Output
      ,ftt.LAST_OFFICE_VISIT
      ,ftt.CITY
      ,ftt.STATE
      ,ftt.ORD_VALUE
INTO #MOST_RECENT
FROM #First_Temp_Table ftt
WHERE ftt.ROW_NUM_DESC = 1
ORDER BY MRN;


---------------------------------------------------------------------------------------------
/*THIRD PASS: Leverages Result Output column from first query to Classify Results*/
----------------------------------------------------------------------------------------------
SELECT ftt.MRN
      ,ftt.PAT_ID
      ,ftt.PROV_NAME PCP
      ,ftt.PAT_NAME
      ,CONVERT(NVARCHAR(30), ftt.LAST_OFFICE_VISIT, 101) AS LAST_OFFICE_VISIT
      ,DATEDIFF(MONTH, ftt.LAST_OFFICE_VISIT, GETDATE()) 'MONTHS AGO'
      ,CONVERT(NVARCHAR(30), ftt.BIRTH_DATE, 101) AS BIRTH_DATE
      ,zpr.NAME RACE
      ,zs.NAME SEX
      ,mr.CITY
      ,mr.STATE
      ,CONVERT(NVARCHAR(30), mr.MOST_RECENT_RESULT_DATE, 101) AS MOST_RECENT_RESULT_DATE
      ,mr.MOST_RECENT_Result_Output
      ,mr.ORD_VALUE
      ,CASE WHEN ISNUMERIC(mr.ORD_VALUE) = 1
                THEN mr.ORD_VALUE
       WHEN mr.ORD_VALUE LIKE '>%'
           THEN 10000000
       ELSE 0.01
       END AS VIRAL_LOAD
INTO #a
FROM #First_Temp_Table ftt
    INNER JOIN #MOST_RECENT mr ON mr.MRN = ftt.MRN
    LEFT JOIN CLARITY.dbo.ZC_PATIENT_RACE zpr ON ftt.PATIENT_RACE_C = zpr.PATIENT_RACE_C
    LEFT JOIN CLARITY.dbo.ZC_SEX zs ON ftt.SEX_C = zs.RCPT_MEM_SEX_C
ORDER BY ftt.MRN;

-- Next ANY Appt
SELECT a.MRN
      ,a.PAT_ID
      ,a.PCP
      ,a.PAT_NAME
      ,a.LAST_OFFICE_VISIT
      ,a.STATE
      ,a.CITY
      ,a.[MONTHS AGO]
      ,a.BIRTH_DATE
      ,a.RACE
      ,a.SEX
      ,a.MOST_RECENT_RESULT_DATE
      ,a.MOST_RECENT_Result_Output
      ,CASE WHEN a.VIRAL_LOAD < 200
                THEN 'SUPPRESSED'
       ELSE 'UNSUPPRESSED'
       END AS 'SUPRESSION_STATUS'
      ,a.ORD_VALUE
      ,pev2.CONTACT_DATE NEXT_APPT
      ,ser2.EXTERNAL_NAME NEXT_APPT_PROV
      ,ROW_NUMBER() OVER (PARTITION BY a.PAT_ID ORDER BY pev2.CONTACT_DATE ASC) AS ROW_NUM_ASC
INTO #b
FROM #a a
    LEFT JOIN CLARITY.dbo.PAT_ENC_VIEW pev2 ON a.PAT_ID = pev2.PAT_ID
                                               AND pev2.APPT_STATUS_C = 1
                                               AND pev2.CONTACT_DATE >= GETDATE()
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser2 ON pev2.VISIT_PROV_ID = ser2.PROV_ID
WHERE a.LAST_OFFICE_VISIT < DATEADD(MONTH, -3, GETDATE());

--Next PCP Appt
SELECT b.MRN
      ,b.PCP
      ,b.PAT_NAME 'PATIENT'
      ,b.LAST_OFFICE_VISIT 'LAST OFFICE VISIT'
      ,b.STATE
      ,b.CITY
      ,b.[MONTHS AGO]
      ,b.BIRTH_DATE
      ,b.RACE
      ,b.SEX
      ,b.MOST_RECENT_RESULT_DATE 'LAST LAB'
      ,b.ORD_VALUE 'LAST VL'
      ,b.SUPRESSION_STATUS
      ,CONVERT(NVARCHAR(30), b.NEXT_APPT, 101) AS 'NEXT ANY APPT'
      ,b.NEXT_APPT_PROV 'NEXT APPT PROVIDER'
      ,CONVERT(NVARCHAR(30), na.NEXT_PCP_APPT, 101) AS 'NEXT PCP APPT'
      ,na.EXTERNAL_NAME 'PCP APPT PROVIDER'
FROM #b b
    LEFT JOIN (SELECT pev.PAT_ID
                     ,pev.CONTACT_DATE NEXT_PCP_APPT
                     ,ser.EXTERNAL_NAME
                     ,ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                                                                 AND pev.APPT_STATUS_C = 1
                                                                 AND pev.CONTACT_DATE >= GETDATE()
               WHERE pev.APPT_STATUS_C = 1
                     AND ser.PROVIDER_TYPE_C IN ( 1, 9, 113 )
                     AND ser.PROV_ID <> '640178' --pulmonologist
    ) na ON b.PAT_ID = na.PAT_ID
            AND na.ROW_NUM_ASC = 1
WHERE b.ROW_NUM_ASC = 1;

DROP TABLE #a
DROP TABLE #b
DROP TABLE #Attribution1
DROP TABLE #Attribution2
DROP TABLE #Attribution3
DROP TABLE #First_Temp_Table
DROP TABLE #MOST_RECENT
