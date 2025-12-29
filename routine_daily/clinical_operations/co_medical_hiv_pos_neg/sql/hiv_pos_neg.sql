/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:   Non-flagged pts with no HIV Dx
 Create Date:   1/11/2019
 Created By:    scogginsm
 System:        javelin.ochin.org
 Requested By:  

 Purpose:       To identify pts in Clarity who are not marked as HIV+ or flagged as HIV-

 Description:   Send direct to PCPs on the list
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:       Changed By:         Change Description:
 ------------       -------------       ---------------------------------------------------
  4/11/2019         scogginsm           Adding last visit and provider
  5/21/2019         Mitch               Adding date limiters on PEV tables to improve runtime. This is ok since the legacy cleanup is done.
  4/21/2020         Mitch               Alteryx

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#exclusion_patients') IS NOT NULL DROP TABLE #exclusion_patients;
SELECT DISTINCT flag.PATIENT_ID
INTO #exclusion_patients
FROM CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW flag
WHERE flag.ACTIVE_C = 1
      AND (flag.PAT_FLAG_TYPE_C = '640005' --PrEP
           OR flag.PAT_FLAG_TYPE_C = '640008' --STI
           OR flag.PAT_FLAG_TYPE_C = '9800035' -- PEP
           OR flag.PAT_FLAG_TYPE_C = '640007' -- AODA HIV-
           OR flag.PAT_FLAG_TYPE_C = '640017' --False positive HIV test
           OR flag.PAT_FLAG_TYPE_C = '640034' --SA64 Monkeypox HIV-
           OR flag.PAT_FLAG_TYPE_C = '9800065' --SA64 Other – Non-HIV
);


IF OBJECT_ID('tempdb..#completed_med_visit') IS NOT NULL DROP TABLE #completed_med_visit;
SELECT DISTINCT pev2.PAT_ID
INTO #completed_med_visit
FROM CLARITY.dbo.PAT_ENC_VIEW pev2
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev2.DEPARTMENT_ID
WHERE pev2.APPT_STATUS_C IN ( 2, 6 ) --Has ever completed a visit
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD';


SELECT id.IDENTITY_ID,
       p.PAT_ID,
       p.PAT_NAME,
       MAX(icd10.CODE) ICD10, --this is important, it removes bad nulls.
       ser.EXTERNAL_NAME PCP,
       ser.PROV_TYPE,
       p.REC_CREATE_DATE
INTO #a
FROM CLARITY.dbo.PATIENT_VIEW p
    LEFT JOIN CLARITY.dbo.PROBLEM_LIST_VIEW plv ON p.PAT_ID = plv.PAT_ID
                                                   AND plv.PROBLEM_STATUS_C = 1 -- Dx is active on problem list
                                                   AND plv.RESOLVED_DATE IS NULL -- Dx is not marked as resolved
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN #completed_med_visit ON p.PAT_ID = #completed_med_visit.PAT_ID
    LEFT JOIN CLARITY.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
                                                     AND icd10.CODE IN ( 'B20', 'Z21', 'Z78.9', 'B97.35' ) --Z78.9 = false positive HIV test
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                                                  AND ser.SERV_AREA_ID = 64
WHERE p.PAT_ID NOT IN ( SELECT #exclusion_patients.PATIENT_ID FROM #exclusion_patients )
GROUP BY id.IDENTITY_ID,
         p.PAT_ID,
         p.PAT_NAME,
         ser.EXTERNAL_NAME,
         p.REC_CREATE_DATE,
         ser.PROV_TYPE;


SELECT a.IDENTITY_ID, a.PAT_ID, a.PAT_NAME, a.PCP, a.PROV_TYPE, a.REC_CREATE_DATE INTO #b FROM #a a WHERE a.ICD10 IS NULL;


SELECT b.IDENTITY_ID,
       b.PROV_TYPE,
       b.PAT_NAME,
       b.PCP,
       b.PAT_ID,
       CAST(b.REC_CREATE_DATE AS DATE) AS PT_CREATED_DATE,
       CAST(pev.CONTACT_DATE AS DATE) AS LAST_VISIT_DATE,
       ser.EXTERNAL_NAME LAST_VISIT_PROVIDER,
       dep.DEPARTMENT_NAME LAST_VISIT_SITE,
       ser.PROV_TYPE PROV_TYPE_2,
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
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2)
       END AS CITY
INTO #c
FROM #b b
    INNER JOIN (SELECT pev.PAT_ID,
                       MAX(pev.PAT_ENC_CSN_ID) LAST_VISIT
                FROM CLARITY.dbo.PAT_ENC_VIEW pev
                WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                --AND pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
                GROUP BY pev.PAT_ID) lv ON b.PAT_ID = lv.PAT_ID
    LEFT JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON lv.LAST_VISIT = pev.PAT_ENC_CSN_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    LEFT JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID;


WITH
    svis AS (
        SELECT pev.PAT_ID,
               CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next Any Appt',
               ser.PROV_NAME 'Next Appt Prov',
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW pev
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
        WHERE pev.APPT_STATUS_C = 1 --Scheduled
    ),
    spvis AS (
        SELECT pev.PAT_ID,
               CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next PCP Appt',
               ser.PROV_NAME 'Next PCP Appt Prov',
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW pev
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
        WHERE pev.APPT_STATUS_C = 1 --Scheduled
              AND ser.PROV_ID <> '640178' --pulmonologist
              AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs
    )
SELECT c.IDENTITY_ID MRN,
       c.PAT_ID,
       c.PAT_NAME,
       c.PCP,
       c.LAST_VISIT_DATE,
       c.LAST_VISIT_PROVIDER,
       c.LAST_VISIT_SITE,
       c.STATE,
       c.CITY,
       CASE WHEN c.PCP IS NOT NULL THEN c.PCP
           ELSE c.LAST_VISIT_PROVIDER
       END AS 'RESPONSIBLE PERSON',
       CAST(svis.[Next Any Appt] AS DATE) AS 'Next Any Appt',
       svis.[Next Appt Prov],
       CAST(spvis.[Next PCP Appt] AS DATE) AS 'Next PCP Appt',
       spvis.[Next PCP Appt Prov]
FROM #c c
    LEFT JOIN svis ON svis.PAT_ID = c.PAT_ID
                      AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN spvis ON spvis.PAT_ID = c.PAT_ID
                       AND spvis.ROW_NUM_ASC = 1 -- First scheduled
WHERE c.IDENTITY_ID NOT IN ( '640000595', '640001179' ) -- To remove one pt last seen in 2012 by Jurg Oggenfuss
;

DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #c;
