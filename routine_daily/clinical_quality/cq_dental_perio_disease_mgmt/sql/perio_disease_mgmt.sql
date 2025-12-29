SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#excl') IS NOT NULL 
DROP TABLE #excl
;

SELECT --get pts with the endentulouos dx code
	plv.PAT_ID
	,MAX(5) 'K Code'
	,NULL 'D5110'
	,NULL 'D5120'

INTO #excl

FROM 
	Clarity.dbo.PROBLEM_LIST_VIEW AS plv
    INNER JOIN Clarity.dbo.CLARITY_EDG AS CLARITY_EDG ON plv.DX_ID = CLARITY_EDG.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON CLARITY_EDG.DX_ID = EDG_CURRENT_ICD10.DX_ID

WHERE 
	EDG_CURRENT_ICD10.CODE = 'K08.109' --"Complete loss of teeth, unspecified cause, unspecified class"
    AND plv.RESOLVED_DATE IS NULL --Active Dx
    AND plv.PROBLEM_STATUS_C = 1 --Active Dx

GROUP BY 
	plv.PAT_ID

UNION

SELECT 
	tdl.PAT_ID
	,NULL 'K Code'
	,MAX(1) 'D5110'
	,NULL 'D5120'

FROM 
	Clarity.dbo.CLARITY_TDL_TRAN_64_VIEW tdl 

WHERE
	tdl.CPT_CODE = 'D5110'
	AND tdl.DETAIL_TYPE NOT IN ( 50, 51 )

GROUP BY 
	tdl.PAT_ID

UNION

SELECT 
	tdl.PAT_ID
	,NULL 'K COde'
	,NULL 'D5110'
	,MAX(1) 'D5120'

FROM 
	Clarity.dbo.CLARITY_TDL_TRAN_64_VIEW tdl 

WHERE
	tdl.CPT_CODE = 'D5120'
	AND tdl.DETAIL_TYPE NOT IN ( 50, 51 )

GROUP BY 
	tdl.PAT_ID

;

IF OBJECT_ID('tempdb..#exclusion') IS NOT NULL 
DROP TABLE #exclusion
;

SELECT 
	e.PAT_ID
	,COALESCE(MAX(e.[K Code]), 0) 'K Code'
	,COALESCE(MAX(e.D5110), 0) 'D5110'
	,COALESCE(MAX(e.D5120), 0) 'D5120'
	,COALESCE(MAX(e.[K Code]), 0) + COALESCE(MAX(e.D5110), 0) + COALESCE(MAX(e.D5120), 0) 'Total Exclusion Score'

INTO #exclusion

FROM 
	#excl e

GROUP BY 
	e.PAT_ID

HAVING 
	COALESCE(MAX(e.[K Code]), 0) + COALESCE(MAX(e.D5110), 0) + COALESCE(MAX(e.D5120), 0) > 1

;

SELECT  pev.PAT_ID,
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
                         WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
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
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT  a1.PAT_ID,
                     a1.STATE,
                     a1.CITY,
                     a1.SITE,
                     a1.LOS,
                     ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'DENTAL';

SELECT  a2.PAT_ID,
                     a2.LOS,
                     a2.CITY,
                     a2.STATE
INTO #Attribution3
FROM #Attribution2 a2
WHERE a2.ROW_NUM_DESC = 1;

--Active dental pts
SELECT DISTINCT pev.PAT_ID
INTO #pop
FROM #Attribution3 a3
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = a3.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.EPISODE_VIEW ev ON ev.PAT_LINK_ID = a3.PAT_ID
WHERE pev.CONTACT_DATE
      BETWEEN DATEADD(MONTH, -12, GETDATE()) AND GETDATE()
      AND ser.PROVIDER_TYPE_C = '108' --Dentist
      AND ev.SUM_BLK_TYPE_ID = 45 --Active Dental Episode
      AND ev.STATUS_C = 1;

SELECT DISTINCT  --to get DM Dx
       pev.PAT_ID
INTO #dm
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE pev.CONTACT_DATE
      BETWEEN DATEADD(MONTH, -12, GETDATE()) AND GETDATE()
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ser.PROVIDER_TYPE_C = '108'
      AND pev.PAT_ID IN ( SELECT DISTINCT  --Get pts with DM on problem list
                                 dm.PAT_ID
                          FROM Clarity.dbo.DM_DIABETES_VIEW dm )
UNION
SELECT DISTINCT pev.PAT_ID
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = p.PAT_ID
    --INNER JOIN Clarity.dbo.DM_DIABETES_VIEW dm ON pev.PAT_ID = dm.PAT_ID -- We can't use the DM Registry here since we moved the DM Dx off the Problem List for DM pts
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE pev.CONTACT_DATE
      BETWEEN DATEADD(MONTH, -12, GETDATE()) AND GETDATE()
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ser.PROVIDER_TYPE_C = '108'
      AND pev.PAT_ID IN ( SELECT DISTINCT  --Get pts with DM on medical history
                                 mhx.PAT_ID
                          FROM Clarity.dbo.MEDICAL_HX_VIEW mhx
                              INNER JOIN Clarity.dbo.CLARITY_EDG edg ON mhx.DX_ID = edg.DX_ID
                              INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                          WHERE icd10.code IN ( 'E08.21', 'E09.9', 'E10.22', 'E10.3419', 'E10.65', 'E10.8', 'E10.9', 'E11.00', 'E11.01', 'E11.10', 'E11.21',
                                                'E11.22', 'E11.29', 'E11.319', 'E11.3299', 'E11.3599', 'E11.40', 'E11.42', 'E11.49', 'E11.51', 'E11.59',
                                                'E11.618', 'E11.621', 'E11.65', 'E11.69', 'E11.8', 'E11.9', 'E13.10', 'E13.43', 'E13.9', 'O24.119' ));

SELECT  --Merge population w DM data
       id.IDENTITY_ID,
       pop.PAT_ID,
       p.PAT_NAME,
       CASE WHEN dm.PAT_ID IS NOT NULL THEN 'Y'
           ELSE 'N'
       END AS DIABETES_YN,
       a3.CITY,
       a3.STATE
INTO #a
FROM #pop pop
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = pop.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pop.PAT_ID = id.PAT_ID
    LEFT JOIN #dm dm ON dm.PAT_ID = pop.PAT_ID
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = pop.PAT_ID;

SELECT  a.PAT_ID,
                     icd.CODE,
                     edg.DX_NAME,
                     dx.CONTACT_DATE,
                     ser.PROV_NAME,
                     ROW_NUMBER() OVER (PARTITION BY a.PAT_ID ORDER BY dx.CONTACT_DATE DESC) AS LAST_VISIT,
                     ROW_NUMBER() OVER (PARTITION BY a.PAT_ID ORDER BY dx.CONTACT_DATE ASC) AS FIRST_VISIT
INTO #code
FROM #a a
    INNER JOIN Clarity.dbo.PAT_ENC_DX_VIEW dx ON dx.PAT_ID = a.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON edg.DX_ID = dx.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd ON icd.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ENC_CSN_ID = dx.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE dx.CONTACT_DATE > DATEADD(MONTH, -13, GETDATE())
      AND icd.CODE IN ( 'K05.221', 'K05.222', 'K05.223' /*Aggressive*/, 'K05.321', 'K05.322', 'K05.323' /*Chronic*/, 'K05.00', 'K05.01', 'K05.10',
                        'K05.11', /*Gingivitis*/ 'K05.311', 'K05.312', 'K05.313' /*Localized, added 6/17/2022 per Dr. Abuzaineh*/ )
      AND ser.PROVIDER_TYPE_C = '119' --only visits with Hygienists per Dental Quality Meeting descision 3/26/2021

;

SELECT  a.IDENTITY_ID MRN,
                     a.PAT_ID,
                     a.PAT_NAME,
                     a.DIABETES_YN,
                     a.CITY,
                     a.STATE,
                     MAX(CASE WHEN c.FIRST_VISIT = 1
                                   AND c.CODE IN ( 'K05.00', 'K05.01', 'K05.10', 'K05.11' ) THEN 0 --Gingivitis
                             WHEN c.FIRST_VISIT = 1
                                  AND c.CODE IN ( 'K05.221', 'K05.321', 'K05.311' ) THEN 1         --Slight
                             WHEN c.FIRST_VISIT = 1
                                  AND c.CODE IN ( 'K05.222', 'K05.322', 'K05.312' ) THEN 2         --Moderate
                             WHEN c.FIRST_VISIT = 1
                                  AND c.CODE IN ( 'K05.223', 'K05.323', 'K05.313' ) THEN 3         --Severe
                             ELSE NULL
                         END) AS FIRST_DX_NUM,
                     MAX(CASE WHEN c.LAST_VISIT = 1
                                   AND c.FIRST_VISIT <> 1
                                   AND c.CODE IN ( 'K05.00', 'K05.01', 'K05.10', 'K05.11' ) THEN 0 --Gingivitis
                             WHEN c.LAST_VISIT = 1
                                  AND c.FIRST_VISIT <> 1
                                  AND c.CODE IN ( 'K05.221', 'K05.321', 'K05.311' ) THEN 1         --Slight
                             WHEN c.LAST_VISIT = 1
                                  AND c.FIRST_VISIT <> 1
                                  AND c.CODE IN ( 'K05.222', 'K05.322', 'K05.312' ) THEN 2         --Moderate
                             WHEN c.LAST_VISIT = 1
                                  AND c.FIRST_VISIT <> 1
                                  AND c.CODE IN ( 'K05.223', 'K05.323', 'K05.313' ) THEN 3         --Severe
                             ELSE NULL
                         END) AS LAST_DX_NUM,
                     MAX(CASE WHEN c.FIRST_VISIT = 1 THEN c.DX_NAME ELSE NULL END) AS FIRST_DX,
                     MAX(CASE WHEN c.LAST_VISIT = 1
                                   AND c.FIRST_VISIT <> 1 THEN c.DX_NAME
                             ELSE NULL
                         END) AS LAST_DX,
                     MAX(CASE WHEN c.FIRST_VISIT = 1 THEN c.CONTACT_DATE ELSE NULL END) AS FIRST_DX_DATE,
                     MAX(CASE WHEN c.LAST_VISIT = 1
                                   AND c.FIRST_VISIT <> 1 THEN c.CONTACT_DATE
                             ELSE NULL
                         END) AS LAST_DX_DATE,
                     MAX(CASE WHEN c.FIRST_VISIT = 1 THEN c.PROV_NAME ELSE NULL END) AS FIRST_DX_PROVIDER,
                     MAX(CASE WHEN c.LAST_VISIT = 1
                                   AND c.FIRST_VISIT <> 1 THEN c.PROV_NAME
                             ELSE NULL
                         END) AS LAST_DX_PROVIDER
INTO #b
FROM #a a
    INNER JOIN #code c ON c.PAT_ID = a.PAT_ID
WHERE c.FIRST_VISIT = 1
      OR c.LAST_VISIT = 1
GROUP BY a.IDENTITY_ID,
         a.PAT_ID,
         a.PAT_NAME,
         a.DIABETES_YN,
         a.CITY,
         a.STATE;

SELECT  b.MRN,
                      b.PAT_NAME,
                      b.DIABETES_YN,
                      b.CITY,
                      b.STATE,
                      b.FIRST_DX_NUM,
                      b.LAST_DX_NUM,
                      b.FIRST_DX_NUM + b.LAST_DX_NUM AS TOTAL_DX,
                      b.FIRST_DX,
                      b.FIRST_DX_DATE,
                      b.FIRST_DX_PROVIDER,
                      b.LAST_DX,
                      b.LAST_DX_DATE,
                      b.LAST_DX_PROVIDER,
                      CASE WHEN b.LAST_DX_NUM IS NULL THEN NULL
                          WHEN b.FIRST_DX_NUM - b.LAST_DX_NUM >= 0 THEN 'MET'
                          ELSE 'UNMET'
                      END AS OUTCOME,
                      svis.[Next Any Appt],
                      svis.[Next Appt Prov],
                      spvis.[Next Dental Appt],
                      spvis.[Next Dental Appt Prov]
FROM #b b
    LEFT JOIN (SELECT  pev.PAT_ID,
                                  CAST(pev.CONTACT_DATE AS DATE) AS 'Next Any Appt',
                                  ser.PROV_NAME 'Next Appt Prov',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled

    ) svis ON svis.PAT_ID = b.PAT_ID
              AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT  pev.PAT_ID,
                                  CAST(pev.CONTACT_DATE AS DATE) AS 'Next Dental Appt',
                                  ser.PROV_NAME 'Next Dental Appt Prov',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') spvis ON spvis.PAT_ID = b.PAT_ID
                                                                                 AND spvis.ROW_NUM_ASC = 1 -- First scheduled

WHERE --Leave this in for final eval since it excludes people who've not had 2+ diagnosis events.
b.LAST_DX IS NOT NULL
AND b.FIRST_DX_NUM + b.LAST_DX_NUM < 6 --To remove pts who start and end at "severe" per Dental Quality WG Meeting 3/26/2021
AND b.PAT_ID NOT IN ( SELECT DISTINCT  -----These two subqueries are to identify edentulus pts for exclusion.------
                             e.PAT_ID
                      FROM #Exclusion e );

DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #dm;
DROP TABLE #pop;
DROP TABLE #code;
DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #Exclusion;
