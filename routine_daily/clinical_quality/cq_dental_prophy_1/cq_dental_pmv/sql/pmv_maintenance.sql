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

IF OBJECT_ID('tempdb..#pat_enc_dep_los') IS NOT NULL DROP TABLE #pat_enc_dep_los;
SELECT PAT_ENC.PAT_ID,
       PAT_ENC.CONTACT_DATE AS LAST_OFFICE_VISIT,
       SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2) AS STATE,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 5, 2)
           WHEN 'MK' THEN 'MILWAUKEE'
           WHEN 'KN' THEN 'KENOSHA'
           WHEN 'GB' THEN 'GREEN BAY'
           WHEN 'WS' THEN 'WAUSAU'
           WHEN 'AP' THEN 'APPLETON'
           WHEN 'EC' THEN 'EAU CLAIRE'
           WHEN 'LC' THEN 'LACROSSE'
           WHEN 'MD' THEN 'MADISON'
           WHEN 'BL' THEN 'BELOIT'
           WHEN 'BI' THEN 'BILLING'
           WHEN 'SL' THEN 'ST LOUIS'
           WHEN 'DN' THEN 'DENVER'
           WHEN 'AS' THEN 'AUSTIN'
           WHEN 'KC' THEN 'KANSAS CITY'
           WHEN 'CG' THEN 'CHICAGO'
           ELSE 'ERROR'
       END AS CITY,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 7, 2)
           WHEN 'MN' THEN 'MAIN LOCATION'
           WHEN 'DR' THEN 'D&R'
           WHEN 'KE' THEN 'KEENEN'
           WHEN 'UC' THEN 'UNIVERSITY OF COLORADO'
           WHEN 'ON' THEN 'AUSTIN MAIN'
           WHEN 'TW' THEN 'AUSTIN OTHER'
           ELSE 'ERROR'
       END AS SITE,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2)
           WHEN 'MD' THEN 'MEDICAL'
           WHEN 'DT' THEN 'DENTAL'
           WHEN 'CM' THEN 'CASE MANAGEMENT'
           WHEN 'RX' THEN 'PHARMACY'
           WHEN 'AD' THEN 'BEHAVIORAL'
           WHEN 'PY' THEN 'BEHAVIORAL'
           WHEN 'BH' THEN 'BEHAVIORAL'
           WHEN 'MH' THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS LOS
INTO #pat_enc_dep_los
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID
WHERE PAT_ENC.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND PAT_ENC.APPT_STATUS_C IN ( 2, 6 );


IF OBJECT_ID('tempdb..#dental_patients') IS NOT NULL DROP TABLE #dental_patients;
WITH
    target_service_line AS (
        SELECT #pat_enc_dep_los.PAT_ID,
               #pat_enc_dep_los.STATE,
               #pat_enc_dep_los.CITY,
               #pat_enc_dep_los.SITE,
               #pat_enc_dep_los.LOS,
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
        WHERE #pat_enc_dep_los.LOS = 'DENTAL'
    )
SELECT target_service_line.PAT_ID,
       target_service_line.LOS,
       target_service_line.CITY,
       target_service_line.STATE
INTO #dental_patients
FROM target_service_line
WHERE target_service_line.ROW_NUM_DESC = 1;


/**
 * Denominator:
 *    - Patients who had a completed visit in the Dental service line and only visits with Hygienists
 *    - Patients who had the following CPT Codes documented within the last 36 months: D4341 and D4342
 *    - Exclude patients who had edentulous D5110 and D5120
 */
IF OBJECT_ID('tempdb..#denominator') IS NOT NULL DROP TABLE #denominator;
SELECT PAT_ENC.PAT_ID,
       ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID
                          ORDER BY CLARITY_TDL_TRAN_64.ORIG_SERVICE_DATE DESC) AS ROW_NUM_DESC
INTO #denominator
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN CLARITY.dbo.CLARITY_TDL_TRAN_64_VIEW AS CLARITY_TDL_TRAN_64 ON PAT_ENC.CHARGE_SLIP_NUMBER = CLARITY_TDL_TRAN_64.CHARGE_SLIP_NUMBER
    INNER JOIN #dental_patients ON PAT_ENC.PAT_ID = #dental_patients.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
                                                              AND CLARITY_SER.PROVIDER_TYPE_C = '119' -- only visits with Hygienists per Dental Quality Meeting descision 4/16/2024

WHERE CLARITY_TDL_TRAN_64.CPT_CODE IN ( 'D4341', 'D4342' )
      AND DATEDIFF(MONTH, CLARITY_TDL_TRAN_64.ORIG_SERVICE_DATE, GETDATE()) <= 36
	  AND CLARITY_TDL_TRAN_64.PAT_ID NOT in
		(SELECT
			e.PAT_ID
		FROM
			#exclusion e 
		)
ORDER BY PAT_ENC.PAT_ID,
         PAT_ENC.CONTACT_DATE;


/**
 * Numerator:
 *    - 3+ dental visits within the last 13 months and the patient has PMV CPT code of D4910
 */
IF OBJECT_ID('tempdb..#numerator') IS NOT NULL DROP TABLE #numerator;
-- Numerator helper: all qualifying PMV encounters + their visit provider
WITH
    pmv_visits AS (
        SELECT
            d.PAT_ID,
            pe.PAT_ENC_CSN_ID,
            pe.CONTACT_DATE,
            pe.VISIT_PROV_ID,
            s.PROV_NAME       AS VISIT_PROV_NAME,
            s.PROV_TYPE       AS VISIT_PROV_TYPE,
            ROW_NUMBER() OVER (
                PARTITION BY d.PAT_ID
                ORDER BY pe.CONTACT_DATE DESC, pe.PAT_ENC_CSN_ID DESC
            ) AS rn
        FROM #denominator d
        INNER JOIN CLARITY.dbo.PAT_ENC_VIEW            AS pe
            ON d.PAT_ID = pe.PAT_ID
        INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW        AS dep
            ON pe.DEPARTMENT_ID = dep.DEPARTMENT_ID
           AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
        INNER JOIN CLARITY.dbo.CLARITY_TDL_TRAN_64_VIEW AS tdl
            ON pe.CHARGE_SLIP_NUMBER = tdl.CHARGE_SLIP_NUMBER
           AND tdl.CPT_CODE = 'D4910'
        INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW        AS s
            ON pe.VISIT_PROV_ID = s.PROV_ID
        WHERE d.ROW_NUM_DESC = 1
          AND DATEDIFF(MONTH, pe.CONTACT_DATE, GETDATE()) <= 13
          AND pe.APPT_STATUS_C IN (2, 6)
    ),
    -- pick the single most recent qualifying PMV encounter per patient
    pmv_latest AS (
        SELECT
            PAT_ID,
            PAT_ENC_CSN_ID,
            CONTACT_DATE,
            VISIT_PROV_ID,
            VISIT_PROV_NAME,
            VISIT_PROV_TYPE
        FROM pmv_visits
        WHERE rn = 1
    ),
    -- keep your counts exactly as before
    pmv_visit_counts AS (
        SELECT PAT_ID, COUNT(DISTINCT PAT_ENC_CSN_ID) AS VISIT_COUNT
        FROM pmv_visits
        GROUP BY PAT_ID
    )

SELECT
    d.PAT_ID,
    idv.IDENTITY_ID          AS MRN,
    p.PAT_NAME,
    'D4910'                  AS CPT_CODE,
    COALESCE(c.VISIT_COUNT, 0) AS VISIT_COUNT,
    ml.VISIT_PROV_ID         AS PMV_VISIT_PROV_ID,
    ml.VISIT_PROV_NAME       AS PMV_VISIT_PROV_NAME,
    ml.VISIT_PROV_TYPE       AS PMV_VISIT_PROV_TYPE
INTO #numerator
FROM #denominator d
INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS idv ON d.PAT_ID = idv.PAT_ID
INNER JOIN CLARITY.dbo.PATIENT_VIEW     AS p   ON d.PAT_ID = p.PAT_ID
LEFT  JOIN pmv_visit_counts             AS c   ON d.PAT_ID = c.PAT_ID
LEFT  JOIN pmv_latest                   AS ml  ON d.PAT_ID = ml.PAT_ID
WHERE d.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#scheduled_dental_visits') IS NOT NULL DROP TABLE #scheduled_dental_visits;
WITH
    scheduled_visits_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_DENTAL_APPT,
               CLARITY_SER.PROV_NAME AS NEXT_DENTAL_APPT_PROV,
               CLARITY_SER.PROV_TYPE AS NEXT_DENTAL_APPT_PROV_TYPE,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
            INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1 -- scheduled
              AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'DT'
              AND PAT_ENC.CONTACT_DATE > GETDATE()
    )
SELECT scheduled_visits_info.PAT_ID,
       scheduled_visits_info.NEXT_DENTAL_APPT,
       scheduled_visits_info.NEXT_DENTAL_APPT_PROV,
       scheduled_visits_info.NEXT_DENTAL_APPT_PROV_TYPE
INTO #scheduled_dental_visits
FROM scheduled_visits_info
WHERE scheduled_visits_info.ROW_NUM_ASC = 1;


IF OBJECT_ID('tempdb..#outcome') IS NOT NULL DROP TABLE #outcome;
SELECT
    n.MRN,
    n.PAT_NAME,
    n.CPT_CODE,
    n.VISIT_COUNT,
    CASE WHEN n.VISIT_COUNT >= 3 THEN 'MET' ELSE 'NOT MET' END AS OUTCOME,
    dp.CITY,
    dp.STATE,
    sdv.NEXT_DENTAL_APPT,
    sdv.NEXT_DENTAL_APPT_PROV,
    sdv.NEXT_DENTAL_APPT_PROV_TYPE,
    n.PMV_VISIT_PROV_ID   AS D4910_VISIT_PROV_ID,
    n.PMV_VISIT_PROV_NAME AS D4910_VISIT_PROV_NAME,
    n.PMV_VISIT_PROV_TYPE AS D4910_VISIT_PROV_TYPE
INTO #outcome
FROM #numerator n
INNER JOIN #dental_patients       AS dp  ON n.PAT_ID = dp.PAT_ID
LEFT  JOIN #scheduled_dental_visits AS sdv ON dp.PAT_ID = sdv.PAT_ID;


SELECT 
    o.MRN,
    o.PAT_NAME,
    o.CPT_CODE,
    o.VISIT_COUNT,
    o.OUTCOME,
    o.CITY,
    o.STATE,
    o.NEXT_DENTAL_APPT,
    o.NEXT_DENTAL_APPT_PROV,
    o.NEXT_DENTAL_APPT_PROV_TYPE,
    o.D4910_VISIT_PROV_NAME AS [Dental Provider],
    CASE 
        WHEN o.CITY = 'MILWAUKEE'   THEN 0.15
        WHEN o.CITY = 'GREEN BAY'   THEN 0.15
        WHEN o.CITY = 'MADISON'     THEN 0.10
        WHEN o.CITY = 'ST LOUIS'    THEN 0.20
        WHEN o.CITY = 'DENVER'      THEN 0.20
        WHEN o.CITY = 'AUSTIN'      THEN 0.25
        WHEN o.CITY = 'KANSAS CITY' THEN 0.00
        ELSE 0.00
    END AS CITY_SPECIFIC_GOALS
FROM #outcome o;



  
