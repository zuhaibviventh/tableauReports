
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#hiv_pats') IS NOT NULL									
DROP TABLE #hiv_pats;

SELECT DISTINCT 
	pev.PAT_ID

INTO #hiv_pats 

FROM 
	Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID


WHERE
	ser.SERV_AREA_ID = 64
    AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
    AND pev.CONTACT_DATE > DATEADD(MM, -12, GETDATE()) --Visit in past year
    AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
    AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051,
                                    8052, 8053, 8054, 8055, 8056 ) -- Office Visits
    AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
    AND icd10.CODE IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
    AND plv.RESOLVED_DATE IS NULL --Active Dx
    AND plv.PROBLEM_STATUS_C = 1 --Active Dx
    AND p4.PAT_LIVING_STAT_C = 1

;
IF OBJECT_ID('tempdb..#insurance_info') IS NOT NULL
DROP TABLE #insurance_info;

SELECT
	COVERAGE_MEM_LIST.PAT_ID,
    V_COVERAGE_PAYOR_PLAN.FIN_CLASS_NAME AS FINANCIAL_CLASS_NAME,
    ROW_NUMBER() OVER (PARTITION BY COVERAGE_MEM_LIST.PAT_ID  ORDER BY COVERAGE_MEM_LIST.MEM_EFF_FROM_DATE DESC) AS ROW_NUM_DESC

INTO #insurance_info

FROM
	CLARITY.dbo.V_COVERAGE_PAYOR_PLAN AS V_COVERAGE_PAYOR_PLAN
    INNER JOIN CLARITY.dbo.COVERAGE_MEM_LIST AS COVERAGE_MEM_LIST ON V_COVERAGE_PAYOR_PLAN.COVERAGE_ID = COVERAGE_MEM_LIST.COVERAGE_ID

WHERE
	COVERAGE_MEM_LIST.MEM_EFF_TO_DATE IS NULL /* Currently enrolled */
    AND COVERAGE_MEM_LIST.MEM_COVERED_YN = 'Y' /* Covered patients */

;

SELECT
	id.IDENTITY_ID AS MRN,
       p.PAT_NAME AS PATIENT,
       MAX(CASE ---To get the Dept ID of the PCP if exists
               WHEN depser.STATE IS NOT NULL THEN depser.STATE ELSE dep.STATE END) AS STATE,
       depser.SERVICE_TYPE,
       depser.SERVICE_LINE,
       depser.SUB_SERVICE_LINE,
       MAX(CASE WHEN ser.PROVIDER_TYPE_C = 102 THEN 1 ELSE 0 END) AS 'MED REVIEW BY PHARMACIST#',
       MAX(CASE WHEN ser.PROVIDER_TYPE_C = 102
                     AND ml.PAT_ENC_CSN_ID IS NOT NULL THEN 'Y'
               ELSE 'N'
           END) AS [MED REVIEW BY PHARMACIST],
       MAX(ser.EXTERNAL_NAME) AS PHARMACIST,
       serpcp.EXTERNAL_NAME AS PCP,
       COALESCE(ii.FINANCIAL_CLASS_NAME, '*Unspecified Financial Class') AS FINANCIAL_CLASS_NAME,
	   GETDATE() AS UPDATE_DTTM

FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_2_VIEW ser2 ON serpcp.PROV_ID = ser2.PROV_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping depser ON ser2.PRIMARY_DEPT_ID = depser.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    LEFT JOIN (SELECT medlst.PAT_ENC_CSN_ID FROM Clarity.dbo.PAT_ENC_CURR_MEDS_VIEW medlst) ml ON pev.PAT_ENC_CSN_ID = ml.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                                                  AND ser.PROVIDER_TYPE_C = 102 -- Pharmacist
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    LEFT JOIN #insurance_info ii ON p.PAT_ID = ii.PAT_ID
                                AND ii.ROW_NUM_DESC = 1

WHERE 
	pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'MD', 'RX' )
      AND p.PAT_ID IN ( SELECT hp.PAT_ID FROM #hiv_pats hp)

GROUP BY 
	p.PAT_ID,
         id.IDENTITY_ID,
         p.PAT_NAME,
         serpcp.EXTERNAL_NAME,
         ii.FINANCIAL_CLASS_NAME
		 
;

DROP TABLE #insurance_info
DROP TABLE #hiv_pats
