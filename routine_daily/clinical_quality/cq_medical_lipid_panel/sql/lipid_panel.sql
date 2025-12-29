SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#active_hiv_patients') IS NOT NULL DROP TABLE #active_hiv_patients;
SELECT pat_enc.PAT_ID,
       clarity_ser.EXTERNAL_NAME AS PCP,
       SUBSTRING(clarity_dep.DEPT_ABBREVIATION, 3, 2) AS STATE,
       CASE SUBSTRING(clarity_dep.DEPT_ABBREVIATION, 5, 2)
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
           ELSE SUBSTRING(clarity_dep.DEPT_ABBREVIATION, 5, 2)
       END AS CITY,
       ROW_NUMBER() OVER (PARTITION BY pat_enc.PAT_ID ORDER BY pat_enc.CONTACT_DATE DESC) AS ROW_NUM_DESC

INTO #active_hiv_patients

FROM 
	CLARITY.dbo.PATIENT_VIEW AS patient
    INNER JOIN CLARITY.dbo.PATIENT_4 AS patient_4 ON patient.PAT_ID = patient_4.PAT_ID
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS pat_enc ON patient.PAT_ID = pat_enc.PAT_ID
    INNER JOIN CLARITY.dbo.PROBLEM_LIST_VIEW AS problem_list ON pat_enc.PAT_ID = problem_list.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_EDG ON problem_list.DX_ID = clarity_edg.DX_ID
    INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 ON clarity_edg.DX_ID = EDG_CURRENT_ICD10.DX_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS clarity_ser ON patient.CUR_PCP_PROV_ID = clarity_ser.PROV_ID
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS clarity_dep ON clarity_dep.DEPARTMENT_ID = pat_enc.DEPARTMENT_ID

WHERE 
	clarity_ser.SERV_AREA_ID = 64
	AND clarity_ser.PROVIDER_TYPE_C IN ( 1, 9, 6, 113, 193 ) -- Physicians and NPs, PAs
	AND pat_enc.CONTACT_DATE > DATEADD(MM, -12, CURRENT_TIMESTAMP) --Visit in past year
	AND pat_enc.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
	AND pat_enc.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051,
										8052, 8053, 8054, 8055, 8056 ) -- Office Visits
	AND SUBSTRING(clarity_dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
	AND EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
	AND problem_list.RESOLVED_DATE IS NULL --Active Dx
	AND problem_list.PROBLEM_STATUS_C = 1 --Active Dx
	AND patient_4.PAT_LIVING_STAT_C = 1
	
;


/* Labs ordered within the last 12 months */
IF OBJECT_ID('tempdb..#lipid_panel_outcomes') IS NOT NULL 
DROP TABLE #lipid_panel_outcomes
;

SELECT 
	ORDER_PROC.PAT_ID,
	'MET' AS lipid_panel_outcome,
	CAST(ORDER_RESULTS.RESULT_DATE AS DATE) AS LAST_LIPID_PANEL_ORDER_DATE, --11/17/2024, per Adam updating this to use result date instead of order date.
	ORDER_PROC.DESCRIPTION 'Lab Panel Ordered',
	CLARITY_COMPONENT.NAME 'Lab Component Name',
	ORDER_RESULTS.ORD_VALUE AS LAST_TOTAL_CHOL_LAB_VALUE,
	ROW_NUMBER() OVER (PARTITION BY ORDER_PROC.PAT_ID ORDER BY ORDER_RESULTS.RESULT_DATE DESC) AS ROW_NUM_DESC

INTO #lipid_panel_outcomes

FROM 
	CLARITY.dbo.ORDER_PROC_VIEW AS ORDER_PROC
	INNER JOIN Clarity.dbo.CLARITY_EAP eap ON eap.PROC_ID = ORDER_PROC.PROC_ID
	INNER JOIN Clarity.dbo.GROUPER_COMPILED_REC_LIST AS gcrl ON gcrl.GROUPER_RECORDS_NUMERIC_ID = eap.PROC_ID
    INNER JOIN CLARITY.dbo.ORDER_RESULTS_VIEW AS ORDER_RESULTS ON ORDER_PROC.ORDER_PROC_ID = ORDER_RESULTS.ORDER_PROC_ID
    INNER JOIN CLARITY.dbo.CLARITY_COMPONENT AS CLARITY_COMPONENT ON ORDER_RESULTS.COMPONENT_ID = CLARITY_COMPONENT.COMPONENT_ID

WHERE 
	DATEDIFF(MONTH, ORDER_RESULTS.RESULT_DATE, GETDATE()) <= 12 --11/17/2024, per Adam updating this to use result date instead of order date.
	AND ((gcrl.BASE_GROUPER_ID IN ('100288') 
	AND gcrl.COMPILED_CONTEXT = 'EAP'
			)
		OR eap.PROC_CODE IN ('LS656','LAB18','LV5602','LV3946','LT427','LCS680','LT421','LP1301','LAS1237','LAB11116','LV484','LAS001','LP2739','LCP019','LV2202','82705','LR1536','LHI237','LAS1378','LP1263','LP3532','LV6824','LP1047','LAB9961','LV5798','LBS498')
		)


	  
;

WITH
    next_any_appt AS (
        SELECT PAT_ENC.PAT_ID,
               CLARITY_SER.PROV_NAME AS NEXT_ANY_VISIT_PROVIDER_NAME,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_ANY_VISIT_APPOINTMENT,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC

        FROM 
			CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID

        WHERE 
			PAT_ENC.APPT_STATUS_C = 1
    ),
    pcp_appt AS (
        SELECT 
			PAT_ENC.PAT_ID,
            CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_PCP_VISIT_APPOINTMENT,
            CLARITY_SER.PROV_NAME AS NEXT_PCP_VISIT_PROVIDER_NAME,
            ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC

        FROM 
			Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID

        WHERE 
			PAT_ENC.APPT_STATUS_C = 1 --Scheduled
            AND CLARITY_SER.PROVIDER_TYPE_C IN ( '1', '6', '9', '113', '193' ) -- Physicians, PAs and NPs
    )
SELECT 
	IDENTITY_ID.IDENTITY_ID AS MRN,
    CAST(PATIENT.BIRTH_DATE AS DATE) AS DOB,
    COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE_CATC,
    COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY_CATC,
	COALESCE(#lipid_panel_outcomes.lipid_panel_outcome, 'NOT MET') AS OUTCOME,
	#lipid_panel_outcomes.LAST_LIPID_PANEL_ORDER_DATE,
	#lipid_panel_outcomes.LAST_TOTAL_CHOL_LAB_VALUE,
	#active_hiv_patients.PCP,
	#active_hiv_patients.STATE,
	#active_hiv_patients.CITY,
	next_any_appt.NEXT_ANY_VISIT_PROVIDER_NAME,
	next_any_appt.NEXT_ANY_VISIT_APPOINTMENT,
	pcp_appt.NEXT_PCP_VISIT_PROVIDER_NAME,
	pcp_appt.NEXT_PCP_VISIT_APPOINTMENT,
	GETDATE() AS UPDATE_DTTM,
	IIF(cp.PAT_ID IS NOT NULL, 'YES', 'NO') AS 'In Any Clinical Pharmacy Cohort',
	#lipid_panel_outcomes.[Lab Panel Ordered],
	#lipid_panel_outcomes.[Lab Component Name],
	PATIENT.PAT_NAME 'Patient'


FROM 
	CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
    INNER JOIN #active_hiv_patients ON IDENTITY_ID.PAT_ID = #active_hiv_patients.PAT_ID
                                       AND #active_hiv_patients.ROW_NUM_DESC = 1
    LEFT JOIN ##patient_race_ethnicity ON PATIENT.PAT_ID = ##patient_race_ethnicity.PAT_ID
    LEFT JOIN #lipid_panel_outcomes ON IDENTITY_ID.PAT_ID = #lipid_panel_outcomes.PAT_ID
                                       AND #lipid_panel_outcomes.ROW_NUM_DESC = 1
    LEFT JOIN next_any_appt ON IDENTITY_ID.PAT_ID = next_any_appt.PAT_ID
                               AND next_any_appt.ROW_NUM_ASC = 1
    LEFT JOIN pcp_appt ON IDENTITY_ID.PAT_ID = pcp_appt.PAT_ID
                          AND pcp_appt.ROW_NUM_ASC = 1
	LEFT JOIN
		(SELECT
			PATIENT_FYI_FLAGS.PATIENT_ID AS PAT_ID
			,MAX(PATIENT_FYI_FLAGS.ACTIVE_C) AS ACTIVE

		FROM
			CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS
			INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI f ON PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = f.BPA_TRIGGER_FYI_C

		WHERE
			f.NAME LIKE 'SA64 Pharm%'
			AND PATIENT_FYI_FLAGS.ACTIVE_C = 1

		GROUP BY
			PATIENT_FYI_FLAGS.PATIENT_ID
			) cp ON cp.PAT_ID = IDENTITY_ID.PAT_ID
;
						  

