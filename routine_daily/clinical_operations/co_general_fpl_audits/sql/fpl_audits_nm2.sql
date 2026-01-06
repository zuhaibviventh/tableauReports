SET NOCOUNT ON
;
SET ANSI_WARNINGS OFF
;

IF OBJECT_ID ('tempdb..#doc_info') IS NOT NULL
	DROP TABLE #doc_info
	;
SELECT
	DOC_INFORMATION.DOC_PT_ID AS PAT_ID
	,DOC_INFORMATION.DOC_RECV_TIME
	,DOC_INFORMATION.DOC_INFO_TYPE_C
	,ZC_DOC_INFO_TYPE.NAME AS DOC_TYPE
	,ROW_NUMBER () OVER (PARTITION BY
							DOC_INFORMATION.DOC_PT_ID
						,ZC_DOC_INFO_TYPE.NAME
						ORDER BY
							DOC_INFORMATION.DOC_RECV_TIME DESC
						) AS ROW_NUM_DESC

INTO
	#doc_info

FROM
	CLARITY.dbo.DOC_INFORMATION_VIEW AS DOC_INFORMATION
	INNER JOIN CLARITY.dbo.ZC_DOC_INFO_TYPE AS ZC_DOC_INFO_TYPE ON DOC_INFORMATION.DOC_INFO_TYPE_C = ZC_DOC_INFO_TYPE.DOC_INFO_TYPE_C

WHERE
	DOC_INFORMATION.DOC_INFO_TYPE_C IN ( '140263', '4100150', '4100026', '4100028', '4100025', '4100027', '103239', '103238', '103241' )
	AND
	(
		DOC_INFORMATION.IS_ESIGNED_YN = 'Y'
		OR	DOC_INFORMATION.IS_SCANNED_YN = 'Y'
	)
;

IF OBJECT_ID ('tempdb..#patient_documents') IS NOT NULL
	DROP TABLE #patient_documents
	;
SELECT
	#doc_info.PAT_ID
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '140263', 'Yes', 'No')) AS FDS_CONSENT_TO_TREAT
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '140263', CAST (#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_CONSENT_TO_TREAT_DT
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '4100150', 'Yes', 'No')) AS SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '4100150', CAST (#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE_DT
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100026', '4100028' ), 'Yes', 'No')) AS SA64_RIGHTS_AND_RESPONSIBILITIES
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100026', '4100028' ), CAST (#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_RIGHTS_AND_RESPONSIBILITIES_DT
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100025', '4100027' ), 'Yes', 'No')) AS SA64_ACKNOWLEDGEMENT_OF_RECEIPT
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100025', '4100027' ), CAST (#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_ACKNOWLEDGEMENT_OF_RECEIPT_DT
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '103239', 'Yes', 'No')) AS FDS_PHOTO_ID
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '103239', CAST (#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_PHOTO_ID_DT
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '103238', 'Yes', 'No')) AS FDS_PRIVATE_INSURANCE
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '103238', CAST (#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_PRIVATE_INSURANCE_DT
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '103241', 'Yes', 'No')) AS FDS_INCOME_VERIFICATION
	,MAX (IIF(#doc_info.DOC_INFO_TYPE_C = '103241', CAST (#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_INCOME_VERIFICATION_DT

INTO
	#patient_documents

FROM
	#doc_info

WHERE
	#doc_info.ROW_NUM_DESC = 1

GROUP BY
	#doc_info.PAT_ID
;

IF OBJECT_ID ('tempdb..#delivery') IS NOT NULL
	DROP TABLE #delivery
	;
WITH visits_info
AS (SELECT
		pev.PAT_ID
		,CAST (pev.CONTACT_DATE AS DATE) AS LAST_OFFICE_VISIT
		,dep.CITY
        ,dep.STATE
        ,dep.SERVICE_TYPE
        ,dep.SERVICE_LINE
        ,dep.SUB_SERVICE_LINE
		,dep.DEPARTMENT_NAME 'Department'

	FROM
		CLARITY.dbo.PAT_ENC_VIEW pev
		LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID

	WHERE
		pev.CONTACT_DATE > DATEADD (MONTH, -12, GETDATE ())
		AND pev.APPT_STATUS_C IN ( 2, 6 ))
	,visit_nums
AS (SELECT
		visits_info.PAT_ID
		,visits_info.LAST_OFFICE_VISIT
		,visits_info.STATE
		,visits_info.CITY
		,visits_info.SITE
        ,visits_info.SERVICE_TYPE
        ,visits_info.SERVICE_LINE
        ,visits_info.SUB_SERVICE_LINE
		,visits_info.Department
		,ROW_NUMBER () OVER (PARTITION BY
								visits_info.PAT_ID
							,visits_info.Department
							ORDER BY
								visits_info.LAST_OFFICE_VISIT DESC
							) AS ROW_NUM_DESC
	FROM
		visits_info)
	,hiv_pats AS (SELECT	DISTINCT
			p.PAT_ID
	FROM
		CLARITY.dbo.PATIENT_VIEW p
		INNER JOIN CLARITY.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
		INNER JOIN CLARITY.dbo.PROBLEM_LIST_VIEW plv ON p.PAT_ID = plv.PAT_ID
		INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
		INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
	WHERE
		icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
		AND plv.RESOLVED_DATE IS NULL --Active Dx
		AND plv.PROBLEM_STATUS_C = 1 --Active Dx
		AND p4.PAT_LIVING_STAT_C = 1)
	,latest_visits AS (SELECT
		visit_nums.PAT_ID
		,visit_nums.LAST_OFFICE_VISIT
		,visit_nums.STATE
		,visit_nums.CITY
		,visit_nums.SITE
        ,visit_nums.SERVICE_TYPE
        ,visit_nums.SERVICE_LINE
        ,visit_nums.SUB_SERVICE_LINE
        ,visit_nums.Department

	FROM
		visit_nums

	WHERE
		visit_nums.PAT_ID IN ( SELECT hiv_pats .PAT_ID FROM		hiv_pats )
		AND visit_nums.ROW_NUM_DESC = 1)

	,hiv_patient_type AS (SELECT	DISTINCT
			PROBLEM_LIST.PAT_ID
		,'HIV+' AS PATIENT_TYPE

	FROM
		CLARITY.dbo.PROBLEM_LIST_VIEW AS PROBLEM_LIST
		INNER JOIN CLARITY.dbo.CLARITY_EDG AS CLARITY_EDG ON PROBLEM_LIST.DX_ID = CLARITY_EDG.DX_ID
		INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON CLARITY_EDG.DX_ID = EDG_CURRENT_ICD10.DX_ID

	WHERE
		EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
		AND PROBLEM_LIST.RESOLVED_DATE IS NULL --Active Dx
		AND PROBLEM_LIST.PROBLEM_STATUS_C = 1	--Active Dx
	)
	,non_hiv_patient_type
AS (SELECT
		PATIENT_FYI_FLAGS.PATIENT_ID
	,MIN (	CASE
				WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640005'
					AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'PrEP'
				WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IN ( '640008', '640034' )
					AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'STI'
				WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '6400017'
					AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'False Positive HIV Test'
				WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '9800035'
					AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'PEP'
				WHEN PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640007'
					AND PATIENT_FYI_FLAGS.ACTIVE_C = 1 THEN 'AODA HIV-'
				ELSE 'Other'
			END
		) AS PATIENT_TYPE

	FROM
		CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS

	WHERE
		PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IN ( /*PrEP*/ '640005', /*Fasle Pos*/ '640017', /*PEP*/ '9800035', /*STI*/ '640008'	/*AODA HIV-*/, '640007', /*Other HIV-*/ '9800065', /*MPX as STI*/ '640034' )
		AND PATIENT_FYI_FLAGS.ACTIVE_C = 1

	GROUP BY
		PATIENT_FYI_FLAGS.PATIENT_ID)

SELECT
	id.IDENTITY_ID 'MRN'
	,p.PAT_NAME 'Patient'
	,fpl.FPL_INCOME 'Income'
	,COALESCE(fpl.FPL_FAMILY_SIZE, 1) 'Household Size'
	,CAST (fpl.FPL_EFF_DATE AS DATE) AS 'FPL Date'
	,fpl.FPL_PERCENTAGE 'FPL%'
	,zp.NAME 'Proof Document'
	,zc.NAME 'Reason'
	,pacv.ACCOUNT_ID
	,CAST (p.BIRTH_DATE AS DATE) 'DOB'
	,CASE
		WHEN hiv_patient_type.PAT_ID IS NOT NULL THEN hiv_patient_type.PATIENT_TYPE
		WHEN non_hiv_patient_type.PATIENT_ID IS NOT NULL THEN non_hiv_patient_type.PATIENT_TYPE
		ELSE 'Other'
	END AS PATIENT_TYPE
	,ROW_NUMBER () OVER (PARTITION BY
							id.PAT_ID
						,latest_visits.Department
						ORDER BY
							fpl.FPL_EFF_DATE DESC
						) AS ROW_NUM_DESC
	,emp.NAME PSR
	,latest_visits.LAST_OFFICE_VISIT AS [Last Office Visit]
	,latest_visits.SERVICE_TYPE  'Service Type'
    ,latest_visits.SERVICE_LINE  'Service Line'
    ,latest_visits.SUB_SERVICE_LINE  'Sub Service Line'
	,latest_visits.CITY 'Site'
	,latest_visits.STATE 'State'
	,latest_visits.Department
	,COALESCE (#patient_documents.FDS_PHOTO_ID, 'No') AS [FDS - Photo ID]
	,#patient_documents.FDS_PHOTO_ID_DT AS [FDS - Photo ID Date]

INTO
	#delivery

FROM
	latest_visits
	LEFT JOIN CLARITY.dbo.PAT_ACCT_CVG_VIEW pacv ON pacv.PAT_ID = latest_visits.PAT_ID
	LEFT JOIN CLARITY.dbo.ACCOUNT_FPL_INFO_VIEW fpl ON fpl.ACCOUNT_ID = pacv.ACCOUNT_ID
													AND fpl.LINE = 1
	LEFT JOIN CLARITY.dbo.ZC_FPL_INCOME_PRF zp ON fpl.FPL_INCOME_PRF_C = zp.FPL_INCOME_PRF_C
	LEFT JOIN CLARITY.dbo.ZC_FPL_REASON_CODE zc ON zc.FPL_REASON_CODE_C = fpl.FPL_REASON_CODE_C
	INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON pacv.PAT_ID = id.PAT_ID
	INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
	LEFT JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS emp ON fpl.FPL_ENTRY_USER_ID = emp.USER_ID
	LEFT JOIN #patient_documents ON latest_visits.PAT_ID = #patient_documents.PAT_ID
	LEFT JOIN hiv_patient_type ON latest_visits.PAT_ID = hiv_patient_type.PAT_ID
	LEFT JOIN non_hiv_patient_type ON latest_visits.PAT_ID = non_hiv_patient_type.PATIENT_ID
;

SELECT
	#delivery.MRN
	,#delivery.Patient
	,#delivery.Income
	,#delivery.[FPL Date]
	,CASE
		WHEN #delivery.[FPL Date] IS NULL THEN 0
		WHEN 12 - DATEDIFF (MONTH, #delivery.[FPL Date], GETDATE ()) < 1 THEN 0
		ELSE 12 - DATEDIFF (MONTH, #delivery.[FPL Date], GETDATE ())
	END AS 'Months Until Due'
	,CASE
		WHEN #delivery.[FPL Date] IS NULL THEN 'Missing'
		WHEN 12 - DATEDIFF (MONTH, #delivery.[FPL Date], GETDATE ()) < 1 THEN 'Expired'
		ELSE 'Current'
	END AS 'FPL Status'
	,#delivery.[FPL%]
	,#delivery.[Household Size]
	,#delivery.[Proof Document]
	,#delivery.Reason
	,#delivery.DOB
	,#delivery.PATIENT_TYPE AS [Patient Type]
	,#delivery.PSR
	,#delivery.[Last Office Visit]
	,#delivery.Site
	,#delivery.State
	,#delivery.['Service Type']
	,#delivery.['Service Line']
	,#delivery.['Sub Service Line']
	,#delivery.Department
	,#delivery.[FDS - Photo ID]
	,#delivery.[FDS - Photo ID Date]
	,CURRENT_TIMESTAMP AS UPDATE_DTTM

FROM
	#delivery
WHERE
	#delivery.ROW_NUM_DESC = 1
;
