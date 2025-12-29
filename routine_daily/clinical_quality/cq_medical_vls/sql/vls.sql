/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: VLS Combined WI and CO Report ALTERYX
 Create Date: 7/15/2018
 Created By:  Mitch Scoggins
 Requested By:  Internal

 Purpose:   To get VLS data and categorize it in various ways. Used to cross-tab data in Crystal Reports

 Description:


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 2/7/2019      Mitch            Adding logic to be able to get data by site
 2/7/2019      Mitch            Adding Missouri
 4/15/2019     Mitch            Updating Active Patient logic to include Dx codes and LOS_PRIME_PROC_ID 
 9/3/2019      Mitch            Adding logic for VLS by Ethnicity
 2/24/2020     Mitch            Updating to new Department name logic
 8/30/2021     Mitch            Adding Clinical Pharmacy FYI flags
 1/31/2022     Mitch            Adding BH and Dental data for filtering
 2/3/2022      Mitch            Adding Pre-DM Cohort
 12/6/2022     Mitch            Adding Sexual Orientation and FPL
 4/6/2023      Benzon           Added Preferred Language
 2/16/2023     Benzon           Mapped Patient Race/Ethnicity per CDC maps
 3/14/2023     Benzon           Removed Patient Race/Ethnicity per CDC maps and revert to old race and ethnicity
 4/5/2024      Benzon           Added homelessness information -- go as far back as 36 months to capture as much info as we can
 4/5/2024      Benzon           Added VLS durability information -- Contact Adam for more info
**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID ('tempdb..#Attribution1') IS NOT NULL
	DROP TABLE #Attribution1;

SELECT
	pev.PAT_ID
	,pev.CONTACT_DATE LAST_OFFICE_VISIT
	,SUBSTRING (dep.DEPT_ABBREVIATION, 3, 2) 'STATE'
	,CASE
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'Milwaukee'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'KN' THEN 'Kenosha'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'GB' THEN 'Green Bay'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'WS' THEN 'Wausau'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'AP' THEN 'Appleton'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'EC' THEN 'Eau Claire'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'LC' THEN 'Lacrosse'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'MD' THEN 'Madison'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'BL' THEN 'Beloit'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'BI' THEN 'Billing'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'SL' THEN 'St Louis'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'DN' THEN 'Denver'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'AS' THEN 'Austin'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'KC' THEN 'Kansas City'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'Chicago'
		ELSE SUBSTRING (dep.DEPT_ABBREVIATION, 5, 2)
	END AS CITY
	,CASE
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 7, 2) = 'MN' THEN 'MAIN LOCATION'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 7, 2) = 'DR' THEN 'D&R'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 7, 2) = 'KE' THEN 'KEENEN'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 7, 2) = 'UC' THEN 'UNIVERSITY OF COLORADO'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 7, 2) = 'ON' THEN 'AUSTIN MAIN'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 7, 2) = 'TW' THEN 'AUSTIN OTHER'
		ELSE SUBSTRING (dep.DEPT_ABBREVIATION, 7, 2)
	END AS 'SITE'
	,CASE
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
		WHEN SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
		ELSE SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2)
	END AS 'LOS'

INTO	#Attribution1

FROM
	CLARITY.dbo.PAT_ENC_VIEW pev
	INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID

WHERE
	pev.CONTACT_DATE > DATEADD (MONTH, -12, GETDATE ())
	AND pev.APPT_STATUS_C IN ( 2, 6 )
;

IF OBJECT_ID ('tempdb..#Attribution2') IS NOT NULL
	DROP TABLE #Attribution2	;

SELECT
	a1.PAT_ID
	,a1.STATE
	,a1.CITY
	,a1.SITE
	,a1.LOS
	,ROW_NUMBER () OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC

INTO	#Attribution2

FROM	
	#Attribution1 a1
WHERE
	a1.LOS = 'MEDICAL'
;


IF OBJECT_ID ('tempdb..#Attribution3') IS NOT NULL
	DROP TABLE #Attribution3
	;
SELECT
	a2.PAT_ID
,a2.LOS
,a2.CITY
,a2.STATE
INTO
	#Attribution3
FROM
	#Attribution2 a2

WHERE
	a2.ROW_NUM_DESC = 1
;


IF OBJECT_ID ('tempdb..#preferred_language') IS NOT NULL
	DROP TABLE #preferred_language	;

SELECT
	id.IDENTITY_ID MRN
	,zl.NAME 'PREFERRED LANGUAGE'

INTO	#preferred_language

FROM
	CLARITY.dbo.IDENTITY_ID_VIEW id
	INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
	LEFT JOIN CLARITY.dbo.ZC_LANGUAGE zl ON zl.LANGUAGE_C = p.LANGUAGE_C
	INNER JOIN #Attribution3 a3 ON a3.PAT_ID = id.PAT_ID
;


/**
 * Viral Durability criteria:
 *  - Must have at least 3 years of VLS data. If they do not have 3+ worth of VLS data, patient cannot be durably suppressed.
 *  - If the patient is consistently and persistently virally suppressed from the latest VLS result date all the way down to 
 *    their previous 36 months of VLS lab data, then they are considered "Durably Suppressed".
 */
IF OBJECT_ID ('tempdb..#vls') IS NOT NULL
	DROP TABLE #vls
	;

SELECT	DISTINCT
	CLARITY_COMPONENT.COMPONENT_ID

INTO
	#vls

FROM
	CLARITY.dbo.CLARITY_COMPONENT AS CLARITY_COMPONENT

WHERE
	CLARITY_COMPONENT.COMMON_NAME = 'HIV VIRAL LOAD'

;
IF OBJECT_ID('tempdb..#vls_info') IS NOT NULL									
DROP TABLE #vls_info;
SELECT
	ORDER_PROC.PAT_ID
	,ORDER_RESULTS.ORD_VALUE
	,ORDER_RESULTS.ORD_NUM_VALUE
	,CASE
		WHEN ORDER_RESULTS.ORD_NUM_VALUE <> 9999999
			AND ORDER_RESULTS.ORD_NUM_VALUE < 200 THEN 'SUPPRESSED'
		WHEN ORDER_RESULTS.ORD_NUM_VALUE <> 9999999
			AND ORDER_RESULTS.ORD_NUM_VALUE > 199 THEN 'UNSUPPRESSED'
		WHEN ORDER_RESULTS.ORD_VALUE LIKE '>%' THEN 'UNSUPPRESSED'
		ELSE 'SUPPRESSED'
	END AS VIRAL_LOAD_SUPPRESSION_STATUS_CATC
	,CAST (ORDER_RESULTS.RESULT_DATE AS DATE) AS RESULT_DATE

INTO
	#vls_info

FROM
	CLARITY.dbo.ORDER_PROC_VIEW AS ORDER_PROC
	INNER JOIN CLARITY.dbo.ORDER_RESULTS_VIEW AS ORDER_RESULTS ON ORDER_PROC.ORDER_PROC_ID = ORDER_RESULTS.ORDER_PROC_ID
	INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON ORDER_PROC.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID
	INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID

WHERE
	ORDER_RESULTS.COMPONENT_ID IN ( SELECT vls.COMPONENT_ID FROM #vls vls )
	AND ORDER_RESULTS.ORD_VALUE NOT IN ( 'Delete', 'See comment' )
	AND ORDER_RESULTS.LAB_STATUS_C IN ( 3, 5 )	-- 3 = Final; 5 = Edited Result - FINAL

;
IF OBJECT_ID('tempdb..#max_date') IS NOT NULL									
DROP TABLE #max_date;
SELECT
	vls_info.PAT_ID
	,MAX (vls_info.RESULT_DATE) AS LATEST_VLS_DATE

INTO
	#max_date

FROM
	#vls_info vls_info

GROUP BY
	vls_info.PAT_ID

;
IF OBJECT_ID('tempdb..#min_date') IS NOT NULL									
DROP TABLE #min_date;
SELECT
	vls_info.PAT_ID
	,MIN (vls_info.RESULT_DATE) AS FIRST_VLS_DATE
INTO
	#min_date
FROM
	#vls_info vls_info
GROUP BY
	vls_info.PAT_ID

;
IF OBJECT_ID('tempdb..#preproc') IS NOT NULL									
DROP TABLE #preproc;
SELECT
	vls_info.PAT_ID
	,vls_info.VIRAL_LOAD_SUPPRESSION_STATUS_CATC

INTO
	#preproc

FROM
	#vls_info vls_info
	INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON vls_info.PAT_ID = PATIENT.PAT_ID
	INNER JOIN #max_date max_date ON vls_info.PAT_ID = max_date.PAT_ID
	INNER JOIN #min_date min_date ON vls_info.PAT_ID = min_date.PAT_ID

WHERE
	(
		vls_info.RESULT_DATE <= max_date.LATEST_VLS_DATE
		AND vls_info.RESULT_DATE >= DATEADD (MONTH, -36, max_date.LATEST_VLS_DATE)
	)
	AND DATEDIFF (YEAR, min_date.FIRST_VLS_DATE, max_date.LATEST_VLS_DATE) >= 3 -- restrict to patients that have 3+ years of VLS data
;

IF OBJECT_ID('tempdb..#unsuppression_counts') IS NOT NULL									
DROP TABLE #unsuppression_counts;

SELECT
	preproc.PAT_ID
	,SUM (	CASE
				WHEN preproc.VIRAL_LOAD_SUPPRESSION_STATUS_CATC = 'UNSUPPRESSED' THEN 1
				ELSE 0
			END
		) AS UNSUPPRESSED_COUNT

INTO
	#unsuppression_counts

FROM
	#preproc preproc

GROUP BY
	preproc.PAT_ID
;
IF OBJECT_ID('tempdb..#a') IS NOT NULL									
DROP TABLE #a;

SELECT
	id.IDENTITY_ID
	,p.PAT_ID
	,p.PAT_NAME
	,p.ZIP
	,p.CUR_PRIM_LOC_ID
	,COALESCE ((CASE
						WHEN zgi.NAME = 'Two Spirit' THEN 'Non-binary/genderqueer'
						ELSE zgi.NAME
					END
					)
				,'Not Asked'
				) AS GENDER
	,CASE
		WHEN MIN (pev.CONTACT_DATE) BETWEEN DATEADD (MONTH, -12, GETDATE ()) AND DATEADD (MONTH, -6, GETDATE ()) THEN 1
		ELSE 0
	END AS 'In-Care'
	,vls_info.ORD_VALUE
	,vls_info.ORD_NUM_VALUE
	,vls_info.RESULT_DATE
	,ser.EXTERNAL_NAME
	,CASE
		WHEN zso.NAME IN ( 'Asexual', 'Omnisexual', 'Pansexual' ) THEN 'Something else'
		WHEN zso.NAME IN ( 'Gay', 'Lesbian' ) THEN 'Lesbian or Gay'
		ELSE zso.NAME
	END AS 'Sexual Orientation'
	,CASE
		WHEN unsuppression_counts.UNSUPPRESSED_COUNT = 0 THEN 'Durably Suppressed'
		WHEN unsuppression_counts.UNSUPPRESSED_COUNT > 0 THEN 'Not Durably Suppressed'
		ELSE 'Patient for less than three years'
	END AS VLS_DURABILITY
	,ROW_NUMBER () OVER (PARTITION BY vls_info.PAT_ID ORDER BY vls_info.RESULT_DATE DESC) AS ROW_NUM_DESC

INTO #a

FROM
	#vls_info vls_info
	INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = vls_info.PAT_ID
	LEFT JOIN CLARITY.dbo.ZC_SEX sex ON p.SEX_C = sex.RCPT_MEM_SEX_C
	INNER JOIN CLARITY.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
	LEFT JOIN CLARITY.dbo.ZC_GENDER_IDENTITY zgi ON zgi.GENDER_IDENTITY_C = p4.GENDER_IDENTITY_C
	INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
	INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID --Need pev in this step to check for In-care
	INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
	INNER JOIN CLARITY.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
	INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
	INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
	INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
	LEFT JOIN CLARITY.dbo.PAT_SEXUAL_ORIENTATION pso ON vls_info.PAT_ID = pso.PAT_ID
														AND pso.LINE = 1
	LEFT JOIN CLARITY.dbo.ZC_SEXUAL_ORIENTATION zso ON pso.SEXUAL_ORIENTATN_C = zso.SEXUAL_ORIENTATION_C
	LEFT JOIN #unsuppression_counts unsuppression_counts ON vls_info.PAT_ID = unsuppression_counts.PAT_ID

WHERE
	(vls_info.RESULT_DATE BETWEEN DATEADD (MONTH, -12, GETDATE ()) AND GETDATE ())
	AND (pev.CONTACT_DATE BETWEEN DATEADD (MONTH, -12, GETDATE ()) AND GETDATE ()) --Visit in past year
	AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
	AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
	AND SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
	AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
	AND plv.RESOLVED_DATE IS NULL --Active Dx
	AND plv.PROBLEM_STATUS_C = 1 --Active Dx
	AND p4.PAT_LIVING_STAT_C = 1
	AND ser.SERV_AREA_ID = 64

GROUP BY
	id.IDENTITY_ID
	,p.PAT_ID
	,vls_info.PAT_ID
	,vls_info.ORD_VALUE
	,vls_info.ORD_NUM_VALUE
	,vls_info.RESULT_DATE
	,p.ZIP
	,p.PAT_NAME
	,p.CUR_PRIM_LOC_ID
	,ser.EXTERNAL_NAME
	,zso.NAME
	,zgi.NAME
	,unsuppression_counts.UNSUPPRESSED_COUNT
;


IF OBJECT_ID ('tempdb..#b') IS NOT NULL
	DROP TABLE #b
	;
SELECT
	a.IDENTITY_ID
	,a.PAT_ID
	,a.ZIP
	,a.GENDER
	,a.PAT_NAME
	,a.CUR_PRIM_LOC_ID
	,a.ORD_VALUE
	,a.EXTERNAL_NAME
	,a.RESULT_DATE LAST_LAB
	,CASE
		WHEN a.ORD_NUM_VALUE <> 9999999 THEN a.ORD_NUM_VALUE
		WHEN a.ORD_VALUE LIKE '>%' THEN 10000000
		ELSE 0
	END AS Result_Output
	,a.VLS_DURABILITY
	,a.[In-Care]
	,a.[Sexual Orientation]
	INTO
		#b
	FROM
		#a a
	WHERE
		a.ROW_NUM_DESC = 1
;


IF OBJECT_ID ('tempdb..#c') IS NOT NULL
	DROP TABLE #c
	;
SELECT
	b.IDENTITY_ID
	,b.PAT_ID
	,p.BIRTH_DATE
	,b.ZIP
	,b.GENDER
	,CASE
		WHEN att.STATE = 'MO' THEN 'Missouri'
		WHEN att.STATE = 'CO' THEN 'Colorado'
		WHEN att.STATE = 'WI' THEN 'Wisconsin'
		WHEN att.STATE = 'TX' THEN 'Texas'
		WHEN att.STATE = 'IL' THEN 'Illinois'
		ELSE att.STATE
	END AS 'STATE'
	,att.CITY
	,zc.NAME COUNTY
	,b.CUR_PRIM_LOC_ID
	,loc.LOC_NAME
	,b.EXTERNAL_NAME
	,b.PAT_NAME
	,b.ORD_VALUE
	,b.LAST_LAB
	,b.Result_Output
	,CASE
		WHEN b.Result_Output < 200 THEN 1
		ELSE 0
	END AS SUPPRESSED
	,CASE
		WHEN b.Result_Output < 200 THEN 'SUPPRESSED'
		ELSE 'UNSUPPRESSED'
	END AS VLS_CATEGORY
	,b.VLS_DURABILITY
	,zeg.NAME ETHNICITY
	,zs.NAME SEX
	,CASE
		WHEN pr.PATIENT_RACE_C IS NULL THEN 'Unknown'
		WHEN pr.PATIENT_RACE_C = 2 THEN 'Black'
		WHEN pr.PATIENT_RACE_C = 6 THEN 'White'
		WHEN pr.PATIENT_RACE_C IN ( 5, 10, 11 ) THEN 'Unknown'
		ELSE zpr.NAME
	END AS RACE
	,zpr.NAME 'All Race'
	,b.[In-Care]
	,bh.[BH PATIENT]
	,dent.[DENTAL PATIENT]
	,b.[Sexual Orientation]

INTO	#c

FROM
	#b b
	INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON b.PAT_ID = p.PAT_ID
	LEFT JOIN CLARITY.dbo.ZC_COUNTY zc ON p.COUNTY_C = zc.COUNTY_C
	LEFT JOIN CLARITY.dbo.CLARITY_LOC loc ON p.CUR_PRIM_LOC_ID = loc.LOC_ID
	LEFT JOIN CLARITY.dbo.PATIENT_RACE pr ON p.PAT_ID = pr.PAT_ID
											AND pr.LINE = 1
	LEFT JOIN CLARITY.dbo.ZC_PATIENT_RACE zpr ON pr.PATIENT_RACE_C = zpr.PATIENT_RACE_C
	LEFT JOIN CLARITY.dbo.ZC_ETHNIC_GROUP zeg ON p.ETHNIC_GROUP_C = zeg.ETHNIC_GROUP_C
	LEFT JOIN CLARITY.dbo.ZC_SEX zs ON p.SEX_C = zs.RCPT_MEM_SEX_C
	INNER JOIN #Attribution2 att ON b.PAT_ID = att.PAT_ID
	LEFT JOIN
	(
		SELECT	DISTINCT
				ev.PAT_LINK_ID PAT_ID
			,'Active in BH' AS 'BH PATIENT'
		FROM
			CLARITY.dbo.EPISODE_VIEW ev
			INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = ev.PAT_LINK_ID
			INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
		WHERE
			ev.SUM_BLK_TYPE_ID = 221
			AND ev.STATUS_C = 1
			AND pev.CONTACT_DATE > DATEADD (MONTH, -12, GETDATE ())
			AND pev.APPT_STATUS_C IN ( 2, 6 )
			AND SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) IN ( 'MH', 'BH', 'PY' )
	) bh ON bh.PAT_ID = b.PAT_ID
	LEFT JOIN
	(
		SELECT	DISTINCT
				ev.PAT_LINK_ID PAT_ID
			,'Active Dental' AS 'DENTAL PATIENT'
		FROM
			CLARITY.dbo.EPISODE_VIEW ev
			INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = ev.PAT_LINK_ID
			INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
		WHERE
			ev.STATUS_C = 1 --Active episode
			AND ev.SUM_BLK_TYPE_ID = 45 --Dental
			AND pev.CONTACT_DATE > DATEADD (MONTH, -12, GETDATE ())
			AND pev.APPT_STATUS_C IN ( 2, 6 )
			AND SUBSTRING (dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
	) dent ON dent.PAT_ID = b.PAT_ID
WHERE
	att.ROW_NUM_DESC = 1
;


IF OBJECT_ID ('tempdb..#d') IS NOT NULL
	DROP TABLE #d
	;
SELECT
	c.IDENTITY_ID
	,c.PAT_ID
	,c.BIRTH_DATE
	,c.ZIP
	,c.GENDER
	,c.STATE
	,c.CITY
	,c.COUNTY
	,c.CUR_PRIM_LOC_ID
	,c.LOC_NAME
	,c.EXTERNAL_NAME
	,c.PAT_NAME
	,c.ORD_VALUE
	,c.LAST_LAB
	,c.Result_Output
	,c.SUPPRESSED
	,c.VLS_CATEGORY
	,c.VLS_DURABILITY
	,c.ETHNICITY
	,c.SEX
	,c.[All Race] 'RACE'
	,CASE
		WHEN c.RACE = 'Black' THEN 'Black/African American'
		WHEN c.RACE = 'White' THEN 'White'
		ELSE NULL
	END AS DISPARITY_RACE
	,CASE
		WHEN c.RACE = 'Black'
			AND c.SUPPRESSED = 1 THEN 1
		WHEN c.RACE = 'Black'
			AND c.SUPPRESSED = 0 THEN 0
	END AS BLACK
	,CASE
		WHEN c.RACE = 'White'
			AND c.SUPPRESSED = 1 THEN 1
		WHEN c.RACE = 'White'
			AND c.SUPPRESSED = 0 THEN 0
	END AS WHITE
	,CASE
		WHEN c.STATE = 'WISCONSIN'
			AND c.SUPPRESSED = 1 THEN 1
		WHEN c.STATE = 'WISCONSIN'
			AND c.SUPPRESSED = 0 THEN 0
	END AS WI_SUPP
	,CASE
		WHEN c.STATE = 'COLORADO'
			AND c.SUPPRESSED = 1 THEN 1
		WHEN c.STATE = 'COLORADO'
			AND c.SUPPRESSED = 0 THEN 0
	END AS CO_SUPP
	,CASE
		WHEN c.STATE = 'MISSOURI'
			AND c.SUPPRESSED = 1 THEN 1
		WHEN c.STATE = 'MISSOURI'
			AND c.SUPPRESSED = 0 THEN 0
	END AS MO_SUPP
	,CASE
		WHEN c.STATE = 'TEXAS'
			AND c.SUPPRESSED = 1 THEN 1
		WHEN c.STATE = 'TEXAS'
			AND c.SUPPRESSED = 0 THEN 0
	END AS TX_SUPP
	,IIF(c.Result_Output < 200, 1, c.[In-Care]) AS IN_CARE		--this is the step that checks EITHER for suppressed or 6+ months
	,c.[BH PATIENT]
	,c.[DENTAL PATIENT]
	,c.[Sexual Orientation]
INTO
	#d
FROM
	#c c
;


--To get active pts in Clinical Pharmacy Cohorts
IF OBJECT_ID ('tempdb..#fyi') IS NOT NULL
	DROP TABLE #fyi
	;
SELECT
	flag.PATIENT_ID PAT_ID
,MAX (IIF(f.NAME IS NOT NULL, 'Y', NULL)) AS 'ACTIVE_CP_COHORT'
INTO
	#fyi
FROM
	CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW flag
	INNER JOIN CLARITY.dbo.ZC_BPA_TRIGGER_FYI f ON flag.PAT_FLAG_TYPE_C = f.BPA_TRIGGER_FYI_C
WHERE
	f.NAME LIKE 'SA64 Pharmacist%'
	AND flag.ACTIVE_C = 1	-- Only currently actives
GROUP BY
	flag.PATIENT_ID
;


-- Get Homelessness status
IF OBJECT_ID ('tempdb..#homelessness') IS NOT NULL
	DROP TABLE #homelessness
	;
SELECT
	IP_FLWSHT_REC.PAT_ID
,CASE IP_FLWSHT_MEAS.MEAS_VALUE
	WHEN '0' THEN 'I have a steady place to live'
	WHEN '1' THEN 'Insecure Housing'
	WHEN '2' THEN 'Insecure Housing'
	ELSE IP_FLWSHT_MEAS.MEAS_VALUE
END AS LIVING_SITUATION
,ROW_NUMBER () OVER (PARTITION BY
						IP_FLWSHT_REC.PAT_ID
					ORDER BY
						COALESCE (IP_FLWSHT_MEAS.RECORDED_TIME, IP_FLWSHT_MEAS.ENTRY_TIME) DESC
					) AS ROW_NUM_DESC
INTO
	#homelessness
FROM
	CLARITY.dbo.IP_FLWSHT_MEAS_VIEW AS IP_FLWSHT_MEAS
	INNER JOIN CLARITY.dbo.IP_FLWSHT_REC_VIEW AS IP_FLWSHT_REC ON IP_FLWSHT_MEAS.FSD_ID = IP_FLWSHT_REC.FSD_ID
WHERE
	IP_FLWSHT_MEAS.FLO_MEAS_ID = '5693'
;

;
IF OBJECT_ID('tempdb..#svis') IS NOT NULL									
DROP TABLE #svis;
SELECT
	pev.PAT_ID
	,CAST (pev.CONTACT_DATE AS DATE) 'Next Any Appt'
	,ser.PROV_NAME 'Next Appt Prov'
	,ROW_NUMBER () OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC

INTO
	#svis
FROM
	CLARITY.dbo.PAT_ENC_VIEW pev
	INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE
	pev.APPT_STATUS_C = 1	--Scheduled

;
IF OBJECT_ID('tempdb..#spvis') IS NOT NULL									
DROP TABLE #spvis;

SELECT
	pev.PAT_ID
	,CAST (pev.CONTACT_DATE AS DATE) 'Next PCP Appt'
	,ser.PROV_NAME 'Next PCP Appt Prov'
	,ROW_NUMBER () OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC

INTO
	#spvis

FROM
	CLARITY.dbo.PAT_ENC_VIEW pev
	INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE
	pev.APPT_STATUS_C = 1 --Scheduled
	AND ser.PROV_ID <> '640178' --pulmonologist
	AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs
;
IF OBJECT_ID('tempdb..#fpl') IS NOT NULL									
DROP TABLE #fpl;

SELECT
	pacv.PAT_ID
	,fplp.FPL_PERCENTAGE
	,ROW_NUMBER () OVER (PARTITION BY pacv.PAT_ID ORDER BY fplp.FPL_EFF_DATE DESC) AS ROW_NUM_DESC

INTO
	#fpl
FROM
	CLARITY.dbo.ACCOUNT_FPL_INFO_VIEW fplp
	INNER JOIN CLARITY.dbo.PAT_ACCT_CVG_VIEW pacv ON fplp.ACCOUNT_ID = pacv.ACCOUNT_ID
WHERE
	fplp.LINE = 1
;

SELECT
	d.IDENTITY_ID MRN
	,d.PAT_ID
	,CAST (d.BIRTH_DATE AS DATE) AS DOB
	,(DATEDIFF (m, d.BIRTH_DATE, GETDATE ()) / 12) AGE
	,d.STATE
	,d.CITY
	,d.ZIP
	,d.GENDER
	,d.IN_CARE
	,[PREFERRED LANGUAGE]
	,COALESCE (d.COUNTY, 'Not in Chart') 'COUNTY'
	,UPPER (d.EXTERNAL_NAME) 'PCP'
	,d.PAT_NAME 'PATIENT'
	,d.ORD_VALUE 'LAST_VL'
	,CAST (d.LAST_LAB AS DATE) AS LAST_LAB
	,d.Result_Output
	,d.SUPPRESSED
	,d.VLS_CATEGORY
	,d.VLS_DURABILITY
	,CASE
		WHEN d.ETHNICITY IS NULL THEN 'Unknown'
		WHEN d.ETHNICITY = '' THEN 'Unknown'
		WHEN d.ETHNICITY = 'Not Collected/Unknown' THEN 'Unknown'
		WHEN d.ETHNICITY = 'Patient Refused' THEN 'Unknown'
		ELSE d.ETHNICITY
	END AS ETHNICITY
	,d.SEX
	,COALESCE (d.RACE, 'Unknown') 'RACE'
	,d.DISPARITY_RACE
	,CAST (GETDATE () AS DATE) 'Report_Date'
	,CASE
		WHEN fyi.ACTIVE_CP_COHORT = 'Y' THEN 'Y'
		ELSE 'N'
	END AS 'CLINICAL PHARMACY COHORT'
	,CASE
		WHEN d.[BH PATIENT] IS NOT NULL
			AND d.[DENTAL PATIENT] IS NULL THEN d.[BH PATIENT]
		WHEN d.[BH PATIENT] IS NULL
			AND d.[DENTAL PATIENT] IS NOT NULL THEN d.[DENTAL PATIENT]
		WHEN d.[BH PATIENT] IS NOT NULL
			AND d.[DENTAL PATIENT] IS NOT NULL THEN 'Active in BH and Dental'
		ELSE 'Non-BH or Dental Patient'
	END AS [DENTAL/BH Status]
	,svis.[Next Any Appt]
	,svis.[Next Appt Prov]
	,spvis.[Next PCP Appt]
	,spvis.[Next PCP Appt Prov]
	,COALESCE (d.[Sexual Orientation], 'Not Asked') AS [Sexual Orientation]
	,fpl.FPL_PERCENTAGE AS [FPL Detail]
	,CASE
		WHEN fpl.FPL_PERCENTAGE IS NULL THEN 'Unknown'
		WHEN fpl.FPL_PERCENTAGE < 100 THEN 'Under 100%'
		WHEN fpl.FPL_PERCENTAGE < 139 THEN '100% - 138%'
		WHEN fpl.FPL_PERCENTAGE < 201 THEN '139% - 200%'
		WHEN fpl.FPL_PERCENTAGE < 251 THEN '201% - 250%'
		WHEN fpl.FPL_PERCENTAGE < 401 THEN '251% - 400%'
		WHEN fpl.FPL_PERCENTAGE < 501 THEN '401% - 500%'
		ELSE 'Over 500%'
	END AS [FPL Category]
	,h.LIVING_SITUATION AS [Homelessness]

FROM
	#d d
	INNER JOIN #preferred_language pl ON d.IDENTITY_ID = pl.MRN
	LEFT JOIN #fyi fyi ON fyi.PAT_ID = d.PAT_ID
	LEFT JOIN #svis svis ON svis.PAT_ID = d.PAT_ID
							AND svis.ROW_NUM_ASC = 1 -- First scheduled
	LEFT JOIN #spvis spvis ON spvis.PAT_ID = d.PAT_ID
							AND spvis.ROW_NUM_ASC = 1 -- First scheduled
	LEFT JOIN #fpl fpl ON fpl.PAT_ID = d.PAT_ID
						AND fpl.ROW_NUM_DESC = 1
	LEFT JOIN #homelessness h ON d.PAT_ID = h.PAT_ID
							AND h.ROW_NUM_DESC = 1

;
DROP TABLE #Attribution1
DROP TABLE #Attribution2
DROP TABLE #Attribution3
DROP TABLE #preferred_language
DROP TABLE #vls
DROP TABLE #vls_info
DROP TABLE #max_date
DROP TABLE #min_date
DROP TABLE #preproc
DROP TABLE #unsuppression_counts
DROP TABLE #a
DROP TABLE #b
DROP TABLE #c
DROP TABLE #d
DROP TABLE #fyi
DROP TABLE #homelessness
DROP TABLE #svis
DROP TABLE #spvis
DROP TABLE #fpl