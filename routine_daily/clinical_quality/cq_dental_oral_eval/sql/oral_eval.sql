SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

/*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT
------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/*Measure: Patients with an Oral Evaluation

Denominator: Patients seen in a dental clinic by a dentist in the past 12 months.

Numerator: Patients who had an Oral Health Eval in the past 13 months, with a code in ('TX130', 'D0120', 'D0150', 'D0180', 'D0140', 'D0160', 'D0170', 'D0190')

Exclusions: None                                
---------------------------------------------------

 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
03/05/2020			Jaya				 Name updated to new Name
9/30/2020			Mitch				Alteryx
8/29/2025			Ben					Added "Oral Eval Provider" column per Quality Dept request
**********************************************************************************************
 */

SELECT TOP 100000000
	pev.PAT_ID
	,CAST(pev.CONTACT_DATE AS DATE) LAST_OFFICE_VISIT
	,SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE'
	,CASE
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWAUKEE'
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
	END AS CITY
	,CASE
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN' THEN 'MAIN LOCATION'
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'DR' THEN 'D&R'
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'KE' THEN 'KEENEN'
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'UC' THEN 'UNIVERSITY OF COLORADO'
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'ON' THEN 'AUSTIN MAIN'
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'TW' THEN 'AUSTIN OTHER'
		ELSE 'ERROR'
	END AS 'SITE'
	,CASE
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
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

FROM 
	Clarity.dbo.PAT_ENC_VIEW pev
	INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID

WHERE
	pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
	AND pev.APPT_STATUS_C IN (2, 6)

;

SELECT  TOP 100000000
	a1.PAT_ID
	,a1.STATE
	,a1.CITY
	,a1.SITE
	,a1.LOS
	,a1.LAST_OFFICE_VISIT
	,ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC

INTO #Attribution2

FROM 
	#Attribution1 a1

WHERE
	a1.LOS = 'DENTAL'

;

SELECT  TOP 100000000
	a2.PAT_ID
	,a2.LOS
	,a2.CITY
	,a2.STATE
	,a2.LAST_OFFICE_VISIT

INTO #Attribution3
	 
FROM 
	#Attribution2 a2
	INNER JOIN Clarity.dbo.EPISODE_VIEW ev ON ev.PAT_LINK_ID = a2.PAT_ID

WHERE
	a2.ROW_NUM_DESC = 1
	AND ev.SUM_BLK_TYPE_ID = 45
	AND ev.STATUS_C = 1

;

SELECT  TOP 100000000
	pev.PAT_ID
	,MAX(t64.ORIG_SERVICE_DATE) 'Oral Eval Date'
	,ser.PROV_NAME AS 'Oral Eval Provider'

INTO #a

FROM 
	Clarity.dbo.PAT_ENC_VIEW pev 
	INNER JOIN #Attribution3 a3 ON a3.PAT_ID = pev.PAT_ID
	LEFT JOIN Clarity.dbo.CLARITY_TDL_TRAN_64_VIEW t64 ON pev.PAT_ENC_CSN_ID = t64.PAT_ENC_CSN_ID
					AND t64.DETAIL_TYPE = 1
					AND t64.CPT_CODE IN ( 'D0120', 'D0150', 'D0180', 'D0140', 'D0160', 'D0170', 'D0190') 
					AND t64.ORIG_SERVICE_DATE BETWEEN DATEADD(MONTH, -13, GETDATE()) AND GETDATE()
	INNER JOIN Clarity.dbo.CLARITY_SER ser ON t64.PERFORMING_PROV_ID = ser.PROV_ID

WHERE
	pev.PAT_ID IN
	(SELECT DISTINCT --Active dental pts
		pev.PAT_ID
	
	FROM 
		#Attribution3 a3
		INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = a3.PAT_ID
		INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID

	WHERE
		pev.CONTACT_DATE BETWEEN DATEADD(MONTH, -12, GETDATE()) AND GETDATE()
		AND ser.PROVIDER_TYPE_C = '108'
		)

GROUP BY 
	pev.PAT_ID
	,ser.PROV_NAME
	
;

------------to normalize the data at the pt level to 1 if they had an oral eval and 0 if they did not. --------------
SELECT TOP 100000000
	id.IDENTITY_ID MRN
	,p.PAT_NAME PATIENT
	,CASE	
		WHEN a.[Oral Eval Date] IS NOT NULL THEN 1 ELSE 0
	END AS ORAL_EVAL
	,CASE	
		WHEN a.[Oral Eval Date] IS NOT NULL THEN 'MET' ELSE 'NOT MET'
	END AS 'MET_YN'
	,CAST(a.[Oral Eval Date] AS DATE) AS 'Oral Eval Date'
	,a.[Oral Eval Provider] AS 'Oral Eval Provider'
	,COALESCE(DATEDIFF(MONTH, a.[Oral Eval Date], GETDATE()), -999) 'Months Since Oral Eval'
	,a3.STATE
	,a3.CITY
	,a3.LAST_OFFICE_VISIT
	,svis.[Next Any Appt] AS 'Next Any Appt'
	,svis.[Next Appt Prov]
	,spvis.[Next Dental Appt] AS 'Next Dental Appt'
	,spvis.[Next Dental Appt Prov]
	,CASE
		WHEN dm.PAT_ID IS NOT NULL THEN 'Y'
		ELSE 'N'
	END AS 'Has Diabetes',
	GETDATE() AS UPDATE_DTTM

FROM
	#Attribution3 a3 
	LEFT JOIN #a a ON a.PAT_ID = a3.PAT_ID
	INNER JOIN Clarity.dbo.PATIENT_VIEW p ON a3.PAT_ID = p.PAT_ID
	INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
	INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON a3.PAT_ID = id.PAT_ID
	LEFT JOIN Clarity.dbo.DM_DIABETES_VIEW dm ON dm.PAT_ID = a3.PAT_ID
	LEFT JOIN
		(SELECT  TOP 1000000
			pev.PAT_ID
			,CAST(pev.CONTACT_DATE AS DATE) AS 'Next Any Appt'
			,ser.PROV_NAME AS 'Next Appt Prov'
			,ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
				
			FROM 
			Clarity.dbo.PAT_ENC_VIEW pev
			INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
	
		WHERE
			pev.APPT_STATUS_C = 1 --Scheduled
	
		) svis ON svis.PAT_ID = id.PAT_ID
				AND svis.ROW_NUM_ASC = 1 -- First scheduled
	LEFT JOIN
		(SELECT  TOP 1000000
			pev.PAT_ID
			,CAST(pev.CONTACT_DATE AS DATE) AS 'Next Dental Appt'
			,ser.PROV_NAME AS 'Next Dental Appt Prov'
			,ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
				
			FROM 
			Clarity.dbo.PAT_ENC_VIEW pev
			INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
			INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
	
		WHERE
			pev.APPT_STATUS_C = 1 --Scheduled
			AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
	
		) spvis ON spvis.PAT_ID = id.PAT_ID
				AND spvis.ROW_NUM_ASC = 1 -- First scheduled	

WHERE
	p4.PAT_LIVING_STAT_C = 1
;

DROP TABLE #a;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
