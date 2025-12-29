/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Medicare Annual Wellness Visit
 Create Date:	6/12/2024
 Created By:	ViventHealth\MScoggins
 System:		ANL-MKE-SVR-100
 Requested By:	Adam C

 Description:
 

 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

 IF OBJECT_ID('tempdb..#Attribution1') IS NOT NULL
  BEGIN
      DROP TABLE #Attribution1;
 END;
 
 IF OBJECT_ID('tempdb..#Attribution2') IS NOT NULL
  BEGIN
      DROP TABLE #Attribution2;
 END;
 
 IF OBJECT_ID('tempdb..#Attribution3') IS NOT NULL
  BEGIN
      DROP TABLE #Attribution3;
 END;
 
 SELECT
 	pev.PAT_ID
 	,pev.CONTACT_DATE LAST_OFFICE_VISIT
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
 		ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2)
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
 		ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) 
 	END AS 'LOS'
 
 INTO #Attribution1
 
 FROM 
 	Clarity.dbo.PAT_ENC_VIEW pev
 	INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
 
 WHERE
 	pev.CONTACT_DATE > DATEADD(MONTH, -18, GETDATE())
 	AND pev.APPT_STATUS_C IN (2, 6)
 
 ;
 
 SELECT 
 	a1.PAT_ID
 	,a1.STATE
 	,a1.CITY
 	,a1.SITE
 	,a1.LOS
 	,ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
 
 INTO #Attribution2
 
 FROM 
 	#Attribution1 a1
 
 WHERE
 	a1.LOS = 'MEDICAL'
 
 ;
 
 SELECT 
 	a2.PAT_ID
 	,a2.LOS
 	,a2.CITY
 	,a2.STATE
 
 INTO #Attribution3
 	 
 FROM 
 	#Attribution2 a2
 
 WHERE
 	a2.ROW_NUM_DESC = 1
 
 ;

IF OBJECT_ID('tempdb..#denom') IS NOT NULL									
DROP TABLE #denom;

SELECT DISTINCT
 	pev.PAT_ID
 	,id.IDENTITY_ID 'MRN'
	,p.PAT_NAME 'Patient'
	,CONVERT(nvarchar(30), p.BIRTH_DATE, 101) AS 'DOB'
	,ser.EXTERNAL_NAME 'PCP'
	,ins.[Insurance Financial Class]
	,ins.[Payor Name]
	,ins.[Plan Name]
	,ins.MEM_EFF_FROM_DATE
	,ins.MEM_EFF_TO_DATE
	--,ins.[Medicare Term Date]
	,CONVERT(nvarchar(30), spvis.[Next PCP Appt], 101) AS 'Next PCP Appt'
	,spvis.[Next PCP Appt Prov]
	,CONVERT(nvarchar(30), svis.[Next Any Appt], 101) AS 'Next Any Appt'
	,svis.[Next Appt Prov]

INTO #denom
	
FROM 
 	Clarity.dbo.PATIENT_VIEW p
	INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
 	INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
 	INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
 	INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
 	INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
 	INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
 	INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
 	INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
	INNER JOIN 
	    (SELECT
	        id.PAT_ID
	        ,zfc.NAME 'Insurance Financial Class'
	        ,epm.PAYOR_ID AS PAYOR_ID
	        ,epm.PAYOR_NAME AS 'Payor Name'
	        ,epp.BENEFIT_PLAN_ID AS PLAN_ID
	        ,epp.BENEFIT_PLAN_NAME AS 'Plan Name'
			,cvg.CVG_TERM_DT 'Medicare Term Date'
			,cvg.CVG_EFF_DT 'Medicare Effective Date'
			,cml.MEM_EFF_FROM_DATE
			,cml.MEM_EFF_TO_DATE

	    FROM 
	        Clarity.dbo.IDENTITY_ID_VIEW id
			INNER JOIN Clarity.dbo.PAT_ACCT_CVG_VIEW patcvg ON patcvg.PAT_ID = ID.PAT_ID
			INNER JOIN Clarity.dbo.COVERAGE cvg ON patcvg.COVERAGE_ID = cvg.COVERAGE_ID
			INNER JOIN Clarity.dbo.COVERAGE_MEM_LIST cml ON cvg.COVERAGE_ID = cml.COVERAGE_ID
			INNER JOIN Clarity.dbo.ACCOUNT_VIEW	acct ON acct.[ACCOUNT_ID]=patcvg.[ACCOUNT_ID]
	        INNER JOIN Clarity.dbo.CLARITY_EPP epp ON cvg.PLAN_ID = epp.BENEFIT_PLAN_ID
	        INNER JOIN Clarity.dbo.CLARITY_EPM epm ON epp.PAYOR_ID = epm.PAYOR_ID
	        INNER JOIN Clarity.dbo.ZC_FIN_CLASS zfc ON epm.FINANCIAL_CLASS = zfc.FIN_CLASS_C
	
	    WHERE
	        patcvg.ACCOUNT_ACTIVE_YN = 'Y' ---Active accounts only
			AND acct.ACCOUNT_TYPE_C = 1 --Personal/Family
			AND (cml.MEM_EFF_TO_DATE IS NULL
				OR cml.MEM_EFF_TO_DATE > GETDATE())
			AND epp.BENEFIT_PLAN_NAME NOT LIKE 'RX%'
			AND epm.FINANCIAL_CLASS IN ('2' /*Medicare*/, '110'/*Medicare HMO*/)
	) AS ins ON ins.PAT_ID = p.PAT_ID
	LEFT JOIN
		(SELECT
			pev.PAT_ID
			,CONVERT(nvarchar(30), pev.CONTACT_DATE, 101) AS  'Next Any Appt'
			,ser.PROV_NAME 'Next Appt Prov'
			,ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
				
			FROM 
			Clarity.dbo.PAT_ENC_VIEW pev
			INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
	
		WHERE
			pev.APPT_STATUS_C = 1 --Scheduled
	
		) svis ON svis.PAT_ID = id.PAT_ID
				AND svis.ROW_NUM_ASC = 1 -- First scheduled
	LEFT JOIN
		(SELECT
			pev.PAT_ID
			,CONVERT(nvarchar(30), pev.CONTACT_DATE, 101) AS  'Next PCP Appt'
			,ser.PROV_NAME 'Next PCP Appt Prov'
			,ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
				
			FROM 
			Clarity.dbo.PAT_ENC_VIEW pev
			INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
	
		WHERE
			pev.APPT_STATUS_C = 1 --Scheduled
			AND ser.PROV_ID <> '640178' --pulmonologist
			AND ser.PROVIDER_TYPE_C IN ('1', '6', '9', '113') -- Physicians, PAs and NPs
	
		) spvis ON spvis.PAT_ID = id.PAT_ID
				AND spvis.ROW_NUM_ASC = 1 -- First scheduled
 		
WHERE
 	ser.SERV_AREA_ID = 64
 	AND ser.PROVIDER_TYPE_C IN ('1', '9', '6', '113') -- Physicians and NPs, PAs
 	AND pev.CONTACT_DATE > DATEADD (MM,-12, GETDATE()) --Visit in past year
 	AND pev.APPT_STATUS_C IN (2, 6) --Visit was completed
 	AND pev.LOS_PRIME_PROC_ID IN (7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 
 								7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056) -- Office Visits
 	AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
 	AND icd10.CODE IN ('B20', 'Z21', 'B97.35') --HIV and Asymptomatic HIV
 	AND plv.RESOLVED_DATE IS NULL --Active Dx
 	AND plv.PROBLEM_STATUS_C = 1 --Active Dx
 	AND p4.PAT_LIVING_STAT_C = 1

;

SELECT -------------------------Need to limit transaction type (excl 50?)
	d.MRN
	,d.Patient
	,d.DOB
	,d.PCP
	,d.[Insurance Financial Class]
	,d.[Payor Name]
	,d.[Plan Name]
	,d.MEM_EFF_FROM_DATE 'Date Medicare Effective'
	,d.MEM_EFF_TO_DATE 'Medicare End Date'
	,d.[Next PCP Appt]
	,d.[Next PCP Appt Prov]
	,d.[Next Any Appt]
	,d.[Next Appt Prov]
	,g.[Service Date]
	,g.CPT_CODE 'Wellness Code Charged'
	,COALESCE(DATEDIFF(MONTH,g.[Service Date], GETDATE()), 99) 'Months Since Last Wellness Charge'
	,CASE
		WHEN g.[Service Date] IS NULL THEN 'Due Now'
		WHEN DATEDIFF(MONTH,g.[Service Date], GETDATE()) > 11 THEN 'Due Now'
		WHEN DATEDIFF(MONTH,g.[Service Date], GETDATE()) > 10 THEN 'Due Soon'
		ELSE 'Not Due' 
	END AS 'Wellness Visit Status'	
	,a3.CITY 'Site'
	,a3.STATE 'State'
FROM 
	#denom d
	INNER JOIN #Attribution3 a3 ON a3.PAT_ID = d.PAT_ID
	LEFT JOIN 
		(SELECT 
		 	tdl.PAT_ID
			,tdl.CPT_CODE
			,tdl.ORIG_SERVICE_DATE 'Service Date'
			,ROW_NUMBER() OVER (PARTITION BY tdl.PAT_ID ORDER BY tdl.ORIG_SERVICE_DATE DESC) AS ROW_NUM_DESC
			
		 FROM 
		 	Clarity.dbo.CLARITY_TDL_TRAN_64_VIEW tdl 
		
		WHERE 
			tdl.CPT_CODE IN ('G0402', 'G0438', 'G0439')
			AND tdl.DETAIL_TYPE NOT IN (50, 51) --Caboodle claim imports
			
		) g ON g.PAT_ID = d.MRN --In the TDL table, PAT_ID is the MRN
				AND g.ROW_NUM_DESC = 1--Most recent charge

;

DROP TABLE #denom
 	
 	