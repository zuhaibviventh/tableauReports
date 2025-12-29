/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Medical Home Touch Report - Tableau
 Create Date: 3/11/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  Lori D.

 Purpose:   

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 2023-04-18     Benzon          Added the following columns: 
                                    [Last Care Plan],
                                    [Care Plan Coordinator],
                                    [Months Since Care Plan],
                                    [Next Medical Visit]
 2023-04-20     Benzon          Added a flag to determine if patient is in a Clinical Pharmacy Cohort
 2023-05-04     Benzon          Added Active and Inactive information for the following FYI flags:
                                    640009    - SA64 Opt out Medical Home
                                    640010    - SA64 No Medical 2ndry DX
                                    640000012 - Ineligible as enrolled in other Care Coordination services
 2023-05-11     Mitch           Excluded deleted providers
 2023-05-11     Mitch           Excluding MCM's who are not in the care team with BOTH relationship code 1010 and Specialty code 413
 2024-01-05     Benzon          Added next PCP appointment information
 2024-01-05     Benzon          Added Last Pharmacy Assessment Date
 2024-02-20     Benzon          Added Last Visit information to grab City and State
 2024-04-12     Benzon          Added Comprehensive Medication Review
 2024-12-13		Mitch			Fixing the logic that calcluates days since last pharmacy assessment
 2024-12-15		Mitch			Fixing logic for next PCP visit

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#last_visit_info') IS NOT NULL 
DROP TABLE #last_visit_info;

SELECT pat_enc.PAT_ID,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2)
           WHEN 'WI' THEN 'Wisconsin'
           WHEN 'CO' THEN 'Colorado'
           WHEN 'MO' THEN 'Missouri'
           WHEN 'TX' THEN 'Texas'
           ELSE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2)
       END AS STATE,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 5, 2)
           WHEN 'MK' THEN 'Milwaukee'
           WHEN 'KN' THEN 'Kenosha'
           WHEN 'GB' THEN 'Green Bay'
           WHEN 'WS' THEN 'Wausau'
           WHEN 'AP' THEN 'Appleton'
           WHEN 'EC' THEN 'Eau Claire'
           WHEN 'LC' THEN 'La Crosse'
           WHEN 'MD' THEN 'Madison'
           WHEN 'BL' THEN 'Beloit'
           WHEN 'BI' THEN 'Billing'
           WHEN 'SL' THEN 'St. Louis'
           WHEN 'DN' THEN 'Denver'
           WHEN 'AS' THEN 'Austin'
           WHEN 'KC' THEN 'Kansas City'
           WHEN 'CG' THEN 'Chicago'
           ELSE 'ERROR'
       END AS CITY,
       ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE DESC) AS RN_DESC

INTO #last_visit_info

FROM 
	CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID

WHERE 
	PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
    AND DATEDIFF(MONTH, PAT_ENC.CONTACT_DATE, GETDATE()) <= 12
    AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'MD'
;


SELECT 
	id1.IDENTITY_ID 'MRN',
       id1.PAT_ID,
       p.PAT_NAME 'Patient',
       ser.PROV_NAME 'Care Team PCP',
       ct.RN 'Care Team RN',
       ct.PharmD 'Care Team Pharmacist',
       ct.Dentist 'Care Team Dentist',
       ct.MCM 'Care Team Case Manager',
       ct.[MH Provider] 'Care Team MH Provider',
       COALESCE(emp.NAME, 'External') 'Patient Registrar',
       CAST(p.REC_CREATE_DATE AS DATE) 'Registration Date',
       loc.LOC_NAME 'Primary Location',
       CASE WHEN ct.RN IS NULL THEN 'No'
           WHEN ct.PharmD IS NULL THEN 'No'
           WHEN ct.Dentist IS NULL THEN 'No'
           WHEN ct.MCM IS NULL THEN 'No'
           WHEN ct.[MH Provider] IS NULL THEN 'No'
           ELSE 'Yes'
       END AS 'Complete Care Team',
       COALESCE(CAST(asmnt.ASSESSMENT_DATE AS DATE), '1901-01-01') 'Last Assessment if Less than 19 Months Old', --This is using the chief comp visit, not smartphrase
       COALESCE(DATEDIFF(DAY, asmnt.ASSESSMENT_DATE, GETDATE()), -1) 'Days Since Assessment',--This is using the chief comp visit, not smartphrase
	   COALESCE(DATEDIFF(DAY, clinical_med_rec.LOG_TIMESTAMP, GETDATE()), -1) 'Days Since Pharmacy Assessment',--This is using the smartphrase
       CAST(sbirt.LAST_SBIRT_DATE AS DATE) 'Last SBIRT',
       COALESCE(DATEDIFF(MONTH, sbirt.LAST_SBIRT_DATE, GETDATE()), -1) 'Months Since SBIRT',
       COALESCE(compvis.[Completed Clinical Visit], 'N') AS 'Completed Clinical Visit',
       COALESCE(compphone.[Patient Phone Contact], 'N') AS 'Completed Patient Phone Contact',
       COALESCE(rfl.Refill, 'N') AS 'Refill',
       COALESCE(ref.Referral, 'N') AS 'Referral',
       COALESCE(interim.Interim, 'N') AS 'Interim Note',
       --,COALESCE(ccn.[Care Coord Note], 'N') AS 'Care Coordination Note'
       --,COALESCE(myc.[MyCHart Message], 'N') AS 'MyChart Message'
       ins.PLAN_ID,
       ins.BENEFIT_PLAN_NAME,
       ins.PAYOR_ID,
       ins.PAYOR_NAME,
       CAST(ins.CVG_EFF_DT AS DATE) AS CVG_EFF_DT,
       CAST(ins.CVG_TERM_DT AS DATE) AS CVG_TERM_DT,
       LEFT(GETDATE(), 11) 'Today',
       COALESCE(ccn.[Care Coord Note], 'N') 'Care Coord Note',
       COALESCE(myc.[MyCHart Message], 'N') 'MyChart Message',
       CAST(cp.LOG_TIMESTAMP AS DATE) AS 'Last Care Plan',
       cp.[SMART PHRASE USER] 'Care Plan Creator',
       COALESCE(cp.[Months Since Care Plan], -1) AS 'Mos Since Care Plan',
       nmd.PROV_NAME 'Next Medical Visit Provider',
       CAST(nmd.[Next Medical Visit] AS DATE) AS 'Next Medical Visit',
       CASE WHEN fyi.ACTIVE_CP_COHORT = 'Y' THEN 'Y'
           ELSE 'N'
       END AS 'CLINICAL PHARMACY COHORT',
       CASE WHEN wai.PAT_ID IS NOT NULL THEN 'Yes'
           ELSE 'No'
       END AS WAI_SMARTPHRASE_USED,
       COALESCE(wai.LOG_TIMESTAMP, '1901-01-01') AS WAI_SMARTPHRASE_USED_DATE,
       COALESCE(wai.MONTHS_SINCE_SA64WAI, -1) AS MONTHS_SINCE_SA64WAI,
       next_pcp_appt_info.[Next PCP Appt],
       next_pcp_appt_info.[Next PCP Appt Prov],
       COALESCE(CAST(clinical_med_rec.LOG_TIMESTAMP AS DATE), '1901-01-01') AS [Last Pharmacy Assessment Date], --This is using the last Smartphrase, not Chief Comp
       lvi.STATE,
       lvi.CITY,
       comprehensive_med_review.ENCOUNTER_DATE AS COMP_MED_REVIEW_DATE,
       comprehensive_med_review.ENCOUNTER_TYPE AS COMP_MED_REVIEW_ENCOUNTER_TYPE,
       comprehensive_med_review.SMARTPHRASE_NAME AS COMP_MED_REVIEW_SMARTPHRASE_USE,
       comprehensive_med_review.SMART_PHRASE_LOG_DATE AS COMP_MED_REVIEW_SMARTPHRASE_USE_DATE--This is using the last Smartphrase, not Chief Comp

FROM Clarity.dbo.IDENTITY_ID_VIEW id1
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id1.PAT_ID
    INNER JOIN #last_visit_info lvi ON p.PAT_ID = lvi.PAT_ID
                                   AND lvi.RN_DESC = 1
    LEFT JOIN Clarity.dbo.CLARITY_EMP_VIEW emp ON p.CREATE_USER_ID = emp.USER_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    LEFT JOIN Clarity.dbo.CLARITY_LOC loc ON p.CUR_PRIM_LOC_ID = loc.LOC_ID
    LEFT JOIN (SELECT ct.PAT_ID,
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C = '3' THEN ser.PROV_NAME END) AS RN,
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C = '102' THEN ser.PROV_NAME END) AS PharmD,
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C = '108' THEN ser.PROV_NAME END) AS Dentist,
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C = '157'
                                    AND ct.SPECIALTY_C = '413' --Care Coordinator
                                    AND ct.RELATIONSHIP_C = '1010' --Case Manager/Care Coordinator
                      THEN         ser.PROV_NAME
                          END) AS MCM,
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C IN ( '171', '136', '117', '134', '10', '164', '110' ) THEN ser.PROV_NAME
                          END) AS 'MH Provider'
               FROM 
					Clarity.dbo.PAT_PCP_VIEW ct
                    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
               WHERE 
					ct.TERM_DATE IS NULL
                    AND ct.DELETED_YN = 'N'
               GROUP BY ct.PAT_ID) ct ON ct.PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT perv.PAT_ID,
                      ROW_NUMBER() OVER (PARTITION BY perv.PAT_ID ORDER BY perv.CONTACT_DATE DESC) AS ROW_NUM_DESC,
                      perv.CONTACT_DATE AS 'ASSESSMENT_DATE'

               FROM Clarity.dbo.PAT_ENC_RSN_VISIT_VIEW perv

               WHERE perv.ENC_REASON_ID = 1237 --CARE MGMT - PRELIMINARY ASSESSMENT
                     AND perv.CONTACT_DATE > DATEADD(MONTH, -19, GETDATE())) asmnt ON asmnt.PAT_ID = id1.PAT_ID
                                                                                      AND asmnt.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT pev.PAT_ID,
                      MAX('Y') AS 'Completed Clinical Visit'

               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID

               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND YEAR(pev.CONTACT_DATE) = YEAR(GETDATE())
                     AND MONTH(pev.CONTACT_DATE) = MONTH(GETDATE())

               GROUP BY pev.PAT_ID) compvis ON compvis.PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT c.CONTACT_PAT_ID,
                      MAX('Y') AS 'Patient Phone Contact'

               FROM Clarity.dbo.CAL_COMM_TRACKING c

               WHERE c.CALL_OUTCOME_C = 1029 --Spoke w pt
                     AND c.COMM_TYPE_C = 4 -- Phone
                     AND YEAR(c.COMM_INSTANT_DTTM) = YEAR(GETDATE())
                     AND MONTH(c.COMM_INSTANT_DTTM) = MONTH(GETDATE())

               GROUP BY c.CONTACT_PAT_ID) compphone ON compphone.CONTACT_PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT omv.PAT_ID,
                      MAX('Y') 'Refill'

               FROM Clarity.dbo.ORDER_MED_VIEW omv
                   INNER JOIN Clarity.dbo.ORDER_DISP_INFO_VIEW odiv ON odiv.ORDER_MED_ID = omv.ORDER_MED_ID

               WHERE odiv.ORD_CNTCT_TYPE_C = 11 --Dispensed
                     AND YEAR(odiv.CONTACT_DATE) = YEAR(GETDATE())
                     AND MONTH(odiv.CONTACT_DATE) = MONTH(GETDATE())

               GROUP BY omv.PAT_ID

               UNION

               SELECT omv.PAT_ID,
                      MAX('Y') 'Refill'

               FROM Clarity.dbo.ORDER_MED_VIEW omv

               WHERE YEAR(omv.ORDERING_DATE) = YEAR(GETDATE())
                     AND MONTH(omv.ORDERING_DATE) = MONTH(GETDATE())
                     AND omv.ORDER_STATUS_C = 2 -- Sent

               GROUP BY omv.PAT_ID) rfl ON rfl.PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT rv.PAT_ID,
                      MAX('Y') AS 'Referral'

               FROM Clarity.dbo.REFERRAL_VIEW rv

               WHERE YEAR(rv.ENTRY_DATE) = YEAR(GETDATE())
                     AND MONTH(rv.ENTRY_DATE) = MONTH(GETDATE())
                     AND rv.RFL_STATUS_C <> 4 -- Canceled

               GROUP BY rv.PAT_ID) ref ON ref.PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT pev2.PAT_ID,
                      MAX('Y') AS 'Interim'

               FROM Clarity.dbo.PAT_ENC_VIEW pev2

               WHERE pev2.ENC_TYPE_C IN ( '1003', '1015', '1017', '1031', '2019' )
                     AND YEAR(pev2.CONTACT_DATE) = YEAR(GETDATE())
                     AND MONTH(pev2.CONTACT_DATE) = MONTH(GETDATE())

               GROUP BY pev2.PAT_ID) interim ON interim.PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT tdl.INT_PAT_ID AS PAT_ID,
                      tdl.ORIG_SERVICE_DATE AS LAST_SBIRT_DATE,
                      ROW_NUMBER() OVER (PARTITION BY tdl.INT_PAT_ID ORDER BY tdl.ORIG_SERVICE_DATE DESC) AS ROW_NUM_DESC

               FROM Clarity.dbo.CLARITY_TDL_TRAN_64_VIEW tdl

               WHERE tdl.CPT_CODE IN ( 'H0049', 'H0050', 'G0396', 'G0397', '99408', '99409' )
                     AND tdl.DETAIL_TYPE IN ( 1, 20, 21 )
                     AND tdl.ORIG_SERVICE_DATE > DATEADD(MONTH, -36, GETDATE())) sbirt ON sbirt.PAT_ID = id1.PAT_ID
                                                                                          AND sbirt.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT hiv.PAT_ID,
                      MAX('Y') 'Care Coord Note'

               FROM Clarity.dbo.CARE_COORDINATION_VIEW ccv
                   INNER JOIN Clarity.dbo.HNO_INFO_VIEW hiv ON ccv.CARE_COORD_NOTE_ID = hiv.NOTE_ID

               WHERE YEAR(hiv.UPDATE_DATE) = YEAR(GETDATE())
                     AND MONTH(hiv.UPDATE_DATE) = MONTH(GETDATE())

               GROUP BY hiv.PAT_ID) ccn ON ccn.PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT mmv.PAT_ID,
                      MAX('Y') 'MyCHart Message'

               FROM Clarity.dbo.MYC_MESG_VIEW mmv

               WHERE YEAR(mmv.CREATED_TIME) = YEAR(GETDATE())
                     AND MONTH(mmv.CREATED_TIME) = MONTH(GETDATE())
                     AND mmv.MYC_MSG_TYP_C <> 999

               GROUP BY mmv.PAT_ID) myc ON myc.PAT_ID = id1.PAT_ID
    INNER JOIN (SELECT DISTINCT --Has WI Medicaid, not incl FP-only
                       pcfo.PAT_ID,
                       cvg.PLAN_ID,
                       epp.BENEFIT_PLAN_NAME,
                       cvg.PAYOR_ID,
                       epm.PAYOR_NAME,
                       cvg.CVG_EFF_DT,
                       cvg.CVG_TERM_DT

                FROM Clarity.dbo.PAT_CVG_FILE_ORDER_VIEW pcfo
                    INNER JOIN Clarity.dbo.PAT_ACCT_CVG_VIEW patcvg ON patcvg.PAT_ID = pcfo.PAT_ID
                    INNER JOIN Clarity.dbo.ACCOUNT_VIEW acct ON acct.[ACCOUNT_ID] = patcvg.[ACCOUNT_ID]
                    INNER JOIN Clarity.dbo.COVERAGE_MEM_LIST cml ON pcfo.COVERAGE_ID = cml.COVERAGE_ID
                    INNER JOIN Clarity.dbo.COVERAGE cvg ON cml.COVERAGE_ID = cvg.COVERAGE_ID
                    INNER JOIN Clarity.dbo.CLARITY_EPP epp ON cvg.PLAN_ID = epp.BENEFIT_PLAN_ID
                    LEFT JOIN Clarity.dbo.CLARITY_EPM epm ON epp.PAYOR_ID = epm.PAYOR_ID

                WHERE patcvg.ACCOUNT_ACTIVE_YN = 'Y' ---Active accounts only
                      AND acct.ACCOUNT_TYPE_C = 1 --Personal/Family
                      AND acct.SERV_AREA_ID = 64 -- Only SA64 Accounts
                      AND (cvg.CVG_TERM_DT IS NULL OR cvg.CVG_TERM_DT > DATEADD(MONTH, -1, GETDATE()))
                      AND epm.RPT_GRP_SEVEN IN ( '1', '2' ) -- I took this from Guma's code. I think this means "Medicaid"
                      AND cvg.CVG_EFF_DT IS NOT NULL
                      AND cvg.PLAN_ID NOT IN ( 2043 /*Community Care Family Plan [COMMUNITY CARE FAMILY CARE PLAN]*/, 1480, /*Family Planning WI*/
                                               2093, /*QMB Medicaid WI*/ 2092 /*SLMB Medicaid WI*/, 363419, 544505, 363406, 363401, 363405, 696801, 471001,
                                               2075, 2074, 2076, 2077, 1983, 544503, 2070, 4259001, 395, 525201, 503901, 490701, 2078, 2068, 2116, 425001,
                                               725901, 1957, 2071, 743601, 2203, 734001, 493401, 2204, 363407, 363503, 363408, 725401, 2072, 544506, 590501,
                                               2072, 550001, 451302, 451301, 2181, 744401, 532101,                          /*OO State*/
                                               592901, 567601, 549001, 570101, 724501, 582201, 569901, 721201, 593301, 711501, 707901, 743501, 493301, 711401,
                                               590401, 696301, 697601, 711201, 744601, 1869, 593201, 607101, 593401, 418001, 1723, 1747, 544401, 713702,
                                               556701, 713701, 324901, 424301, 434501, 438801, 1690, 732701, 749401, 731, 578501, 422001, 417901,
                                               541101, /*Family Care*/ 497201, 589801 )) ins ON ins.PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT id.IDENTITY_ID,
                      p.PAT_NAME,
                      dep.DEPARTMENT_NAME,
                      cs.SMARTPHRASE_NAME,
                      cs.SMARTPHRASE_ID,
                      ce.NAME 'SMART PHRASE USER',
                      sl.LOG_TIMESTAMP,
                      DATEDIFF(MONTH, sl.LOG_TIMESTAMP, GETDATE()) 'Months Since Care Plan',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY sl.LOG_TIMESTAMP DESC) AS ROW_NUM_DESC

               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
                   INNER JOIN Clarity.dbo.SMARTTOOL_LOGGER_VIEW sl ON sl.CSN = pev.PAT_ENC_CSN_ID
                   INNER JOIN Clarity.dbo.CLARITY_EMP ce ON sl.USER_ID = ce.USER_ID
                   INNER JOIN Clarity.dbo.CL_SPHR cs ON sl.SMARTPHRASE_ID = cs.SMARTPHRASE_ID

               WHERE dep.SERV_AREA_ID = 64
                     AND sl.LOG_TIMESTAMP > DATEADD(MONTH, -12, GETDATE())
                     AND cs.SMARTPHRASE_NAME LIKE '%VHCAREPLAN%') cp ON cp.IDENTITY_ID = id1.IDENTITY_ID
                                                                        AND cp.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT id.IDENTITY_ID MRN,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) 'Next Medical Visit',
                      ser.PROV_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC

               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID

               WHERE pev.APPT_STATUS_C = 1
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                     AND ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs, and NPs
    ) nmd ON nmd.MRN = id1.IDENTITY_ID
             AND nmd.ROW_NUM_ASC = 1
    LEFT JOIN (
              /* To get active pts in Clinical Pharmacy Cohorts */
              SELECT flag.PATIENT_ID PAT_ID,
                     MAX(CASE WHEN f.NAME IS NOT NULL THEN 'Y' END) AS 'ACTIVE_CP_COHORT'

              FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                  INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI f ON flag.PAT_FLAG_TYPE_C = f.BPA_TRIGGER_FYI_C

              WHERE
				f.name LIKE 'SA64 Pharmacist%'
				AND flag.ACTIVE_C = 1 -- Only currently actives

              GROUP BY flag.PATIENT_ID) fyi ON fyi.PAT_ID = id1.PAT_ID
    LEFT JOIN (SELECT flag.PATIENT_ID PAT_ID,
                      CASE WHEN flag.PAT_FLAG_TYPE_C = '640000012' THEN 'Ineligible as enrolled in other Care Coordination services'
                          WHEN flag.PAT_FLAG_TYPE_C = '640010' THEN 'No Medical 2ndry DX'
                          WHEN flag.PAT_FLAG_TYPE_C = '640009' THEN 'Opt out Medical Home'
                      END AS MEDICAL_HOME_FLAG_NAME,
                      CASE WHEN flag.ACTIVE_C = 1 THEN 'ACTIVE'
                          WHEN flag.ACTIVE_C = 2 THEN 'INACTIVE'
                      END AS MEDICAL_HOME_CURRENT_FLAG_STATUS,
                      CLARITY_EMP.NAME AS MEDICAL_HOME_FLAG_ENTERED_BY,
                      flag.LAST_UPDATE_INST AS MEDICAL_HOME_FLAG_LAST_UPDATE_DATETIME,
                      ROW_NUMBER() OVER (PARTITION BY flag.PATIENT_ID ORDER BY flag.LAST_UPDATE_INST DESC) AS ROW_NUM_DESC

               FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                   INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI f ON flag.PAT_FLAG_TYPE_C = f.BPA_TRIGGER_FYI_C
                   INNER JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON flag.ENTRY_PERSON_ID = CLARITY_EMP.USER_ID

               WHERE flag.PAT_FLAG_TYPE_C IN ( '640000012', '640010', '640009' )) medical_home_flags ON id1.PAT_ID = medical_home_flags.PAT_ID
                                                                                                        AND medical_home_flags.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT PAT_ENC.PAT_ID,
                      SMARTTOOL_LOGGER.LOG_TIMESTAMP,
                      DATEDIFF(MONTH, CAST(SMARTTOOL_LOGGER.LOG_DATE AS DATE), CURRENT_TIMESTAMP) AS MONTHS_SINCE_SA64WAI,
                      ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY SMARTTOOL_LOGGER.LOG_TIMESTAMP DESC) AS ROW_NUM_DESC

               FROM Clarity.dbo.SMARTTOOL_LOGGER_VIEW AS SMARTTOOL_LOGGER
                   INNER JOIN Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC ON SMARTTOOL_LOGGER.CSN = PAT_ENC.PAT_ENC_CSN_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON PAT_ENC.PAT_ID = IDENTITY_ID.PAT_ID
                   INNER JOIN Clarity.dbo.CL_SPHR AS CL_SPHR ON SMARTTOOL_LOGGER.SMARTPHRASE_ID = CL_SPHR.SMARTPHRASE_ID

               WHERE CL_SPHR.SMARTPHRASE_ID = 1560171 -- SmartPhrase Name = SA64WAI
    ) wai ON id1.PAT_ID = wai.PAT_ID
             AND wai.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT pev.PAT_ID,
                      CAST(pev.CONTACT_DATE AS DATE) AS 'Next PCP Appt',
                      ser.PROV_NAME 'Next PCP Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC

               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID

               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND ser.PROV_ID <> '640178' --pulmonologist
                     AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs
                     AND CAST(pev.CONTACT_DATE AS DATE) >= GETDATE()) next_pcp_appt_info ON id1.PAT_ID = next_pcp_appt_info.PAT_ID
                                                                                            AND next_pcp_appt_info.ROW_NUM_ASC = 1
    LEFT JOIN 
			(SELECT PAT_ENC.PAT_ID,
                      SMARTTOOL_LOGGER.LOG_TIMESTAMP,
                      ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY SMARTTOOL_LOGGER.LOG_TIMESTAMP DESC) AS ROW_NUM_DESC
               
			   FROM Clarity.dbo.SMARTTOOL_LOGGER_VIEW AS SMARTTOOL_LOGGER
                   INNER JOIN Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC ON SMARTTOOL_LOGGER.CSN = PAT_ENC.PAT_ENC_CSN_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON PAT_ENC.PAT_ID = IDENTITY_ID.PAT_ID
                   INNER JOIN Clarity.dbo.CL_SPHR AS CL_SPHR ON SMARTTOOL_LOGGER.SMARTPHRASE_ID = CL_SPHR.SMARTPHRASE_ID
              
			   WHERE 
					CL_SPHR.SMARTPHRASE_ID = 568339 -- SMARTPHRASE_NAME = CLINICALMEDREC
				) clinical_med_rec ON id1.PAT_ID = clinical_med_rec.PAT_ID
                          AND clinical_med_rec.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT PATIENT.PAT_ID,
                      CAST(PAT_ENC_RSN_VISIT.CONTACT_DATE AS DATE) AS ENCOUNTER_DATE,
                      ZC_DISP_ENC_TYPE.NAME AS ENCOUNTER_TYPE,
                      CASE WHEN CL_SPHR.SMARTPHRASE_ID = 568339 THEN CL_SPHR.SMARTPHRASE_NAME
                          ELSE NULL
                      END AS SMARTPHRASE_NAME,
                      CASE WHEN CL_SPHR.SMARTPHRASE_ID = 568339 THEN  CAST(SMARTTOOL_LOGGER.LOG_TIMESTAMP AS DATE) 
					  END AS SMART_PHRASE_LOG_DATE,
                      ROW_NUMBER() OVER (PARTITION BY PATIENT.PAT_ID ORDER BY PAT_ENC_RSN_VISIT.CONTACT_DATE DESC) AS ROW_NUM_DESC
               
			   FROM CLARITY.dbo.PAT_ENC_RSN_VISIT_VIEW AS PAT_ENC_RSN_VISIT
                   INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON PAT_ENC_RSN_VISIT.PAT_ID = PATIENT.PAT_ID
                   INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON PAT_ENC_RSN_VISIT.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID
                   INNER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE AS ZC_DISP_ENC_TYPE ON PAT_ENC.ENC_TYPE_C = ZC_DISP_ENC_TYPE.DISP_ENC_TYPE_C
                   LEFT JOIN CLARITY.dbo.SMARTTOOL_LOGGER_VIEW AS SMARTTOOL_LOGGER ON PAT_ENC_RSN_VISIT.PAT_ENC_CSN_ID = SMARTTOOL_LOGGER.CSN
                   LEFT JOIN CLARITY.dbo.CL_SPHR AS CL_SPHR ON SMARTTOOL_LOGGER.SMARTPHRASE_ID = CL_SPHR.SMARTPHRASE_ID
               
			   WHERE 
					(PAT_ENC_RSN_VISIT.ENC_REASON_ID = 1419 OR CL_SPHR.SMARTPHRASE_ID = 568339)
				) comprehensive_med_review ON id1.PAT_ID = comprehensive_med_review.PAT_ID
                                              AND comprehensive_med_review.ROW_NUM_DESC = 1
---------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE ser.SERV_AREA_ID = 64
      AND id1.PAT_ID NOT IN ( SELECT DISTINCT --No HIV- flag
									f.PATIENT_ID

                              FROM 
									Clarity.dbo.PATIENT_FYI_FLAGS_VIEW f

                              WHERE 
									f.ACTIVE_C = 1
                                    AND f.PAT_FLAG_TYPE_C IN ( '640005', '640008', '640007', '640009', '640010' )
								)
      AND id1.PAT_ID IN ( SELECT DISTINCT--Wi Medical Visit
								pev1.PAT_ID

                          FROM 
								Clarity.dbo.PAT_ENC_VIEW pev1
								INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev1.DEPARTMENT_ID

                          WHERE 
								pev1.APPT_STATUS_C IN ( 2, 6 )
                                AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                                AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'WI'
						)
;
DROP TABLE #last_visit_info