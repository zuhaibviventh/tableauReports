/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Medical 7 - Females screened for cervical cancer in the past 3 years
 Create Date:	8/23/2018
 Created By:	scogginsm
 System:		javelin.ochin.org
 Requested By:	Internal dashboard

 Purpose:		Female patients 21 - 65 years of age at the beginning of the measurement period and no history of hysterectomy.

 Description:	Met = had a pap within the past 3 years
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
01/17/2019			Jaya				Included MO
4/15/2019			Mitch				Updating Active Patient logic to include Dx codes and LOS_PRIME_PROC_ID 
8/2/2019			Mitch				Update for living pt check in the PATIENT_4 table
10/15/2019			Mitch				Updating per https://ella.ochin.org/moodle/mod/glossary/showentry.php?courseid=2&eid=1433&displayformat=dictionary to add more HM Topics
10/24/2019			Mitch				Updated to add the PATIENT_HMT_STATUS_VIEW table
02/24/2020			Jaya				Updated to new Department name logic
5/8/2020			Jaya				Updated sex_at_birth data
11/5/2020			Mitch				Updating from Component IDs to Common Names
02/02/2021			Jaya				Added PA to the Provider_Type_C
3/17/2021			Mitch				Updating for Alteryx
3/17/2021			Mitch				Using Wellness Registries instead of Health Maint

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#Attribution1') IS NOT NULL DROP TABLE #Attribution1;
SELECT pev.PAT_ID,
       pev.DEPARTMENT_ID,
       dep.DEPARTMENT_NAME LAST_VISIT_DEPT,
       pev.PAT_ENC_CSN_ID LAST_VISIT_ID,
       dep.STATE,
       dep.CITY,
       dep.SITE,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS 'LOS',
	   dep.SERVICE_TYPE,
	   dep.SERVICE_LINE, 
	   dep.SUB_SERVICE_LINE
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );


IF OBJECT_ID('tempdb..#Attribution2') IS NOT NULL DROP TABLE #Attribution2;
SELECT a1.PAT_ID,
       a1.LAST_VISIT_ID,
       a1.LOS,
       a1.CITY,
       a1.STATE,
	   a1.SERVICE_TYPE,
	   a1.SERVICE_LINE, 
	   a1.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_VISIT_ID DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL'
      AND a1.PAT_ID IN ( SELECT DISTINCT pev.PAT_ID
                         FROM Clarity.dbo.PATIENT_VIEW p
                             INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
                             INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
                             INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
                             INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                             INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                             INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                             INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                         WHERE ser.SERV_AREA_ID = 64
                               AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
                               AND pev.CONTACT_DATE > DATEADD(MM, -12, GETDATE()) --Visit in past year
                               AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
                               AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048,
                                                              8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
                               AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                               AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
                               AND plv.RESOLVED_DATE IS NULL --Active Dx
                               AND plv.PROBLEM_STATUS_C = 1 --Active Dx
                               AND p4.PAT_LIVING_STAT_C = 1 );


IF OBJECT_ID('tempdb..#HM') IS NOT NULL DROP TABLE #HM;
SELECT f13.PAT_ID,
       f13.CERV_CAN_SCRN_HM_C
INTO #HM
FROM Clarity.dbo.DM_WLL_F_13_29_VIEW f13
WHERE f13.AGE > 20
      AND f13.HAS_CERVIX_2_YN = 'Y'
UNION
SELECT f49.PAT_ID, f49.CERV_CAN_SCRN_HM_C FROM Clarity.dbo.DM_WLL_F_30_49_VIEW f49 WHERE f49.HAS_CERVIX_2_YN = 'Y'
UNION
SELECT f69.PAT_ID,
       f69.CERV_CAN_SCRN_HM_C
FROM Clarity.dbo.DM_WLL_F_50_69_VIEW f69
WHERE f69.AGE < 65
      AND f69.HAS_CERVIX_2_YN = 'Y';

SELECT id.IDENTITY_ID,
       p.PAT_NAME,
       CASE WHEN hm.CERV_CAN_SCRN_HM_C = 12 THEN 1 --11=Yes is due, 12= No is not due, 13 = NA (ZC_NO_YES_NA)
           ELSE 0
       END AS PAP,
       CASE WHEN hm.CERV_CAN_SCRN_HM_C = 12 THEN 'Met' --11=Yes, 12= No, 13 = NA
           ELSE 'Not Met'
       END AS MET_YN,
       a2.STATE,
       a2.CITY,
	   a2.SERVICE_TYPE 'Service Type',
	   a2.SERVICE_LINE 'Service Line', 
	   a2.SUB_SERVICE_LINE 'Sub-Service Line',
       ser.PROV_NAME PCP,
       CAST(na.NEXT_PCP_APPT AS DATE) AS 'NEXT PCP APPT',
       na.EXTERNAL_NAME 'PCP APPT PROVIDER',
       svis.[Next Any Appt],
       svis.[Next Appt Prov],
       COALESCE(zgi.NAME, 'Not Recorded') 'GENDER IDENTITY',
       COALESCE(cp.[Any CP Cohort], 'N') 'Enrolled in Any CP Cohort',
       patient_race_ethnicity.RACE_CATEGORY,
       patient_race_ethnicity.ETHNICITY_CATEGORY,
       GETDATE() AS UPDATED_DTTM
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p4.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_GENDER_IDENTITY zgi ON zgi.GENDER_IDENTITY_C = p4.GENDER_IDENTITY_C
    INNER JOIN #HM hm ON p.PAT_ID = hm.PAT_ID
    INNER JOIN #Attribution2 a2 ON p.PAT_ID = a2.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
    LEFT JOIN (SELECT pev.PAT_ID,
                      pev.CONTACT_DATE NEXT_PCP_APPT,
                      ser.EXTERNAL_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1
                     AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
                     AND ser.PROV_ID <> '640178' --pulmonologist
    ) na ON a2.PAT_ID = na.PAT_ID
            AND na.ROW_NUM_ASC = 1
    LEFT JOIN (SELECT pev.PAT_ID,
                      CAST(pev.CONTACT_DATE AS DATE) AS 'Next Any Appt',
                      ser.PROV_NAME 'Next Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled

    ) svis ON svis.PAT_ID = id.PAT_ID
              AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT flag.PATIENT_ID AS PAT_ID,
                      MAX(CASE WHEN f.name LIKE 'SA64 Pharmacist%' THEN 'Y' END) AS 'Any CP Cohort',
                      MAX(CASE WHEN f.name = 'SA64 Pharmacist - HTN' THEN 'Y' END) AS 'CP HTN Cohort',
                      MAX(CASE WHEN flag.PAT_FLAG_TYPE_C = '640025' THEN 'Y' END) AS 'Dietitian Care',
                      MAX(CASE WHEN f.name = 'SA64 Pharmacist: Pre-DM' THEN 'Y' END) AS 'CP Pre-DM Cohort',
                      MAX(CASE WHEN f.name = 'SA64 Pharmacist - Anticoagulation' THEN 'Y' END) AS 'CP AntiCoag Cohort',
                      MAX(CASE WHEN f.name = 'SA64 Pharmacist - DM' THEN 'Y' END) AS 'CP Dabetes Cohort',
                      MAX(CASE WHEN f.name = 'SA64 Pharmacist - Tobacco' THEN 'Y' END) AS 'CP Tobacco Cohort',
                      MAX(CASE WHEN f.name = 'SA64 Pharmacist - Miscellaneous' THEN 'Y' END) AS 'CP Misc Cohort',
                      MAX(CASE WHEN f.name = 'SA64 PHARMACIST- PrEP' THEN 'Y' END) AS 'CP PrEP Cohort'
               FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                   INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI f ON flag.PAT_FLAG_TYPE_C = f.BPA_TRIGGER_FYI_C
               WHERE flag.ACTIVE_C = 1
               GROUP BY flag.PATIENT_ID) cp ON id.PAT_ID = cp.PAT_ID
    LEFT JOIN ##patient_race_ethnicity AS patient_race_ethnicity ON id.PAT_ID = patient_race_ethnicity.PAT_ID
WHERE a2.ROW_NUM_DESC = 1
      AND hm.CERV_CAN_SCRN_HM_C IN ( 11, 12 ) --Exlcudes 13, N/A, from the Denominator -- Sent email to Adam on 9/11 asking if I should do this. 9/12 Adam said "Yes" remove

;
