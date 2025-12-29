/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Breast Cancer Screening Quality
 Create Date: 10/10/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  

 Purpose:   

 Description: https://ecqi.healthit.gov/ecqm/ec/2023/cms125v11
 

 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#bc') IS NOT NULL DROP TABLE #bc;
WITH
    bc_union AS (
        SELECT f50.PAT_ID,
               f50.AGE,
               f50.BREAST_CAN_SCRN_HM_C, --11=Yes is due, 12= No is not due, 13 = NA (ZC_NO_YES_NA)
               CASE WHEN f50.BREAST_CAN_SCRN_HM_C = 12 THEN 'Met' --11=Yes, 12= No, 13 = NA
                   ELSE 'Not Met'
               END AS MET_YN
        FROM Clarity.dbo.DM_WLL_F_50_69_VIEW f50
        WHERE f50.AGE > 51.0
              AND f50.BREAST_CAN_SCRN_HM_C <> 13 -- Exclusion
        UNION
        SELECT f70.PAT_ID,
               f70.AGE,
               f70.BREAST_CAN_SCRN_HM_C, --11=Yes is due, 12= No is not due, 13 = NA (ZC_NO_YES_NA)
               CASE WHEN f70.BREAST_CAN_SCRN_HM_C = 12 THEN 'Met' --11=Yes, 12= No, 13 = NA
                   ELSE 'Not Met'
               END AS MET_YN
        FROM Clarity.dbo.DM_WLL_F_70_VIEW f70
        WHERE f70.AGE < 75.0
              AND f70.BREAST_CAN_SCRN_HM_C <> 13 -- Exclusion
    )
SELECT bc_union.PAT_ID, bc_union.AGE, bc_union.BREAST_CAN_SCRN_HM_C, bc_union.MET_YN INTO #bc FROM bc_union;


IF OBJECT_ID('tempdb..#medical') IS NOT NULL DROP TABLE #medical;
WITH
    attribution AS (
        SELECT pev.PAT_ID,
               pev.CONTACT_DATE LAST_OFFICE_VISIT,
               dep.STATE,
               dep.CITY,
               CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
                   WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
                   ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2)
               END AS 'LOS',
			   dep.SERVICE_TYPE,
			   dep.SERVICE_LINE, 
			   dep.SUB_SERVICE_LINE
        FROM Clarity.dbo.PAT_ENC_VIEW pev
            INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
        WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
              AND pev.APPT_STATUS_C IN ( 2, 6 )
    )
SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
	   a1.SERVICE_TYPE,
	   a1.SERVICE_LINE, 
	   a1.SUB_SERVICE_LINE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #medical
FROM attribution a1
WHERE a1.LOS = 'MEDICAL';


IF OBJECT_ID('tempdb..#next_appt') IS NOT NULL DROP TABLE #next_appt;
WITH
    next_appt_proc AS (
        SELECT pev.PAT_ID,
               pev.CONTACT_DATE NEXT_PCP_APPT,
               ser.EXTERNAL_NAME,
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM Clarity.dbo.PAT_ENC_VIEW pev
            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
        WHERE pev.APPT_STATUS_C = 1
              AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
              AND ser.PROV_ID <> '640178' --pulmonologist
    )
SELECT next_appt_proc.PAT_ID,
       next_appt_proc.NEXT_PCP_APPT,
       next_appt_proc.EXTERNAL_NAME
INTO #next_appt
FROM next_appt_proc
WHERE next_appt_proc.ROW_NUM_ASC = 1;


IF OBJECT_ID('tempdb..#svis') IS NOT NULL DROP TABLE #svis;
WITH
    svis_proc AS (
        SELECT pev.PAT_ID,
               CAST(pev.CONTACT_DATE AS DATE) AS 'Next Any Appt',
               ser.PROV_NAME 'Next Appt Prov',
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM Clarity.dbo.PAT_ENC_VIEW pev
            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
        WHERE pev.APPT_STATUS_C = 1 --Scheduled
    )
SELECT svis_proc.PAT_ID,
       svis_proc.[Next Any Appt],
       svis_proc.[Next Appt Prov]
INTO #svis
FROM svis_proc
WHERE svis_proc.ROW_NUM_ASC = 1;


IF OBJECT_ID('tempdb..#hiv_patients') IS NOT NULL DROP TABLE #hiv_patients;
SELECT DISTINCT pev.PAT_ID
INTO #hiv_patients
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
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                     8053, 8054, 8055, 8056 ) -- Office Visits
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
      AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
      AND p4.PAT_LIVING_STAT_C = 1;


SELECT id.IDENTITY_ID 'MRN',
       p.PAT_NAME 'Patient',
       bc.AGE 'Age',
       bc.MET_YN 'Screened for Breast Cancer',
       att.STATE 'State',
       att.CITY 'City',
	   att.SERVICE_TYPE 'Service Type',
	   att.SERVICE_LINE 'Service Line', 
	   att.SUB_SERVICE_LINE 'Sub-Service Line',
       ser.EXTERNAL_NAME 'PCP',
       CAST(na.NEXT_PCP_APPT AS DATE) AS 'Next PCP Appt',
       na.EXTERNAL_NAME 'PCP Appt Provider',
       svis.[Next Any Appt],
       svis.[Next Appt Prov],
       p.ZIP,
       GETDATE() AS UPDATED_DTTM
FROM Clarity.dbo.IDENTITY_ID_VIEW id
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    INNER JOIN #hiv_patients ON id.PAT_ID = #hiv_patients.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN #bc AS bc ON bc.PAT_ID = id.PAT_ID
    INNER JOIN #medical att ON att.PAT_ID = id.PAT_ID
                               AND att.ROW_NUM_DESC = 1
    LEFT JOIN #next_appt na ON id.PAT_ID = na.PAT_ID
    LEFT JOIN #svis svis ON svis.PAT_ID = id.PAT_ID;

