/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Medical - Patients with Diabetes and MicroAlb
 Create Date: 3/18/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  Adam C

 Purpose:   Active pts with a Dx of Diabetes, and whether they've had a MicroAlb in the last 12 months

 Description:
 
 BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
9/11/2022     Mitch       Removing patients who:
                      1. Are on an ACEi/ARB/ARNI
                      2. Have Diabetic Nephropathy
                      3. Are on Dialysis
11/2/2022     Mitch       Adding Pts on ACEi/ARB/ARNI back in per Leslie's 11/2/2022 email
**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
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
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
	   a1.SERVICE_TYPE,
	   a1.SERVICE_LINE, 
	   a1.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT a2.PAT_ID,
       a2.LOS,
       a2.CITY,
       a2.STATE,
	   a2.SERVICE_TYPE,
	   a2.SERVICE_LINE, 
	   a2.SUB_SERVICE_LINE
INTO #Attribution3
FROM #Attribution2 a2
WHERE a2.ROW_NUM_DESC = 1
      AND a2.PAT_ID IN ( SELECT DISTINCT pev.PAT_ID
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
                               AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
                               AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
                               AND plv.RESOLVED_DATE IS NULL --Active Dx
                               AND plv.PROBLEM_STATUS_C = 1 --Active Dx
                               AND p4.PAT_LIVING_STAT_C = 1 );

SELECT a3.PAT_ID,
       a3.LOS,
       a3.CITY,
       a3.STATE,
	   a3.SERVICE_TYPE,
	   a3.SERVICE_LINE, 
	   a3.SUB_SERVICE_LINE,	   
       dm.UR_MALB_LAST_DT 'Last Urine MicroAlb Date',
       DATEDIFF(MONTH, dm.UR_MALB_LAST_DT, GETDATE()) 'Months Ago',
       CASE WHEN DATEDIFF(MONTH, dm.UR_MALB_LAST_DT, GETDATE()) < 13 THEN 'Met'
           ELSE 'Not Met'
       END AS 'MET YN',
       CASE WHEN DATEDIFF(MONTH, dm.UR_MALB_LAST_DT, GETDATE()) < 13 THEN 1
           ELSE 0
       END AS 'MET NUMBER',
       id.IDENTITY_ID MRN,
       p.PAT_NAME 'Patient',
       ser.EXTERNAL_NAME 'PCP'
INTO #a
FROM #Attribution3 a3
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON a3.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON a3.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.DM_DIABETES_VIEW dm ON dm.PAT_ID = a3.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
WHERE a3.PAT_ID NOT IN ( SELECT DISTINCT plv.PAT_ID
                         FROM Clarity.dbo.PROBLEM_LIST_VIEW plv
                             INNER JOIN Clarity.dbo.CLARITY_EDG edg ON edg.DX_ID = plv.DX_ID
                             INNER JOIN Clarity.dbo.GROUPER_COMPILED_REC_LIST gc ON gc.GROUPER_RECORDS_NUMERIC_ID = edg.DX_ID
                             INNER JOIN Clarity.dbo.GROUPER_ITEMS gi ON gc.BASE_GROUPER_ID = gi.GROUPER_ID
                         WHERE gi.GROUPER_ID IN ( '107089' /*Diabetic Nephropathy*/, '5200000134' /*Dialysis*/ )
                               AND plv.RESOLVED_DATE IS NULL
                               AND plv.PROBLEM_STATUS_C = 1 );

-- Next ANY Appt
SELECT a.PAT_ID,
       a.LOS,
       a.CITY,
       a.STATE,
	   a.SERVICE_TYPE,
	   a.SERVICE_LINE, 
	   a.SUB_SERVICE_LINE,
       a.[Last Urine MicroAlb Date],
       a.[MET YN],
       a.[MET NUMBER],
       a.MRN,
       a.Patient,
       a.PCP,
       CAST(pev2.CONTACT_DATE AS DATE) NEXT_APPT,
       ser2.EXTERNAL_NAME NEXT_APPT_PROV,
       ROW_NUMBER() OVER (PARTITION BY a.PAT_ID ORDER BY pev2.CONTACT_DATE ASC) AS ROW_NUM_ASC,
       a.[Months Ago]
INTO #b
FROM #a a
    LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev2 ON a.PAT_ID = pev2.PAT_ID
                                               AND pev2.APPT_STATUS_C = 1
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser2 ON pev2.VISIT_PROV_ID = ser2.PROV_ID;

-- Next PCP Appt
SELECT b.PAT_ID,
       b.LOS,
       b.CITY 'City',
       b.STATE 'State',
	   b.SERVICE_TYPE 'Service Type',
	   b.SERVICE_LINE 'Service Line' , 
	   b.SUB_SERVICE_LINE 'Sub-Service Line',
       CAST(b.[Last Urine MicroAlb Date] AS DATE) 'Last Urine MicroAlb Date',
       COALESCE(b.[Months Ago], 0) AS 'Months Ago',
       CASE WHEN hm.PAT_ID IS NOT NULL THEN 'Met'
           ELSE b.[MET YN]
       END AS 'MET YN',
       CASE WHEN hm.PAT_ID IS NOT NULL THEN 1
           ELSE b.[MET NUMBER]
       END AS 'MET NUMBER',
       b.MRN,
       b.Patient,
       b.PCP,
       b.NEXT_APPT,
       b.NEXT_APPT_PROV,
       na.NEXT_PCP_APPT,
       na.EXTERNAL_NAME 'PCP APPT PROVIDER',
       MAX(CASE WHEN flag.PAT_FLAG_TYPE_C = '640013' THEN 'YES' ELSE 'NO' END) AS 'IN CLINICAL PHARMACY COHORT',
       MAX(CASE WHEN flag.PAT_FLAG_TYPE_C = '640018' THEN 'YES' ELSE 'NO' END) AS 'IN DIETITIAN CARE',
       zpr.NAME 'RACE',
       CASE WHEN zeg.NAME IS NULL THEN 'Unknown'
           WHEN zeg.NAME = '' THEN 'Unknown'
           WHEN zeg.NAME = 'Not Collected/Unknown' THEN 'Unknown'
           WHEN zeg.NAME = 'Patient Refused' THEN 'Unknown'
           ELSE zeg.NAME
       END AS ETHNICITY
FROM #b b
    LEFT JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag ON b.PAT_ID = flag.PATIENT_ID
                                                         AND flag.PAT_FLAG_TYPE_C IN ( '640013', '640018' ) --Diabetic AND pre-DM cohort
                                                         AND flag.ACTIVE_C = 1
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON pr.PAT_ID = b.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON zpr.PATIENT_RACE_C = pr.PATIENT_RACE_C
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = b.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_ETHNIC_GROUP zeg ON zeg.ETHNIC_GROUP_C = p.ETHNIC_GROUP_C
    LEFT JOIN (SELECT TOP 100000000 ----To get pts who have satisfied the HM
                      hmh.PAT_ID
               FROM Clarity.dbo.HM_HISTORY_VIEW hmh
               WHERE HMH.HM_TOPIC_ID = 36 --Diabetes Microalbumin
                     AND HMH.HM_TYPE_C IN ( 1, 4 ) --  HMO_STATUS_ID of 'Done'                  
                     AND HMH.HM_HX_DATE > DATEADD(MONTH, -13, GETDATE())) hm ON hm.PAT_ID = b.PAT_ID
    LEFT JOIN (SELECT TOP 100000000 pev.PAT_ID,
                                    CAST(pev.CONTACT_DATE AS DATE) NEXT_PCP_APPT,
                                    ser.EXTERNAL_NAME,
                                    ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1
                     AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
                     AND ser.PROV_ID <> '640178' --pulmonologist
    ) na ON b.PAT_ID = na.PAT_ID
            AND na.ROW_NUM_ASC = 1
WHERE b.ROW_NUM_ASC = 1
GROUP BY b.PAT_ID,
         b.PAT_ID,
         b.LOS,
         b.CITY,
         b.STATE,
		 b.SERVICE_TYPE,
	     b.SERVICE_LINE, 
	     b.SUB_SERVICE_LINE,
         b.[Last Urine MicroAlb Date],
         b.[MET YN],
         b.[MET NUMBER],
         b.MRN,
         b.Patient,
         b.PCP,
         b.NEXT_APPT,
         b.NEXT_APPT_PROV,
         na.NEXT_PCP_APPT,
         na.NEXT_PCP_APPT,
         na.EXTERNAL_NAME,
         zpr.NAME,
         zeg.NAME,
         b.[Months Ago],
         hm.PAT_ID;

DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;