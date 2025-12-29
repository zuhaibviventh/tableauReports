/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:   CM 01 - Risk Stratification
 Create Date:   3/6/2020
 Created By:    ARCW\mscoggins
 System:        SQL-MKE-DEV-001
 Requested By:  NCQA

 Purpose:       

 Description:

 Category   Condition   None    Column2 Column1
Behavorial Health Conditions    Psychiatric diagnosis   None    Depression, Anxiety Bipolar, Schizophrenia
        0   1   2
    Substance Use disorder  None    Tobacco Alcohol, Cocaine, Opioid, Meth, NOS
        0   1   2
High Utilization    Number of Meds  <4  4-6 >6
        0   1   2
Chronic Medical Conditions  HIV <200        >200
        0       2
    DM  None    <9  A1c>9
        0   1   2
    HTN None    >140/90 
        0   1   
    COPD    None    Yes 
        0   1   
    Heart/Vascular disease  None    CAD Peripheral Vascular or Arterial Disease
        0   1   2
    CKD None    CKD CKD stage 4 or HD
        0   1   3
    Obesity None    BMI >30 BMI = or > 40
        0   1   2
    Cognitive impairment    None    Mild Cognitive Impairment   Dementia
        0       2
Social Determinant of Health    Housing instability Steady place    Worried No steady place to live
        0   1   2
    Financial Resources Not Hard    Somewhat Hard   Very Hard
        0   1   2
    Transportation difficulty   No  Yes, non-medical issues Yes, medical issues
        0   1   2

Risk Category   Score
Healthy <3
Low -Risk   4-8
Rising-Risk 9-15
High-Risk   >15


 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:       Changed By:         Change Description:
 ------------       -------------       ---------------------------------------------------
 10/21/2022         Mitch               Adding Last and next visits for Clinical Pharmacists
 10/21/2022         Mitch               Adding Last and next visits for Dietitian/Nutritionist

**********************************************************************************************

 */

SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;
IF OBJECT_ID('tempdb..#Attribution1') IS NOT NULL									
DROP TABLE #Attribution1;
SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWUAKEE'
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
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
           ELSE 'ERROR'
       END AS CITY,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN' THEN 'MAIN LOCATION'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'DR' THEN 'D&R'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'KE' THEN 'KEENEN'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'UC' THEN 'UNIVERSITY OF COLORADO'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'ON' THEN 'AUSTIN MAIN'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'TW' THEN 'AUSTIN OTHER'
           ELSE 'ERROR'
       END AS 'SITE',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
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
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;

SELECT p.PAT_ID,
       CASE WHEN p4.SEX_ASGN_AT_BIRTH_C IN ( 1, 2 ) THEN zb.NAME
           WHEN p4.GENDER_IDENTITY_C IN ( 1, 4 ) THEN 'Female'
           WHEN p4.GENDER_IDENTITY_C IN ( 2, 3 ) THEN 'Male'
           WHEN (p4.GENDER_IDENTITY_C IS NULL OR p4.GENDER_IDENTITY_C NOT IN ( 1, 2, 3, 4 ))
                AND p.SEX_C IN ( 1, 5 ) THEN 'Female'
           WHEN (p4.GENDER_IDENTITY_C IS NULL OR p4.GENDER_IDENTITY_C NOT IN ( 1, 2, 3, 4 ))
                AND p.SEX_C IN ( 2, 4 ) THEN 'Male'
           ELSE 'UNKNOWN'
       END SEX_AT_BIRTH
INTO #SEX
FROM Clarity.dbo.PATIENT_4 p4
    LEFT JOIN Clarity.dbo.ZC_SEX_ASGN_AT_BIRTH zb ON zb.SEX_ASGN_AT_BIRTH_C = p4.SEX_ASGN_AT_BIRTH_C
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = p4.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p4.PAT_ID
WHERE id.IDENTITY_TYPE_ID = 64;

/*Patient Cohort*/
SELECT att.PAT_ID,
       id.IDENTITY_ID,
       p.PAT_NAME,
       p.BIRTH_DATE,
       sex.SEX_AT_BIRTH SEX,
       att.CITY,
       att.STATE,
       ser.EXTERNAL_NAME PCP
INTO #COHORT
FROM #Attribution3 att
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = att.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON att.PAT_ID = id.PAT_ID
    INNER JOIN #SEX sex ON att.PAT_ID = sex.PAT_ID
WHERE att.PAT_ID IN ( SELECT DISTINCT  pev.PAT_ID
                      FROM Clarity.dbo.PATIENT_VIEW p
                          INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
                          INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
                          INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
                          INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                          INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                          INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                          INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                      WHERE ser.SERV_AREA_ID = 64
                            AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
                            AND pev.CONTACT_DATE > DATEADD(MM, -12, CURRENT_TIMESTAMP) --Visit in past year
                            AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
                            AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048,
                                                           8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
                            AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
                            AND icd10.CODE IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
                            AND plv.RESOLVED_DATE IS NULL --Active Dx
                            AND plv.PROBLEM_STATUS_C = 1 --Active Dx
                            AND p4.PAT_LIVING_STAT_C = 1 );

/*Psych Disorders*/
SELECT c.PAT_ID,
       MAX(CASE WHEN icd10.CODE LIKE 'F31.%' --Bipolar, Schizophrenia
                     OR icd10.CODE = 'F20.9'
                     OR icd10.CODE = 'F20.81'
                     OR icd10.CODE LIKE 'F23.%'
                     OR icd10.CODE = 'F25.0'
                     OR icd10.CODE = 'F25.1'
                     OR icd10.CODE = 'F34.0'
                     OR icd10.CODE = 'F06.33'
                     OR icd10.CODE = 'F06.34'
                     OR icd10.CODE = 'F21'
                     OR icd10.CODE = 'F22'
                     OR icd10.CODE = 'F06.0'
                     OR icd10.CODE = 'F06.1'
                     OR icd10.CODE = 'F06.2'
                     OR icd10.CODE = 'F28'
                     OR icd10.CODE = 'F29' THEN 2
               ELSE 1
           END) AS 'Psychiatric Diagnosis',
       MIN(CASE WHEN icd10.CODE LIKE 'F31.%' --Bipolar, Schizophrenia
                     OR icd10.CODE = 'F20.9'
                     OR icd10.CODE = 'F20.81'
                     OR icd10.CODE LIKE 'F23.%'
                     OR icd10.CODE = 'F25.0'
                     OR icd10.CODE = 'F25.1'
                     OR icd10.CODE = 'F34.0'
                     OR icd10.CODE = 'F06.33'
                     OR icd10.CODE = 'F06.34'
                     OR icd10.CODE = 'F21'
                     OR icd10.CODE = 'F22'
                     OR icd10.CODE = 'F06.0'
                     OR icd10.CODE = 'F06.1'
                     OR icd10.CODE = 'F06.2'
                     OR icd10.CODE = 'F28'
                     OR icd10.CODE = 'F29' THEN 'Bipolar or Schizophrenia'
               ELSE 'Depression'
           END) AS 'Psychiatric Diagnosis Type'
INTO #PSYCHDX
FROM #COHORT c
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON c.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
WHERE icd10.CODE IN ( 'F32.9', 'F32.0', 'F32.1', 'F32.2', 'F32.3', 'F32.5', 'F33.9', 'F33.0', 'F33.1', 'F33.2', 'F33.3', 'F33.42', 'F32.89', 'F34.1', 'F32.4',
                      'F33.40', 'F33.41', 'F33.8', 'F32.81', 'F33.4', 'F32.8' ) --Depression
      OR icd10.CODE LIKE 'F31.%' --Bipolar, Schizophrenia
      OR icd10.CODE = 'F20.9'
      OR icd10.CODE = 'F20.81'
      OR icd10.CODE LIKE 'F23.%'
      OR icd10.CODE = 'F25.0'
      OR icd10.CODE = 'F25.1'
      OR icd10.CODE = 'F34.0'
      OR icd10.CODE = 'F06.33'
      OR icd10.CODE = 'F06.34'
      OR icd10.CODE = 'F21'
      OR icd10.CODE = 'F22'
      OR icd10.CODE = 'F06.0'
      OR icd10.CODE = 'F06.1'
      OR icd10.CODE = 'F06.2'
      OR icd10.CODE = 'F28'
      OR icd10.CODE = 'F29'
GROUP BY c.PAT_ID;

/*Substance Use, tobacco and AODA*/
SELECT c.PAT_ID,
       CASE WHEN sm.TOBACCO_USER_C = 1 THEN 1
           ELSE 0
       END AS TOBACCO_USER,
       'Tobacco' 'SUBSTANCE'
INTO #SMOKE
FROM #COHORT c
    INNER JOIN (SELECT pev.PAT_ID,
                       shv.TOBACCO_USER_C,
                       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
                FROM Clarity.dbo.PAT_ENC_VIEW pev
                    INNER JOIN Clarity.dbo.SOCIAL_HX_VIEW shv ON pev.PAT_ENC_CSN_ID = shv.HX_LNK_ENC_CSN
                WHERE shv.TOBACCO_USER_C <> 3 -- "Not Asked"
    ) sm ON sm.PAT_ID = c.PAT_ID
            AND sm.ROW_NUM_DESC = 1;

SELECT DISTINCT c.PAT_ID,
                2 'Substance Use Disorder',
                'AODA' 'SUBSTANCE'
INTO #SUD
FROM #COHORT c
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON plv.PAT_ID = c.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd ON edg.DX_ID = icd.DX_ID
WHERE (icd.CODE LIKE 'F10%'
       OR icd.CODE LIKE 'F11%'
       OR icd.CODE LIKE 'F12%'
       OR icd.CODE LIKE 'F13%'
       OR icd.CODE LIKE 'F14%'
       OR icd.CODE LIKE 'F15%'
       OR icd.CODE LIKE 'F16%'
       OR icd.CODE LIKE 'F19%')
      AND plv.PROBLEM_STATUS_C = 1
UNION
SELECT DISTINCT c.PAT_ID,
                2 'Substance Use Disorder',
                'AODA' 'SUBSTANCE'
FROM #COHORT c
    INNER JOIN Clarity.dbo.ORDER_MED_VIEW omv ON c.PAT_ID = omv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
WHERE (LOWER(med.NAME) LIKE 'naltrexone%'
       OR LOWER(med.NAME) LIKE 'vivitrol%'
       OR LOWER(med.NAME) LIKE 'buprenorphine%'
       OR LOWER(med.NAME) LIKE 'buprenex%'
       OR LOWER(med.NAME) LIKE 'disulfiram%'
       OR LOWER(med.NAME) LIKE 'antabuse%'
       OR LOWER(med.NAME) LIKE 'aamprosate%'
       OR LOWER(med.NAME) LIKE 'campral%'
       OR LOWER(med.GENERIC_NAME) LIKE 'naltrexone%'
       OR LOWER(med.GENERIC_NAME) LIKE 'vivitrol%'
       OR LOWER(med.GENERIC_NAME) LIKE 'buprenorphine%'
       OR LOWER(med.GENERIC_NAME) LIKE 'buprenex%'
       OR LOWER(med.GENERIC_NAME) LIKE 'disulfiram%'
       OR LOWER(med.GENERIC_NAME) LIKE 'antabuse%'
       OR LOWER(med.GENERIC_NAME) LIKE 'aamprosate%'
       OR LOWER(med.GENERIC_NAME) LIKE 'campral%')
      AND omv.ORDERING_DATE > DATEADD(MONTH, -12, GETDATE());

SELECT c.PAT_ID,
       MAX(CASE WHEN sud.[Substance Use Disorder] IS NOT NULL THEN 2
               WHEN s.TOBACCO_USER = 1 THEN 1
               ELSE 0
           END) AS 'Substance Use',
       MIN(CASE WHEN sud.[Substance Use Disorder] IS NOT NULL THEN 'AODA'
               WHEN s.TOBACCO_USER = 1 THEN 'Tobacco'
               ELSE NULL
           END) AS 'Substance'
INTO #SUD_SUM
FROM #COHORT c
    LEFT JOIN #SMOKE s ON s.PAT_ID = c.PAT_ID
    LEFT JOIN #SUD sud ON sud.PAT_ID = c.PAT_ID
GROUP BY c.PAT_ID;

/*MED Count Section*/
SELECT c.PAT_ID,
       MAX(medlst.CONTACT_DATE) MAXDATE
INTO #MEDCOUNT
FROM #COHORT c
    INNER JOIN Clarity.dbo.PAT_ENC_CURR_MEDS_VIEW medlst ON c.PAT_ID = medlst.PAT_ID
    INNER JOIN Clarity.dbo.ORDER_MED_VIEW omv ON omv.ORDER_MED_ID = medlst.CURRENT_MED_ID
WHERE omv.ORDER_CLASS_C <> 6 --OTC
      AND medlst.CONTACT_DATE > DATEADD(MONTH, -13, GETDATE())
GROUP BY c.PAT_ID;

SELECT m.PAT_ID,
       COUNT(DISTINCT cm.NAME) MED_COUNT
INTO #MEDS
FROM #MEDCOUNT m
    INNER JOIN Clarity.dbo.PAT_ENC_CURR_MEDS_VIEW meds ON meds.PAT_ID = m.PAT_ID
                                                          AND m.MAXDATE = meds.CONTACT_DATE
    INNER JOIN Clarity.dbo.ORDER_MED_VIEW omv ON omv.ORDER_MED_ID = meds.CURRENT_MED_ID
    INNER JOIN Clarity.dbo.CLARITY_MEDICATION cm ON cm.MEDICATION_ID = omv.MEDICATION_ID
WHERE omv.ORDER_CLASS_C <> 6 --OTC
      AND cm.EQUIP_STATUS_YN = 'N'
      AND cm.THERA_CLASS_C NOT IN ( 1051, 1052, 1034, 1054, 1063 ) --Vitamins, contraceptives, herbals, MISCELLANEOUS MEDICAL SUPPLIES OR DEVICES
GROUP BY m.PAT_ID;

SELECT mc.PAT_ID,
       mc.MED_COUNT,
       CASE WHEN mc.MED_COUNT < 7 THEN 0
           WHEN mc.MED_COUNT > 10 THEN 2
           ELSE 1
       END AS NUM_OF_MEDS
INTO #MEDGRP
FROM #MEDS mc;

/*HIV VIral Load*/
SELECT c.PAT_ID,
       CASE WHEN orv.ORD_NUM_VALUE <> 9999999 THEN orv.ORD_NUM_VALUE
           WHEN orv.ORD_VALUE LIKE '>%' THEN 10000000.0
           ELSE 0.0
       END AS Result_Output,
       ROW_NUMBER() OVER (PARTITION BY opv.PAT_ID ORDER BY orv.RESULT_DATE DESC) AS ROW_NUM_DESC
INTO #VL
FROM #COHORT c
    INNER JOIN Clarity.dbo.ORDER_PROC_VIEW opv ON opv.PAT_ID = c.PAT_ID
    INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
    INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON cc.COMPONENT_ID = orv.COMPONENT_ID
WHERE orv.RESULT_DATE >= DATEADD(MM, -14, GETDATE())
      AND cc.COMMON_NAME = 'HIV VIRAL LOAD'
      AND orv.ORD_VALUE NOT IN ( 'Delete', 'See comment' );

SELECT vl.PAT_ID,
       CASE WHEN vl.Result_Output > 199 THEN 2
           ELSE 0
       END AS HIV_VL,
       vl.Result_Output 'VIRAL LOAD'
INTO #Viral_Load
FROM #VL vl
WHERE vl.ROW_NUM_DESC = 1;

/*Diabetes*/
SELECT dm.PAT_ID,
       CASE WHEN dm.HBA1C_LAST IS NULL THEN 2
           WHEN dm.HBA1C_LAST > 9.0 THEN 2
           ELSE 1
       END AS HBA1c,
       dm.HBA1C_LAST
INTO #dm
FROM #COHORT c
    INNER JOIN Clarity.dbo.DM_DIABETES_VIEW dm ON dm.PAT_ID = c.PAT_ID;

SELECT c.PAT_ID,
       CASE WHEN well.BP_SYS_LAST > 140.0 THEN 1
           WHEN well.BP_DIA_LAST > 90.0 THEN 1
           ELSE 0
       END AS BP,
       CONVERT(VARCHAR(3), well.BP_SYS_LAST) + '/' + CONVERT(VARCHAR(3), well.BP_DIA_LAST) AS 'BLOOD PRESSURE'
INTO #bp
FROM #COHORT c
    INNER JOIN Clarity.dbo.DM_WLL_ALL_VIEW well ON well.PAT_ID = c.PAT_ID
WHERE well.HAS_HYPERTENSION_YN = 'Y'
;
IF OBJECT_ID('tempdb..#COPD') IS NOT NULL									
DROP TABLE #COPD;
SELECT 
	copd.PAT_ID
	,MAX(CASE
		WHEN i.CODE = 'J96.11' THEN 1
		ELSE 1
	END) AS COPD
	,MAX(CASE
		WHEN i.CODE = 'J96.11' THEN 'Y'
		ELSE 'Y'
	END) AS 'HAS COPD' 

INTO #COPD

FROM 
	Clarity.dbo.DM_COPD_VIEW copd
	LEFT JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON plv.PAT_ID = copd.PAT_ID
	LEFT JOIN Clarity.dbo.CLARITY_EDG edg ON edg.DX_ID = plv.DX_ID
	LEFT JOIN Clarity.dbo.EDG_CURRENT_ICD10 i ON i.DX_ID = edg.DX_ID

GROUP BY 
	copd.PAT_ID
;

/*CAD*/
SELECT c.PAT_ID,
       MAX(CASE WHEN gcrl.BASE_GROUPER_ID = '2100000168' THEN 2 ELSE 1 END) AS HVD,
       MAX(CASE WHEN gcrl.BASE_GROUPER_ID = '2100000168' THEN 'PERIPHERAL VASCULAR DISEASE'
               ELSE 'CORONARY ARTERY DISEASE'
           END) AS HVD_TYPE
INTO #HVD
FROM #COHORT c
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW pl ON pl.PAT_ID = c.PAT_ID
    INNER JOIN Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl ON pl.DX_ID = gcrl.GROUPER_RECORDS_NUMERIC_ID
WHERE gcrl.BASE_GROUPER_ID IN ( '760000027' /*EDG CONCEPT CORONARY ARTERY DISEASE*/, '2100000168' /*EDG ICD CMS CCM PERIPHERAL VASCULAR DISEASE*/ )
      AND gcrl.COMPILED_CONTEXT = 'EDG' -- Compiled for EDG
      AND pl.PROBLEM_STATUS_C = 1 -- Active problems only
      AND pl.RESOLVED_DATE IS NULL
GROUP BY c.PAT_ID;

/*CKD or Hemodialysis*/
SELECT c.PAT_ID,
       MAX(CASE WHEN i.CODE IN ('N18.4', 'E11.22', 'Z79.4')  THEN 2 --Stage 4
               WHEN gcrl.BASE_GROUPER_ID = '972013' THEN 2                    -- Dialysis
               ELSE 1
           END) AS CKD,
       MIN(CASE WHEN i.CODE IN ('N18.4', 'E11.22', 'Z79.4')  THEN 'CKD Stage IV' --Stage 4
               WHEN gcrl.BASE_GROUPER_ID = '972013' THEN 'Dialysis'                        -- Dialysis
               ELSE 'CKD Stage III or Lower'
           END) AS CKD_SEVERITY
INTO #CKD
FROM #COHORT c
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW pl ON pl.PAT_ID = c.PAT_ID
    INNER JOIN Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl ON pl.DX_ID = gcrl.GROUPER_RECORDS_NUMERIC_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON pl.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 i ON i.DX_ID = edg.DX_ID
WHERE gcrl.BASE_GROUPER_ID IN ( '100185' /*DM: EDG CHRONIC KIDNEY DISEASE (CKD) SNOMED*/, '972013' /*DIALYSIS PATIENT*/ )
      AND gcrl.COMPILED_CONTEXT = 'EDG' -- Compiled for EDG
      AND pl.PROBLEM_STATUS_C = 1 -- Active problems only
      AND pl.RESOLVED_DATE IS NULL
GROUP BY c.PAT_ID;

/*Obesity*/
SELECT c.PAT_ID,
       CASE WHEN w.BMI_LAST > 40.0 THEN 2
           WHEN w.BMI_LAST > 30.0 THEN 1
           ELSE 0
       END AS BMI,
       w.BMI_LAST
INTO #BMI
FROM #COHORT c
    INNER JOIN Clarity.dbo.DM_WLL_ALL_VIEW w ON w.PAT_ID = c.PAT_ID;

/*Cognitive impairment*/
SELECT c.PAT_ID,
       MAX(CASE WHEN icd10.CODE = 'F03.90' THEN 2 --unspec. dementia  (placeholder until Leslie ges me an update)
               WHEN icd10.CODE LIKE 'I69%' THEN 1 --Cognitive Impairement
               WHEN icd10.CODE = 'R41.89' THEN 1  --Cognitive Impairement
               ELSE 0
           END) AS CI,
       MAX(CASE WHEN icd10.CODE = 'F03.90' THEN 'Dementia'              --unspec. dementia  (placeholder until Leslie ges me an update)
               WHEN icd10.CODE LIKE 'I69%' THEN 'Cognitive Impairement' --Cognitive Impairement
               WHEN icd10.CODE = 'R41.89' THEN 'Cognitive Impairement'  --Cognitive Impairement
               ELSE NULL
           END) AS CI_SEVERITY
INTO #COG
FROM #COHORT c
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON c.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
WHERE icd10.CODE LIKE 'I69%' --Cognitive Impairement
      OR icd10.CODE = 'R41.89' --Cognitive Impairement
      OR icd10.CODE = 'F03.90' -- Unspecified dementia
GROUP BY c.PAT_ID;

/*********************************************Social Determinants of Health****************************************************/

/*Housing Instability*/
SELECT c.PAT_ID,
       CASE WHEN meas.MEAS_VALUE = '1' THEN 1
           WHEN meas.MEAS_VALUE = '2' THEN 2
           ELSE 0
       END AS HOUSING,
       CASE WHEN meas.MEAS_VALUE = '1' THEN 'Worried'
           WHEN meas.MEAS_VALUE = '2' THEN 'Unstable'
           ELSE 'Stable'
       END AS HOUSING_PROBLEMS
INTO #HOUSING
FROM #COHORT c
    INNER JOIN (SELECT pev.PAT_ID,
                       meas.MEAS_VALUE,
                       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY meas.RECORDED_TIME DESC) AS ROW_NUM_DESC
                FROM Clarity.dbo.PAT_ENC_VIEW pev
                    INNER JOIN Clarity.dbo.IP_FLWSHT_REC_VIEW ifrv ON pev.INPATIENT_DATA_ID = ifrv.INPATIENT_DATA_ID
                    INNER JOIN Clarity.dbo.IP_FLWSHT_MEAS_VIEW meas ON ifrv.FSD_ID = meas.FSD_ID
                WHERE meas.FLO_MEAS_ID = '5693' --R SDH HOUSING LIVING SITUATION
                      AND meas.RECORDED_TIME > '3/1/2020' --Since SDOH started in March of 2020
    ) meas ON meas.PAT_ID = c.PAT_ID
              AND meas.ROW_NUM_DESC = 1;

/*Transportation Difficulty */
SELECT c.PAT_ID,
       CASE WHEN meas.MEAS_VALUE = 'Yes, it has kept me from medical appointments or getting medications' THEN 2
           WHEN meas.MEAS_VALUE = 'Yes, it has kept me from medical appointments or getting medications;Yes, it has kept me from non-medical meetings, appointments, work, or getting things that I need' THEN
               2
           WHEN meas.MEAS_VALUE = 'Yes, it has kept me from non-medical meetings, appointments, work, or getting things that I need' THEN 1
           ELSE 0
       END AS TRANSPORT_NEEDS,
       CASE WHEN meas.MEAS_VALUE IS NULL THEN 'No Value'
           WHEN meas.MEAS_VALUE = 'Yes, it has kept me from medical appointments or getting medications' THEN 'Med Transport'
           WHEN meas.MEAS_VALUE = 'Yes, it has kept me from medical appointments or getting medications;Yes, it has kept me from non-medical meetings, appointments, work, or getting things that I need' THEN
               ' Med and Other Transport'
           WHEN meas.MEAS_VALUE = 'Yes, it has kept me from non-medical meetings, appointments, work, or getting things that I need' THEN 'Other Tansport'
           ELSE 'No Transp Problems'
       END AS TRANSPORT_PROBLEMS
INTO #TRANS
FROM #COHORT c
    INNER JOIN (SELECT pev.PAT_ID,
                       meas.MEAS_VALUE,
                       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY meas.RECORDED_TIME DESC) AS ROW_NUM_DESC
                FROM Clarity.dbo.PAT_ENC_VIEW pev
                    INNER JOIN Clarity.dbo.IP_FLWSHT_REC_VIEW ifrv ON pev.INPATIENT_DATA_ID = ifrv.INPATIENT_DATA_ID
                    INNER JOIN Clarity.dbo.IP_FLWSHT_MEAS_VIEW meas ON ifrv.FSD_ID = meas.FSD_ID
                WHERE meas.FLO_MEAS_ID = '2373' --R SDOH QB: TRANSPORTATION WITH ADDITIONAL RESPONSES
                      AND meas.RECORDED_TIME > '3/1/2020' --Since SDOH started in March of 2020
    ) meas ON meas.PAT_ID = c.PAT_ID
              AND meas.ROW_NUM_DESC = 1;

/* Financial Resources*/
SELECT c.PAT_ID,
       CASE WHEN meas.MEAS_VALUE = 'Very Hard' THEN 2
           WHEN meas.MEAS_VALUE = 'Somewhat hard' THEN 1
           ELSE 0
       END AS FINANCIAL_STRAIN,
       CASE WHEN meas.MEAS_VALUE IS NULL THEN 'No Value'
           WHEN meas.MEAS_VALUE = 'Very Hard' THEN 'Very Hard'
           WHEN meas.MEAS_VALUE = 'Somewhat hard' THEN 'Somewhat hard'
           ELSE 'Not Hard'
       END AS FINANCIAL
INTO #FINANCE
FROM #COHORT c
    INNER JOIN (SELECT pev.PAT_ID,
                       meas.MEAS_VALUE,
                       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY meas.RECORDED_TIME DESC) AS ROW_NUM_DESC
                FROM Clarity.dbo.PAT_ENC_VIEW pev
                    INNER JOIN Clarity.dbo.IP_FLWSHT_REC_VIEW ifrv ON pev.INPATIENT_DATA_ID = ifrv.INPATIENT_DATA_ID
                    INNER JOIN Clarity.dbo.IP_FLWSHT_MEAS_VIEW meas ON ifrv.FSD_ID = meas.FSD_ID
                WHERE meas.FLO_MEAS_ID = '3434' --RR FRS SEVERITY
                      AND meas.RECORDED_TIME > '3/1/2020' --Since SDOH started in March of 2020
    ) meas ON meas.PAT_ID = c.PAT_ID
              AND meas.ROW_NUM_DESC = 1;

/*Final Select*/
SELECT c.PAT_ID,
       c.IDENTITY_ID,
       c.PAT_NAME,
       c.BIRTH_DATE,
       c.SEX,
       c.CITY,
       c.STATE,
       c.PCP,
       CASE WHEN pd.[Psychiatric Diagnosis] IS NULL THEN 0
           ELSE pd.[Psychiatric Diagnosis]
       END AS [Psychiatric Diagnosis],
       pd.[Psychiatric Diagnosis Type],
       CASE WHEN sud.[Substance Use] IS NULL THEN 0
           WHEN sud.[Substance Use] = 3 THEN 2 --Since we're not adding tobacco and AODA together
           ELSE sud.[Substance Use]
       END AS [Substance Use],
       sud.Substance,
       CASE WHEN med.NUM_OF_MEDS IS NULL THEN 0
           ELSE med.NUM_OF_MEDS
       END AS NUM_OF_MEDS,
       med.MED_COUNT,
       CASE WHEN vl.HIV_VL IS NULL THEN 0
           ELSE vl.HIV_VL
       END AS HIV_VL,
       vl.[VIRAL LOAD],
       CASE WHEN dm.HBA1c IS NULL THEN 0
           ELSE dm.HBA1c
       END AS HBA1c,
       dm.HBA1C_LAST,
       CASE WHEN bp.BP IS NULL THEN 0
           ELSE bp.BP
       END AS BP,
       bp.[BLOOD PRESSURE],
       CASE WHEN copd.COPD IS NULL THEN 0
           ELSE copd.COPD
       END AS COPD,
       copd.[HAS COPD],
       CASE WHEN hvd.HVD IS NULL THEN 0
           ELSE hvd.HVD
       END AS HVD,
       hvd.HVD_TYPE,
       CASE WHEN ckd.CKD IS NULL THEN 0
           ELSE ckd.CKD
       END AS CKD,
       ckd.CKD_SEVERITY,
       CASE WHEN bmi.BMI IS NULL THEN 0
           ELSE bmi.BMI
       END AS BMI,
       bmi.BMI_LAST,
       CASE WHEN h.HOUSING IS NULL THEN 0
           ELSE h.HOUSING
       END AS HOUSING,
       h.HOUSING_PROBLEMS,
       CASE WHEN tf.TRANSPORT_NEEDS IS NULL THEN 0
           ELSE tf.TRANSPORT_NEEDS
       END AS TRANSPORT_NEEDS,
       tf.TRANSPORT_PROBLEMS,
       CASE WHEN f.FINANCIAL_STRAIN IS NULL THEN 0
           ELSE f.FINANCIAL_STRAIN
       END AS FINANCIAL_STRAIN,
       f.FINANCIAL,
       CASE WHEN cog.CI IS NULL THEN 0
           ELSE cog.CI
       END AS CI,
       cog.CI_SEVERITY
INTO #FINAL
FROM #COHORT c
    LEFT JOIN #PSYCHDX pd ON pd.PAT_ID = c.PAT_ID
    LEFT JOIN #SUD_SUM sud ON sud.PAT_ID = c.PAT_ID
    LEFT JOIN #MEDGRP med ON med.PAT_ID = c.PAT_ID
    LEFT JOIN #Viral_Load vl ON vl.PAT_ID = c.PAT_ID
    LEFT JOIN #dm dm ON dm.PAT_ID = c.PAT_ID
    LEFT JOIN #bp bp ON bp.PAT_ID = c.PAT_ID
    LEFT JOIN #COPD copd ON copd.PAT_ID = c.PAT_ID
    LEFT JOIN #HVD hvd ON hvd.PAT_ID = c.PAT_ID
    LEFT JOIN #CKD ckd ON ckd.PAT_ID = c.PAT_ID
    LEFT JOIN #BMI bmi ON bmi.PAT_ID = c.PAT_ID
    LEFT JOIN #HOUSING h ON h.PAT_ID = c.PAT_ID
    LEFT JOIN #TRANS tf ON tf.PAT_ID = c.PAT_ID
    LEFT JOIN #FINANCE f ON f.PAT_ID = c.PAT_ID
    LEFT JOIN #COG cog ON cog.PAT_ID = c.PAT_ID;

SELECT fs.IDENTITY_ID MRN,
       fs.PAT_NAME,
       (DATEDIFF(m, fs.BIRTH_DATE, GETDATE()) / 12) AGE,
       fs.SEX,
       fs.CITY,
       fs.STATE,
       fs.PCP,
       fs.[Psychiatric Diagnosis Type],
       fs.[Psychiatric Diagnosis] 'Psychiatric Diagnosis Risk',
       fs.Substance Substance,
       fs.[Substance Use] 'Substance Use Risk',
       fs.MED_COUNT 'Med Count',
       fs.NUM_OF_MEDS 'Med Risk',
       fs.[VIRAL LOAD] 'Last VL',
       fs.HIV_VL 'VL Risk',
       fs.HBA1C_LAST 'Last A1c (DM only)',
       fs.HBA1c 'A1c Risk',
       fs.[BLOOD PRESSURE] 'Last BP (HTN Only)',
       fs.BP 'BP Risk',
       fs.[HAS COPD],
       fs.COPD 'COPD Risk',
       fs.HVD_TYPE,
       fs.HVD 'HVD Risk',
       fs.CKD_SEVERITY,
       fs.CKD 'CKD Risk',
       fs.BMI_LAST,
       fs.BMI 'BMI Risk',
       fs.CI_SEVERITY,
       fs.CI 'CI Risk',
       fs.HOUSING_PROBLEMS,
       fs.HOUSING 'Housing Risk',
       fs.TRANSPORT_PROBLEMS,
       fs.TRANSPORT_NEEDS 'Transport Risk',
       fs.FINANCIAL 'Financial Problem',
       fs.FINANCIAL_STRAIN 'Financial Risk',
       fs.[Psychiatric Diagnosis] + fs.[Substance Use] + fs.NUM_OF_MEDS + fs.HIV_VL + fs.HBA1c + fs.BP + fs.COPD + fs.CI + fs.HVD + fs.CKD + fs.BMI
       + fs.HOUSING + fs.TRANSPORT_NEEDS + fs.FINANCIAL_STRAIN AS 'Risk Score',
       CASE WHEN fs.[Psychiatric Diagnosis] + fs.[Substance Use] + fs.NUM_OF_MEDS + fs.HIV_VL + fs.HBA1c + fs.BP + fs.COPD + fs.CI + fs.HVD + fs.CKD + fs.BMI
                 + fs.HOUSING + fs.TRANSPORT_NEEDS + fs.FINANCIAL_STRAIN > 6 THEN '4. High-Risk'
           WHEN fs.[Psychiatric Diagnosis] + fs.[Substance Use] + fs.NUM_OF_MEDS + fs.HIV_VL + fs.HBA1c + fs.BP + fs.COPD + fs.CI + fs.HVD + fs.CKD + fs.BMI
                + fs.HOUSING + fs.TRANSPORT_NEEDS + fs.FINANCIAL_STRAIN > 3 THEN '3. Rising-Risk'
           WHEN fs.[Psychiatric Diagnosis] + fs.[Substance Use] + fs.NUM_OF_MEDS + fs.HIV_VL + fs.HBA1c + fs.BP + fs.COPD + fs.CI + fs.HVD + fs.CKD + fs.BMI
                + fs.HOUSING + fs.TRANSPORT_NEEDS + fs.FINANCIAL_STRAIN > 1 THEN '2. Low-Risk'
           ELSE '1. Healthy'
       END AS 'RISK CATEGORY',
       CAST(cp.LOG_TIMESTAMP AS DATE) AS 'Last Care Plan',
       cp.[Months Since Care Plan],
       cp.[SMART PHRASE USER] 'Care Plan Creator',
       CAST(ps.[Last Visit] AS DATE) AS 'Last BH Visit',
       ps.PROV_NAME 'Last BH Visit Provider',
       CAST(lps.[Next BH Visit] AS DATE) AS 'Next BH Visit',
       lps.PROV_NAME 'Next BH Visit Provider',
       lcp.PROV_NAME 'Last CP Provider',
       CAST(lcp.[Last CP Visit] AS DATE) AS 'Last CP Visit',
       ncp.PROV_NAME 'Next CP Visit Provider',
       CAST(ncp.[Next CP Visit] AS DATE) AS 'Next CP Visit',
       lrd.PROV_NAME 'Last Nutritionist Visit Provider',
       CAST(lrd.[Last Nutritionist Visit] AS DATE) AS 'Last Nutritionist Visit',
       nrd.PROV_NAME 'Next Nutritionist Visit Provider',
       CAST(nrd.[Next Nutritionist Visit] AS DATE) AS 'Next Nutritionist Visit',
       nmd.PROV_NAME 'Next Medical Visit Provider',
       CAST(nmd.[Next Medical Visit] AS DATE) AS 'Next Medical Visit',
       lmd.PROV_NAME 'Last Medical Visit Provider',
       CAST(lmd.[Last Medical Visit] AS DATE) AS 'Last Medical Visit',
       IIF(wi_mm.PAT_ID IS NOT NULL, 'Yes', 'No') AS 'Wisconsin Medicaid Medical Home Patient',
       CAST(CURRENT_TIMESTAMP AS DATE) AS TODAY
FROM #FINAL fs
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
                     AND cs.SMARTPHRASE_NAME LIKE '%VHCAREPLAN%') cp ON cp.IDENTITY_ID = fs.IDENTITY_ID
                                                                        AND cp.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT id.IDENTITY_ID MRN,
                      pev.CONTACT_DATE 'Last Visit',
                      ser.PROV_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'PY', 'MH', 'BH' )
    --AND ser.PROVIDER_TYPE_C NOT IN ('164', '136', '129')
    ) ps ON ps.MRN = fs.IDENTITY_ID
            AND ps.ROW_NUM_DESC = 1
    /* Next BH Visit */
    LEFT JOIN (SELECT id.IDENTITY_ID MRN,
                      pev.CONTACT_DATE 'Next BH Visit',
                      ser.PROV_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C = 1
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'PY', 'MH', 'BH' )
    --AND ser.PROVIDER_TYPE_C NOT IN ('164', '136', '129')
    ) lps ON lps.MRN = ps.MRN
             AND lps.ROW_NUM_ASC = 1
    LEFT JOIN (SELECT --------Last Clinical Pharm
               id.IDENTITY_ID MRN,
               CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Last CP Visit',
               ser.PROV_NAME,
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                     AND ser.PROVIDER_TYPE_C IN ( '102', '173', '185' )) lcp ON lcp.MRN = fs.IDENTITY_ID
                                                                                AND lcp.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT --------Next Clinical Pharm
               id.IDENTITY_ID MRN,
               CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) 'Next CP Visit',
               ser.PROV_NAME,
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C = 1
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                     AND ser.PROVIDER_TYPE_C IN ( '102', '173', '185' )) ncp ON ncp.MRN = fs.IDENTITY_ID
                                                                                AND ncp.ROW_NUM_ASC = 1
    LEFT JOIN (SELECT --------Last LD/RD
               id.IDENTITY_ID MRN,
               CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Last Nutritionist Visit',
               ser.PROV_NAME,
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                     AND ser.PROVIDER_TYPE_C IN ( '142', '104', '228' )) lrd ON lrd.MRN = fs.IDENTITY_ID
                                                                                AND lrd.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT --------Next LD/RD
               id.IDENTITY_ID MRN,
               CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) 'Next Nutritionist Visit',
               ser.PROV_NAME,
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C = 1
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                     AND ser.PROVIDER_TYPE_C IN ( '142', '104', '228' )) nrd ON nrd.MRN = fs.IDENTITY_ID
                                                                                AND nrd.ROW_NUM_ASC = 1
    LEFT JOIN (SELECT id.IDENTITY_ID MRN,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) 'Last Medical Visit',
                      ser.PROV_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                     AND ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs, and NPs
    ) lmd ON lmd.MRN = fs.IDENTITY_ID
             AND lmd.ROW_NUM_DESC = 1
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
    ) nmd ON nmd.MRN = fs.IDENTITY_ID
             AND nmd.ROW_NUM_ASC = 1
    LEFT JOIN (SELECT PAT_ENC.PAT_ID,
                      ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
                   INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
                                                                             AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2) = 'WI'
                   INNER JOIN CLARITY.dbo.ARPB_VISITS_VIEW AS arpb_visits ON PAT_ENC.PAT_ENC_CSN_ID = arpb_visits.PRIM_ENC_CSN_ID
                   INNER JOIN CLARITY.dbo.ZC_FINANCIAL_CLASS AS zc_financial_class ON arpb_visits.ACCT_FIN_CLASS_C = zc_financial_class.FINANCIAL_CLASS
                                                                                      AND zc_financial_class.NAME = 'Medicaid'
               WHERE PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
                     AND DATEDIFF(MONTH, PAT_ENC.CONTACT_DATE, GETDATE()) <= 12) wi_mm ON fs.PAT_ID = wi_mm.PAT_ID
                                                                                          AND wi_mm.ROW_NUM_DESC = 1;

DROP TABLE #SEX;
DROP TABLE #COHORT;
DROP TABLE #PSYCHDX;
DROP TABLE #SUD;
DROP TABLE #SMOKE;
DROP TABLE #SUD_SUM;
DROP TABLE #MEDCOUNT;
DROP TABLE #MEDGRP;
DROP TABLE #MEDS;
DROP TABLE #VL;
DROP TABLE #Viral_Load;
DROP TABLE #dm;
DROP TABLE #bp;
DROP TABLE #COPD;
DROP TABLE #HVD;
DROP TABLE #CKD;
DROP TABLE #BMI;
DROP TABLE #COG;
DROP TABLE #HOUSING;
DROP TABLE #TRANS;
DROP TABLE #FINANCE;
DROP TABLE #FINAL;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;