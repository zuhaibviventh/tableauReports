
/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Medical 12 & 13 - Patients with diabetes with controlled or poorly controlled A1c

 Create Date: 8/23/2018
 Created By:  sharmaj
 System:    javelin.ochin.org
 Requested By:  Internal Dashboard

 Purpose: Medical Quality Measure checks on controlled or poorly controlled A1C level among active, diabetic and HIV+ patients

 Description: 
      Denominator Definitions: Patients with a diagnosis of diabetes 
      Numerator Definitions: Most recent A1c < 7 
      OR Most recent A1c > 9

 
BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 1/18/2019      Jaya        Included MO
 4/8/2019     Mitch       Changing the denom to be all diabetic pts, not just with A1cs
04/19/2019      Mitch       Updating Active Patient logic to include Dx codes and LOS_PRIME_PROC_ID
7/5/2019      Mitch       Complete rewrite using the DM registry
02/24/2020      Jaya          Updated to new Department Name
4/6/2020      Mitch       Updating to allow it to work in Alteryx
02/02/2021      Jaya        Added PA to the Provider_Type_C
07/02/2021      Jaya        Added Dietitian flag
10/1/2021     Mitch       Adding CP Cohorts
2/3/2022      Mitch       Adding Pre-DM Cohort
2/24/2022     Mitch       Adding Ethnicity
**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT pev.PAT_ID,
       pev.DEPARTMENT_ID,
       dep.DEPARTMENT_NAME LAST_VISIT_DEPT,
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
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );


SELECT a1.PAT_ID,
       a1.LAST_OFFICE_VISIT,
       a1.LOS,
       a1.CITY,
       a1.STATE,
	   a1.SERVICE_TYPE,
	   a1.SERVICE_LINE, 
	   a1.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';


IF OBJECT_ID('tempdb..#active_hiv_patients') IS NOT NULL DROP TABLE #active_hiv_patients;
SELECT DISTINCT pev.PAT_ID
INTO #active_hiv_patients
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
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


SELECT id.PAT_ID,
       id.IDENTITY_ID,
       p.PAT_NAME,
       ser.EXTERNAL_NAME PCP,
       att.STATE,
       att.CITY,
	   att.SERVICE_TYPE,
	   att.SERVICE_LINE, 
	   att.SUB_SERVICE_LINE,
       CASE WHEN dmr.HBA1C_LAST_DT < DATEADD(MONTH, -12, GETDATE()) THEN NULL
           ELSE dmr.HBA1C_LAST
       END AS LAST_A1c,
       CAST(dmr.HBA1C_LAST_DT AS DATE) RESULT_DATE,
       CASE WHEN dmr.HBA1C_LAST IS NULL THEN 'Over 7'
           WHEN dmr.HBA1C_LAST_DT < DATEADD(MONTH, -12, GETDATE()) THEN 'Over 7'
           WHEN dmr.HBA1C_LAST < 7.0 THEN 'Under 7'
           ELSE 'Over 7'
       END AS 'Controlled_A1C_<_7',
       CASE WHEN dmr.HBA1C_LAST IS NULL THEN 'Over 8'
           WHEN dmr.HBA1C_LAST_DT < DATEADD(MONTH, -12, GETDATE()) THEN 'Over 8'
           WHEN dmr.HBA1C_LAST > 8.0 THEN 'Over 8'
           ELSE 'Under 8'
       END AS 'Poor_A1C_8+'
INTO #a
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.DM_DIABETES_VIEW dmr ON p.PAT_ID = dmr.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN #Attribution2 att ON p.PAT_ID = att.PAT_ID
    INNER JOIN #active_hiv_patients ON p.PAT_ID = #active_hiv_patients.PAT_ID
WHERE att.ROW_NUM_DESC = 1;


SELECT a.PAT_ID,
       a.IDENTITY_ID MRN,
       a.PAT_NAME,
       a.PCP,
       a.STATE,
       a.CITY,
	   a.SERVICE_TYPE,
	   a.SERVICE_LINE, 
	   a.SUB_SERVICE_LINE,
       a.LAST_A1c,
       a.RESULT_DATE,
       a.[Controlled_A1C_<_7],
       a.[Poor_A1C_8+],
       CAST(pev2.CONTACT_DATE AS DATE) NEXT_APPT,
       ser2.EXTERNAL_NAME NEXT_APPT_PROV,
       ROW_NUMBER() OVER (PARTITION BY a.PAT_ID ORDER BY pev2.CONTACT_DATE ASC) AS ROW_NUM_ASC
INTO #b
FROM #a a
    LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev2 ON a.PAT_ID = pev2.PAT_ID
                                               AND pev2.APPT_STATUS_C = 1
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser2 ON pev2.VISIT_PROV_ID = ser2.PROV_ID;


IF OBJECT_ID('tempdb..#diabetes_foot_exam') IS NOT NULL DROP TABLE #diabetes_foot_exam;
SELECT PATIENT_HMT_STATUS.PAT_ID,
       CAST(PATIENT_HMT_STATUS.IDEAL_RETURN_DT AS DATE) AS HEALTH_MAINTENANCE_DUE_DATE,
       CLARITY_HM_TOPIC.NAME AS HEALTH_MAINTENANCE_TOPIC_NAME,
       ZC_HMT_DUE_STATUS.NAME AS HEALTH_MAINTENANCE_STATUS
INTO #diabetes_foot_exam
FROM CLARITY.dbo.PATIENT_HMT_STATUS_VIEW AS PATIENT_HMT_STATUS
    INNER JOIN CLARITY.dbo.ZC_HMT_DUE_STATUS AS ZC_HMT_DUE_STATUS ON PATIENT_HMT_STATUS.HMT_DUE_STATUS_C = ZC_HMT_DUE_STATUS.HMT_DUE_STATUS_C
    INNER JOIN CLARITY.dbo.CLARITY_HM_TOPIC AS CLARITY_HM_TOPIC ON PATIENT_HMT_STATUS.QUALIFIED_HMT_ID = CLARITY_HM_TOPIC.HM_TOPIC_ID
WHERE CLARITY_HM_TOPIC.HM_TOPIC_ID IN ( 25 );


IF OBJECT_ID('tempdb..#dm_retinopathy_screening') IS NOT NULL DROP TABLE #dm_retinopathy_screening;
SELECT PATIENT_HMT_STATUS.PAT_ID,
       CAST(PATIENT_HMT_STATUS.IDEAL_RETURN_DT AS DATE) AS HEALTH_MAINTENANCE_DUE_DATE,
       CLARITY_HM_TOPIC.NAME AS HEALTH_MAINTENANCE_TOPIC_NAME,
       ZC_HMT_DUE_STATUS.NAME AS HEALTH_MAINTENANCE_STATUS
INTO #dm_retinopathy_screening
FROM CLARITY.dbo.PATIENT_HMT_STATUS_VIEW AS PATIENT_HMT_STATUS
    INNER JOIN CLARITY.dbo.ZC_HMT_DUE_STATUS AS ZC_HMT_DUE_STATUS ON PATIENT_HMT_STATUS.HMT_DUE_STATUS_C = ZC_HMT_DUE_STATUS.HMT_DUE_STATUS_C
    INNER JOIN CLARITY.dbo.CLARITY_HM_TOPIC AS CLARITY_HM_TOPIC ON PATIENT_HMT_STATUS.QUALIFIED_HMT_ID = CLARITY_HM_TOPIC.HM_TOPIC_ID
WHERE CLARITY_HM_TOPIC.HM_TOPIC_ID IN ( 28 );


IF OBJECT_ID('tempdb..#dm_microalbumin') IS NOT NULL DROP TABLE #dm_microalbumin;
SELECT PATIENT_HMT_STATUS.PAT_ID,
       CAST(PATIENT_HMT_STATUS.IDEAL_RETURN_DT AS DATE) AS HEALTH_MAINTENANCE_DUE_DATE,
       CLARITY_HM_TOPIC.NAME AS HEALTH_MAINTENANCE_TOPIC_NAME,
       ZC_HMT_DUE_STATUS.NAME AS HEALTH_MAINTENANCE_STATUS
INTO #dm_microalbumin
FROM CLARITY.dbo.PATIENT_HMT_STATUS_VIEW AS PATIENT_HMT_STATUS
    INNER JOIN CLARITY.dbo.ZC_HMT_DUE_STATUS AS ZC_HMT_DUE_STATUS ON PATIENT_HMT_STATUS.HMT_DUE_STATUS_C = ZC_HMT_DUE_STATUS.HMT_DUE_STATUS_C
    INNER JOIN CLARITY.dbo.CLARITY_HM_TOPIC AS CLARITY_HM_TOPIC ON PATIENT_HMT_STATUS.QUALIFIED_HMT_ID = CLARITY_HM_TOPIC.HM_TOPIC_ID
WHERE CLARITY_HM_TOPIC.HM_TOPIC_ID IN ( 36 );


IF OBJECT_ID('tempdb..#oral_eval_for_diabetic_adults') IS NOT NULL DROP TABLE #oral_eval_for_diabetic_adults;
WITH
    oral_eval AS (
        SELECT PATIENT_HMT_STATUS.PAT_ID,
               CAST(PATIENT_HMT_STATUS.IDEAL_RETURN_DT AS DATE) AS HEALTH_MAINTENANCE_DUE_DATE,
               'Oral Evaluation for Diabetic Adults' AS HEALTH_MAINTENANCE_TOPIC_NAME,
               ZC_HMT_DUE_STATUS.NAME AS HEALTH_MAINTENANCE_STATUS
        FROM CLARITY.dbo.PATIENT_HMT_STATUS_VIEW AS PATIENT_HMT_STATUS
            INNER JOIN CLARITY.dbo.ZC_HMT_DUE_STATUS AS ZC_HMT_DUE_STATUS ON PATIENT_HMT_STATUS.HMT_DUE_STATUS_C = ZC_HMT_DUE_STATUS.HMT_DUE_STATUS_C
            INNER JOIN CLARITY.dbo.CLARITY_HM_TOPIC AS CLARITY_HM_TOPIC ON PATIENT_HMT_STATUS.QUALIFIED_HMT_ID = CLARITY_HM_TOPIC.HM_TOPIC_ID
        WHERE CLARITY_HM_TOPIC.HM_TOPIC_ID IN ( 20 )
    )
SELECT DM_WLL_ALL.PAT_ID,
       oral_eval.HEALTH_MAINTENANCE_DUE_DATE,
       oral_eval.HEALTH_MAINTENANCE_TOPIC_NAME,
       oral_eval.HEALTH_MAINTENANCE_STATUS
INTO #oral_eval_for_diabetic_adults
FROM Clarity.dbo.DM_WLL_ALL_VIEW AS DM_WLL_ALL
    INNER JOIN oral_eval ON DM_WLL_ALL.PAT_ID = oral_eval.PAT_ID
WHERE DM_WLL_ALL.HAS_DIABETES_YN = 'Y';


IF OBJECT_ID('tempdb..#mini_cog_info') IS NOT NULL DROP TABLE #mini_cog_info;
WITH
    mini_cog_flsht AS (
        SELECT IP_FLWSHT_REC.PAT_ID,
               IP_FLWSHT_MEAS.RECORDED_TIME,
               IP_FLO_GP_DATA.FLO_MEAS_NAME,
               IP_FLWSHT_MEAS.MEAS_VALUE,
               ROW_NUMBER() OVER (PARTITION BY IP_FLWSHT_REC.PAT_ID
                                  ORDER BY COALESCE(IP_FLWSHT_MEAS.RECORDED_TIME, IP_FLWSHT_MEAS.ENTRY_TIME) DESC) AS ROW_NUM_DESC
        FROM Clarity.dbo.IP_FLWSHT_MEAS_VIEW AS IP_FLWSHT_MEAS
            INNER JOIN Clarity.dbo.IP_FLWSHT_REC_VIEW AS IP_FLWSHT_REC ON IP_FLWSHT_MEAS.FSD_ID = IP_FLWSHT_REC.FSD_ID
            INNER JOIN Clarity.dbo.IP_FLO_GP_DATA AS IP_FLO_GP_DATA ON IP_FLWSHT_MEAS.FLO_MEAS_ID = IP_FLO_GP_DATA.FLO_MEAS_ID
        WHERE IP_FLWSHT_MEAS.FLO_MEAS_ID = '601005'
              AND DATEDIFF(MI, IP_FLWSHT_MEAS.RECORDED_TIME, IP_FLWSHT_MEAS.ENTRY_TIME) < 1440
    )
SELECT mini_cog_flsht.PAT_ID,
       mini_cog_flsht.MEAS_VALUE AS MINI_COG_SCORE
INTO #mini_cog_info
FROM mini_cog_flsht
WHERE mini_cog_flsht.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#dental_visits') IS NOT NULL DROP TABLE #dental_visits;
SELECT PATIENT.PAT_ID,
       MAX('Yes') AS DENTAL_VISIT
INTO #dental_visits
FROM clarity.dbo.patient_view AS PATIENT
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC ON PAT_ENC.PAT_ID = PATIENT.PAT_ID
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID
WHERE PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
      AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'DT'
      AND PAT_ENC.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
GROUP BY PATIENT.PAT_ID;


IF OBJECT_ID('tempdb..#delivery') IS NOT NULL DROP TABLE #delivery;
SELECT b.PAT_ID,
       b.MRN,
       b.PAT_NAME,
       b.PCP,
       b.STATE,
       b.CITY,
	   b.SERVICE_TYPE,
	   b.SERVICE_LINE, 
	   b.SUB_SERVICE_LINE,
       COALESCE(ROUND(b.LAST_A1c, 2), -1) AS LAST_A1c,
       b.RESULT_DATE,
       b.[Controlled_A1C_<_7],
       b.[Poor_A1C_8+],
       b.NEXT_APPT,
       b.NEXT_APPT_PROV,
       COALESCE(#dental_visits.DENTAL_VISIT, 'No') AS 'Had a Dental Visit(s)',
       CAST(na.NEXT_PCP_APPT AS DATE) AS 'NEXT PCP APPT',
       na.EXTERNAL_NAME 'PCP APPT PROVIDER',
       MAX(CASE WHEN flag.PAT_FLAG_TYPE_C = '640013' THEN 'YES' ELSE 'NO' END) AS 'IN CLINICAL PHARMACY COHORT',
       MAX(CASE WHEN flag.PAT_FLAG_TYPE_C = '640018' THEN 'YES' ELSE 'NO' END) AS 'IN DIETITIAN CARE',
       COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE,
       COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY,
       CASE WHEN #mini_cog_info.PAT_ID IS NOT NULL THEN 'Yes'
           ELSE 'No'
       END AS MINI_COG_COMPLETED,
       #mini_cog_info.MINI_COG_SCORE,
       #diabetes_foot_exam.HEALTH_MAINTENANCE_DUE_DATE AS HEALTH_MAINTENANCE_DUE_DATE__diabetes_foot_exam,
       COALESCE(#diabetes_foot_exam.HEALTH_MAINTENANCE_TOPIC_NAME, 'Diabetes Foot Exam') AS HEALTH_MAINTENANCE_TOPIC_NAME__diabetes_foot_exam,
       COALESCE(#diabetes_foot_exam.HEALTH_MAINTENANCE_STATUS, 'Unknown Status') AS HEALTH_MAINTENANCE_STATUS__diabetes_foot_exam,
       #dm_retinopathy_screening.HEALTH_MAINTENANCE_DUE_DATE AS HEALTH_MAINTENANCE_DUE_DATE__dm_retinopathy_screening,
       COALESCE(#dm_retinopathy_screening.HEALTH_MAINTENANCE_TOPIC_NAME, 'Retinopathy Screening') AS HEALTH_MAINTENANCE_TOPIC_NAME__dm_retinopathy_screening,
       COALESCE(#dm_retinopathy_screening.HEALTH_MAINTENANCE_STATUS, 'Unknown Status') AS HEALTH_MAINTENANCE_STATUS__dm_retinopathy_screening,
       #dm_microalbumin.HEALTH_MAINTENANCE_DUE_DATE AS HEALTH_MAINTENANCE_DUE_DATE__dm_microalbumin,
       COALESCE(#dm_microalbumin.HEALTH_MAINTENANCE_TOPIC_NAME, 'Diabetes Microalbumin') AS HEALTH_MAINTENANCE_TOPIC_NAME__dm_microalbumin,
       COALESCE(#dm_microalbumin.HEALTH_MAINTENANCE_STATUS, 'Unknown Status') AS HEALTH_MAINTENANCE_STATUS__dm_microalbumin,
       #oral_eval_for_diabetic_adults.HEALTH_MAINTENANCE_DUE_DATE AS HEALTH_MAINTENANCE_DUE_DATE__oral_eval_for_diabetic_adults,
       COALESCE(#oral_eval_for_diabetic_adults.HEALTH_MAINTENANCE_TOPIC_NAME, 'Oral Evaluation for Diabetic Adults') AS HEALTH_MAINTENANCE_TOPIC_NAME__oral_eval_for_diabetic_adults,
       COALESCE(#oral_eval_for_diabetic_adults.HEALTH_MAINTENANCE_STATUS, 'Unknown Status') AS HEALTH_MAINTENANCE_STATUS__oral_eval_for_diabetic_adults
INTO #delivery
FROM #b b
    LEFT JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag ON b.PAT_ID = flag.PATIENT_ID
                                                         AND flag.PAT_FLAG_TYPE_C IN ( '640013', '640018', '64000011' ) --Diabetic AND pre-DM cohort
                                                         AND flag.ACTIVE_C = 1
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = b.PAT_ID
    LEFT JOIN ##patient_race_ethnicity ON p.PAT_ID = ##patient_race_ethnicity.PAT_ID
    LEFT JOIN (SELECT pev.PAT_ID,
                      pev.CONTACT_DATE NEXT_PCP_APPT,
                      ser.EXTERNAL_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1
                     AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
                     AND ser.PROV_ID <> '640178' --pulmonologist
    ) na ON b.PAT_ID = na.PAT_ID
            AND na.ROW_NUM_ASC = 1
    LEFT JOIN #diabetes_foot_exam ON b.PAT_ID = #diabetes_foot_exam.PAT_ID
    LEFT JOIN #dm_retinopathy_screening ON b.PAT_ID = #dm_retinopathy_screening.PAT_ID
    LEFT JOIN #dm_microalbumin ON b.PAT_ID = #dm_microalbumin.PAT_ID
    LEFT JOIN #oral_eval_for_diabetic_adults ON b.PAT_ID = #oral_eval_for_diabetic_adults.PAT_ID
    LEFT JOIN #mini_cog_info ON b.PAT_ID = #mini_cog_info.PAT_ID
    LEFT JOIN #dental_visits ON b.PAT_ID = #dental_visits.PAT_ID
WHERE b.ROW_NUM_ASC = 1
GROUP BY b.PAT_ID,
         b.LAST_A1c,
         na.NEXT_PCP_APPT,
         ##patient_race_ethnicity.ETHNICITY_CATEGORY,
         b.MRN,
         b.PAT_NAME,
         b.PCP,
         b.STATE,
         b.CITY,
		 b.SERVICE_TYPE,
		b.SERVICE_LINE, 
		b.SUB_SERVICE_LINE,
         b.RESULT_DATE,
         b.[Controlled_A1C_<_7],
         b.[Poor_A1C_8+],
         b.NEXT_APPT,
         b.NEXT_APPT_PROV,
         na.EXTERNAL_NAME,
         ##patient_race_ethnicity.RACE_CATEGORY,
         #diabetes_foot_exam.HEALTH_MAINTENANCE_DUE_DATE,
         #diabetes_foot_exam.HEALTH_MAINTENANCE_TOPIC_NAME,
         #diabetes_foot_exam.HEALTH_MAINTENANCE_STATUS,
         #dm_retinopathy_screening.HEALTH_MAINTENANCE_DUE_DATE,
         #dm_retinopathy_screening.HEALTH_MAINTENANCE_TOPIC_NAME,
         #dm_retinopathy_screening.HEALTH_MAINTENANCE_STATUS,
         #dm_microalbumin.HEALTH_MAINTENANCE_DUE_DATE,
         #dm_microalbumin.HEALTH_MAINTENANCE_TOPIC_NAME,
         #dm_microalbumin.HEALTH_MAINTENANCE_STATUS,
         #oral_eval_for_diabetic_adults.HEALTH_MAINTENANCE_DUE_DATE,
         #oral_eval_for_diabetic_adults.HEALTH_MAINTENANCE_TOPIC_NAME,
         #oral_eval_for_diabetic_adults.HEALTH_MAINTENANCE_STATUS,
         #mini_cog_info.PAT_ID,
         #mini_cog_info.MINI_COG_SCORE,
         #dental_visits.DENTAL_VISIT;


SELECT #delivery.PAT_ID,
       #delivery.MRN,
       #delivery.PAT_NAME,
       #delivery.PCP,
       #delivery.STATE 'State',
       #delivery.CITY 'City',
	   #delivery.SERVICE_TYPE 'Service Type',
	   #delivery.SERVICE_LINE 'Service Line', 
	   #delivery.SUB_SERVICE_LINE 'Sub-Service Line',
       #delivery.LAST_A1c,
       #delivery.RESULT_DATE,
       #delivery.[Controlled_A1C_<_7],
       #delivery.[Poor_A1C_8+],
       #delivery.NEXT_APPT,
       #delivery.NEXT_APPT_PROV,
       #delivery.[NEXT PCP APPT],
       #delivery.[PCP APPT PROVIDER],
       #delivery.[IN CLINICAL PHARMACY COHORT],
       #delivery.[IN DIETITIAN CARE],
       #delivery.RACE,
       #delivery.ETHNICITY,
       #delivery.MINI_COG_COMPLETED,
       #delivery.MINI_COG_SCORE,
       #delivery.[Had a Dental Visit(s)],
       #delivery.HEALTH_MAINTENANCE_DUE_DATE__diabetes_foot_exam AS HEALTH_MAINTENANCE_DUE_DATE,
       #delivery.HEALTH_MAINTENANCE_TOPIC_NAME__diabetes_foot_exam AS HEALTH_MAINTENANCE_TOPIC_NAME,
       #delivery.HEALTH_MAINTENANCE_STATUS__diabetes_foot_exam AS HEALTH_MAINTENANCE_STATUS
FROM #delivery
UNION ALL
SELECT #delivery.PAT_ID,
       #delivery.MRN,
       #delivery.PAT_NAME,
       #delivery.PCP,
       #delivery.STATE 'State',
       #delivery.CITY 'City',
	   #delivery.SERVICE_TYPE 'Service Type',
	   #delivery.SERVICE_LINE 'Service Line', 
	   #delivery.SUB_SERVICE_LINE 'Sub-Service Line',
       #delivery.LAST_A1c,
       #delivery.RESULT_DATE,
       #delivery.[Controlled_A1C_<_7],
       #delivery.[Poor_A1C_8+],
       #delivery.NEXT_APPT,
       #delivery.NEXT_APPT_PROV,
       #delivery.[NEXT PCP APPT],
       #delivery.[PCP APPT PROVIDER],
       #delivery.[IN CLINICAL PHARMACY COHORT],
       #delivery.[IN DIETITIAN CARE],
       #delivery.RACE,
       #delivery.ETHNICITY,
       #delivery.MINI_COG_COMPLETED,
       #delivery.MINI_COG_SCORE,
       #delivery.[Had a Dental Visit(s)],
       #delivery.HEALTH_MAINTENANCE_DUE_DATE__dm_retinopathy_screening AS HEALTH_MAINTENANCE_DUE_DATE,
       #delivery.HEALTH_MAINTENANCE_TOPIC_NAME__dm_retinopathy_screening AS HEALTH_MAINTENANCE_TOPIC_NAME,
       #delivery.HEALTH_MAINTENANCE_STATUS__dm_retinopathy_screening AS HEALTH_MAINTENANCE_STATUS
FROM #delivery
UNION ALL
SELECT #delivery.PAT_ID,
       #delivery.MRN,
       #delivery.PAT_NAME,
       #delivery.PCP,
       #delivery.STATE 'State',
       #delivery.CITY 'City',
	   #delivery.SERVICE_TYPE 'Service Type',
	   #delivery.SERVICE_LINE 'Service Line', 
	   #delivery.SUB_SERVICE_LINE 'Sub-Service Line',
       #delivery.LAST_A1c,
       #delivery.RESULT_DATE,
       #delivery.[Controlled_A1C_<_7],
       #delivery.[Poor_A1C_8+],
       #delivery.NEXT_APPT,
       #delivery.NEXT_APPT_PROV,
       #delivery.[NEXT PCP APPT],
       #delivery.[PCP APPT PROVIDER],
       #delivery.[IN CLINICAL PHARMACY COHORT],
       #delivery.[IN DIETITIAN CARE],
       #delivery.RACE,
       #delivery.ETHNICITY,
       #delivery.MINI_COG_COMPLETED,
       #delivery.MINI_COG_SCORE,
       #delivery.[Had a Dental Visit(s)],
       #delivery.HEALTH_MAINTENANCE_DUE_DATE__dm_microalbumin AS HEALTH_MAINTENANCE_DUE_DATE,
       #delivery.HEALTH_MAINTENANCE_TOPIC_NAME__dm_microalbumin AS HEALTH_MAINTENANCE_TOPIC_NAME,
       #delivery.HEALTH_MAINTENANCE_STATUS__dm_microalbumin AS HEALTH_MAINTENANCE_STATUS
FROM #delivery
UNION ALL
SELECT #delivery.PAT_ID,
       #delivery.MRN,
       #delivery.PAT_NAME,
       #delivery.PCP,
       #delivery.STATE 'State',
       #delivery.CITY 'City',
	   #delivery.SERVICE_TYPE 'Service Type',
	   #delivery.SERVICE_LINE 'Service Line', 
	   #delivery.SUB_SERVICE_LINE 'Sub-Service Line',
       #delivery.LAST_A1c,
       #delivery.RESULT_DATE,
       #delivery.[Controlled_A1C_<_7],
       #delivery.[Poor_A1C_8+],
       #delivery.NEXT_APPT,
       #delivery.NEXT_APPT_PROV,
       #delivery.[NEXT PCP APPT],
       #delivery.[PCP APPT PROVIDER],
       #delivery.[IN CLINICAL PHARMACY COHORT],
       #delivery.[IN DIETITIAN CARE],
       #delivery.RACE,
       #delivery.ETHNICITY,
       #delivery.MINI_COG_COMPLETED,
       #delivery.MINI_COG_SCORE,
       #delivery.[Had a Dental Visit(s)],
       #delivery.HEALTH_MAINTENANCE_DUE_DATE__oral_eval_for_diabetic_adults AS HEALTH_MAINTENANCE_DUE_DATE,
       #delivery.HEALTH_MAINTENANCE_TOPIC_NAME__oral_eval_for_diabetic_adults AS HEALTH_MAINTENANCE_TOPIC_NAME,
       #delivery.HEALTH_MAINTENANCE_STATUS__oral_eval_for_diabetic_adults AS HEALTH_MAINTENANCE_STATUS
FROM #delivery
ORDER BY MRN;


DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
