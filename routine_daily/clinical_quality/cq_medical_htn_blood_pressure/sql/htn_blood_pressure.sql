/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Medical 8 - Pts with hypertension and BP less than 140 over 90
 Create Date: 8/31/2018
 Created By:  scogginsm
 System:    javelin.ochin.org
 Requested By:  Internal Dashboard

 Purpose:   �Percentage of patients whose blood pressure was adequately controlled - < 140/90.�

 Description: DENOM: Patients who have a diagnosis of hypertension 
        NUM: Has a BP documented in the past 12 months AND Blood pressure less than 140/90
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 1/17/2019      Jaya        Include MO
 8/2/2019     Mitch       Update for living pt check in the PATIENT_4 table
 4/15/2019      Mitch       Updating Active Patient logic to include Dx codes and LOS_PRIME_PROC_ID 
 02/24/2020     Jaya        Updated to new Department name logic
 9/29/2020      Mitch       Alteryx
 02/02/2021     Jaya        Added PA to the Provider_Type_C
 07/02/2021     Jaya        Added Dietitian flag
 10/1/2021      Mitch       Adding CP flags
 2/3/2022     Mitch       Adding new CP flag for Pre-DM
 2/24/2022      Mitch       Adding Ethnicity
 1/11/2024     Benzon       Restructure
**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

/* Capture attribution to department LOs */
IF OBJECT_ID('tempdb..#pat_enc_dep_los') IS NOT NULL DROP TABLE #pat_enc_dep_los;
SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
       SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2) AS STATE,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 5, 2)
           WHEN 'MK' THEN 'MILWAUKEE'
           WHEN 'KN' THEN 'KENOSHA'
           WHEN 'GB' THEN 'GREEN BAY'
           WHEN 'WS' THEN 'WAUSAU'
           WHEN 'AP' THEN 'APPLETON'
           WHEN 'EC' THEN 'EAU CLAIRE'
           WHEN 'LC' THEN 'LACROSSE'
           WHEN 'MD' THEN 'MADISON'
           WHEN 'BL' THEN 'BELOIT'
           WHEN 'BI' THEN 'BILLING'
           WHEN 'SL' THEN 'ST LOUIS'
           WHEN 'KC' THEN 'KANSAS CITY'
           WHEN 'DN' THEN 'DENVER'
           WHEN 'AS' THEN 'AUSTIN'
		   WHEN 'CG' THEN 'CHICAGO'
           ELSE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 5, 2)
       END AS CITY,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 7, 2)
           WHEN 'MN' THEN 'MAIN LOCATION'
           WHEN 'DR' THEN 'D&R'
           WHEN 'KE' THEN 'KEENEN'
           WHEN 'UC' THEN 'UNIVERSITY OF COLORADO'
           WHEN 'ON' THEN 'AUSTIN MAIN'
           WHEN 'TW' THEN 'AUSTIN OTHER'
           ELSE 'ERROR'
       END AS 'SITE',
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2)
           WHEN 'MD' THEN 'MEDICAL'
           WHEN 'DT' THEN 'DENTAL'
           WHEN 'CM' THEN 'CASE MANAGEMENT'
           WHEN 'RX' THEN 'PHARMACY'
           WHEN 'AD' THEN 'BEHAVIORAL'
           WHEN 'PY' THEN 'BEHAVIORAL'
           WHEN 'BH' THEN 'BEHAVIORAL'
           WHEN 'MH' THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS 'LOS',
       CLARITY_DEP.DEPARTMENT_NAME
INTO #pat_enc_dep_los
FROM CLARITY.dbo.PAT_ENC_VIEW pev
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.APPT_STATUS_C IN ( 2, 6 )
      AND DATEDIFF(MONTH, pev.CONTACT_DATE, GETDATE()) <= 12;


/* Capture Medical Home patients */
IF OBJECT_ID('tempdb..#medical_patients') IS NOT NULL DROP TABLE #medical_patients;
WITH
    target_service_line AS (
        SELECT #pat_enc_dep_los.PAT_ID,
               #pat_enc_dep_los.STATE,
               #pat_enc_dep_los.CITY,
               #pat_enc_dep_los.SITE,
               #pat_enc_dep_los.LOS,
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
               #pat_enc_dep_los.DEPARTMENT_NAME,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
        WHERE #pat_enc_dep_los.LOS = 'MEDICAL'
    )
SELECT target_service_line.PAT_ID,
       target_service_line.LOS,
       target_service_line.CITY,
       target_service_line.STATE,
       target_service_line.SITE,
       target_service_line.DEPARTMENT_NAME
INTO #medical_patients
FROM target_service_line
WHERE target_service_line.ROW_NUM_DESC = 1;

IF OBJECT_ID('tempdb..#active_hiv_patients') IS NOT NULL DROP TABLE #active_hiv_patients;
SELECT DISTINCT pev.PAT_ID
INTO #active_hiv_patients
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
WHERE ser.SERV_AREA_ID = 64
      AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
      AND pev.CONTACT_DATE > DATEADD(MM, -12, CURRENT_TIMESTAMP) --Visit in past year
      AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                     8053, 8054, 8055, 8056 ) -- Office Visits
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
      AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
      AND p4.PAT_LIVING_STAT_C = 1;

IF OBJECT_ID('tempdb..#denominator') IS NOT NULL DROP TABLE #denominator;
SELECT #active_hiv_patients.PAT_ID,
       #medical_patients.LOS,
       #medical_patients.CITY,
       #medical_patients.STATE,
       #medical_patients.DEPARTMENT_NAME
INTO #denominator
FROM #active_hiv_patients
    INNER JOIN #medical_patients ON #active_hiv_patients.PAT_ID = #medical_patients.PAT_ID;

IF OBJECT_ID('tempdb..#datamart_info') IS NOT NULL DROP TABLE #datamart_info;
WITH
    dietitian_care AS (
        SELECT PATIENT_FYI_FLAGS.PATIENT_ID AS PAT_ID,
               MAX(PATIENT_FYI_FLAGS.ACTIVE_C) AS ACTIVE
        FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS
        WHERE PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640025' -- HTN- Dietitian care
              AND PATIENT_FYI_FLAGS.ACTIVE_C = 1
        GROUP BY PATIENT_FYI_FLAGS.PATIENT_ID
    ),
    in_cp_cohort AS (
        SELECT PATIENT_FYI_FLAGS.PATIENT_ID AS PAT_ID,
               MAX(PATIENT_FYI_FLAGS.ACTIVE_C) AS ACTIVE
        FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS
        WHERE PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C = '640012' -- 'SA64 Pharmacist - HTN'
              AND PATIENT_FYI_FLAGS.ACTIVE_C = 1
        GROUP BY PATIENT_FYI_FLAGS.PATIENT_ID
    )
SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       #denominator.PAT_ID,
       #denominator.CITY,
       #denominator.STATE,
       CAST(DM_WLL_ALL.BP_SYS_LAST_DT AS DATE) AS LAST_BP,
       DM_WLL_ALL.BP_SYS_LAST AS LATEST_SYSTOLIC,
       DM_WLL_ALL.BP_DIA_LAST AS LATEST_DIASTOLIC,
       COALESCE(CLARITY_SER.EXTERNAL_NAME, 'No Assigned PCP') AS PCP_NAME,
       IIF(in_cp_cohort.PAT_ID IS NOT NULL, 'YES', 'NO') AS IN_CLINICAL_PHARMACY_COHORT,
       IIF(dietitian_care.PAT_ID IS NOT NULL, 'YES', 'NO') AS IN_DIETITIAN_CARE
INTO #datamart_info
FROM #denominator
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON #denominator.PAT_ID = PATIENT.PAT_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON #denominator.PAT_ID = IDENTITY_ID.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PATIENT.CUR_PCP_PROV_ID = CLARITY_SER.PROV_ID
    INNER JOIN CLARITY.dbo.DM_WLL_ALL_VIEW AS DM_WLL_ALL ON #denominator.PAT_ID = DM_WLL_ALL.PAT_ID
    LEFT JOIN dietitian_care ON #denominator.PAT_ID = dietitian_care.PAT_ID
    LEFT JOIN in_cp_cohort ON #denominator.PAT_ID = in_cp_cohort.PAT_ID
WHERE DM_WLL_ALL.HAS_HYPERTENSION_YN = 'Y';


WITH
    scheduled_visits AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_ANY_APPT,
               CLARITY_SER.PROV_NAME AS NEXT_APPT_PROV,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1 --Scheduled
    ),
    scheduled_pcp_visits AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_PCP_APPT,
               CLARITY_SER.PROV_NAME AS NEXT_PCP_PROV,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1 --Scheduled 
              AND CLARITY_SER.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs
    ),
    episode_activities AS (
        SELECT EPISODE.PAT_LINK_ID AS PAT_ID,
               MAX(CASE WHEN EPISODE.SUM_BLK_TYPE_ID = 221 THEN 'Y' ELSE 'N' END) AS ACTIVE_BH,
               MAX(CASE WHEN EPISODE.SUM_BLK_TYPE_ID = 45 THEN 'Y' ELSE 'N' END) AS ACTIVE_DENTAL
        FROM Clarity.dbo.EPISODE_VIEW AS EPISODE
            INNER JOIN Clarity.dbo.CLARITY_EMP_VIEW CLARITY_EMP ON EPISODE.L_UPDATE_USER_ID = CLARITY_EMP.USER_ID
        WHERE EPISODE.SUM_BLK_TYPE_ID IN ( 45, 221 ) -- Dental and BH
              AND EPISODE.STATUS_C = 1
        GROUP BY EPISODE.PAT_LINK_ID
    ),
    diabetic_dialysis AS (
        SELECT DISTINCT PROBLEM_LIST.PAT_ID
        FROM Clarity.dbo.PROBLEM_LIST_VIEW AS PROBLEM_LIST
            INNER JOIN Clarity.dbo.CLARITY_EDG AS CLARITY_EDG ON CLARITY_EDG.DX_ID = PROBLEM_LIST.DX_ID
            INNER JOIN Clarity.dbo.GROUPER_COMPILED_REC_LIST AS GROUPER_COMPILED_REC_LIST ON GROUPER_COMPILED_REC_LIST.GROUPER_RECORDS_NUMERIC_ID = CLARITY_EDG.DX_ID
            INNER JOIN Clarity.dbo.GROUPER_ITEMS AS GROUPER_ITEMS ON GROUPER_COMPILED_REC_LIST.BASE_GROUPER_ID = GROUPER_ITEMS.GROUPER_ID
        WHERE GROUPER_ITEMS.GROUPER_ID IN ( '107089' /*Diabetic Nephropathy*/, '5200000134' /*Dialysis*/ )
              AND PROBLEM_LIST.RESOLVED_DATE IS NULL
              AND PROBLEM_LIST.PROBLEM_STATUS_C = 1
    ),
    latest_visits_info AS (
        SELECT PAT_ENC.PAT_ID,
               CLARITY_SER.PROV_TYPE,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE DESC) AS ROW_NUM_DESC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
              AND DATEDIFF(MONTH, PAT_ENC.CONTACT_DATE, GETDATE()) <= 12
    )
SELECT #datamart_info.MRN AS IDENTITY_ID,
       #datamart_info.PAT_ID,
       PATIENT.PAT_NAME,
       #datamart_info.STATE,
       #datamart_info.CITY,
       #datamart_info.PCP_NAME AS PROV_NAME,
       #datamart_info.LAST_BP,
       #datamart_info.LATEST_SYSTOLIC,
       #datamart_info.LATEST_DIASTOLIC,
       #datamart_info.IN_CLINICAL_PHARMACY_COHORT AS [IN CLINICAL PHARM COHORT],
       COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY,
       CASE WHEN #datamart_info.LATEST_SYSTOLIC < 140
                 AND #datamart_info.LATEST_DIASTOLIC < 90 THEN 1
           ELSE 0
       END AS MET_YN,
       CASE WHEN #datamart_info.LATEST_SYSTOLIC < 140
                 AND #datamart_info.LATEST_DIASTOLIC < 90 THEN 'MET'
           ELSE 'NOTMET'
       END AS OUTCOME,
       #datamart_info.IN_DIETITIAN_CARE AS [IN DIETITIAN CARE],
       COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE,
       scheduled_visits.NEXT_ANY_APPT AS [Next Any Appt],
       scheduled_visits.NEXT_APPT_PROV AS [Next Appt Prov],
       scheduled_pcp_visits.NEXT_PCP_APPT AS [Next PCP Appt],
       scheduled_pcp_visits.NEXT_PCP_PROV AS [Next PCP Appt Prov],
       COALESCE(episode_activities.ACTIVE_BH, 'N') AS [Active BH],
       COALESCE(episode_activities.ACTIVE_DENTAL, 'N') AS [Active Dental],
       latest_visits_info.PROV_TYPE AS LATEST_VISIT_PROV_TYPE
FROM #datamart_info
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON #datamart_info.PAT_ID = PATIENT.PAT_ID
    INNER JOIN latest_visits_info ON #datamart_info.PAT_ID = latest_visits_info.PAT_ID
                                     AND latest_visits_info.ROW_NUM_DESC = 1
    LEFT JOIN ##patient_race_ethnicity ON PATIENT.PAT_ID = ##patient_race_ethnicity.PAT_ID
    LEFT JOIN scheduled_visits ON #datamart_info.PAT_ID = scheduled_visits.PAT_ID
                                  AND scheduled_visits.ROW_NUM_ASC = 1
    LEFT JOIN scheduled_pcp_visits ON #datamart_info.PAT_ID = scheduled_pcp_visits.PAT_ID
                                      AND scheduled_pcp_visits.ROW_NUM_ASC = 1
    LEFT JOIN episode_activities ON #datamart_info.PAT_ID = episode_activities.PAT_ID
WHERE #datamart_info.PAT_ID NOT IN ( SELECT diabetic_dialysis.PAT_ID FROM diabetic_dialysis );
