/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Patients w MDD and C-SSRS in Last 12 Months
 Create Date: 5/30/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  

 Purpose:   

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 6/21/2022      Mitch       Adding visit counts for minimum visit filter in Tableau
 1/27/2023      Mitch       Update inclusion criteria to include BH/MH episodes that were closed during the last 12 months
 1/27/2023      Mitch       Update the check for last visit to 12 months for both Psych and MHT
 12/11/2023     Benzon      Refactored
 2/1/2024       Benzon      Updated to remove BH_PROVIDER, add Last Visit Provider and Date
 2/15/2024      Benzon      Updated to follow Measure Description as closely as possible
 3/05/2024      Benzon      Added race and ethnicity information

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

/*
Visits within the last 12 months
*/
IF OBJECT_ID('tempdb..#pat_enc_dep_los') IS NOT NULL DROP TABLE #pat_enc_dep_los;
SELECT PAT_ENC.PAT_ID,
       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS LAST_OFFICE_VISIT,
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
           WHEN 'DN' THEN 'DENVER'
           WHEN 'AS' THEN 'AUSTIN'
           WHEN 'KC' THEN 'KANSAS CITY'
           WHEN 'CG' THEN 'CHICAGO'
           ELSE 'ERROR'
       END AS CITY,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 7, 2)
           WHEN 'MN' THEN 'MAIN LOCATION'
           WHEN 'DR' THEN 'D&R'
           WHEN 'KE' THEN 'KEENEN'
           WHEN 'UC' THEN 'UNIVERSITY OF COLORADO'
           WHEN 'ON' THEN 'AUSTIN MAIN'
           WHEN 'TW' THEN 'AUSTIN OTHER'
           ELSE 'ERROR'
       END AS SITE,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2)
           WHEN 'MD' THEN 'MEDICAL'
           WHEN 'DT' THEN 'DENTAL'
           WHEN 'CM' THEN 'CASE MANAGEMENT'
           WHEN 'RX' THEN 'PHARMACY'
           WHEN 'AD' THEN 'SUS'
           WHEN 'PY' THEN 'Psych'
           WHEN 'BH' THEN 'Mental Health Therapy'
           WHEN 'MH' THEN 'Mental Health Therapy'
           ELSE 'ERROR'
       END AS LOS,
       CLARITY_SER.PROV_NAME AS VISIT_PROVIDER,
       PAT_ENC.PAT_ENC_CSN_ID
INTO #pat_enc_dep_los
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
WHERE PAT_ENC.CONTACT_DATE BETWEEN '10/1/2023' AND '9/30/2024'
      AND PAT_ENC.APPT_STATUS_C IN ( 2, 6 );


IF OBJECT_ID('tempdb..#full_bh_patients') IS NOT NULL DROP TABLE #full_bh_patients;
WITH
    active_psych_patients AS (
        /**
       * Patients who have an active open episode of care of the type "Mental Health"
       * AND 
       * has completed a Psychiatric office visit in a BH department within the last 180 days
       */
        SELECT #pat_enc_dep_los.PAT_ID,
               #pat_enc_dep_los.CITY,
               #pat_enc_dep_los.STATE,
               #pat_enc_dep_los.LOS,
               #pat_enc_dep_los.VISIT_PROVIDER,
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
            INNER JOIN CLARITY.dbo.EPISODE_LINK_VIEW AS EPISODE_LINK ON #pat_enc_dep_los.PAT_ENC_CSN_ID = EPISODE_LINK.PAT_ENC_CSN_ID
            INNER JOIN CLARITY.dbo.EPISODE_VIEW AS EPISODE ON EPISODE_LINK.EPISODE_ID = EPISODE.EPISODE_ID
        WHERE EPISODE.STATUS_C = 1
              AND EPISODE.SUM_BLK_TYPE_ID = 221
              AND #pat_enc_dep_los.LOS = 'Psych'
              AND DATEDIFF(DAY, #pat_enc_dep_los.LAST_OFFICE_VISIT, '9/30/2024') <= 180
    ),
    active_mental_health_patients AS (
        /**
       * Patients who have an active open episode of care of the type "Mental Health"
       * AND 
       * has completed a Mental Health office visit in a BH department within the last 90 days
       */
        SELECT #pat_enc_dep_los.PAT_ID,
               #pat_enc_dep_los.CITY,
               #pat_enc_dep_los.STATE,
               #pat_enc_dep_los.LOS,
               #pat_enc_dep_los.VISIT_PROVIDER,
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
            INNER JOIN CLARITY.dbo.EPISODE_LINK_VIEW AS EPISODE_LINK ON #pat_enc_dep_los.PAT_ENC_CSN_ID = EPISODE_LINK.PAT_ENC_CSN_ID
            INNER JOIN CLARITY.dbo.EPISODE_VIEW AS EPISODE ON EPISODE_LINK.EPISODE_ID = EPISODE.EPISODE_ID
        WHERE EPISODE.STATUS_C = 1
              AND EPISODE.SUM_BLK_TYPE_ID = 221
              AND #pat_enc_dep_los.LOS IN ( 'SUS', 'Mental Health Therapy' )
              AND DATEDIFF(DAY, #pat_enc_dep_los.LAST_OFFICE_VISIT, '9/30/2024') <= 90
    ),
    full_bh_cohort AS (
        SELECT active_psych_patients.PAT_ID,
               active_psych_patients.CITY,
               active_psych_patients.STATE,
               active_psych_patients.LOS,
               active_psych_patients.VISIT_PROVIDER,
               active_psych_patients.LAST_OFFICE_VISIT
        FROM active_psych_patients
        WHERE active_psych_patients.ROW_NUM_DESC = 1
        UNION
        SELECT active_mental_health_patients.PAT_ID,
               active_mental_health_patients.CITY,
               active_mental_health_patients.STATE,
               active_mental_health_patients.LOS,
               active_mental_health_patients.VISIT_PROVIDER,
               active_mental_health_patients.LAST_OFFICE_VISIT
        FROM active_mental_health_patients
        WHERE active_mental_health_patients.ROW_NUM_DESC = 1
    ),
    preproc AS (
        SELECT full_bh_cohort.PAT_ID,
               full_bh_cohort.CITY,
               full_bh_cohort.STATE,
               full_bh_cohort.LOS,
               full_bh_cohort.VISIT_PROVIDER,
               full_bh_cohort.LAST_OFFICE_VISIT,
               ROW_NUMBER() OVER (PARTITION BY full_bh_cohort.PAT_ID
                                  ORDER BY full_bh_cohort.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM full_bh_cohort
    )
SELECT preproc.PAT_ID,
       preproc.CITY,
       preproc.STATE,
       preproc.LOS,
       preproc.VISIT_PROVIDER,
       preproc.LAST_OFFICE_VISIT
INTO #full_bh_patients
FROM preproc
WHERE preproc.ROW_NUM_DESC = 1;




/**
 * All patients with Suicide Risk Assessment screening within the last 12 months
 */
IF OBJECT_ID('tempdb..#ssrs') IS NOT NULL DROP TABLE #ssrs;
SELECT PATIENT.PAT_ID,
       'Met' AS OUTCOME,
       CAST(IP_FLWSHT_MEAS.RECORDED_TIME AS DATE) AS DATE_OF_LAST_SCREENER,
       ROW_NUMBER() OVER (PARTITION BY PATIENT.PAT_ID
ORDER BY IP_FLWSHT_MEAS.RECORDED_TIME DESC) AS ROW_NUM_DESC
INTO #ssrs
FROM CLARITY.dbo.PATIENT_VIEW AS PATIENT
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON PATIENT.PAT_ID = IDENTITY_ID.PAT_ID
    INNER JOIN CLARITY.dbo.IP_FLWSHT_REC_VIEW AS IP_FLWSHT_REC ON PATIENT.PAT_ID = IP_FLWSHT_REC.PAT_ID
    LEFT JOIN CLARITY.dbo.IP_FLWSHT_MEAS_VIEW AS IP_FLWSHT_MEAS ON IP_FLWSHT_REC.FSD_ID = IP_FLWSHT_MEAS.FSD_ID
    LEFT JOIN CLARITY.dbo.IP_FLO_GP_DATA AS IP_FLO_GP_DATA ON IP_FLWSHT_MEAS.FLO_MEAS_ID = IP_FLO_GP_DATA.FLO_MEAS_ID
WHERE IP_FLWSHT_MEAS.RECORDED_TIME BETWEEN '10/1/2023' AND '9/30/2024'
      AND IP_FLWSHT_MEAS.FLO_MEAS_ID = '4918';


/**
 * All patiens with Major Depressive Disorder diagnosis
 */
IF OBJECT_ID('tempdb..#patients_w_mdd_dx') IS NOT NULL DROP TABLE #patients_w_mdd_dx;
SELECT PROBLEM_LIST.PAT_ID,
       PAT_ENC.PAT_ENC_CSN_ID,
       CLARITY_SER.PROV_NAME AS BH_PROVIDER,
       ROW_NUMBER() OVER (PARTITION BY PROBLEM_LIST.PAT_ID ORDER BY PAT_PCP.EFF_DATE DESC) AS ROW_NUM_DESC
INTO #patients_w_mdd_dx
FROM CLARITY.dbo.PROBLEM_LIST_VIEW AS PROBLEM_LIST
    INNER JOIN CLARITY.dbo.CLARITY_EDG AS CLARITY_EDG ON PROBLEM_LIST.DX_ID = CLARITY_EDG.DX_ID
    INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON CLARITY_EDG.DX_ID = EDG_CURRENT_ICD10.DX_ID
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON PROBLEM_LIST.PAT_ID = PAT_ENC.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.PAT_PCP_VIEW AS PAT_PCP ON PROBLEM_LIST.PAT_ID = PAT_PCP.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_PCP.PCP_PROV_ID = CLARITY_SER.PROV_ID
    INNER JOIN CLARITY.dbo.EPISODE_VIEW AS EPISODE ON PROBLEM_LIST.PAT_ID = EPISODE.PAT_LINK_ID
    INNER JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON EPISODE.L_UPDATE_USER_ID = CLARITY_EMP.USER_ID
WHERE EDG_CURRENT_ICD10.CODE IN ( 'F32.4', 'F32.5', 'F32.9', 'F33', 'F33.0', 'F33.1', 'F33.2', 'F33.3', 'F33.4', 'F33.41', 'F33.42', 'F33.9' ) -- Provided BY Caren E. (MDD list)
      AND (PROBLEM_LIST.RESOLVED_DATE IS NULL
           OR PROBLEM_LIST.RESOLVED_DATE > DATEADD(MONTH, -12, '9/30/2024'))
      AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) IN ( 'MH', 'BH', 'PY' )
      AND PAT_ENC.CONTACT_DATE > DATEADD(MONTH, -12, '9/30/2024')
      AND PAT_PCP.RELATIONSHIP_C IN ( 1, 3 )
      AND (PAT_PCP.TERM_DATE IS NULL
           OR PAT_PCP.TERM_DATE > DATEADD(MONTH, -12, '9/30/2024'))
      AND EPISODE.SUM_BLK_TYPE_ID = 221
      AND (EPISODE.END_DATE IS NULL
           OR EPISODE.END_DATE > DATEADD(MONTH, -12, '9/30/2024'));


IF OBJECT_ID('tempdb..#scheduled_visits') IS NOT NULL DROP TABLE #scheduled_visits;
SELECT PAT_ENC.PAT_ID,
       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_ANY_APPT,
       CLARITY_SER.PROV_NAME AS NEXT_APPT_PROV,
       ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
INTO #scheduled_visits
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
WHERE PAT_ENC.APPT_STATUS_C = 1;


IF OBJECT_ID('tempdb..#scheduled_pcp_visits') IS NOT NULL DROP TABLE #scheduled_pcp_visits;
SELECT PAT_ENC.PAT_ID,
       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_PCP_APPT,
       CLARITY_SER.PROV_NAME AS NEXT_PCP_APPT_PROV,
       ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
INTO #scheduled_pcp_visits
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
WHERE PAT_ENC.APPT_STATUS_C = 1
      AND CLARITY_SER.PROV_ID <> '640178' -- Pulmonologist
      AND CLARITY_SER.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ); -- Physicians, PAs, and NPs


WITH
    mdd_dx_visit_count AS (
        SELECT #patients_w_mdd_dx.PAT_ID,
               COUNT(DISTINCT #patients_w_mdd_dx.PAT_ENC_CSN_ID) AS TOTAL_VISITS
        FROM #patients_w_mdd_dx
        GROUP BY PAT_ID
    ),
    patients_w_mdd_dx AS (
        SELECT #patients_w_mdd_dx.PAT_ID,
               #patients_w_mdd_dx.BH_PROVIDER
        FROM #patients_w_mdd_dx
        WHERE #patients_w_mdd_dx.ROW_NUM_DESC = 1
    ),
    all_patients_info_w_mdd_dx AS (
        SELECT patients_w_mdd_dx.PAT_ID,
               patients_w_mdd_dx.BH_PROVIDER,
               mdd_dx_visit_count.TOTAL_VISITS
        FROM patients_w_mdd_dx
            INNER JOIN mdd_dx_visit_count ON patients_w_mdd_dx.PAT_ID = mdd_dx_visit_count.PAT_ID
    )
SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       PATIENT.PAT_NAME AS [Patient],
       PATIENT.ZIP,
       --COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE_CATEGORY,
       --COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY_CATEGORY,
       #scheduled_visits.NEXT_ANY_APPT AS [Next Any Appt],
       #scheduled_visits.NEXT_APPT_PROV AS [Next Appt Prov],
       #scheduled_pcp_visits.NEXT_PCP_APPT AS [Next PCP Appt],
       #scheduled_pcp_visits.NEXT_PCP_APPT_PROV AS [Next PCP Appt Prov],
       #full_bh_patients.CITY,
       #full_bh_patients.STATE,
       #full_bh_patients.LOS,
       #full_bh_patients.VISIT_PROVIDER AS [Last Visit Provider],
       #full_bh_patients.LAST_OFFICE_VISIT AS [Last Office Visit],
       COALESCE(#ssrs.OUTCOME, 'Not Met') AS OUTCOME,
       #ssrs.DATE_OF_LAST_SCREENER,
       all_patients_info_w_mdd_dx.TOTAL_VISITS
FROM CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
    INNER JOIN #full_bh_patients ON PATIENT.PAT_ID = #full_bh_patients.PAT_ID
    INNER JOIN all_patients_info_w_mdd_dx ON #full_bh_patients.PAT_ID = all_patients_info_w_mdd_dx.PAT_ID
    LEFT JOIN #ssrs ON #full_bh_patients.PAT_ID = #ssrs.PAT_ID
                       AND #ssrs.ROW_NUM_DESC = 1
    LEFT JOIN #scheduled_visits ON #full_bh_patients.PAT_ID = #scheduled_visits.PAT_ID
                                   AND #scheduled_visits.ROW_NUM_ASC = 1
    LEFT JOIN #scheduled_pcp_visits ON #full_bh_patients.PAT_ID = #scheduled_pcp_visits.PAT_ID
                                       AND #scheduled_pcp_visits.ROW_NUM_ASC = 1
    --LEFT JOIN ##patient_race_ethnicity ON PATIENT.PAT_ID = ##patient_race_ethnicity.PAT_ID;
