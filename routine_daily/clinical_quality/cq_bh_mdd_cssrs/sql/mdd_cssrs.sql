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
 1/10/2025		Mitch		Removing MDD dx as a criterion for inclusion, wants all BH pts
 12/04/2025     Hari        Added MHT and Psychiatrist care team tracking

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
       PAT_ENC.DEPARTMENT_ID,
       CLARITY_SER.PROVIDER_TYPE_C,
       CLARITY_DEP.STATE,
       CLARITY_DEP.CITY,
       CLARITY_DEP.SITE,
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
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = pat_enc.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
WHERE DATEDIFF(MONTH, PAT_ENC.CONTACT_DATE, GETDATE()) <= 12
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
               #pat_enc_dep_los.DEPARTMENT_ID,
               #pat_enc_dep_los.PROVIDER_TYPE_C,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
            INNER JOIN CLARITY.dbo.EPISODE_LINK_VIEW AS EPISODE_LINK ON #pat_enc_dep_los.PAT_ENC_CSN_ID = EPISODE_LINK.PAT_ENC_CSN_ID
            INNER JOIN CLARITY.dbo.EPISODE_VIEW AS EPISODE ON EPISODE_LINK.EPISODE_ID = EPISODE.EPISODE_ID
        WHERE EPISODE.STATUS_C = 1
              AND EPISODE.SUM_BLK_TYPE_ID = 221
              AND #pat_enc_dep_los.LOS = 'Psych'
              AND DATEDIFF(DAY, #pat_enc_dep_los.LAST_OFFICE_VISIT, GETDATE()) <= 180
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
               #pat_enc_dep_los.DEPARTMENT_ID,
               #pat_enc_dep_los.PROVIDER_TYPE_C,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
            INNER JOIN CLARITY.dbo.EPISODE_LINK_VIEW AS EPISODE_LINK ON #pat_enc_dep_los.PAT_ENC_CSN_ID = EPISODE_LINK.PAT_ENC_CSN_ID
            INNER JOIN CLARITY.dbo.EPISODE_VIEW AS EPISODE ON EPISODE_LINK.EPISODE_ID = EPISODE.EPISODE_ID
        WHERE EPISODE.STATUS_C = 1
              AND EPISODE.SUM_BLK_TYPE_ID = 221
              AND #pat_enc_dep_los.LOS IN ( 'SUS', 'Mental Health Therapy' )
              AND DATEDIFF(DAY, #pat_enc_dep_los.LAST_OFFICE_VISIT, GETDATE()) <= 90
    ),
    full_bh_cohort AS (
        SELECT active_psych_patients.PAT_ID,
               active_psych_patients.CITY,
               active_psych_patients.STATE,
               active_psych_patients.LOS,
               active_psych_patients.VISIT_PROVIDER,
               active_psych_patients.LAST_OFFICE_VISIT,
               active_psych_patients.DEPARTMENT_ID,
               active_psych_patients.PROVIDER_TYPE_C
        FROM active_psych_patients
        WHERE active_psych_patients.ROW_NUM_DESC = 1
        UNION
        SELECT active_mental_health_patients.PAT_ID,
               active_mental_health_patients.CITY,
               active_mental_health_patients.STATE,
               active_mental_health_patients.LOS,
               active_mental_health_patients.VISIT_PROVIDER,
               active_mental_health_patients.LAST_OFFICE_VISIT,
               active_mental_health_patients.DEPARTMENT_ID,
               active_mental_health_patients.PROVIDER_TYPE_C
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
               full_bh_cohort.DEPARTMENT_ID,
               full_bh_cohort.PROVIDER_TYPE_C,
               ROW_NUMBER() OVER (PARTITION BY full_bh_cohort.PAT_ID
                                  ORDER BY full_bh_cohort.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM full_bh_cohort
    )
SELECT preproc.PAT_ID,
       preproc.CITY,
       preproc.STATE,
       preproc.LOS,
       preproc.VISIT_PROVIDER,
       preproc.LAST_OFFICE_VISIT,
       preproc.DEPARTMENT_ID,
       preproc.PROVIDER_TYPE_C
INTO #full_bh_patients
FROM preproc
WHERE preproc.ROW_NUM_DESC = 1;


-- *** NEW: Mental Health Therapist assignments ***
IF OBJECT_ID('tempdb..#mht') IS NOT NULL DROP TABLE #mht;
WITH mht_ranked AS (
    SELECT ct.PAT_ID,
           ser.PROV_NAME AS [MH Therapist],
           ROW_NUMBER() OVER (PARTITION BY ct.PAT_ID ORDER BY ct.EFF_DATE DESC) AS rn
    FROM Clarity.dbo.PAT_PCP_VIEW ct
        INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
    WHERE ct.TERM_DATE IS NULL
          AND ct.DELETED_YN = 'N'
          AND ser.PROVIDER_TYPE_C IN ('171', '117', '134', '10', '110', '177', '175', '227')
)
SELECT PAT_ID, [MH Therapist]
INTO #mht
FROM mht_ranked
WHERE rn = 1;


-- *** NEW: Psychiatrist assignments ***
IF OBJECT_ID('tempdb..#psych') IS NOT NULL DROP TABLE #psych;
WITH psych_ranked AS (
    SELECT ct.PAT_ID,
           ser.PROV_NAME AS [Psychiatrist],
           ROW_NUMBER() OVER (PARTITION BY ct.PAT_ID ORDER BY ct.EFF_DATE DESC) AS rn
    FROM Clarity.dbo.PAT_PCP_VIEW ct
        INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
    WHERE ct.TERM_DATE IS NULL
          AND ct.DELETED_YN = 'N'
          AND ser.PROVIDER_TYPE_C IN ('136', '164', '129')
)
SELECT PAT_ID, [Psychiatrist]
INTO #psych
FROM psych_ranked
WHERE rn = 1;


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
WHERE DATEDIFF(MONTH, IP_FLWSHT_MEAS.RECORDED_TIME, GETDATE()) <= 12
      AND IP_FLWSHT_MEAS.FLO_MEAS_ID = '4918';


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


-- *** UPDATED: Final SELECT with care team assignments ***
SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       PATIENT.PAT_NAME AS [Patient],
       PATIENT.ZIP,
       COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE_CATEGORY,
       COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY_CATEGORY,
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
       -99 AS [Total Visits],
       #full_bh_patients.DEPARTMENT_ID AS [Department ID],
       #full_bh_patients.PROVIDER_TYPE_C,
       #mht.[MH Therapist],
       IIF(#mht.PAT_ID IS NOT NULL, 1, 0) AS is_mht_assigned,
       #psych.[Psychiatrist],
       IIF(#psych.PAT_ID IS NOT NULL, 1, 0) AS is_psych_assigned
FROM CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
    INNER JOIN #full_bh_patients ON PATIENT.PAT_ID = #full_bh_patients.PAT_ID
    LEFT JOIN #ssrs ON #full_bh_patients.PAT_ID = #ssrs.PAT_ID
                       AND #ssrs.ROW_NUM_DESC = 1
    LEFT JOIN #scheduled_visits ON #full_bh_patients.PAT_ID = #scheduled_visits.PAT_ID
                                   AND #scheduled_visits.ROW_NUM_ASC = 1
    LEFT JOIN #scheduled_pcp_visits ON #full_bh_patients.PAT_ID = #scheduled_pcp_visits.PAT_ID
                                       AND #scheduled_pcp_visits.ROW_NUM_ASC = 1
    LEFT JOIN ##patient_race_ethnicity ON PATIENT.PAT_ID = ##patient_race_ethnicity.PAT_ID
    LEFT JOIN #mht ON PATIENT.PAT_ID = #mht.PAT_ID
    LEFT JOIN #psych ON PATIENT.PAT_ID = #psych.PAT_ID;