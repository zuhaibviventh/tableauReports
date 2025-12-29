SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

/*
Visits within the last 12 months
*/
IF OBJECT_ID('tempdb..#pat_enc_dep_los') IS NOT NULL DROP TABLE #pat_enc_dep_los;
SELECT PAT_ENC.PAT_ID,
       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS LAST_OFFICE_VISIT,
       CLARITY_DEP.CITY,
       CLARITY_DEP.STATE,
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
       PAT_ENC.PAT_ENC_CSN_ID,
       PAT_ENC.DEPARTMENT_ID
INTO #pat_enc_dep_los
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = pat_enc.DEPARTMENT_ID
INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
WHERE DATEDIFF(MONTH, PAT_ENC.CONTACT_DATE, GETDATE()) <= 12
      AND PAT_ENC.APPT_STATUS_C IN ( 2, 6 );


IF OBJECT_ID('tempdb..#full_bh_patients') IS NOT NULL DROP TABLE #full_bh_patients;
WITH
    active_psych_patients AS (
        SELECT #pat_enc_dep_los.PAT_ID,
               #pat_enc_dep_los.CITY,
               #pat_enc_dep_los.STATE,
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
               #pat_enc_dep_los.DEPARTMENT_ID,
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
        SELECT #pat_enc_dep_los.PAT_ID,
               #pat_enc_dep_los.CITY,
               #pat_enc_dep_los.STATE,
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
                #pat_enc_dep_los.DEPARTMENT_ID,
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
               active_psych_patients.LAST_OFFICE_VISIT,
               active_psych_patients.DEPARTMENT_ID
        FROM active_psych_patients
        WHERE active_psych_patients.ROW_NUM_DESC = 1
        UNION
        SELECT active_mental_health_patients.PAT_ID,
               active_mental_health_patients.CITY,
               active_mental_health_patients.STATE,
               active_mental_health_patients.LAST_OFFICE_VISIT,
               active_mental_health_patients.DEPARTMENT_ID
        FROM active_mental_health_patients
        WHERE active_mental_health_patients.ROW_NUM_DESC = 1
    ),
    preproc AS (
        SELECT full_bh_cohort.PAT_ID,
               full_bh_cohort.CITY,
               full_bh_cohort.STATE,
               full_bh_cohort.LAST_OFFICE_VISIT,
               full_bh_cohort.DEPARTMENT_ID,
               ROW_NUMBER() OVER (PARTITION BY full_bh_cohort.PAT_ID
                                  ORDER BY full_bh_cohort.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM full_bh_cohort
    )
SELECT preproc.PAT_ID, preproc.CITY, preproc.STATE, preproc.DEPARTMENT_ID INTO #full_bh_patients FROM preproc WHERE preproc.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#bh_care_teams') IS NOT NULL DROP TABLE #bh_care_teams;
WITH
    episode_info AS (
        SELECT PAT_ENC.PAT_ID,
               CLARITY_SER.PROV_NAME AS [BH Provider],
               CAST(EPISODE.START_DATE AS DATE) AS [Last BH Start Date],
               CAST(EPISODE.END_DATE AS DATE) AS [Last BH End Date],
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID
                                  ORDER BY PAT_PCP.EFF_DATE DESC,
                                           EPISODE.START_DATE DESC) AS ROW_NUM_DESC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
            INNER JOIN CLARITY.dbo.PAT_PCP_VIEW AS PAT_PCP ON PAT_ENC.PAT_ID = PAT_PCP.PAT_ID
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_PCP.PCP_PROV_ID = CLARITY_SER.PROV_ID
            INNER JOIN CLARITY.dbo.EPISODE_VIEW AS EPISODE ON PAT_ENC.PAT_ID = EPISODE.PAT_LINK_ID
            INNER JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON EPISODE.L_UPDATE_USER_ID = CLARITY_EMP.USER_ID
        WHERE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) IN ( 'MH', 'BH', 'PY', 'AD' )
              AND PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
              AND PAT_ENC.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
              AND PAT_PCP.RELATIONSHIP_C IN ('3','1')
              AND (PAT_PCP.TERM_DATE IS NULL
                   OR PAT_PCP.TERM_DATE > DATEADD(MONTH, -12, GETDATE()))
              AND EPISODE.SUM_BLK_TYPE_ID = 221
              AND (EPISODE.END_DATE IS NULL
                   OR EPISODE.END_DATE > DATEADD(MONTH, -12, GETDATE()))
    )
SELECT episode_info.PAT_ID,
       episode_info.[BH Provider],
       episode_info.[Last BH Start Date],
       episode_info.[Last BH End Date]
INTO #bh_care_teams
FROM episode_info
WHERE episode_info.ROW_NUM_DESC = 1;


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


IF OBJECT_ID('tempdb..#gad_7_info') IS NOT NULL DROP TABLE #gad_7_info;
WITH
    gad_info AS (
        SELECT PATIENT.PAT_ID,
               CAST(IP_FLWSHT_MEAS.RECORDED_TIME AS DATE) AS [GAD-7 Date],
               IP_FLWSHT_MEAS.MEAS_VALUE AS [Gad-7 Total Score],
               COALESCE(VISIT_SER.PROV_NAME, CLARITY_EMP.NAME, 'MYCHART BACKGROUND USER') AS [Provider],
               VISIT_SER.PROVIDER_TYPE_C,
               ROW_NUMBER() OVER (PARTITION BY PATIENT.PAT_ID ORDER BY IP_FLWSHT_MEAS.RECORDED_TIME DESC) AS ROW_NUM_DESC
        FROM CLARITY.dbo.PATIENT_VIEW AS PATIENT
            INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON PATIENT.PAT_ID = IDENTITY_ID.PAT_ID
            INNER JOIN CLARITY.dbo.IP_FLWSHT_REC_VIEW AS IP_FLWSHT_REC ON PATIENT.PAT_ID = IP_FLWSHT_REC.PAT_ID
            INNER JOIN CLARITY.dbo.IP_FLWSHT_MEAS_VIEW AS IP_FLWSHT_MEAS ON IP_FLWSHT_REC.FSD_ID = IP_FLWSHT_MEAS.FSD_ID
            INNER JOIN CLARITY.dbo.IP_FLO_GP_DATA AS IP_FLO_GP_DATA ON IP_FLWSHT_MEAS.FLO_MEAS_ID = IP_FLO_GP_DATA.FLO_MEAS_ID
            LEFT JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON IP_FLWSHT_MEAS.TAKEN_USER_ID = CLARITY_EMP.USER_ID
            LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON CLARITY_EMP.PROV_ID = CLARITY_SER.PROV_ID
            INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON IP_FLWSHT_REC.INPATIENT_DATA_ID = PAT_ENC.INPATIENT_DATA_ID
            LEFT  JOIN CLARITY.dbo.CLARITY_SER_VIEW AS VISIT_SER ON PAT_ENC.VISIT_PROV_ID = VISIT_SER.PROV_ID
            INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
        WHERE IP_FLWSHT_MEAS.RECORDED_TIME > DATEADD(MONTH, -12, GETDATE())
      AND IP_FLWSHT_MEAS.FLO_MEAS_ID = '1393'
      AND IP_FLWSHT_MEAS.MEAS_VALUE IS NOT NULL
      AND (
            SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) IN ( 'MH', 'BH', 'PY', 'AD' )
         OR IP_FLWSHT_MEAS.ENTRY_USER_ID = '32000'
          )
    )
SELECT gad_info.PAT_ID,
       gad_info.[GAD-7 Date],
       gad_info.[Gad-7 Total Score],
       gad_info.Provider,
       gad_info.PROVIDER_TYPE_C
INTO #gad_7_info
FROM gad_info
WHERE gad_info.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#scheduled_visits') IS NOT NULL DROP TABLE #scheduled_visits;
WITH
    scheduled_visits_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS [Next Any Appt],
               CLARITY_SER.PROV_NAME AS [Next Appt Prov],
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1
    )
SELECT scheduled_visits_info.PAT_ID,
       scheduled_visits_info.[Next Any Appt],
       scheduled_visits_info.[Next Appt Prov]
INTO #scheduled_visits
FROM scheduled_visits_info
WHERE scheduled_visits_info.ROW_NUM_ASC = 1;


IF OBJECT_ID('tempdb..#scheduled_pcp_visits') IS NOT NULL DROP TABLE #scheduled_pcp_visits;
WITH
    scheduled_visits_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS [Next PCP Appt],
               CLARITY_SER.PROV_NAME AS [Next PCP Appt Prov],
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1
              AND CLARITY_SER.PROV_ID <> '640178'
              AND CLARITY_SER.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' )
    )
SELECT scheduled_visits_info.PAT_ID,
       scheduled_visits_info.[Next PCP Appt],
       scheduled_visits_info.[Next PCP Appt Prov]
INTO #scheduled_pcp_visits
FROM scheduled_visits_info
WHERE scheduled_visits_info.ROW_NUM_ASC = 1;


-- *** UPDATED: Final SELECT with care team assignments ***
SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       PATIENT.PAT_NAME AS [Patient],
       #bh_care_teams.[BH Provider],
       #bh_care_teams.[Last BH Start Date],
       #bh_care_teams.[Last BH End Date],
       PATIENT.ZIP,
       COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE_CATEGORY,
       COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY_CATEGORY,
       #scheduled_visits.[Next Any Appt],
       #scheduled_visits.[Next Appt Prov],
       #scheduled_pcp_visits.[Next PCP Appt],
       #scheduled_pcp_visits.[Next PCP Appt Prov],
       #full_bh_patients.CITY,
       #full_bh_patients.STATE,
       #gad_7_info.[GAD-7 Date],
       #gad_7_info.[Gad-7 Total Score],
       #gad_7_info.Provider AS [GAD-7 Provider],
       #gad_7_info.PROVIDER_TYPE_C,
       #full_bh_patients.DEPARTMENT_ID AS [Department ID],
       #mht.[MH Therapist],
       IIF(#mht.PAT_ID IS NOT NULL, 1, 0) AS is_mht_assigned,
       #psych.[Psychiatrist],
       IIF(#psych.PAT_ID IS NOT NULL, 1, 0) AS is_psych_assigned,
       IIF(#gad_7_info.PAT_ID IS NOT NULL, 'Met', 'Not Met') AS OUTCOME
FROM CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
    INNER JOIN #full_bh_patients ON PATIENT.PAT_ID = #full_bh_patients.PAT_ID
    LEFT JOIN #bh_care_teams ON PATIENT.PAT_ID = #bh_care_teams.PAT_ID
    LEFT JOIN #gad_7_info ON PATIENT.PAT_ID = #gad_7_info.PAT_ID
    LEFT JOIN #scheduled_visits ON PATIENT.PAT_ID = #scheduled_visits.PAT_ID
    LEFT JOIN #scheduled_pcp_visits ON PATIENT.PAT_ID = #scheduled_pcp_visits.PAT_ID
    LEFT JOIN ##patient_race_ethnicity ON PATIENT.PAT_ID = ##patient_race_ethnicity.PAT_ID
    LEFT JOIN #mht ON PATIENT.PAT_ID = #mht.PAT_ID
    LEFT JOIN #psych ON PATIENT.PAT_ID = #psych.PAT_ID;