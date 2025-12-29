SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

/*
Visits within the last 12 months
*/
IF OBJECT_ID('tempdb..#pat_enc_dep_los') IS NOT NULL DROP TABLE #pat_enc_dep_los;
SELECT PAT_ENC.PAT_ID,
       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS LAST_OFFICE_VISIT,
       CLARITY_DEP.STATE,
       CLARITY_DEP.CITY,
       CLARITY_DEP.SITE,
       CLARITY_DEP.Service_line,
       CLARITY_DEP.Service_Type,
       CLARITY_DEP.Sub_Service_Line,
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
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
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
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
               #pat_enc_dep_los.PAT_ENC_CSN_ID,
               #pat_enc_dep_los.LOS,
               #pat_enc_dep_los.Service_line,
               #pat_enc_dep_los.Sub_Service_line,
               #pat_enc_dep_los.Service_Type,
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
               #pat_enc_dep_los.LAST_OFFICE_VISIT,
               #pat_enc_dep_los.PAT_ENC_CSN_ID,
               #pat_enc_dep_los.LOS,
               #pat_enc_dep_los.Service_line,
               #pat_enc_dep_los.Sub_Service_line,
               #pat_enc_dep_los.Service_Type,
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
               active_psych_patients.PAT_ENC_CSN_ID,
               active_psych_patients.LOS,
               active_psych_patients.service_line,
               active_psych_patients.sub_service_line,
               active_psych_patients.service_type
        FROM active_psych_patients
        WHERE active_psych_patients.ROW_NUM_DESC = 1
        UNION
        SELECT active_mental_health_patients.PAT_ID,
               active_mental_health_patients.CITY,
               active_mental_health_patients.STATE,
               active_mental_health_patients.LAST_OFFICE_VISIT,
               active_mental_health_patients.PAT_ENC_CSN_ID,
               active_mental_health_patients.LOS,
               active_mental_health_patients.service_line,
               active_mental_health_patients.sub_service_line,
               active_mental_health_patients.service_type
        FROM active_mental_health_patients
        WHERE active_mental_health_patients.ROW_NUM_DESC = 1
    ),
    preproc AS (
        SELECT full_bh_cohort.PAT_ID,
               full_bh_cohort.CITY,
               full_bh_cohort.STATE,
               full_bh_cohort.LAST_OFFICE_VISIT,
               full_bh_cohort.PAT_ENC_CSN_ID,
               full_bh_cohort.LOS,
              full_bh_cohort.service_line,
               full_bh_cohort.sub_service_line,
               full_bh_cohort.service_type,
               ROW_NUMBER() OVER (PARTITION BY full_bh_cohort.PAT_ID
                                  ORDER BY full_bh_cohort.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM full_bh_cohort
    )
SELECT preproc.PAT_ID,
       preproc.CITY,
       preproc.STATE,
       preproc.PAT_ENC_CSN_ID,
       preproc.LOS,
       preproc.LAST_OFFICE_VISIT,
      preproc.service_line,
       preproc.sub_service_line,
       preproc.service_type
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


IF OBJECT_ID('tempdb..#phq9') IS NOT NULL DROP TABLE #phq9;
WITH
    visits_and_phq9 AS (
        SELECT #full_bh_patients.PAT_ID,
               #full_bh_patients.CITY,
               #full_bh_patients.STATE,
               #full_bh_patients.LOS,
               #full_bh_patients.service_line,
       #full_bh_patients.sub_service_line,
       #full_bh_patients.service_type,
               #full_bh_patients.LAST_OFFICE_VISIT,
               CAST(IP_FLWSHT_REC.RECORD_DATE AS DATE) AS PHQ9_RECORDED_DATE,
               IIF(IP_FLWSHT_MEAS.FLO_MEAS_ID = '1042', IP_FLWSHT_MEAS.MEAS_VALUE, NULL) AS PHQ9_Q10,
               IIF(IP_FLWSHT_MEAS.FLO_MEAS_ID IN ( '1043', '1044' ), IP_FLWSHT_MEAS.MEAS_VALUE, NULL) AS PHQ9_SUM,
               ROW_NUMBER() OVER (PARTITION BY #full_bh_patients.PAT_ID,
                                               IP_FLWSHT_MEAS.FLO_MEAS_ID
                                  ORDER BY IP_FLWSHT_REC.RECORD_DATE DESC) AS ROW_NUM_DESC
        FROM #full_bh_patients
            INNER JOIN CLARITY.dbo.IP_FLWSHT_REC_VIEW AS IP_FLWSHT_REC ON #full_bh_patients.PAT_ID = IP_FLWSHT_REC.PAT_ID
            INNER JOIN CLARITY.dbo.IP_FLWSHT_MEAS_VIEW AS IP_FLWSHT_MEAS ON IP_FLWSHT_REC.FSD_ID = IP_FLWSHT_MEAS.FSD_ID
        WHERE DATEDIFF(MONTH, IP_FLWSHT_REC.RECORD_DATE, GETDATE()) <= 12
              AND IP_FLWSHT_MEAS.FLO_MEAS_ID IN ( '1042', '1043', '1044' )
    ),
    phq9_info AS (
        SELECT visits_and_phq9.PAT_ID,
               visits_and_phq9.CITY,
               visits_and_phq9.STATE,
               visits_and_phq9.LOS,
               visits_and_phq9.LAST_OFFICE_VISIT,
               visits_and_phq9.PHQ9_RECORDED_DATE,
               visits_and_phq9.service_line,
       visits_and_phq9.sub_service_line,
       visits_and_phq9.service_type,
               MAX(visits_and_phq9.PHQ9_Q10) AS PHQ9_Q10,
               MAX(visits_and_phq9.PHQ9_SUM) AS PHQ9_SUM,
               ROW_NUMBER() OVER (PARTITION BY visits_and_phq9.PAT_ID
                                  ORDER BY visits_and_phq9.PHQ9_RECORDED_DATE DESC) AS ROW_NUM_DESC
        FROM visits_and_phq9
        WHERE visits_and_phq9.ROW_NUM_DESC = 1
        GROUP BY visits_and_phq9.PAT_ID,
                 visits_and_phq9.CITY,
                 visits_and_phq9.STATE,
                 visits_and_phq9.LOS,
                 visits_and_phq9.LAST_OFFICE_VISIT,
                 visits_and_phq9.PHQ9_RECORDED_DATE,
                 visits_and_phq9.service_line,
       visits_and_phq9.sub_service_line,
       visits_and_phq9.service_type
    ),
    q10_score_proc_1 AS (
        SELECT phq9_info.PAT_ID,
               phq9_info.CITY,
               phq9_info.STATE,
               phq9_info.LOS,
               phq9_info.LAST_OFFICE_VISIT,
               phq9_info.PHQ9_RECORDED_DATE,
               phq9_info.PHQ9_Q10,
               phq9_info.PHQ9_SUM,
               phq9_info.service_line,
       phq9_info.sub_service_line,
       phq9_info.service_type
        FROM phq9_info
        WHERE phq9_info.ROW_NUM_DESC = 1
    ),
    q10_score_proc_2 AS (
        SELECT q10_score_proc_1.PAT_ID,
               q10_score_proc_1.CITY,
               q10_score_proc_1.STATE,
               q10_score_proc_1.LOS,
               q10_score_proc_1.LAST_OFFICE_VISIT,
               q10_score_proc_1.PHQ9_RECORDED_DATE,
               q10_score_proc_1.service_line,
       q10_score_proc_1.sub_service_line,
       q10_score_proc_1.service_type,
               CASE WHEN q10_score_proc_1.PHQ9_Q10 IS NULL
                         AND q10_score_proc_1.PHQ9_SUM <> '0' THEN 999
                   WHEN q10_score_proc_1.PHQ9_Q10 IS NULL
                        AND q10_score_proc_1.PHQ9_SUM = '0' THEN 0
                   WHEN q10_score_proc_1.PHQ9_Q10 = 'Not difficult at all' THEN 10
                   WHEN q10_score_proc_1.PHQ9_Q10 = 'Somewhat difficult' THEN 1
                   WHEN q10_score_proc_1.PHQ9_Q10 = 'Very difficult' THEN 2
                   WHEN q10_score_proc_1.PHQ9_Q10 = 'Extremely difficult' THEN 3
               END AS PHQ9_Q10,
               q10_score_proc_1.PHQ9_SUM
        FROM q10_score_proc_1
    )
SELECT q10_score_proc_2.PAT_ID,
       q10_score_proc_2.CITY,
       q10_score_proc_2.STATE,
       q10_score_proc_2.LOS,
       q10_score_proc_2.LAST_OFFICE_VISIT,
       q10_score_proc_2.PHQ9_RECORDED_DATE,
       q10_score_proc_2.PHQ9_Q10,
       q10_score_proc_2.service_line,
       q10_score_proc_2.sub_service_line,
       q10_score_proc_2.service_type,
       COALESCE(q10_score_proc_2.PHQ9_SUM, 0) AS PHQ9_SUM,
       CASE WHEN q10_score_proc_2.PHQ9_Q10 IN ( 0, 1 ) THEN 'MET'
           WHEN q10_score_proc_2.PHQ9_Q10 IN ( 2, 3 ) THEN 'NOT MET'
       END AS OUTCOME,
       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C IN ( '136', '164', '129' ), CLARITY_SER.PROV_NAME, NULL)) AS PSYCHIATRY_PROVIDER,
       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' ), CLARITY_SER.PROV_NAME, NULL)) AS MH_TEAM_MEMBER
INTO #phq9
FROM q10_score_proc_2
    LEFT JOIN CLARITY.dbo.PAT_PCP_VIEW AS PAT_PCP ON q10_score_proc_2.PAT_ID = PAT_PCP.PAT_ID
                                                     AND PAT_PCP.RELATIONSHIP_C IN ( '1', '3' )
                                                     AND PAT_PCP.TERM_DATE IS NULL
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_PCP.PCP_PROV_ID = CLARITY_SER.PROV_ID
WHERE q10_score_proc_2.PHQ9_Q10 IN ( 0, 1, 2, 3 )
GROUP BY q10_score_proc_2.PAT_ID,
         q10_score_proc_2.CITY,
         q10_score_proc_2.STATE,
         q10_score_proc_2.LOS,
         q10_score_proc_2.LAST_OFFICE_VISIT,
         q10_score_proc_2.PHQ9_RECORDED_DATE,
         q10_score_proc_2.PHQ9_Q10,
         q10_score_proc_2.service_line,
       q10_score_proc_2.sub_service_line,
       q10_score_proc_2.service_type,
         q10_score_proc_2.PHQ9_SUM;


IF OBJECT_ID('tempdb..#scheduled_visits') IS NOT NULL DROP TABLE #scheduled_visits;
WITH
    scheduled_visits_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_ANY_APPT,
               CLARITY_SER.PROV_NAME AS NEXT_ANY_APPT_PROV,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1 -- scheduled
    )
SELECT scheduled_visits_info.PAT_ID,
       scheduled_visits_info.NEXT_ANY_APPT,
       scheduled_visits_info.NEXT_ANY_APPT_PROV
INTO #scheduled_visits
FROM scheduled_visits_info
WHERE scheduled_visits_info.ROW_NUM_ASC = 1;


IF OBJECT_ID('tempdb..#scheduled_pcp_visits') IS NOT NULL DROP TABLE #scheduled_pcp_visits;
WITH
    scheduled_visits_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(PAT_ENC.CONTACT_DATE AS DATE) AS NEXT_PCP_APPT,
               CLARITY_SER.PROV_NAME AS NEXT_PCP_APPT_PROV,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE PAT_ENC.APPT_STATUS_C = 1 -- scheduled
              AND CLARITY_SER.PROV_ID <> '640178' -- Pulmonologist
              AND CLARITY_SER.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs, and NPs
    )
SELECT scheduled_visits_info.PAT_ID,
       scheduled_visits_info.NEXT_PCP_APPT,
       scheduled_visits_info.NEXT_PCP_APPT_PROV
INTO #scheduled_pcp_visits
FROM scheduled_visits_info
WHERE scheduled_visits_info.ROW_NUM_ASC = 1;


-- *** UPDATED: Final SELECT with care team assignments ***
SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       PATIENT.PAT_NAME AS PATIENT_NAME,
       #phq9.LAST_OFFICE_VISIT AS VISIT_DATE,
       #phq9.LOS AS DEPARTMENT_NAME,
       #phq9.CITY,
       #phq9.STATE,
       #phq9.service_line,
       #phq9.sub_service_line,
       #phq9.service_type,
       COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE_CATEGORY,
       COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY_CATEGORY,
       #phq9.PHQ9_RECORDED_DATE,
       #phq9.PHQ9_Q10,
       #phq9.PHQ9_SUM,
       #phq9.OUTCOME AS MET_YN,
       #phq9.PSYCHIATRY_PROVIDER,
       #phq9.MH_TEAM_MEMBER,
       #scheduled_visits.NEXT_ANY_APPT,
       #scheduled_visits.NEXT_ANY_APPT_PROV,
       #scheduled_pcp_visits.NEXT_PCP_APPT,
       #scheduled_pcp_visits.NEXT_PCP_APPT_PROV,
       #mht.[MH Therapist],
       IIF(#mht.PAT_ID IS NOT NULL, 1, 0) AS is_mht_assigned,
       #psych.[Psychiatrist],
       IIF(#psych.PAT_ID IS NOT NULL, 1, 0) AS is_psych_assigned
FROM #phq9
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON #phq9.PAT_ID = PATIENT.PAT_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON PATIENT.PAT_ID = IDENTITY_ID.PAT_ID
    LEFT JOIN #scheduled_visits ON PATIENT.PAT_ID = #scheduled_visits.PAT_ID
    LEFT JOIN #scheduled_pcp_visits ON PATIENT.PAT_ID = #scheduled_pcp_visits.PAT_ID
    LEFT JOIN ##patient_race_ethnicity ON PATIENT.PAT_ID = ##patient_race_ethnicity.PAT_ID
    LEFT JOIN #mht ON PATIENT.PAT_ID = #mht.PAT_ID
    LEFT JOIN #psych ON PATIENT.PAT_ID = #psych.PAT_ID;