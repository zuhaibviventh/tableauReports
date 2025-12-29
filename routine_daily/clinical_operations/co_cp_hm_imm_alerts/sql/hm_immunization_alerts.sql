SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#hmt_immunizations__info') IS NOT NULL DROP TABLE #hmt_immunizations__info;
SELECT PATIENT_HMT_STATUS.PAT_ID,
       CAST(PATIENT_HMT_STATUS.IDEAL_RETURN_DT AS DATE) AS HM_IMM_DUE_DATE,
       SUBSTRING(CLARITY_HM_TOPIC.NAME, 5, LEN(CLARITY_HM_TOPIC.NAME)) AS HM_IMMUNIZATION_NAME,
       ZC_HMT_DUE_STATUS.NAME AS HM_IMMUNIZATION_STATUS
INTO #hmt_immunizations__info
FROM CLARITY.dbo.PATIENT_HMT_STATUS_VIEW AS PATIENT_HMT_STATUS
    INNER JOIN CLARITY.dbo.ZC_HMT_DUE_STATUS AS ZC_HMT_DUE_STATUS ON PATIENT_HMT_STATUS.HMT_DUE_STATUS_C = ZC_HMT_DUE_STATUS.HMT_DUE_STATUS_C
    INNER JOIN CLARITY.dbo.CLARITY_HM_TOPIC AS CLARITY_HM_TOPIC ON PATIENT_HMT_STATUS.QUALIFIED_HMT_ID = CLARITY_HM_TOPIC.HM_TOPIC_ID
WHERE CLARITY_HM_TOPIC.HM_TOPIC_ID IN ( 209, 120, 107, 108, 105, 100, 141, 12, 109, 130, 112, 132, 110, 151, 155, 153, 113, 160, 161, 162, 133, 104, 54, 134,
                                        103, 131, 46, 135 )
      /* Incomplete */
      --AND ZC_HMT_DUE_STATUS.HMT_DUE_STATUS_C != 6;


IF OBJECT_ID('tempdb..#hmt_immunizations') IS NOT NULL DROP TABLE #hmt_immunizations;
SELECT #hmt_immunizations__info.PAT_ID,
       #hmt_immunizations__info.HM_IMM_DUE_DATE,
       CASE WHEN #hmt_immunizations__info.HM_IMMUNIZATION_NAME = 'Meningococcal' THEN 'Meningo.'
           WHEN #hmt_immunizations__info.HM_IMMUNIZATION_NAME = 'Meningococcal B' THEN 'Meningo. B'
           WHEN #hmt_immunizations__info.HM_IMMUNIZATION_NAME = 'Mpox (formerly Monkeypox)' THEN 'Mpox'
           ELSE #hmt_immunizations__info.HM_IMMUNIZATION_NAME
       END AS HM_IMMUNIZATION_NAME,
       #hmt_immunizations__info.HM_IMMUNIZATION_STATUS
INTO #hmt_immunizations
FROM #hmt_immunizations__info;


IF OBJECT_ID('tempdb..#delivery') IS NOT NULL DROP TABLE #delivery;
SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       PATIENT.PAT_NAME,
       CAST(PATIENT.BIRTH_DATE AS DATE) AS DOB,
       ##most_recent_visit.CITY,
       ##most_recent_visit.STATE,
       ##most_recent_visit.VISIT_DATE AS LATEST_VISIT_DATE,
       ##most_recent_visit.VISIT_DEPARTMENT_NAME,
       COALESCE(zc_gender_identity.NAME, 'Unknown') AS GENDER,
       COALESCE(zc_patient_race.NAME, 'Unknown') AS RACE,
       COALESCE(zc_ethnic_group.NAME, 'Unknown') AS ETHNICITY,
       CASE WHEN ##active_hiv_patients.PATIENT_TYPE IS NOT NULL THEN ##active_hiv_patients.PATIENT_TYPE
           WHEN ##flag_type.PATIENT_TYPE IS NOT NULL THEN ##flag_type.PATIENT_TYPE
           ELSE 'Other'
       END AS PATIENT_TYPE,
       COALESCE(CLARITY_SER.PROV_NAME, 'NO PCP ASSIGNED') AS PCP,
       #hmt_immunizations.HM_IMM_DUE_DATE,
       #hmt_immunizations.HM_IMMUNIZATION_NAME,
       #hmt_immunizations.HM_IMMUNIZATION_STATUS,
       ##next_medical_appointment.NEXT_MEDICAL_APPOINTMENT,
       ##next_medical_appointment.NEXT_APPT_PROVIDER,
       COALESCE(##cp_cohorts.CP_COHORT, 'Not in CP Cohort') AS CP_COHORT,
       CASE WHEN DATEDIFF(MONTH, ##most_recent_visit.VISIT_DATE, CURRENT_TIMESTAMP) <= 13 THEN 'Y'
           ELSE 'N'
       END AS LAST_VISIT_WITHIN_13_MO,
       CURRENT_TIMESTAMP AS UPDATE_DTTM
INTO #delivery
FROM Clarity.dbo.PATIENT_VIEW AS PATIENT
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON PATIENT.PAT_ID = IDENTITY_ID.PAT_ID
    INNER JOIN #hmt_immunizations ON PATIENT.PAT_ID = #hmt_immunizations.PAT_ID
    INNER JOIN ##most_recent_visit ON PATIENT.PAT_ID = ##most_recent_visit.PAT_ID
                                     AND ##most_recent_visit.ROW_NUM_DESC = 1
    LEFT JOIN ##next_medical_appointment ON ##next_medical_appointment.PAT_ID = PATIENT.PAT_ID
                                          AND ##next_medical_appointment.RN_ASC = 1
    LEFT JOIN CLARITY.dbo.PATIENT_4 AS patient_4 ON PATIENT.PAT_ID = patient_4.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_GENDER_IDENTITY AS zc_gender_identity ON patient_4.GENDER_IDENTITY_C = zc_gender_identity.GENDER_IDENTITY_C
    LEFT JOIN CLARITY.dbo.PATIENT_RACE AS patient_race ON patient.PAT_ID = patient_race.PAT_ID
                                                          AND patient_race.LINE = 1
    LEFT JOIN CLARITY.dbo.ZC_PATIENT_RACE AS zc_patient_race ON patient_race.PATIENT_RACE_C = zc_patient_race.PATIENT_RACE_C
    LEFT JOIN CLARITY.dbo.ZC_ETHNIC_GROUP AS zc_ethnic_group ON patient.ETHNIC_GROUP_C = zc_ethnic_group.ETHNIC_GROUP_C
    LEFT JOIN ##flag_type ON PATIENT.PAT_ID = ##flag_type.PATIENT_ID
    LEFT JOIN ##active_hiv_patients ON PATIENT.PAT_ID = ##active_hiv_patients.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PATIENT.CUR_PCP_PROV_ID = CLARITY_SER.PROV_ID
    LEFT JOIN ##cp_cohorts ON PATIENT.PAT_ID = ##cp_cohorts.PAT_ID
GROUP BY PATIENT.BIRTH_DATE,
         zc_gender_identity.NAME,
         zc_patient_race.NAME,
         zc_ethnic_group.NAME,
         ##active_hiv_patients.PATIENT_TYPE,
         ##flag_type.PATIENT_TYPE,
         IDENTITY_ID.IDENTITY_ID,
         PATIENT.PAT_NAME,
         CLARITY_SER.PROV_NAME,
         HM_IMM_DUE_DATE,
         HM_IMMUNIZATION_NAME,
         HM_IMMUNIZATION_STATUS,
         ##next_medical_appointment.NEXT_MEDICAL_APPOINTMENT,
         ##next_medical_appointment.NEXT_APPT_PROVIDER,
         ##cp_cohorts.CP_COHORT,
         ##most_recent_visit.CITY,
         ##most_recent_visit.STATE,
         ##most_recent_visit.VISIT_DATE,
         ##most_recent_visit.VISIT_DEPARTMENT_NAME;


SELECT * FROM #delivery WHERE DATEDIFF(MONTH, LATEST_VISIT_DATE, CURRENT_TIMESTAMP) <= 36 ORDER BY LATEST_VISIT_DATE ASC;
