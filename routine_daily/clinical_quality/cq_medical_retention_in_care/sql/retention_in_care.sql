SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#active_hiv_patients') IS NOT NULL DROP TABLE #active_hiv_patients;
SELECT pat_enc.PAT_ID
INTO #active_hiv_patients
FROM CLARITY.dbo.PATIENT_VIEW AS patient
    INNER JOIN CLARITY.dbo.PATIENT_4 AS patient_4 ON patient.PAT_ID = patient_4.PAT_ID
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS pat_enc ON patient.PAT_ID = pat_enc.PAT_ID
    INNER JOIN CLARITY.dbo.PROBLEM_LIST_VIEW AS problem_list ON pat_enc.PAT_ID = problem_list.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_EDG ON problem_list.DX_ID = clarity_edg.DX_ID
	INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pat_enc.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON clarity_edg.DX_ID = EDG_CURRENT_ICD10.DX_ID
	INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pat_enc.DEPARTMENT_ID
WHERE EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND problem_list.RESOLVED_DATE IS NULL --Active Dx
      AND problem_list.PROBLEM_STATUS_C = 1 --Active Dx
	  AND ser.DEA_NUMBER IS NOT NULL
	  AND pat_enc.APPT_STATUS_C IN (2, 6)
	  AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
GROUP BY pat_enc.PAT_ID;


IF OBJECT_ID('tempdb..#baseline_cohort') IS NOT NULL DROP TABLE #baseline_cohort;
WITH
    visits_info AS (
        SELECT pev.PAT_ID,
               CAST(pev.CONTACT_DATE AS DATE) AS MEDICAL_VISIT_DT,
               pev.PAT_ENC_CSN_ID,
               pev.VISIT_PROV_ID,
               CLARITY_DEP.STATE,
               CLARITY_DEP.CITY,
			   CLARITY_DEP.SERVICE_TYPE,
			   CLARITY_DEP.SERVICE_LINE, 
			   CLARITY_DEP.SUB_SERVICE_LINE,
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
        FROM CLARITY.dbo.PAT_ENC_VIEW pev
            LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = pev.DEPARTMENT_ID
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON pev.VISIT_PROV_ID = CLARITY_SER.PROV_ID
        WHERE pev.APPT_STATUS_C IN ( 2, 6 )
              AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'MD'
              AND CLARITY_SER.DEA_NUMBER IS NOT NULL
              AND DATEDIFF(MONTH, pev.CONTACT_DATE, GETDATE()) <= 12
			  AND CLARITY_SER.DEA_NUMBER IS NOT NULL
			  AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'MD'
			AND pev.APPT_STATUS_C IN (2, 6)
              AND pev.PAT_ID IN ( SELECT #active_hiv_patients.PAT_ID FROM #active_hiv_patients )
    )
SELECT visits_info.PAT_ID,
       visits_info.MEDICAL_VISIT_DT,
       visits_info.VISIT_PROV_ID,
       visits_info.PAT_ENC_CSN_ID,
       visits_info.STATE,
       visits_info.CITY,
	   visits_info.SERVICE_TYPE,
	   visits_info.SERVICE_LINE, 
	   visits_info.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY PAT_ID ORDER BY MEDICAL_VISIT_DT DESC) AS ROW_NUM_DESC
INTO #baseline_cohort
FROM visits_info
WHERE visits_info.ROW_NUM_DESC >= 2;


IF OBJECT_ID('tempdb..#medical_visit_comparisons') IS NOT NULL DROP TABLE #medical_visit_comparisons;
WITH
    date_differences AS (
        SELECT initial.PAT_ID,
               ABS(DATEDIFF(DAY, initial.MEDICAL_VISIT_DT, comparator.MEDICAL_VISIT_DT)) AS diff_in_days
        FROM #baseline_cohort AS initial
            INNER JOIN #baseline_cohort AS comparator ON initial.PAT_ID = comparator.PAT_ID
    )
SELECT date_differences.PAT_ID,
       CASE WHEN date_differences.diff_in_days >= 90 THEN 'MET'
           ELSE 'NOT MET'
       END AS OUTCOME
INTO #medical_visit_comparisons
FROM date_differences;


IF OBJECT_ID('tempdb..#vls_component') IS NOT NULL DROP TABLE #vls_component;
SELECT DISTINCT CLARITY_COMPONENT.COMPONENT_ID
INTO #vls_component
FROM CLARITY.dbo.CLARITY_COMPONENT AS CLARITY_COMPONENT
WHERE CLARITY_COMPONENT.COMMON_NAME = 'HIV VIRAL LOAD';


IF OBJECT_ID('tempdb..#vls_comparisons') IS NOT NULL DROP TABLE #vls_comparisons;
WITH
    vls_info AS (
        SELECT ORDER_PROC.PAT_ID,
               ORDER_PROC.PAT_ENC_CSN_ID,
               CAST(ORDER_PROC.ORDERING_DATE AS DATE) AS VLS_ORDER_DATE
        FROM CLARITY.dbo.ORDER_PROC_VIEW AS ORDER_PROC
            INNER JOIN CLARITY.dbo.ORDER_RESULTS_VIEW AS ORDER_RESULTS ON ORDER_PROC.ORDER_PROC_ID = ORDER_RESULTS.ORDER_PROC_ID
            INNER JOIN #vls_component ON ORDER_RESULTS.COMPONENT_ID = #vls_component.COMPONENT_ID
        WHERE ORDER_RESULTS.ORD_VALUE NOT IN ( 'Delete', 'See comment' )
              AND DATEDIFF(MONTH, ORDER_RESULTS.RESULT_DATE, GETDATE()) <= 12
    ),
    vls_visit_comparison AS (
        SELECT #baseline_cohort.PAT_ID,
               ABS(DATEDIFF(DAY, MEDICAL_VISIT_DT, vls_info.VLS_ORDER_DATE)) AS diff_in_days
        FROM vls_info
            INNER JOIN #baseline_cohort ON vls_info.PAT_ID = #baseline_cohort.PAT_ID
    )
SELECT vls_visit_comparison.PAT_ID,
       CASE WHEN vls_visit_comparison.diff_in_days >= 90 THEN 'MET'
           ELSE 'NOT MET'
       END AS OUTCOME
INTO #vls_comparisons
FROM vls_visit_comparison;


IF OBJECT_ID('tempdb..#outcomes') IS NOT NULL DROP TABLE #outcomes;
WITH
    outcomes AS (
        SELECT * FROM #medical_visit_comparisons
        UNION ALL
        SELECT * FROM #vls_comparisons
    )
SELECT outcomes.*,
       ROW_NUMBER() OVER (PARTITION BY outcomes.PAT_ID ORDER BY outcomes.OUTCOME ASC) AS ROW_NUM_ASC
INTO #outcomes
FROM outcomes;


SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       PATIENT.PAT_NAME AS PATIENT_NAME,
       #baseline_cohort.MEDICAL_VISIT_DT AS LAST_OFFICE_VISIT,
       #baseline_cohort.CITY,
       #baseline_cohort.STATE,
	   #baseline_cohort.SERVICE_TYPE 'Service Type',
	   #baseline_cohort.SERVICE_LINE 'Service Line', 
	   #baseline_cohort.SUB_SERVICE_LINE 'Sub-Service Line',
       CLARITY_SER.PROV_NAME AS VISIT_PROVIDER,
       COALESCE(zc_gender_identity.NAME, 'Unknown') AS GENDER_CATC,
       COALESCE(zc_patient_race.NAME, 'Unknown') AS RACE_CATC,
       COALESCE(zc_ethnic_group.NAME, 'Unknown') AS ETHNICITY_CATC,
       (DATEDIFF(m, PATIENT.BIRTH_DATE, CURRENT_TIMESTAMP) / 12) PAT_AGE_N,
       CASE WHEN ZC_SEXUAL_ORIENTATION.NAME IS NULL THEN 'Unknown'
           WHEN ZC_SEXUAL_ORIENTATION.NAME = 'Choose not to disclose' THEN 'Unknown'
           WHEN ZC_SEXUAL_ORIENTATION.NAME = 'Don''t know' THEN 'Unknown'
           WHEN ZC_SEXUAL_ORIENTATION.NAME = 'Gay' THEN 'Lesbian or Gay'
           ELSE ZC_SEXUAL_ORIENTATION.NAME
       END AS SEXUAL_ORIENTATION_CATC,
       #outcomes.OUTCOME,
       CURRENT_TIMESTAMP AS DATA_UPDATE
FROM #baseline_cohort
    INNER JOIN Clarity.dbo.PATIENT_VIEW AS PATIENT ON #baseline_cohort.PAT_ID = PATIENT.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_4 AS PATIENT_4 ON PATIENT_4.PAT_ID = #baseline_cohort.PAT_ID
    LEFT JOIN CLARITY.dbo.PAT_SEXUAL_ORIENTATION PAT_SEXUAL_ORIENTATION ON PAT_SEXUAL_ORIENTATION.PAT_ID = #baseline_cohort.PAT_ID
                                                                           AND PAT_SEXUAL_ORIENTATION.LINE = 1
    LEFT JOIN CLARITY.dbo.ZC_SEXUAL_ORIENTATION ZC_SEXUAL_ORIENTATION ON PAT_SEXUAL_ORIENTATION.SEXUAL_ORIENTATN_C = ZC_SEXUAL_ORIENTATION.SEXUAL_ORIENTATION_C
    LEFT JOIN CLARITY.dbo.ZC_ETHNIC_GROUP AS zc_ethnic_group ON PATIENT.ETHNIC_GROUP_C = zc_ethnic_group.ETHNIC_GROUP_C
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON #baseline_cohort.PAT_ID = IDENTITY_ID.PAT_ID
    LEFT JOIN Clarity.dbo.PATIENT_RACE AS patient_race ON PATIENT.PAT_ID = patient_race.PAT_ID
                                                          AND patient_race.LINE = 1
    LEFT JOIN CLARITY.dbo.ZC_PATIENT_RACE AS zc_patient_race ON patient_race.PATIENT_RACE_C = zc_patient_race.PATIENT_RACE_C
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON #baseline_cohort.VISIT_PROV_ID = CLARITY_SER.PROV_ID
    LEFT JOIN Clarity.dbo.ZC_GENDER_IDENTITY AS ZC_GENDER_IDENTITY ON ZC_GENDER_IDENTITY.GENDER_IDENTITY_C = PATIENT_4.GENDER_IDENTITY_C
    LEFT JOIN #outcomes ON #baseline_cohort.PAT_ID = #outcomes.PAT_ID
                           AND #outcomes.ROW_NUM_ASC = 1
WHERE #baseline_cohort.ROW_NUM_DESC = 1;
