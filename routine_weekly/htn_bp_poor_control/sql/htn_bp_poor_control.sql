SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#pat_enc_dep_los') IS NOT NULL
    DROP TABLE #pat_enc_dep_los;
SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK'
                THEN 'MILWAUKEE'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KN'
           THEN 'KENOSHA'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'GB'
           THEN 'GREEN BAY'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'WS'
           THEN 'WAUSAU'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AP'
           THEN 'APPLETON'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'EC'
           THEN 'EAU CLAIRE'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'LC'
           THEN 'LACROSSE'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MD'
           THEN 'MADISON'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'BL'
           THEN 'BELOIT'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'BI'
           THEN 'BILLING'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'SL'
           THEN 'ST LOUIS'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'DN'
           THEN 'DENVER'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AS'
           THEN 'AUSTIN'
		WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
       ELSE 'ERROR'
       END AS CITY,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN'
                THEN 'MAIN LOCATION'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'DR'
           THEN 'D&R'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'KE'
           THEN 'KEENEN'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'UC'
           THEN 'UNIVERSITY OF COLORADO'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'ON'
           THEN 'AUSTIN MAIN'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'TW'
           THEN 'AUSTIN OTHER'
       ELSE 'ERROR'
       END AS 'SITE',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                THEN 'MEDICAL'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
           THEN 'DENTAL'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM'
           THEN 'CASE MANAGEMENT'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX'
           THEN 'PHARMACY'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD'
           THEN 'BEHAVIORAL'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY'
           THEN 'BEHAVIORAL'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH'
           THEN 'BEHAVIORAL'
       WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH'
           THEN 'BEHAVIORAL'
       ELSE 'ERROR'
       END AS 'LOS'
INTO #pat_enc_dep_los
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

IF OBJECT_ID('tempdb..#active_hiv_patients') IS NOT NULL
    DROP TABLE #active_hiv_patients;
SELECT pat_enc.PAT_ID
INTO #active_hiv_patients
FROM Clarity.dbo.PATIENT_VIEW AS patient
    INNER JOIN Clarity.dbo.PATIENT_4 AS patient_4 ON patient.PAT_ID = patient_4.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW AS pat_enc ON patient.PAT_ID = pat_enc.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW AS problem_list ON pat_enc.PAT_ID = problem_list.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG ON problem_list.DX_ID = clarity_edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 ON clarity_edg.DX_ID = EDG_CURRENT_ICD10.DX_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW AS clarity_ser ON patient.CUR_PCP_PROV_ID = clarity_ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS clarity_dep ON clarity_dep.DEPARTMENT_ID = pat_enc.DEPARTMENT_ID
WHERE clarity_ser.SERV_AREA_ID = 64
      AND clarity_ser.PROVIDER_TYPE_C IN ( 1, 9, 6, 113 ) -- Physicians and NPs, PAs
      AND pat_enc.CONTACT_DATE > DATEADD(MM, -12, CURRENT_TIMESTAMP) --Visit in past year
      AND pat_enc.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
      AND pat_enc.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951,
                                         7952, 7953, 7954, 7970, 7971, 7972,
                                         7973, 7974, 8047, 8048, 8049, 8050,
                                         8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
      AND SUBSTRING(clarity_dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
      AND EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
      AND problem_list.RESOLVED_DATE IS NULL --Active Dx
      AND problem_list.PROBLEM_STATUS_C = 1 --Active Dx
      AND patient_4.PAT_LIVING_STAT_C = 1
GROUP BY pat_enc.PAT_ID;

/* HTN - Dietitian care */
IF OBJECT_ID('tempdb..#htn_dietician_care') IS NOT NULL
    DROP TABLE #htn_dietician_care;
SELECT flag.PATIENT_ID AS PAT_ID,
       MAX(flag.ACTIVE_C) ACTIVE
INTO #htn_dietician_care
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
WHERE flag.PAT_FLAG_TYPE_C = '640025'
      AND flag.ACTIVE_C = 1
GROUP BY flag.PATIENT_ID;

/* HTN - Clinical Pharmacist */
IF OBJECT_ID('tempdb..#htn_clinical_pharmacist') IS NOT NULL
    DROP TABLE #htn_clinical_pharmacist;
SELECT flag.PATIENT_ID AS PAT_ID,
       MAX(flag.ACTIVE_C) ACTIVE
INTO #htn_clinical_pharmacist
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
    INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI f ON flag.PAT_FLAG_TYPE_C = f.BPA_TRIGGER_FYI_C
WHERE f.NAME = 'SA64 Pharmacist - HTN'
      AND flag.ACTIVE_C = 1
GROUP BY flag.PATIENT_ID;

IF OBJECT_ID('tempdb..#cohort_information') IS NOT NULL
    DROP TABLE #cohort_information;
WITH target_service_line AS (
    SELECT #pat_enc_dep_los.PAT_ID,
           #pat_enc_dep_los.STATE,
           #pat_enc_dep_los.CITY,
           #pat_enc_dep_los.SITE,
           #pat_enc_dep_los.LOS,
           #pat_enc_dep_los.LAST_OFFICE_VISIT,
           ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                              ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
    FROM #pat_enc_dep_los
    WHERE #pat_enc_dep_los.LOS = 'MEDICAL'
)
SELECT id.IDENTITY_ID,
       id.PAT_ID,
       patient.PAT_NAME,
       target_service_line.CITY,
       target_service_line.STATE,
       dwav.BP_SYS_LAST_DT AS LAST_BP,
       dwav.BP_SYS_LAST AS LATEST_SYSTOLIC,
       dwav.BP_DIA_LAST AS LATEST_DIASTOLIC,
       ser.EXTERNAL_NAME,
       CASE WHEN #htn_clinical_pharmacist.PAT_ID IS NOT NULL
                THEN 'YES'
       ELSE 'NO'
       END AS 'IN CLINICAL PARM COHORT',
       CASE WHEN #htn_dietician_care.PAT_ID IS NOT NULL
                THEN 'YES'
       ELSE 'NO'
       END AS 'IN DIETITIAN CARE'
INTO #cohort_information
FROM Clarity.dbo.PATIENT_VIEW AS patient
    INNER JOIN target_service_line ON target_service_line.PAT_ID = patient.PAT_ID
    INNER JOIN #active_hiv_patients ON #active_hiv_patients.PAT_ID = patient.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON patient.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON patient.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.DM_WLL_ALL_VIEW dwav ON patient.PAT_ID = dwav.PAT_ID
    LEFT JOIN #htn_dietician_care ON #htn_dietician_care.PAT_ID = patient.PAT_ID
    LEFT JOIN #htn_clinical_pharmacist ON #htn_clinical_pharmacist.PAT_ID = patient.PAT_ID
WHERE target_service_line.ROW_NUM_DESC = 1
      AND dwav.HAS_HYPERTENSION_YN = 'Y'
ORDER BY ser.EXTERNAL_NAME DESC;


WITH scheduled_visits AS (
    SELECT pev.PAT_ID,
           pev.CONTACT_DATE 'Next Any Appt',
           ser.PROV_NAME 'Next Appt Prov',
           ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
    FROM Clarity.dbo.PAT_ENC_VIEW pev
        INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    WHERE pev.APPT_STATUS_C = 1 --Scheduled
),
     scheduled_physician_visits AS (
    SELECT pev.PAT_ID,
           pev.CONTACT_DATE 'Next PCP Appt',
           ser.PROV_NAME 'Next PCP Appt Prov',
           ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
    FROM Clarity.dbo.PAT_ENC_VIEW pev
        INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    WHERE pev.APPT_STATUS_C = 1 --Scheduled
          AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs
),
     dental_bh_episodes AS (
    SELECT ev.PAT_LINK_ID PAT_ID,
           MAX(CASE WHEN ev.SUM_BLK_TYPE_ID = 221 THEN 'Y' ELSE 'N' END) AS 'Active BH',
           MAX(CASE WHEN ev.SUM_BLK_TYPE_ID = 45 THEN 'Y' ELSE 'N' END) AS 'Active Dental'
    FROM Clarity.dbo.EPISODE_VIEW ev
        INNER JOIN Clarity.dbo.CLARITY_EMP_VIEW emp ON ev.L_UPDATE_USER_ID = emp.USER_ID
    WHERE ev.SUM_BLK_TYPE_ID IN ( 45, 221 ) -- Dental and BH
          AND ev.STATUS_C = 1
    GROUP BY ev.PAT_LINK_ID
),
     excluded_groupers AS (
    SELECT DISTINCT plv.PAT_ID
    FROM Clarity.dbo.PROBLEM_LIST_VIEW plv
        INNER JOIN Clarity.dbo.CLARITY_EDG edg ON edg.DX_ID = plv.DX_ID
        INNER JOIN Clarity.dbo.GROUPER_COMPILED_REC_LIST gc ON gc.GROUPER_RECORDS_NUMERIC_ID = edg.DX_ID
        INNER JOIN Clarity.dbo.GROUPER_ITEMS gi ON gc.BASE_GROUPER_ID = gi.GROUPER_ID
    WHERE gi.GROUPER_ID IN ( '107089', '5200000134' )
          /* Diabetic Necropathy = 107089
    Dialysis = 5200000134 */
          AND plv.RESOLVED_DATE IS NULL
          AND plv.PROBLEM_STATUS_C = 1
)
SELECT #cohort_information.IDENTITY_ID,
       #cohort_information.PAT_ID,
       #cohort_information.PAT_NAME,
       #cohort_information.STATE,
       #cohort_information.CITY,
       #cohort_information.EXTERNAL_NAME PROV_NAME,
       #cohort_information.LAST_BP,
       #cohort_information.LATEST_SYSTOLIC,
       #cohort_information.LATEST_DIASTOLIC,
       #cohort_information.[IN CLINICAL PARM COHORT],
       CASE WHEN zeg.NAME IS NULL
                THEN 'Unknown'
       WHEN zeg.NAME = ''
           THEN 'Unknown'
       WHEN zeg.NAME = 'Not Collected/Unknown'
           THEN 'Unknown'
       WHEN zeg.NAME = 'Patient Refused'
           THEN 'Unknown'
       ELSE zeg.NAME
       END AS ETHNICITY,
       CASE WHEN #cohort_information.LATEST_SYSTOLIC < 140
                 AND #cohort_information.LATEST_DIASTOLIC < 90
                THEN 1
       ELSE 0
       END AS MET_YN,
       CASE WHEN #cohort_information.LATEST_SYSTOLIC < 140
                 AND #cohort_information.LATEST_DIASTOLIC < 90
                THEN 'MET'
       ELSE 'NOTMET'
       END AS OUTCOME,
       #cohort_information.[IN DIETITIAN CARE],
       zpr.NAME 'RACE',
       scheduled_visits.[Next Any Appt],
       scheduled_visits.[Next Appt Prov],
       scheduled_physician_visits.[Next PCP Appt],
       scheduled_physician_visits.[Next PCP Appt Prov],
       COALESCE(dental_bh_episodes.[Active BH], 'N') 'Active BH',
       COALESCE(dental_bh_episodes.[Active Dental], 'Y') 'Active Dental'
FROM #cohort_information
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON pr.PAT_ID = #cohort_information.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON zpr.PATIENT_RACE_C = pr.PATIENT_RACE_C
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = #cohort_information.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_ETHNIC_GROUP zeg ON zeg.ETHNIC_GROUP_C = p.ETHNIC_GROUP_C
    LEFT JOIN scheduled_visits ON scheduled_visits.PAT_ID = #cohort_information.PAT_ID
                                  AND scheduled_visits.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN scheduled_physician_visits ON scheduled_physician_visits.PAT_ID = #cohort_information.PAT_ID
                                            AND scheduled_physician_visits.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN dental_bh_episodes ON dental_bh_episodes.PAT_ID = #cohort_information.PAT_ID
WHERE #cohort_information.PAT_ID NOT IN ( SELECT excluded_groupers.PAT_ID FROM excluded_groupers );
