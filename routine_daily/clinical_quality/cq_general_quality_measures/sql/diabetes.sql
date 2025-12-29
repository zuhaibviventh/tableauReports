SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#latest_visit_info') IS NOT NULL DROP TABLE #latest_visit_info;
SELECT PAT_ENC.PAT_ID,
       PAT_ENC.PAT_ENC_CSN_ID,
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
           ELSE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 5, 2)
       END AS CITY,
       CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2)
           WHEN 'MD' THEN 'MEDICAL'
           WHEN 'DT' THEN 'DENTAL'
           ELSE 'ERROR'
       END AS DEPARTMENT_NAME,
       CLARITY_SER.PROV_NAME AS PCP,
       ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE DESC) AS ROW_NUM_DESC
INTO #latest_visit_info
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_ENC.PCP_PROV_ID = CLARITY_SER.PROV_ID -- ensures a visit with a VH provider
WHERE PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
      AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) IN ( 'MD' )
      AND DATEDIFF(MONTH, PAT_ENC.CONTACT_DATE, GETDATE()) <= 12;


IF OBJECT_ID('tempdb..#patient_dm_information') IS NOT NULL DROP TABLE #patient_dm_information;
WITH
    dm_dx AS (
        SELECT 'DIABETES' AS category,
               edg_current_icd10.dx_id,
               edg_current_icd10.code AS dx_code
        FROM CLARITY.dbo.EDG_CURRENT_ICD10 AS edg_current_icd10
            INNER JOIN CLARITY.dbo.CLARITY_EDG AS clarity_edg ON edg_current_icd10.dx_id = clarity_edg.dx_id
        WHERE edg_current_icd10.code LIKE 'E0[89]%'
              OR edg_current_icd10.code LIKE 'E1[013]%'
    ),
    latest_dm_dx AS (
        SELECT PAT_ENC_DX.PAT_ID,
               CAST(PAT_ENC_DX.CONTACT_DATE AS DATE) AS LATEST_HTN_DX_DATE,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC_DX.PAT_ID
ORDER BY PAT_ENC_DX.CONTACT_DATE DESC) AS ROW_NUM_DESC
        FROM CLARITY.dbo.PAT_ENC_DX_VIEW AS PAT_ENC_DX
            INNER JOIN dm_dx ON PAT_ENC_DX.DX_ID = dm_dx.DX_ID
    )
SELECT latest_dm_dx.PAT_ID,
       latest_dm_dx.LATEST_HTN_DX_DATE
INTO #patient_dm_information
FROM latest_dm_dx
WHERE latest_dm_dx.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#latest_hemo_a1c_lab_info') IS NOT NULL DROP TABLE #latest_hemo_a1c_lab_info;
SELECT DM_DIABETES.PAT_ID,
       DM_DIABETES.HBA1C_LAST AS LATEST_HEMOGLOBIN_A1C_LAB_VALUE,
       CAST(DM_DIABETES.HBA1C_LAST_DT AS DATE) AS LATEST_LAB_RESULT_DATE
INTO #latest_hemo_a1c_lab_info
FROM CLARITY.dbo.DM_DIABETES_VIEW AS DM_DIABETES;


SELECT PATIENT.PAT_NAME AS PATIENT_NAME,
       (DATEDIFF(MONTH, PATIENT.BIRTH_DATE, GETDATE()) / 12) AS PATIENT_AGE,
       IDENTITY_ID.IDENTITY_ID AS MRN,
       #latest_visit_info.STATE,
       #latest_visit_info.CITY,
       #latest_visit_info.DEPARTMENT_NAME,
       #latest_visit_info.PCP,
       CASE WHEN #patient_dm_information.PAT_ID IS NOT NULL THEN 'MET'
           ELSE 'NOT MET'
       END AS DIABETES_DX,
       CASE WHEN LATEST_HEMOGLOBIN_A1C_LAB_VALUE < 7 THEN 'Under 7'
           WHEN (LATEST_HEMOGLOBIN_A1C_LAB_VALUE BETWEEN 7 AND 9) THEN '7 - 9'
           WHEN LATEST_HEMOGLOBIN_A1C_LAB_VALUE > 9 THEN 'Over 9'
       END AS HBA1C_CATEGORY,
       #latest_hemo_a1c_lab_info.LATEST_HEMOGLOBIN_A1C_LAB_VALUE,
       #latest_hemo_a1c_lab_info.LATEST_LAB_RESULT_DATE,
       CURRENT_TIMESTAMP AS UPDATE_DTTM
FROM #latest_visit_info
    INNER JOIN #latest_hemo_a1c_lab_info ON #latest_visit_info.PAT_ID = #latest_hemo_a1c_lab_info.PAT_ID
    LEFT JOIN #patient_dm_information ON #latest_visit_info.PAT_ID = #patient_dm_information.PAT_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON #latest_visit_info.PAT_ID = IDENTITY_ID.PAT_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
WHERE ROW_NUM_DESC = 1;

