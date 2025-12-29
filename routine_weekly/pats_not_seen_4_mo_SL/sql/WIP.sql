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
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE()) --can't do this since looking for pts not seen in a long time
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
WHERE clarity_ser.SERV_AREA_ID = 64
      AND clarity_ser.PROVIDER_TYPE_C IN ( 1, 9, 6, 113 ) -- Physicians and NPs, PAs
      --AND pev.CONTACT_DATE > DATEADD (MM,-12, CURRENT_TIMESTAMP) --Can't do since looking for OOC pts
      AND pat_enc.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
      AND pat_enc.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951,
                                         7952, 7953, 7954, 7970, 7971, 7972,
                                         7973, 7974, 8047, 8048, 8049, 8050,
                                         8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
      AND pat_enc.DEPARTMENT_ID IN ( 64001001, 64002001, 64003001, 64011001,
                                     64012002, 64013001 ) -- Visit was in a medical department
      AND EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
      AND problem_list.RESOLVED_DATE IS NULL --Active Dx
      AND problem_list.PROBLEM_STATUS_C = 1 --Active Dx
      AND patient_4.PAT_LIVING_STAT_C = 1
GROUP BY pat_enc.PAT_ID;

IF OBJECT_ID('tempdb..#active_hiv_medical_patients') IS NOT NULL
    DROP TABLE #active_hiv_medical_patients;
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
SELECT target_service_line.PAT_ID,
       target_service_line.LOS,
       target_service_line.CITY,
       target_service_line.STATE,
       target_service_line.LAST_OFFICE_VISIT
INTO #active_hiv_medical_patients
FROM target_service_line
    INNER JOIN #active_hiv_patients ON #active_hiv_patients.PAT_ID = target_service_line.PAT_ID
WHERE target_service_line.ROW_NUM_DESC = 1;

/* Defines results output */
IF OBJECT_ID('tempdb..#result_output_definition') IS NOT NULL
    DROP TABLE #result_output_definition;
SELECT id.IDENTITY_ID AS MRN,
       #active_hiv_medical_patients.LAST_OFFICE_VISIT,
       #active_hiv_medical_patients.CITY,
       #active_hiv_medical_patients.STATE,
       p.PAT_ID,
       orv.ORD_VALUE,
       orv.RESULT_DATE,
       p.BIRTH_DATE,
       p.PAT_NAME,
       pr.PATIENT_RACE_C,
       p.SEX_C,
       CASE WHEN ISNUMERIC(orv.ORD_VALUE) = 1
                THEN orv.ORD_VALUE
       WHEN orv.ORD_VALUE LIKE '>%'
           THEN 10000000
       ELSE 0.01
       END AS Result_Output,
       ROW_NUMBER() OVER (PARTITION BY opv.PAT_ID ORDER BY orv.RESULT_DATE DESC) AS ROW_NUM_DESC,
       ser.EXTERNAL_NAME PROV_NAME
INTO #result_output_definition
FROM Clarity.dbo.ORDER_PROC_VIEW opv
    INNER JOIN #active_hiv_medical_patients ON opv.PAT_ID = #active_hiv_medical_patients.PAT_ID
    INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = #active_hiv_medical_patients.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON pr.PAT_ID = p.PAT_ID
                                             AND pr.LINE = 1
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON cc.COMPONENT_ID = orv.COMPONENT_ID
WHERE orv.RESULT_DATE > DATEADD(MONTH, -36, GETDATE()) --Need this to be long ago
      AND cc.COMMON_NAME = 'HIV VIRAL LOAD'
      AND orv.ORD_VALUE NOT IN ( 'Delete', 'See comment' );

/* Identify MRNs that belong to patients whose earliest lab result was >= 200 */
IF OBJECT_ID('tempdb..#most_recent') IS NOT NULL DROP TABLE #most_recent;
SELECT #result_output_definition.MRN,
       #result_output_definition.RESULT_DATE AS MOST_RECENT_RESULT_DATE,
       #result_output_definition.Result_Output AS MOST_RECENT_RESULT_OUTPUT,
       #result_output_definition.LAST_OFFICE_VISIT,
       #result_output_definition.CITY,
       #result_output_definition.STATE,
       #result_output_definition.ORD_VALUE
INTO #most_recent
FROM #result_output_definition
WHERE #result_output_definition.ROW_NUM_DESC = 1
ORDER BY MRN;

/* Leverage Result Output column from first query to Classify Results */
IF OBJECT_ID('tempdb..#patient_results') IS NOT NULL
    DROP TABLE #patient_results;
SELECT #result_output_definition.MRN,
       #result_output_definition.PAT_ID,
       #result_output_definition.PROV_NAME AS PCP,
       #result_output_definition.PAT_NAME,
       CONVERT(NVARCHAR(30), #result_output_definition.LAST_OFFICE_VISIT, 101) AS LAST_OFFICE_VISIT,
       DATEDIFF(MONTH, #result_output_definition.LAST_OFFICE_VISIT, GETDATE()) AS 'MONTHS AGO',
       CONVERT(NVARCHAR(30), #result_output_definition.BIRTH_DATE, 101) AS BIRTH_DATE,
       zpr.NAME AS RACE,
       zs.NAME AS SEX,
       #most_recent.CITY,
       #most_recent.STATE,
       CONVERT(NVARCHAR(30), #most_recent.MOST_RECENT_RESULT_DATE, 101) AS MOST_RECENT_RESULT_DATE,
       #most_recent.MOST_RECENT_RESULT_OUTPUT,
       #most_recent.ORD_VALUE,
       CASE WHEN ISNUMERIC(#most_recent.ORD_VALUE) = 1
                THEN #most_recent.ORD_VALUE
       WHEN #most_recent.ORD_VALUE LIKE '>%'
           THEN 10000000
       ELSE 0.01
       END AS VIRAL_LOAD
INTO #patient_results
FROM #result_output_definition
    INNER JOIN #most_recent ON #result_output_definition.MRN = #most_recent.MRN
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON #result_output_definition.PATIENT_RACE_C = zpr.PATIENT_RACE_C
    LEFT JOIN Clarity.dbo.ZC_SEX zs ON #result_output_definition.SEX_C = zs.RCPT_MEM_SEX_C
ORDER BY #result_output_definition.MRN;

/* Next Any Appt */
IF OBJECT_ID('tempdb..#nxt_any_appt') IS NOT NULL DROP TABLE #nxt_any_appt;
SELECT #patient_results.MRN,
       #patient_results.PAT_ID,
       #patient_results.PCP,
       #patient_results.PAT_NAME,
       #patient_results.LAST_OFFICE_VISIT,
       #patient_results.STATE,
       #patient_results.CITY,
       #patient_results.[MONTHS AGO],
       #patient_results.BIRTH_DATE,
       #patient_results.RACE,
       #patient_results.SEX,
       #patient_results.MOST_RECENT_RESULT_DATE,
       #patient_results.MOST_RECENT_RESULT_OUTPUT,
       CASE WHEN #patient_results.VIRAL_LOAD < 200
                THEN 'SUPPRESSED'
       ELSE 'UNSUPPRESSED'
       END AS 'SUPRESSION_STATUS',
       #patient_results.ORD_VALUE,
       pev2.CONTACT_DATE NEXT_APPT,
       ser2.EXTERNAL_NAME NEXT_APPT_PROV,
       ROW_NUMBER() OVER (PARTITION BY #patient_results.PAT_ID ORDER BY pev2.CONTACT_DATE ASC) AS ROW_NUM_ASC
INTO #nxt_any_appt
FROM #patient_results
    LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev2 ON #patient_results.PAT_ID = pev2.PAT_ID
                                               AND pev2.APPT_STATUS_C = 1
                                               AND pev2.CONTACT_DATE >= GETDATE()
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser2 ON pev2.VISIT_PROV_ID = ser2.PROV_ID
WHERE #patient_results.LAST_OFFICE_VISIT < DATEADD(MONTH, -3, GETDATE());

/* Next PCP Appt */
SELECT #nxt_any_appt.MRN,
       #nxt_any_appt.PCP,
       #nxt_any_appt.PAT_NAME AS 'PATIENT',
       #nxt_any_appt.LAST_OFFICE_VISIT AS 'LAST OFFICE VISIT',
       #nxt_any_appt.STATE,
       #nxt_any_appt.CITY,
       #nxt_any_appt.[MONTHS AGO],
       #nxt_any_appt.BIRTH_DATE,
       #nxt_any_appt.RACE,
       #nxt_any_appt.SEX,
       #nxt_any_appt.MOST_RECENT_RESULT_DATE AS 'LAST LAB',
       #nxt_any_appt.ORD_VALUE AS 'LAST VL',
       #nxt_any_appt.SUPRESSION_STATUS,
       CONVERT(NVARCHAR(30), #nxt_any_appt.NEXT_APPT, 101) AS 'NEXT ANY APPT',
       #nxt_any_appt.NEXT_APPT_PROV 'NEXT APPT PROVIDER',
       CONVERT(NVARCHAR(30), na.NEXT_PCP_APPT, 101) AS 'NEXT PCP APPT',
       na.EXTERNAL_NAME 'PCP APPT PROVIDER'
FROM #nxt_any_appt
    LEFT JOIN (SELECT pev.PAT_ID,
                      pev.CONTACT_DATE NEXT_PCP_APPT,
                      ser.EXTERNAL_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                                                                 AND pev.APPT_STATUS_C = 1
                                                                 AND pev.CONTACT_DATE >= GETDATE()
               WHERE pev.APPT_STATUS_C = 1
                     AND ser.PROVIDER_TYPE_C IN ( 1, 9, 113 )
                     AND ser.PROV_ID <> '640178' --pulmonologist
    ) na ON #nxt_any_appt.PAT_ID = na.PAT_ID
            AND na.ROW_NUM_ASC = 1
WHERE #nxt_any_appt.ROW_NUM_ASC = 1;
