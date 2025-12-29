/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Care Team Monitoring - Tableau
 Create Date: 1/31/2023
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  Cheryl T

 Purpose:   Monitor to make sure all HIV+ medical pts have a full care team - PCP, RN, MH/BH, Dentist, CM and Pharmacist

 Description:
 

 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#ap') IS NOT NULL DROP TABLE #ap;
SELECT pev.PAT_ID,
       pev.CONTACT_DATE AS LAST_OFFICE_VISIT,
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWAUKEE'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KN' THEN 'KENOSHA'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'GB' THEN 'GREEN BAY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'WS' THEN 'WAUSAU'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AP' THEN 'APPLETON'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'EC' THEN 'EAU CLAIRE'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'LC' THEN 'LACROSSE'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MD' THEN 'MADISON'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'BL' THEN 'BELOIT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'BI' THEN 'BILLING'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'SL' THEN 'ST LOUIS'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'DN' THEN 'DENVER'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AS' THEN 'AUSTIN'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KC' THEN 'KANSAS CITY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2)
       END AS CITY,
       CASE SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2)
           WHEN 'MD' THEN 'MEDICAL'
           WHEN 'DT' THEN 'DENTAL'
           WHEN 'CM' THEN 'CASE MANAGEMENT'
           WHEN 'RX' THEN 'PHARMACY'
           WHEN 'AD' THEN 'BEHAVIORAL'
           WHEN 'PY' THEN 'BEHAVIORAL'
           WHEN 'BH' THEN 'BEHAVIORAL'
           WHEN 'MH' THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS LOS,
       ser.PROV_NAME 'PCP',
       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
INTO #ap
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE ser.SERV_AREA_ID = 64
      AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
      AND pev.CONTACT_DATE > DATEADD(MM, -12, GETDATE()) --Visit in past year
      AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                     8053, 8054, 8055, 8056 ) -- Office Visits
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' -- Visit was in a medical department
      AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
      AND p4.PAT_LIVING_STAT_C = 1;


IF OBJECT_ID('tempdb..#pharm') IS NOT NULL DROP TABLE #pharm;
SELECT ct.PAT_ID,
       ser.PROV_NAME PharmD
INTO #pharm
FROM Clarity.dbo.PAT_PCP_VIEW ct --ZC_TRTMT_TEAM_REL (shows RELATIONSHIP_C)
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
WHERE ct.TERM_DATE IS NULL
      AND ct.DELETED_YN = 'N' --sometimes people delete the provider instead of terming them
      AND ser.PROVIDER_TYPE_C = '102';


IF OBJECT_ID('tempdb..#rn') IS NOT NULL DROP TABLE #rn;
SELECT ct.PAT_ID,
       ser.PROV_NAME RN
INTO #rn
FROM Clarity.dbo.PAT_PCP_VIEW ct --ZC_TRTMT_TEAM_REL (shows RELATIONSHIP_C)
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
WHERE ct.TERM_DATE IS NULL
      AND ct.DELETED_YN = 'N' --sometimes people delete the provider instead of terming them
      AND ser.PROVIDER_TYPE_C = '3';


IF OBJECT_ID('tempdb..#dt') IS NOT NULL DROP TABLE #dt;
SELECT ct.PAT_ID,
       ser.PROV_NAME AS Dentist
INTO #dt
FROM Clarity.dbo.PAT_PCP_VIEW ct --ZC_TRTMT_TEAM_REL (shows RELATIONSHIP_C)
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
WHERE ct.TERM_DATE IS NULL
      AND ct.DELETED_YN = 'N' --sometimes people delete the provider instead of terming them
      AND ser.PROVIDER_TYPE_C = '108';


IF OBJECT_ID('tempdb..#cm') IS NOT NULL DROP TABLE #cm;
SELECT ct.PAT_ID,
       ser.PROV_NAME AS 'Case Manager'
INTO #cm
FROM Clarity.dbo.PAT_PCP_VIEW ct --ZC_TRTMT_TEAM_REL (shows RELATIONSHIP_C)
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
WHERE ct.TERM_DATE IS NULL
      AND ct.DELETED_YN = 'N' --sometimes people delete the provider instead of terming them
      AND ct.RELATIONSHIP_C = '1010'; --1010  Care Coordinator


IF OBJECT_ID('tempdb..#mht') IS NOT NULL DROP TABLE #mht;
SELECT ct.PAT_ID,
       ser.PROV_NAME AS 'MH Therapist'
INTO #mht
FROM Clarity.dbo.PAT_PCP_VIEW ct --ZC_TRTMT_TEAM_REL (shows RELATIONSHIP_C)
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
WHERE ct.TERM_DATE IS NULL
      AND ct.DELETED_YN = 'N' --sometimes people delete the provider instead of terming them
      AND ser.PROVIDER_TYPE_C IN ( '171', '117', '134', '10', '110', '177', '175', '227' );


IF OBJECT_ID('tempdb..#psyc') IS NOT NULL DROP TABLE #psyc;
SELECT ct.PAT_ID,
       ser.PROV_NAME AS 'Psychiatrist'
INTO #psyc
FROM Clarity.dbo.PAT_PCP_VIEW ct --ZC_TRTMT_TEAM_REL (shows RELATIONSHIP_C)
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
WHERE ct.TERM_DATE IS NULL
      AND ct.DELETED_YN = 'N' --sometimes people delete the provider instead of terming them
      AND ser.PROVIDER_TYPE_C IN ( '136', '164', '129' );


IF OBJECT_ID('tempdb..#tc') IS NOT NULL DROP TABLE #tc;
SELECT ct.PAT_ID,
       ser.PROV_NAME AS 'Team Coordinator'
INTO #tc
FROM Clarity.dbo.PAT_PCP_VIEW ct --ZC_TRTMT_TEAM_REL (shows RELATIONSHIP_C)
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
WHERE ct.TERM_DATE IS NULL
      AND ct.DELETED_YN = 'N' --sometimes people delete the provider instead of terming them
      AND ct.RELATIONSHIP_C = '1016'; --1016  Team Coordinator


IF OBJECT_ID('tempdb..#hiv_dx') IS NOT NULL DROP TABLE #hiv_dx;
WITH
    hiv_dx AS (
        SELECT IDENTITY_ID.PAT_ID,
               MIN(CASE WHEN IP_FLWSHT_MEAS.FLO_MEAS_ID = '1434' THEN DATEADD(DAY, CAST(IP_FLWSHT_MEAS.MEAS_VALUE AS INT), '12/31/1840')
                       WHEN EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21' )
                            AND PROBLEM_LIST.NOTED_DATE IS NOT NULL THEN PROBLEM_LIST.NOTED_DATE
                       ELSE PROBLEM_LIST.DATE_OF_ENTRY
                   END) AS HIV_DX_DATE
        FROM CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
            LEFT JOIN CLARITY.dbo.PROBLEM_LIST_VIEW AS PROBLEM_LIST ON IDENTITY_ID.PAT_ID = PROBLEM_LIST.PAT_ID
            LEFT JOIN CLARITY.dbo.CLARITY_EDG AS CLARITY_EDG ON PROBLEM_LIST.DX_ID = CLARITY_EDG.DX_ID
            LEFT JOIN CLARITY.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON CLARITY_EDG.DX_ID = EDG_CURRENT_ICD10.DX_ID
                                                                            AND EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21' )
            LEFT JOIN CLARITY.dbo.IP_FLWSHT_REC_VIEW AS IP_FLWSHT_REC ON IDENTITY_ID.PAT_ID = IP_FLWSHT_REC.PAT_ID
            LEFT JOIN CLARITY.dbo.IP_FLWSHT_MEAS_VIEW AS IP_FLWSHT_MEAS ON IP_FLWSHT_REC.FSD_ID = IP_FLWSHT_MEAS.FSD_ID
                                                                           AND IP_FLWSHT_MEAS.FLO_MEAS_ID IN ( '1434', '2952' )
        GROUP BY IDENTITY_ID.PAT_ID
    )
SELECT hiv_dx.PAT_ID, CAST(hiv_dx.HIV_DX_DATE AS DATE) AS HIV_DX_DATE INTO #hiv_dx FROM hiv_dx;


IF OBJECT_ID('tempdb..#latest_vls_info') IS NOT NULL DROP TABLE #latest_vls_info;
WITH
    vls AS (
        SELECT DISTINCT CLARITY_COMPONENT.COMPONENT_ID
        FROM CLARITY.dbo.CLARITY_COMPONENT AS CLARITY_COMPONENT
        WHERE CLARITY_COMPONENT.COMMON_NAME = 'HIV VIRAL LOAD'
    ),
    vls_info AS (
        SELECT ORDER_PROC.PAT_ID,
               ORDER_RESULTS.ORD_VALUE AS VIRAL_LOAD_RNA_VALUE,
               CASE WHEN ORDER_RESULTS.ORD_NUM_VALUE <> 9999999
                         AND ORDER_RESULTS.ORD_NUM_VALUE < 200 THEN 'SUPPRESSED'
                   WHEN ORDER_RESULTS.ORD_NUM_VALUE <> 9999999
                        AND ORDER_RESULTS.ORD_NUM_VALUE > 199 THEN 'UNSUPPRESSED'
                   WHEN ORDER_RESULTS.ORD_VALUE LIKE '>%' THEN 'UNSUPPRESSED'
                   ELSE 'SUPPRESSED'
               END AS VIRAL_LOAD_SUPPRESSION_STATUS_CATC,
               CASE WHEN ORDER_RESULTS.ORD_NUM_VALUE <> 9999999 THEN 'DETECTABLE'
                   WHEN ORDER_RESULTS.ORD_NUM_VALUE <> 9999999
                        AND ORDER_RESULTS.ORD_VALUE LIKE '>%' THEN 'DETECTABLE'
                   ELSE 'UNDETECTABLE'
               END AS VIRAL_LOAD_DETECTION_STATUS_CATC,
               CAST(ORDER_RESULTS.RESULT_DATE AS DATE) AS VLS_RESULT_DT,
               ROW_NUMBER() OVER (PARTITION BY ORDER_PROC.PAT_ID ORDER BY ORDER_RESULTS.RESULT_DATE DESC) AS ROW_NUM_DESC
        FROM CLARITY.dbo.ORDER_PROC_VIEW AS ORDER_PROC
            INNER JOIN CLARITY.dbo.ORDER_RESULTS_VIEW AS ORDER_RESULTS ON ORDER_PROC.ORDER_PROC_ID = ORDER_RESULTS.ORDER_PROC_ID
        WHERE ORDER_RESULTS.COMPONENT_ID IN ( SELECT vls.COMPONENT_ID FROM vls )
              AND ORDER_RESULTS.ORD_VALUE NOT IN ( 'Delete', 'See comment' )
              AND ORDER_RESULTS.LAB_STATUS_C IN ( 3, 5 ) -- 3 = Final; 5 = Edited Result - FINAL
    )
SELECT vls_info.PAT_ID,
       vls_info.VIRAL_LOAD_RNA_VALUE,
       vls_info.VIRAL_LOAD_SUPPRESSION_STATUS_CATC,
       vls_info.VIRAL_LOAD_DETECTION_STATUS_CATC,
       vls_info.VLS_RESULT_DT
INTO #latest_vls_info
FROM vls_info
WHERE vls_info.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#pat_enc_dep_los') IS NOT NULL DROP TABLE #pat_enc_dep_los;
SELECT PAT_ENC.PAT_ID,
       PAT_ENC.CONTACT_DATE AS LAST_OFFICE_VISIT,
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
       END AS LOS
INTO #pat_enc_dep_los
FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID
WHERE PAT_ENC.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND PAT_ENC.APPT_STATUS_C IN ( 2, 6 );


IF OBJECT_ID('tempdb..#last_appts') IS NOT NULL DROP TABLE #last_appts;
WITH
    medical_patients AS (
        SELECT #pat_enc_dep_los.PAT_ID,
               CAST(#pat_enc_dep_los.LAST_OFFICE_VISIT AS DATE) AS LAST_MEDICAL_APPOINTMENT,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
        WHERE #pat_enc_dep_los.LOS = 'MEDICAL'
    ),
    dental_patients AS (
        SELECT #pat_enc_dep_los.PAT_ID,
               CAST(#pat_enc_dep_los.LAST_OFFICE_VISIT AS DATE) AS LAST_DENTAL_APPOINTMENT,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
        WHERE #pat_enc_dep_los.LOS = 'DENTAL'
    ),
    bh_patients AS (
        SELECT #pat_enc_dep_los.PAT_ID,
               CAST(#pat_enc_dep_los.LAST_OFFICE_VISIT AS DATE) AS LAST_BH_APPOINTMENT,
               ROW_NUMBER() OVER (PARTITION BY #pat_enc_dep_los.PAT_ID
                                  ORDER BY #pat_enc_dep_los.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #pat_enc_dep_los
        WHERE LOS = 'BEHAVIORAL'
    )
SELECT PATIENT.PAT_ID,
       medical_patients.LAST_MEDICAL_APPOINTMENT,
       dental_patients.LAST_DENTAL_APPOINTMENT,
       bh_patients.LAST_BH_APPOINTMENT
INTO #last_appts
FROM CLARITY.dbo.PATIENT_VIEW AS PATIENT
    LEFT JOIN medical_patients ON PATIENT.PAT_ID = medical_patients.PAT_ID
                                  AND medical_patients.ROW_NUM_DESC = 1
    LEFT JOIN dental_patients ON PATIENT.PAT_ID = dental_patients.PAT_ID
                                 AND dental_patients.ROW_NUM_DESC = 1
    LEFT JOIN bh_patients ON PATIENT.PAT_ID = bh_patients.PAT_ID
                             AND bh_patients.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#missed_appointments') IS NOT NULL DROP TABLE #missed_appointments;
WITH
    appointments AS (
        SELECT PAT_ENC.PAT_ID,
               CASE ZC_APPT_STATUS.NAME
                   WHEN 'Canceled' THEN 'Cancelled'
                   WHEN 'Completed' THEN 'Kept'
                   WHEN 'Late Cancel' THEN 'Missed'
                   WHEN 'Late - Patient too late to be seen' THEN 'Missed'
                   WHEN 'Left without seen' THEN 'Missed'
                   WHEN 'No Coverage' THEN 'Missed'
                   WHEN 'No Show' THEN 'Missed'
                   WHEN 'Scheduled' THEN 'Missed'
                   WHEN 'Arrived' THEN 'Kept'
                   WHEN 'Non Encounter Visit' THEN 'Other- Non Encounter Visit'
                   WHEN 'Void' THEN 'Other - Void'
                   WHEN 'Late - Rescheduled' THEN 'Missed'
                   WHEN 'Patient not cooperative' THEN 'Other - Patient not cooperative'
                   ELSE 'ERROR'
               END APPOINTMENTS,
               PAT_ENC.CONTACT_DATE
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.ZC_APPT_STATUS AS ZC_APPT_STATUS ON PAT_ENC.APPT_STATUS_C = ZC_APPT_STATUS.APPT_STATUS_C
        WHERE ZC_APPT_STATUS.NAME NOT IN ( 'Non Encounter Visit', 'Patient not cooperative', 'Void' )
              AND DATEDIFF(MONTH, PAT_ENC.CONTACT_DATE, GETDATE()) <= 12
    )
SELECT appointments.PAT_ID,
       COUNT(appointments.PAT_ID) AS MISSED_APPTS_COUNT
INTO #missed_appointments
FROM appointments
WHERE appointments.APPOINTMENTS = 'Missed'
GROUP BY appointments.PAT_ID;


IF OBJECT_ID('tempdb..#last_careplan_info') IS NOT NULL DROP TABLE #last_careplan_info;
WITH
    careplan_info AS (
        SELECT PAT_ENC.PAT_ID,
               CAST(SMARTTOOL_LOGGER.LOG_TIMESTAMP AS DATE) AS LAST_CARE_PLAN,
               ROW_NUMBER() OVER (PARTITION BY PAT_ENC.PAT_ID ORDER BY SMARTTOOL_LOGGER.LOG_TIMESTAMP DESC) AS ROW_NUM_DESC
        FROM CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC
            INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON PAT_ENC.PAT_ID = PATIENT.PAT_ID
            INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
            INNER JOIN CLARITY.dbo.SMARTTOOL_LOGGER_VIEW AS SMARTTOOL_LOGGER ON PAT_ENC.PAT_ENC_CSN_ID = SMARTTOOL_LOGGER.CSN
            INNER JOIN CLARITY.dbo.CL_SPHR AS CL_SPHR ON SMARTTOOL_LOGGER.SMARTPHRASE_ID = CL_SPHR.SMARTPHRASE_ID
        WHERE CLARITY_DEP.SERV_AREA_ID = 64
              AND DATEDIFF(MONTH, SMARTTOOL_LOGGER.LOG_TIMESTAMP, GETDATE()) <= 12
              AND CL_SPHR.SMARTPHRASE_NAME LIKE '%VHCAREPLAN%'
    )
SELECT careplan_info.PAT_ID,
       careplan_info.LAST_CARE_PLAN
INTO #last_careplan_info
FROM careplan_info
WHERE careplan_info.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#a') IS NOT NULL DROP TABLE #a;
SELECT DISTINCT id.IDENTITY_ID 'MRN',
                p.PAT_NAME 'Patient',
                ap.STATE,
                ap.CITY,
                ap.PCP,
                rn.RN,
                mht.[MH Therapist],
                psyc.Psychiatrist,
                dt.Dentist,
                cm.[Case Manager],
                pharm.PharmD,
                tc.[Team Coordinator],
                CASE WHEN rn.RN IS NULL THEN 'Missing Member(s)'
                    WHEN mht.[MH Therapist] IS NULL
                         AND psyc.Psychiatrist IS NULL THEN 'Missing Member(s)'
                    WHEN dt.Dentist IS NULL THEN 'Missing Member(s)'
                    WHEN cm.[Case Manager] IS NULL THEN 'Missing Member(s)'
                    WHEN pharm.PharmD IS NULL THEN 'Missing Member(s)'
                    ELSE 'Complete'
                END AS 'Care Team Status',
                #hiv_dx.HIV_DX_DATE,
                #latest_vls_info.VIRAL_LOAD_RNA_VALUE,
                #latest_vls_info.VIRAL_LOAD_SUPPRESSION_STATUS_CATC,
                #latest_vls_info.VIRAL_LOAD_DETECTION_STATUS_CATC,
                #latest_vls_info.VLS_RESULT_DT,
                #last_appts.LAST_MEDICAL_APPOINTMENT,
                #last_appts.LAST_DENTAL_APPOINTMENT,
                #last_appts.LAST_BH_APPOINTMENT,
                COALESCE(#missed_appointments.MISSED_APPTS_COUNT, 0) as MISSED_APPTS_COUNT,
                #last_careplan_info.LAST_CARE_PLAN
INTO #a
FROM Clarity.dbo.IDENTITY_ID_VIEW id
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    INNER JOIN #ap ap ON ap.PAT_ID = id.PAT_ID
                         AND ap.ROW_NUM_DESC = 1
    LEFT JOIN #pharm pharm ON pharm.PAT_ID = id.PAT_ID
    LEFT JOIN #rn rn ON rn.PAT_ID = id.PAT_ID
    LEFT JOIN #dt dt ON dt.PAT_ID = id.PAT_ID
    LEFT JOIN #cm cm ON cm.PAT_ID = id.PAT_ID
    LEFT JOIN #mht mht ON mht.PAT_ID = id.PAT_ID
    LEFT JOIN #psyc psyc ON psyc.PAT_ID = id.PAT_ID
    LEFT JOIN #tc tc ON tc.PAT_ID = id.PAT_ID
    LEFT JOIN #hiv_dx ON id.PAT_ID = #hiv_dx.PAT_ID
    LEFT JOIN #latest_vls_info ON id.PAT_ID = #latest_vls_info.PAT_ID
    LEFT JOIN #last_appts ON id.PAT_ID = #last_appts.PAT_ID
    LEFT JOIN #missed_appointments ON id.PAT_ID = #missed_appointments.PAT_ID
    LEFT JOIN #last_careplan_info ON id.PAT_ID = #last_careplan_info.PAT_ID;


IF OBJECT_ID('tempdb..#c') IS NOT NULL DROP TABLE #c;
SELECT a.MRN, ROW_NUMBER() OVER (PARTITION BY a.mrn ORDER BY a.pcp DESC) AS ROW_NUM_DESC INTO #c FROM #a a;


IF OBJECT_ID('tempdb..#b') IS NOT NULL DROP TABLE #b;
SELECT a.MRN, 'Duplicate(s)' 'Duplicate Care Team Member(s)' INTO #b FROM #c a WHERE a.ROW_NUM_DESC > 1;


SELECT DISTINCT a.MRN,
                a.Patient,
                a.STATE,
                a.CITY,
                a.PCP,
                a.RN,
                a.[MH Therapist],
                a.Psychiatrist,
                a.Dentist,
                a.[Case Manager] 'Care Coordinator',
                a.PharmD,
                a.[Team Coordinator],
                a.[Care Team Status],
                COALESCE(b.[Duplicate Care Team Member(s)], 'No Duplicates') 'Duplicate Care Team Member(s)',
                CASE WHEN a.[Care Team Status] = 'Missing Member(s)' THEN 'Needs Attention'
                    WHEN b.[Duplicate Care Team Member(s)] = 'Duplicate(s)' THEN 'Needs Attention'
                    ELSE 'All Good'
                END AS 'Overall Status',
                a.HIV_DX_DATE,
                a.VIRAL_LOAD_RNA_VALUE,
                a.VIRAL_LOAD_SUPPRESSION_STATUS_CATC,
                a.VIRAL_LOAD_DETECTION_STATUS_CATC,
                a.VLS_RESULT_DT,
                a.LAST_MEDICAL_APPOINTMENT,
                a.LAST_DENTAL_APPOINTMENT,
                a.LAST_BH_APPOINTMENT,
                a.MISSED_APPTS_COUNT,
                a.LAST_CARE_PLAN AS LAST_CARE_PLAN_DATE,
                GETDATE() AS UPDATE_DTTM
FROM #a a
    LEFT JOIN #b b ON b.MRN = a.MRN;
