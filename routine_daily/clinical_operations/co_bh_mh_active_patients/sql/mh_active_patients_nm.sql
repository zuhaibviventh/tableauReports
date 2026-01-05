/* javelin.ochin.org */
/* Purpose: To provide the list of patients to BH providers who haven't been 
            seen in last 6 months or more 
*/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#visit_info') IS NOT NULL DROP TABLE #visit_info;
SELECT pev.PAT_ID,
       CAST(pev.CONTACT_DATE AS DATE) AS LAST_OFFICE_VISIT,
       dep.STATE,
       ser.PROV_NAME,
       dep.CITY,
       dep.SITE,
       dep.SERVICE_TYPE,
	   dep.SERVICE_LINE,
	   dep.SUB_SERVICE_LINE
INTO #visit_info
FROM Clarity.dbo.PAT_ENC_VIEW pev
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pev.VISIT_PROV_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -60, GETDATE()) --Longer lookback for pts not seen in a long time
      AND pev.APPT_STATUS_C IN ( 2, 6 );


IF OBJECT_ID('tempdb..#bh_patients') IS NOT NULL DROP TABLE #bh_patients;
WITH
    bh_patients_helper AS (
        SELECT #visit_info.PAT_ID,
               #visit_info.LAST_OFFICE_VISIT,
               #visit_info.STATE,
               #visit_info.PROV_NAME,
               #visit_info.CITY,
               #visit_info.SITE,
               #visit_info.SERVICE_TYPE,
	           #visit_info.SERVICE_LINE,
	           #visit_info.SUB_SERVICE_LINE,
               ROW_NUMBER() OVER (PARTITION BY #visit_info.PAT_ID
ORDER BY #visit_info.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
        FROM #visit_info
        WHERE SERVICE_LINE = 'BEHAVIORAL'
    )
SELECT * INTO #bh_patients FROM bh_patients_helper WHERE bh_patients_helper.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#mh_patients') IS NOT NULL DROP TABLE #mh_patients;
SELECT p.PAT_ID,
       p.PAT_NAME,
       id.IDENTITY_ID AS MRN,
       CAST(pev.CONTACT_DATE AS DATE) AS 'Last Visit',
       ser.PROV_NAME,
       ser.PROV_ID,
       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
INTO #mh_patients
FROM CLARITY.dbo.pat_enc_VIEW pev
    INNER JOIN Clarity.dbo.PATIENT_VIEW AS p ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS id ON id.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.clarity_ser ser ON ser.PROV_ID = pev.VISIT_PROV_ID
    INNER JOIN Clarity.dbo.EPISODE_LINK_VIEW AS elv ON elv.PAT_ENC_CSN_ID = pev.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.EPISODE_VIEW AS ev ON elv.EPISODE_ID = ev.EPISODE_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.APPT_STATUS_C IN ( 2, 6 )
      AND ev.SUM_BLK_TYPE_ID = 221
      AND ev.STATUS_C = 1
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'PY', 'MH', 'BH' )
      AND ser.PROVIDER_TYPE_C NOT IN ( '164', '136', '129' );


IF OBJECT_ID('tempdb..#final_cohort') IS NOT NULL DROP TABLE #final_cohort;
WITH
    mh_bh_cohort AS (
        SELECT #bh_patients.PAT_ID,
               #bh_patients.STATE,
               #bh_patients.PROV_NAME,
               #bh_patients.CITY,
               #bh_patients.SITE,
               #bh_patients.SERVICE_TYPE,
	           #bh_patients.SERVICE_LINE,
	           #bh_patients.SUB_SERVICE_LINE,
               #mh_patients.PAT_NAME,
               #mh_patients.MRN,
               CAST(#mh_patients.[Last Visit] AS DATE) AS LAST_VISIT,
               DATEDIFF(MONTH, [Last Visit], CURRENT_TIMESTAMP) AS MONTHS_SINCE_SEEN,
               #mh_patients.PROV_NAME AS LAST_VISIT_PROVIDER,
               #mh_patients.PROV_ID AS LAST_VISIT_PROVIDER_ID
        FROM #bh_patients
            INNER JOIN #mh_patients ON #bh_patients.PAT_ID = #mh_patients.PAT_ID
        WHERE #mh_patients.ROW_NUM_DESC = 1
    ),
    mh_care_team AS (
        SELECT ct.PAT_ID,
               ser.PROV_NAME AS MH_THERAPIST,
               ser.PROV_ID
        FROM Clarity.dbo.PAT_PCP_VIEW ct --ZC_TRTMT_TEAM_REL (shows RELATIONSHIP_C)
            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
        WHERE ct.TERM_DATE IS NULL
              AND ct.DELETED_YN = 'N' --sometimes people delete the provider instead of terming them
              AND ser.PROVIDER_TYPE_C IN ( '171', '117', '134', '10', '110', '177', '175', '227' )
    )
SELECT mh_bh_cohort.PAT_ID,
       mh_bh_cohort.STATE,
       mh_bh_cohort.PROV_NAME,
       mh_bh_cohort.CITY,
       mh_bh_cohort.SITE,
       mh_bh_cohort.SERVICE_TYPE,
       mh_bh_cohort.SERVICE_LINE,
       mh_bh_cohort.SUB_SERVICE_LINE,
       mh_bh_cohort.PAT_NAME,
       mh_bh_cohort.MRN,
       mh_bh_cohort.LAST_VISIT,
       mh_bh_cohort.MONTHS_SINCE_SEEN,
       mh_bh_cohort.LAST_VISIT_PROVIDER,
       CASE WHEN CLARITY_DEP.DEPARTMENT_ID IS NOT NULL THEN CAST(PAT_ENC.CONTACT_DATE AS DATE)
           ELSE NULL
       END AS NEXT_VISIT_DATE,
       CLARITY_DEP.DEPARTMENT_NAME AS NEXT_VISIT_DEPARTMENT,
       CASE WHEN mh_bh_cohort.LAST_VISIT_PROVIDER_ID = mh_care_team.PROV_ID THEN 'Y'
           ELSE 'N'
       END AS LAST_VISIT_PROVIDER_IN_CARE_TEAM_YN,
       ROW_NUMBER() OVER (PARTITION BY mh_bh_cohort.PAT_ID ORDER BY PAT_ENC.CONTACT_DATE DESC) AS ROW_NUM_DESC
INTO #final_cohort
FROM mh_bh_cohort
    LEFT JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON mh_bh_cohort.PAT_ID = PAT_ENC.PAT_ID
                                                     AND PAT_ENC.APPT_STATUS_C = 1
                                                     AND PAT_ENC.CONTACT_DATE > CURRENT_TIMESTAMP
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
                                                             AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'PY', 'MH', 'BH' )
    LEFT JOIN mh_care_team ON mh_bh_cohort.PAT_ID = mh_care_team.PAT_ID;


SELECT #final_cohort.MRN,
       #final_cohort.PAT_NAME,
       #final_cohort.CITY,
       #final_cohort.STATE,
       #final_cohort.PROV_NAME,
       #final_cohort.SERVICE_TYPE,
       #final_cohort.SERVICE_LINE,
       #final_cohort.SUB_SERVICE_LINE,
       #final_cohort.LAST_VISIT,
       #final_cohort.MONTHS_SINCE_SEEN,
       #final_cohort.LAST_VISIT_PROVIDER,
       #final_cohort.NEXT_VISIT_DATE,
       #final_cohort.NEXT_VISIT_DEPARTMENT,
       #final_cohort.LAST_VISIT_PROVIDER_IN_CARE_TEAM_YN
FROM #final_cohort
WHERE #final_cohort.ROW_NUM_DESC = 1;
