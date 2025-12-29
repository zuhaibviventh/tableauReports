SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT id.IDENTITY_ID MRN,
       pev.PAT_ID,
       p.PAT_NAME PATIENT,
       CAST(pev.CONTACT_DATE AS DATE) FIRST_ATTEMPTED_APPT,
       zas.NAME FIRST_APPT_STATUS,
       dep.DEPARTMENT_NAME,
       prc.PRC_NAME VISIT_TYPE,
       ser.PROV_NAME VISIT_PROVIDER,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'WI' THEN 'WISCONSIN'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'MO' THEN 'MISSOURI'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'CO' THEN 'COLORADO'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'TX' THEN 'TEXAS'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2)
       END AS STATE,
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
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KC' THEN 'KANSAS CITY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'DN' THEN 'DENVER'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'AS' THEN 'AUSTIN'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
           ELSE 'ERROR'
       END AS CITY,
       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_DESC
INTO #a
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.ZC_APPT_STATUS zas ON zas.APPT_STATUS_C = pev.APPT_STATUS_C
    INNER JOIN Clarity.dbo.CLARITY_PRC prc ON pev.APPT_PRC_ID = prc.PRC_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE pev.CONTACT_DATE BETWEEN '10/1/2023' AND '9/30/2024' --> DATEADD(MONTH, -24, GETDATE())
      AND pev.APPT_STATUS_C IS NOT NULL --To exclude interim notes or phone contacts from counting as baseline visit attempt
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND pev.APPT_PRC_ID IN ( 3, 319 )
      AND p.PAT_ID NOT IN ( SELECT DISTINCT flag.PATIENT_ID
                            FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                            WHERE flag.ACTIVE_C = 1
                                  AND (flag.PAT_FLAG_TYPE_C = '640005' --PrEP
                                       OR flag.PAT_FLAG_TYPE_C = '640008' --STI
                                       OR flag.PAT_FLAG_TYPE_C = '9800035' -- PEP
                                       OR flag.PAT_FLAG_TYPE_C = '640007' -- AODA HIV-
                                       OR flag.PAT_FLAG_TYPE_C = '640017') --False positive HIV test
)
      AND CAST(pev.CONTACT_DATE AS DATE) <= GETDATE();


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


SELECT a.MRN,
       a.PAT_ID,
       a.PATIENT,
       a.FIRST_ATTEMPTED_APPT,
       a.FIRST_APPT_STATUS,
       a.DEPARTMENT_NAME,
       a.VISIT_TYPE,
       a.VISIT_PROVIDER,
       a.CITY,
       a.STATE,
       fc.FIRST_COMPLETE,
       COALESCE(DATEDIFF(DAY, a.FIRST_ATTEMPTED_APPT, fc.FIRST_COMPLETE), -1) DAYS_TO_COMPLETE,
       CASE WHEN DATEDIFF(DAY, a.FIRST_ATTEMPTED_APPT, fc.FIRST_COMPLETE) < 91 THEN 'MET'
           ELSE 'UNMET'
       END AS MET_YN,
       CASE WHEN DATEDIFF(DAY, a.FIRST_ATTEMPTED_APPT, fc.FIRST_COMPLETE) < 91 THEN 1
           ELSE 0
       END AS MET_NUM,
       CASE WHEN DATEDIFF(DAY, a.FIRST_ATTEMPTED_APPT, fc.FIRST_COMPLETE) < 31 THEN 'MET'
           ELSE 'UNMET'
       END AS 'Linked in 30 Days',
       CASE WHEN DATEDIFF(DAY, a.FIRST_ATTEMPTED_APPT, fc.FIRST_COMPLETE) < 31 THEN 1
           ELSE 0
       END AS 'Num Linked in 30 Days',
       #scheduled_visits.NEXT_ANY_APPT AS [Next Any Appt],
       #scheduled_visits.NEXT_APPT_PROV AS [Next Appt Prov],
       #scheduled_pcp_visits.NEXT_PCP_APPT AS [Next PCP Appt],
       #scheduled_pcp_visits.NEXT_PCP_APPT_PROV AS [Next PCP Appt Prov],
       second_appts.NEXT_ANY_APPT AS [Second Any Appt],
       second_appts.NEXT_APPT_PROV AS [Second Any Appt Prov],
       third_appts.NEXT_ANY_APPT AS [Third Any Appt],
       third_appts.NEXT_APPT_PROV AS [Third Any Appt Prov]
FROM #a a
    LEFT JOIN (SELECT pev.PAT_ID,
                      CAST(pev.CONTACT_DATE AS DATE) FIRST_COMPLETE,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                   INNER JOIN #a a ON a.PAT_ID = pev.PAT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                     AND pev.CONTACT_DATE BETWEEN '10/1/2023' AND '9/30/2024' -- > '12/31/2018'
                     AND pev.CONTACT_DATE >= a.FIRST_ATTEMPTED_APPT --First completed appt on or after new pt sched

    ) fc ON fc.PAT_ID = a.PAT_ID
            AND fc.ROW_NUM_ASC = 1
    LEFT JOIN #scheduled_visits ON a.PAT_ID = #scheduled_visits.PAT_ID
                                   AND #scheduled_visits.ROW_NUM_ASC = 1
    LEFT JOIN #scheduled_visits AS second_appts ON a.PAT_ID = second_appts.PAT_ID
                                                   AND second_appts.ROW_NUM_ASC = 2
    LEFT JOIN #scheduled_visits AS third_appts ON a.PAT_ID = third_appts.PAT_ID
                                                  AND third_appts.ROW_NUM_ASC = 3
    LEFT JOIN #scheduled_pcp_visits ON a.PAT_ID = #scheduled_pcp_visits.PAT_ID
                                       AND #scheduled_pcp_visits.ROW_NUM_ASC = 1
WHERE a.ROW_NUM_DESC = 1;


DROP TABLE #a;

