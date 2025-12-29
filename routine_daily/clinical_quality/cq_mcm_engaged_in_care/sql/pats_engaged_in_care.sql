SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT id.IDENTITY_ID NEW_PT,
       pev.PAT_ID,
       p.PAT_NAME,
       pev.CONTACT_DATE FIRST_VISIT,
       dep.DEPARTMENT_NAME,
       ser.EXTERNAL_NAME VISIT_PROVIDER,
       ser.EXTERNAL_NAME PCP,
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_DESC
INTO #a
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
WHERE pev.APPT_STATUS_C IN ( 2, 6 )
      AND pev.CONTACT_DATE > '12/31/2018' -- For Alteryx/Tableau
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND pev.APPT_PRC_ID IN ( 3, 319 )
      AND p.PAT_ID NOT IN ( SELECT DISTINCT --Just to exclude HIV- pts who had a mis-coded visit type
                                   flag.PATIENT_ID
                            FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                            WHERE flag.ACTIVE_C = 1
                                  AND (flag.PAT_FLAG_TYPE_C = '640005' --PrEP
                                       OR flag.PAT_FLAG_TYPE_C = '640008' --STI
                                       OR flag.PAT_FLAG_TYPE_C = '9800035' -- PEP
                                       OR flag.PAT_FLAG_TYPE_C = '640007' -- AODA HIV-
                                       OR flag.PAT_FLAG_TYPE_C = '640017') --False positive HIV test
);

SELECT a.NEW_PT,
       a.PAT_ID,
       a.PAT_NAME,
       a.FIRST_VISIT,
       a.DEPARTMENT_NAME,
       a.VISIT_PROVIDER,
       a.STATE,
       a.PCP,
       COUNT(pev.CONTACT_DATE) NUM_VISITS,
       CASE 
         WHEN COUNT(pev.CONTACT_DATE) > 1 THEN 'MET'
         ELSE 'UNMET'
       END AS MET_YN,
       CASE 
         WHEN COUNT(pev.CONTACT_DATE) > 1 THEN 1
         ELSE 0
       END AS MET_NUM,
       DATEDIFF(DAY, a.FIRST_VISIT, GETDATE()) 'DAYS_AGO'
FROM #a a
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = a.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE a.ROW_NUM_DESC = 1
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND pev.CONTACT_DATE > '12/31/2018'
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND pev.CONTACT_DATE
      BETWEEN a.FIRST_VISIT AND DATEADD(DAY, 90, a.FIRST_VISIT) --A second visit within 90 days
--AND a.PCP IS NOT NULL
GROUP BY a.NEW_PT,
         a.PAT_ID,
         a.PCP,
         a.PAT_NAME,
         a.FIRST_VISIT,
         a.DEPARTMENT_NAME,
         a.VISIT_PROVIDER,
         a.STATE;

DROP TABLE #a;