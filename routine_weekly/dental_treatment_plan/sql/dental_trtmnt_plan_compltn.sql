SET NOCOUNT ON;

SELECT id.IDENTITY_ID MRN,
       id.PAT_ID,
       p.PAT_NAME 'Patient',
       zc.NAME 'Current County',
       zs.NAME 'Current State',
       dtp.[Treatment Plan Initiated],
       dtp.[iService County],
       dtp.[iService State]
INTO #a
FROM CLARITY.dbo.IDENTITY_ID_VIEW id
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    INNER JOIN CLARITY.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    INNER JOIN CLARITY.dbo.ZC_COUNTY zc ON zc.COUNTY_C = p.COUNTY_C
    INNER JOIN CLARITY.dbo.ZC_STATE zs ON p.STATE_C = zs.STATE_C
    INNER JOIN (SELECT CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWAUKEE'
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
                       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
                       pev.PAT_ID,
                       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
                FROM CLARITY.dbo.PAT_ENC_VIEW pev
                    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                WHERE pev.CONTACT_DATE > DATEADD(MONTH, -24, GETDATE())
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'TX'
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
                      AND pev.APPT_STATUS_C IN ( 2, 6 )) site ON id.PAT_ID = site.PAT_ID
                                                                 AND site.ROW_NUM_DESC = 1 -- Last Visit
    INNER JOIN -- Limit to pts seen at -24 to -13 months so they have time to be successful.
    (SELECT DISTINCT ---does not have to be current active pts
            pev.PAT_ID,
            t64.ORIG_SERVICE_DATE 'Treatment Plan Initiated',
            zcx.NAME 'iService County',
            zsx.NAME 'iService State'
     FROM CLARITY.dbo.CLARITY_TDL_TRAN_64_VIEW t64
         INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON pev.PAT_ENC_CSN_ID = t64.PAT_ENC_CSN_ID
         LEFT JOIN CLARITY.dbo.PAT_ADDR_CHNG_HX pax ON pax.PAT_ID = pev.PAT_ID --Address at time of service
                                                       AND pax.EFF_START_DATE <= t64.ORIG_SERVICE_DATE
                                                       AND pax.EFF_END_DATE > t64.ORIG_SERVICE_DATE
         LEFT JOIN CLARITY.dbo.ZC_COUNTY zcx ON pax.COUNTY_HX_C = zcx.COUNTY_C
         LEFT JOIN CLARITY.dbo.ZC_STATE zsx ON pax.STATE_HX_C = zsx.STATE_C
     WHERE t64.CPT_CODE = 'DTPLIN'
           AND t64.ORIG_SERVICE_DATE <= DATEADD(MONTH, -13, GETDATE())
           AND t64.DETAIL_TYPE = 1) dtp ON dtp.PAT_ID = id.PAT_ID
WHERE p4.PAT_LIVING_STAT_C = 1;
SELECT a.PAT_ID,
       CASE WHEN t64.ORIG_SERVICE_DATE <= DATEADD(MONTH, 12, a.[Treatment Plan Initiated]) THEN t64.ORIG_SERVICE_DATE
           ELSE NULL
       END AS 'Treatment Plan Completed',
       zcx.NAME 'Service County',
       zsx.NAME 'Service State'
INTO #b
FROM #a a
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = a.PAT_ID
    INNER JOIN CLARITY.dbo.CLARITY_TDL_TRAN_64_VIEW t64 ON t64.PAT_ENC_CSN_ID = pev.PAT_ENC_CSN_ID
    LEFT JOIN CLARITY.dbo.PAT_ADDR_CHNG_HX pax ON pax.PAT_ID = a.PAT_ID --Address at time of service
                                                  AND pax.EFF_START_DATE <= t64.ORIG_SERVICE_DATE
                                                  AND pax.EFF_END_DATE > t64.ORIG_SERVICE_DATE
    LEFT JOIN CLARITY.dbo.ZC_COUNTY zcx ON pax.COUNTY_HX_C = zcx.COUNTY_C
    LEFT JOIN CLARITY.dbo.ZC_STATE zsx ON pax.STATE_HX_C = zsx.STATE_C
WHERE t64.CPT_CODE = 'POTXC'
      AND t64.ORIG_SERVICE_DATE > a.[Treatment Plan Initiated]
      AND t64.DETAIL_TYPE = 1;
SELECT a.MRN,
       --,a.PAT_ID
       a.Patient,
       CASE ---Picks the address where plan was completed first, initiated second and current address if neither other exists.
           WHEN b.[Service County] IS NOT NULL THEN b.[Service County]
           WHEN a.[iService County] IS NOT NULL THEN a.[iService County]
           ELSE a.[Current County]
       END AS 'Service County',
       CASE ---Picks the address where plan was completed first, initiated second and current address if neither other exists.
           WHEN b.[Service State] IS NOT NULL THEN b.[Service State]
           WHEN a.[iService State] IS NOT NULL THEN a.[iService State]
           ELSE a.[Current State]
       END AS 'Service State',
       a.[Current County],
       a.[Current State],
       a.[Treatment Plan Initiated],
       CONVERT(NVARCHAR(30), a.[Treatment Plan Initiated], 101) AS 'Treatment Plan Init',
       CONVERT(NVARCHAR(30), b.[Treatment Plan Completed], 101) 'Treatment Plan Completed',
       CASE WHEN b.[Treatment Plan Completed] IS NOT NULL THEN 'Y'
           ELSE 'N'
       END AS 'Met Y/N'
INTO #c
FROM #a a
    LEFT JOIN #b b ON b.PAT_ID = a.PAT_ID;

SELECT c.MRN,
       c.Patient,
       c.[Service County],
       c.[Service State],
       c.[Current County],
       c.[Current State],
       c.[Treatment Plan Initiated],
       c.[Treatment Plan Init],
       MAX(c.[Treatment Plan Completed]) 'Treatment Plan Completed',
       MAX(c.[Met Y/N]) 'Met Y/N'
FROM #c c
GROUP BY c.MRN,
         c.Patient,
         c.[Service County],
         c.[Service State],
         c.[Current County],
         c.[Current State],
         c.[Treatment Plan Initiated],
         c.[Treatment Plan Init];

DROP TABLE #c;
DROP TABLE #b;
DROP TABLE #a;
