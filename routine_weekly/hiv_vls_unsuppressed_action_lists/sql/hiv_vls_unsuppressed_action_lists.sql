SET NOCOUNT ON;

SELECT TOP 10000000 pev.PAT_ID,
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
                    WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KC'
                        THEN 'KANSAS CITY'
                    ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2)
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
                    ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2)
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
                    ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2)
                    END AS 'LOS'
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT TOP 10000000 a1.PAT_ID,
                    a1.STATE,
                    a1.CITY,
                    a1.SITE,
                    a1.LOS,
                    ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT TOP 10000000 a2.PAT_ID,
                    a2.LOS,
                    a2.CITY,
                    a2.STATE
INTO #Attribution3
FROM #Attribution2 a2
WHERE a2.ROW_NUM_DESC = 1;

SELECT TOP 10000000 id.IDENTITY_ID,
                    p.PAT_ID,
                    p.PAT_NAME,
                    p.ZIP,
                    p.CUR_PRIM_LOC_ID,
                    COALESCE(zgi.NAME, 'Not Asked') AS GENDER,
                    CASE WHEN MIN(pev.CONTACT_DATE)
                              BETWEEN DATEADD(MONTH, -12, GETDATE()) AND DATEADD(
                                                                         MONTH,
                                                                         -6,
                                                                         GETDATE())
                             THEN 1
                    ELSE 0
                    END AS 'In-Care',
                    orv.ORD_VALUE,
                    orv.ORD_NUM_VALUE,
                    orv.RESULT_DATE,
                    ser.EXTERNAL_NAME,
                    zso.NAME 'Sexual Orientation',
                    ROW_NUMBER() OVER (PARTITION BY opv.PAT_ID ORDER BY orv.RESULT_DATE DESC) AS ROW_NUM_DESC
INTO #a
FROM Clarity.dbo.ORDER_PROC_VIEW opv
    INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
    INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON cc.COMPONENT_ID = orv.COMPONENT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = opv.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_SEX sex ON p.SEX_C = sex.RCPT_MEM_SEX_C
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_GENDER_IDENTITY zgi ON zgi.GENDER_IDENTITY_C = p4.GENDER_IDENTITY_C
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID --Need pev in this step to check for In-care
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    LEFT JOIN Clarity.dbo.PAT_SEXUAL_ORIENTATION pso ON opv.PAT_ID = pso.PAT_ID
                                                        AND pso.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_SEXUAL_ORIENTATION zso ON pso.SEXUAL_ORIENTATN_C = zso.SEXUAL_ORIENTATION_C
WHERE orv.RESULT_DATE
      BETWEEN DATEADD(MONTH, -12, GETDATE()) AND GETDATE()
      AND pev.CONTACT_DATE
      BETWEEN DATEADD(MONTH, -12, GETDATE()) AND GETDATE() --Visit in past year
      AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951,
                                     7952, 7953, 7954, 7970, 7971, 7972,
                                     7973, 7974, 8047, 8048, 8049, 8050,
                                     8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND orv.ORD_VALUE NOT IN ( 'Delete', 'See comment' )
      AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
      AND p4.PAT_LIVING_STAT_C = 1
      AND ser.SERV_AREA_ID = 64
      AND cc.COMMON_NAME = 'HIV VIRAL LOAD'
GROUP BY id.IDENTITY_ID,
         p.PAT_ID,
         p.ZIP,
         opv.PAT_ID,
         p.PAT_NAME,
         p.CUR_PRIM_LOC_ID,
         orv.ORD_VALUE,
         orv.ORD_NUM_VALUE,
         orv.RESULT_DATE,
         ser.EXTERNAL_NAME,
         zso.NAME,
         zgi.NAME;

SELECT TOP 10000000 a.IDENTITY_ID,
                    a.PAT_ID,
                    a.ZIP,
                    a.GENDER,
                    a.PAT_NAME,
                    a.CUR_PRIM_LOC_ID,
                    a.ORD_VALUE,
                    a.EXTERNAL_NAME,
                    a.RESULT_DATE LAST_LAB,
                    CASE WHEN a.ORD_NUM_VALUE <> 9999999
                             THEN a.ORD_NUM_VALUE
                    WHEN a.ORD_VALUE LIKE '>%'
                        THEN 10000000
                    ELSE 0
                    END AS Result_Output,
                    a.[In-Care],
                    a.[Sexual Orientation]
INTO #b
FROM #a a
WHERE a.ROW_NUM_DESC = 1;

SELECT TOP 10000000 b.IDENTITY_ID,
                    b.PAT_ID,
                    p.BIRTH_DATE,
                    b.ZIP,
                    b.GENDER,
                    CASE WHEN att.STATE = 'MO'
                             THEN 'MISSOURI'
                    WHEN att.STATE = 'CO'
                        THEN 'COLORADO'
                    WHEN att.STATE = 'WI'
                        THEN 'WISCONSIN'
                    WHEN att.STATE = 'TX'
                        THEN 'TEXAS'
                    ELSE att.STATE
                    END AS 'STATE',
                    att.CITY,
                    zc.NAME COUNTY,
                    b.CUR_PRIM_LOC_ID,
                    loc.LOC_NAME,
                    b.EXTERNAL_NAME,
                    b.PAT_NAME,
                    b.ORD_VALUE,
                    b.LAST_LAB,
                    b.Result_Output,
                    CASE WHEN b.Result_Output < 200
                             THEN 1
                    ELSE 0
                    END AS SUPPRESSED,
                    CASE WHEN b.Result_Output < 200
                             THEN 'SUPPRESSED'
                    ELSE 'UNSUPPRESSED'
                    END AS VLS_CATEGORY,
                    zeg.NAME ETHNICITY,
                    zs.NAME SEX,
                    CASE WHEN pr.PATIENT_RACE_C IS NULL
                             THEN 'Unknown'
                    WHEN pr.PATIENT_RACE_C = 2
                        THEN 'Black'
                    WHEN pr.PATIENT_RACE_C = 6
                        THEN 'White'
                    WHEN pr.PATIENT_RACE_C IN ( 5, 10, 11 )
                        THEN 'Unknown'
                    ELSE zpr.NAME
                    END AS RACE,
                    zpr.NAME 'All Race',
                    b.[In-Care],
                    bh.[BH PATIENT],
                    dent.[DENTAL PATIENT],
                    b.[Sexual Orientation]
INTO #c
FROM #b b
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON b.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_COUNTY zc ON p.COUNTY_C = zc.COUNTY_C
    LEFT JOIN Clarity.dbo.CLARITY_LOC loc ON p.CUR_PRIM_LOC_ID = loc.LOC_ID
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON p.PAT_ID = pr.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON pr.PATIENT_RACE_C = zpr.PATIENT_RACE_C
    LEFT JOIN Clarity.dbo.ZC_ETHNIC_GROUP zeg ON p.ETHNIC_GROUP_C = zeg.ETHNIC_GROUP_C
    LEFT JOIN Clarity.dbo.ZC_SEX zs ON p.SEX_C = zs.RCPT_MEM_SEX_C
    INNER JOIN #Attribution2 att ON b.PAT_ID = att.PAT_ID
    LEFT JOIN (SELECT DISTINCT TOP 10000000 ev.PAT_LINK_ID PAT_ID,
                                            'Active in BH' AS 'BH PATIENT'
               FROM Clarity.dbo.EPISODE_VIEW ev
                   INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = ev.PAT_LINK_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE ev.SUM_BLK_TYPE_ID = 221
                     AND ev.STATUS_C = 1
                     AND pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
                     AND pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'MH',
                                                                     'BH',
                                                                     'PY' )) bh ON bh.PAT_ID = b.PAT_ID
    LEFT JOIN (SELECT DISTINCT TOP 10000000 ev.PAT_LINK_ID PAT_ID,
                                            'Active Dental' AS 'DENTAL PATIENT'
               FROM Clarity.dbo.EPISODE_VIEW ev
                   INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = ev.PAT_LINK_ID
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
               WHERE ev.STATUS_C = 1 --Active episode
                     AND ev.SUM_BLK_TYPE_ID = 45 --Dental
                     AND pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
                     AND pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') dent ON dent.PAT_ID = b.PAT_ID
WHERE att.ROW_NUM_DESC = 1;

SELECT TOP 10000000 c.IDENTITY_ID,
                    c.PAT_ID,
                    c.BIRTH_DATE,
                    c.ZIP,
                    c.GENDER,
                    c.STATE,
                    c.CITY,
                    c.COUNTY,
                    c.CUR_PRIM_LOC_ID,
                    c.LOC_NAME,
                    c.EXTERNAL_NAME,
                    c.PAT_NAME,
                    c.ORD_VALUE,
                    c.LAST_LAB,
                    c.Result_Output,
                    c.SUPPRESSED,
                    c.VLS_CATEGORY,
                    c.ETHNICITY,
                    c.SEX,
                    c.[All Race] 'RACE',
                    CASE WHEN c.RACE = 'Black'
                             THEN 'Black/African American'
                    WHEN c.RACE = 'White'
                        THEN 'White'
                    ELSE NULL
                    END AS DISPARITY_RACE,
                    CASE WHEN c.RACE = 'Black'
                              AND c.SUPPRESSED = 1
                             THEN 1
                    WHEN c.RACE = 'Black'
                         AND c.SUPPRESSED = 0
                        THEN 0
                    END AS BLACK,
                    CASE WHEN c.RACE = 'White'
                              AND c.SUPPRESSED = 1
                             THEN 1
                    WHEN c.RACE = 'White'
                         AND c.SUPPRESSED = 0
                        THEN 0
                    END AS WHITE,
                    CASE WHEN c.STATE = 'WISCONSIN'
                              AND c.SUPPRESSED = 1
                             THEN 1
                    WHEN c.STATE = 'WISCONSIN'
                         AND c.SUPPRESSED = 0
                        THEN 0
                    END AS WI_SUPP,
                    CASE WHEN c.STATE = 'COLORADO'
                              AND c.SUPPRESSED = 1
                             THEN 1
                    WHEN c.STATE = 'COLORADO'
                         AND c.SUPPRESSED = 0
                        THEN 0
                    END AS CO_SUPP,
                    CASE WHEN c.STATE = 'MISSOURI'
                              AND c.SUPPRESSED = 1
                             THEN 1
                    WHEN c.STATE = 'MISSOURI'
                         AND c.SUPPRESSED = 0
                        THEN 0
                    END AS MO_SUPP,
                    CASE WHEN c.STATE = 'TEXAS'
                              AND c.SUPPRESSED = 1
                             THEN 1
                    WHEN c.STATE = 'TEXAS'
                         AND c.SUPPRESSED = 0
                        THEN 0
                    END AS TX_SUPP,
                    CASE WHEN c.Result_Output < 200
                             THEN 1 ---this is the step that checks EITHER for suppressed or 6+ months
                    ELSE c.[In-Care]
                    END AS IN_CARE,
                    c.[BH PATIENT],
                    c.[DENTAL PATIENT],
                    c.[Sexual Orientation]
INTO #d
FROM #c c;
SELECT TOP 1000000 --To get active pts in Clinical Pharmacy Cohorts
       flag.PATIENT_ID PAT_ID,
       MAX(CASE WHEN f.NAME IS NOT NULL THEN 'Y' END) AS 'ACTIVE_CP_COHORT'
INTO #fyi
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
    INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI f ON flag.PAT_FLAG_TYPE_C = f.BPA_TRIGGER_FYI_C
WHERE
--flag.PAT_FLAG_TYPE_C IN ('640011', '640012', '640013') ---HTN, DM, AntiCoag
f.NAME LIKE 'SA64 Pharmacist%'
AND flag.ACTIVE_C = 1 -- Only currently actives

GROUP BY flag.PATIENT_ID;

SELECT TOP 10000000 d.IDENTITY_ID MRN,
                    d.PAT_ID,
                    CONVERT(NVARCHAR(30), d.BIRTH_DATE, 101) AS DOB,
                    (DATEDIFF(m, d.BIRTH_DATE, GETDATE()) / 12) AGE,
                    d.STATE,
                    d.CITY,
                    d.ZIP,
                    d.GENDER,
                    d.IN_CARE,
                    COALESCE(d.COUNTY, 'Not in Chart') 'COUNTY',
                    UPPER(d.EXTERNAL_NAME) 'PCP',
                    d.PAT_NAME 'PATIENT',
                    d.ORD_VALUE 'LAST_VL',
                    CONVERT(NVARCHAR(30), d.LAST_LAB, 101) AS LAST_LAB,
                    d.Result_Output,
                    d.SUPPRESSED,
                    d.VLS_CATEGORY,
                    CASE WHEN d.ETHNICITY IS NULL
                             THEN 'Unknown'
                    WHEN d.ETHNICITY = ''
                        THEN 'Unknown'
                    WHEN d.ETHNICITY = 'Not Collected/Unknown'
                        THEN 'Unknown'
                    WHEN d.ETHNICITY = 'Patient Refused'
                        THEN 'Unknown'
                    ELSE d.ETHNICITY
                    END AS ETHNICITY,
                    d.SEX,
                    COALESCE(d.RACE, 'Unknown') 'RACE',
                    d.DISPARITY_RACE,
                    GETDATE() 'Report_Date',
                    CASE WHEN fyi.ACTIVE_CP_COHORT = 'Y'
                             THEN 'Y'
                    ELSE 'N'
                    END AS 'CLINICAL PHARMACY COHORT',
                    CASE WHEN d.[BH PATIENT] IS NULL
                             THEN 'Non-BH Patient'
                    ELSE d.[BH PATIENT]
                    END AS 'BH STATUS',
                    CASE WHEN d.[DENTAL PATIENT] IS NULL
                             THEN 'Non-dental Patient'
                    ELSE d.[DENTAL PATIENT]
                    END AS 'DENTAL STATUS',
                    svis.[Next Any Appt],
                    svis.[Next Appt Prov],
                    spvis.[Next PCP Appt],
                    spvis.[Next PCP Appt Prov],
                    COALESCE(d.[Sexual Orientation], 'Not Asked') 'Sexual Orientation',
                    fpl.FPL_PERCENTAGE 'FPL Detail',
                    CASE WHEN fpl.FPL_PERCENTAGE IS NULL
                             THEN 'Unknown'
                    WHEN fpl.FPL_PERCENTAGE < 100
                        THEN 'Under 100%'
                    WHEN fpl.FPL_PERCENTAGE < 139
                        THEN '100% - 138%'
                    WHEN fpl.FPL_PERCENTAGE < 201
                        THEN '139% - 200%'
                    WHEN fpl.FPL_PERCENTAGE < 251
                        THEN '201% - 250%'
                    WHEN fpl.FPL_PERCENTAGE < 401
                        THEN '251% - 400%'
                    WHEN fpl.FPL_PERCENTAGE < 501
                        THEN '401% - 500%'
                    ELSE 'Over 500%'
                    END AS 'FPL Category'
FROM #d d
    LEFT JOIN #fyi fyi ON fyi.PAT_ID = d.PAT_ID
    LEFT JOIN (SELECT TOP 1000000 pev.PAT_ID,
                                  pev.CONTACT_DATE 'Next Any Appt',
                                  ser.PROV_NAME 'Next Appt Prov',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled

    ) svis ON svis.PAT_ID = d.PAT_ID
              AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT TOP 1000000 pev.PAT_ID,
                                  pev.CONTACT_DATE 'Next PCP Appt',
                                  ser.PROV_NAME 'Next PCP Appt Prov',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND ser.PROV_ID <> '640178' --pulmonologist
                     AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs

    ) spvis ON spvis.PAT_ID = d.PAT_ID
               AND spvis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT pacv.PAT_ID,
                      fplp.FPL_PERCENTAGE,
                      ROW_NUMBER() OVER (PARTITION BY pacv.PAT_ID ORDER BY fplp.FPL_EFF_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.ACCOUNT_FPL_INFO_VIEW fplp
                   INNER JOIN Clarity.dbo.PAT_ACCT_CVG_VIEW pacv ON fplp.ACCOUNT_ID = pacv.ACCOUNT_ID
               WHERE fplp.LINE = 1) fpl ON fpl.PAT_ID = d.PAT_ID
                                           AND fpl.ROW_NUM_DESC = 1;

DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #c;
DROP TABLE #d;
DROP TABLE #fyi;