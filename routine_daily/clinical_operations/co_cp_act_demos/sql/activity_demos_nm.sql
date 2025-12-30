/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT TOP 10000000 pev.PAT_ID,
                    pev.CONTACT_DATE LAST_OFFICE_VISIT,
                    dep.STATE,
                    dep.CITY,
                    dep.SITE,
                    dep.LOS
                    /*
                    CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN' THEN 'MAIN LOCATION'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'DR' THEN 'D&R'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'KE' THEN 'KEENEN'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'UC' THEN 'UNIVERSITY OF COLORADO'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'ON' THEN 'AUSTIN MAIN'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'TW' THEN 'AUSTIN OTHER'
                        ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2)
                    END AS 'SITE',
                    CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
                        ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2)
                    END AS 'LOS'
                    */
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE())
--  AND pev.APPT_STATUS_C IN (2, 6)

;

SELECT TOP 10000000 a1.PAT_ID,
                    a1.STATE,
                    a1.CITY,
                    a1.SITE,
                    a1.LOS,
                    ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1;

SELECT TOP 10000000 a2.PAT_ID,
                    a2.LOS,
                    a2.CITY,
                    a2.STATE
INTO #Attribution3
FROM #Attribution2 a2
WHERE a2.ROW_NUM_DESC = 1;

SELECT TOP 10000000 --To get denominator of each pt and the program(s) they are/were in
       flag.PATIENT_ID PAT_ID,
       MAX(flag.ACCT_NOTE_INSTANT) FLAG_DATE, --MAX to get Latest enrollment date
       MIN(z2.NAME) PROGRAM_STATUS,           --MIN to pick Active over Inactive if both exist
       fyi.NAME CP_PROGRAM
INTO #pop2
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
    INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI fyi ON flag.PAT_FLAG_TYPE_C = fyi.BPA_TRIGGER_FYI_C
    INNER JOIN Clarity.dbo.ZC_ACTIVE_STATUS_2 z2 ON flag.ACTIVE_C = z2.ACTIVE_STATUS_2_C
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON flag.PATIENT_ID = p.PAT_ID -- Just to pull out test pts at the first step

WHERE fyi.name LIKE 'SA64 Pharmacist%'
GROUP BY flag.PATIENT_ID,
         fyi.NAME;

SELECT TOP 10000000 p2.PAT_ID,
                    MIN(p2.FLAG_DATE) EARLIEST_ENROLLMENT --For pts who are enrolled in more than one program

INTO #pop1
FROM #pop2 p2
GROUP BY p2.PAT_ID;

SELECT TOP 10000000 p2.PAT_ID,
                    id.IDENTITY_ID MRN,
                    p2.FLAG_DATE,
                    p2.PROGRAM_STATUS,
                    p2.CP_PROGRAM,
                    p1.EARLIEST_ENROLLMENT
INTO #pop
FROM #pop2 p2
    LEFT JOIN #pop1 p1 ON p1.PAT_ID = p2.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p2.PAT_ID = id.PAT_ID;

SELECT TOP 10000000 -- Visit-level info DISTINCT --using DISTINCT since some pts have more than one flag so it is duplicating their lines
       p.PAT_ID,
       eap.PROC_CODE,
       eap.PROC_NAME,
       pev.CONTACT_DATE,
       ser.PROV_NAME,
       pev.PAT_ENC_CSN_ID,
       zas.NAME APPT_STATUS,
       prc.PRC_NAME VISIT_TYPE,
       MAX(x.TOTAL_CHG_AMOUNT) TOTAL_CHG_AMOUNT,
       MAX(x.TOTAL_PAY_AMOUNT) TOTAL_PAY_AMOUNT,
       MAX(zfc.NAME) VISIT_INSURANCE,
       dep.DEPT_ABBREVIATION,
       dep.DEPARTMENT_NAME
INTO #visits
FROM #pop p
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = p.PAT_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    LEFT JOIN Clarity.dbo.ZC_APPT_STATUS zas ON zas.APPT_STATUS_C = pev.APPT_STATUS_C
    LEFT JOIN Clarity.dbo.CLARITY_PRC prc ON pev.APPT_PRC_ID = prc.PRC_ID
    LEFT JOIN Clarity.dbo.X_VISIT_DATE_VIEW x ON pev.CHARGE_SLIP_NUMBER = x.CHARGE_SLIP_NUMBER
    LEFT JOIN Clarity.dbo.CLARITY_EAP eap ON pev.LOS_PRIME_PROC_ID = eap.PROC_ID
    LEFT JOIN Clarity.dbo.ZC_FINANCIAL_CLASS zfc ON x.ORIGINAL_FIN_CLASS = zfc.FINANCIAL_CLASS
WHERE pev.CONTACT_DATE >= (p.EARLIEST_ENROLLMENT - 7) --7-days prior to FYI Flag
      AND pev.ENC_TYPE_C NOT IN ( '9000', '9001', '109', '2010' )
      AND ser.PROVIDER_TYPE_C IN ( '102', '173', '185' )
GROUP BY p.PAT_ID,
         eap.PROC_CODE,
         eap.PROC_NAME,
         pev.CONTACT_DATE,
         ser.PROV_NAME,
         pev.PAT_ENC_CSN_ID,
         zas.NAME,
         prc.PRC_NAME,
         dep.DEPT_ABBREVIATION,
         dep.DEPARTMENT_NAME
UNION
SELECT DISTINCT TOP 10000000 --using DISTINCT since some pts have more than one flag so it is duplicating their lines
       b.PAT_ID,
       'SmartPhrase' PROC_CODE,
       'SmartPhrase' PROC_NAME,
       pev.CONTACT_DATE,
       ser.PROV_NAME,
       pev.PAT_ENC_CSN_ID,
       'Completed' APPT_STATUS,
       'SmartPhrase' AS VISIT_TYPE,
       NULL TOTAL_CHG_AMOUNT,
       NULL TOTAL_PAY_AMOUNT,
       NULL VISIT_INSURANCE,
       dep.DEPT_ABBREVIATION,
       dep.DEPARTMENT_NAME
FROM Clarity.dbo.SMARTTOOL_LOGGER_VIEW sl
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON sl.CSN = pev.PAT_ENC_CSN_ID
    INNER JOIN #pop b ON b.PAT_ID = pev.PAT_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.CLARITY_EMP_VIEW ce ON sl.USER_ID = ce.USER_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ce.PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CL_SPHR cs ON sl.SMARTPHRASE_ID = cs.SMARTPHRASE_ID
                                         AND cs.SMARTPHRASE_ID = '1076999' --Clinical Pharmacy Touch

WHERE sl.LOG_DATE >= (b.EARLIEST_ENROLLMENT - 7) --7-days prior to FYI Flag
;

/* Pt-level data ----using DISTINCT since some pts have more than one flag so it is duplicating their lines */
SELECT DISTINCT TOP 10000000 pop.PAT_ID,
                             p.PAT_NAME,
                             zpr.NAME RACE,
                             CASE WHEN bs.NAME IS NOT NULL THEN bs.NAME
                                 ELSE sex.NAME
                             END AS BIRTH_SEX,
                             gi.NAME GENDER_IDENTITY,
                             LEFT(p.ZIP, 5) ZIP,
                             zeg.NAME ETHNICITY,
                             well.ASCVD_10_YR_SCORE,
                             well.AGE,
                             well.SMOKING_USER_YN SMOKER,
                             ser.PROV_NAME PCP,
                             shx.IDU,
                             rx.PHARMACY_NAME PREFERRED_PHARMACY,
                             fpl.fpl_percentage
INTO #Demo
FROM #pop pop
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = pop.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    LEFT JOIN Clarity.dbo.PATIENT_4 p4 ON p4.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON pr.PAT_ID = p.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON zpr.PATIENT_RACE_C = pr.PATIENT_RACE_C
    LEFT JOIN Clarity.dbo.ZC_SEX_ASGN_AT_BIRTH bs ON bs.SEX_ASGN_AT_BIRTH_C = p4.SEX_ASGN_AT_BIRTH_C
    LEFT JOIN Clarity.dbo.ZC_SEX sex ON p.SEX_C = sex.RCPT_MEM_SEX_C
    LEFT JOIN Clarity.dbo.ZC_GENDER_IDENTITY gi ON gi.GENDER_IDENTITY_C = p4.GENDER_IDENTITY_C
    LEFT JOIN Clarity.dbo.ZC_ETHNIC_GROUP zeg ON zeg.ETHNIC_GROUP_C = p.ETHNIC_GROUP_C
    LEFT JOIN Clarity.dbo.DM_WLL_ALL_VIEW well ON well.PAT_ID = pop.PAT_ID
    LEFT JOIN (SELECT TOP 10000000 f.PAT_ID,
                                   MIN(f.fpl_percentage) fpl_Percentage
               FROM Clarity.dbo.X_FPL_MAX_VIEW f
               GROUP BY f.PAT_ID) fpl ON fpl.PAT_ID = pop.PAT_ID
    LEFT JOIN (SELECT TOP 10000000 sh.PAT_ID,
                                   sh.IV_DRUG_USER_YN IDU,
                                   ROW_NUMBER() OVER (PARTITION BY sh.PAT_ID ORDER BY sh.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.SOCIAL_HX_VIEW sh) shx ON pop.PAT_ID = shx.PAT_ID
                                                          AND shx.ROW_NUM_DESC = 1
    LEFT JOIN Clarity.dbo.PAT_PREF_PHARMACY_VIEW pharm ON pop.PAT_ID = pharm.PAT_ID
                                                          AND pharm.LINE = 1
    LEFT JOIN Clarity.dbo.RX_PHR rx ON pharm.PREF_PHARMACY_ID = rx.PHARMACY_ID;

SELECT TOP 10000000 pop.PAT_ID,
                    pop.MRN,
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - HTN' THEN 'Y' ELSE NULL END) AS 'HTN Cohort',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - HTN' THEN pop.FLAG_DATE
                            ELSE NULL
                        END) AS 'HTN Cohort Date',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - HTN' THEN pop.PROGRAM_STATUS
                            ELSE NULL
                        END) AS 'HTN Cohort Status',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 PHARMACIST- PrEP' THEN 'Y' ELSE NULL END) AS 'PrEP Cohort',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 PHARMACIST- PrEP' THEN pop.FLAG_DATE
                            ELSE NULL
                        END) AS 'PrEP Cohort Date',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 PHARMACIST- PrEP' THEN pop.PROGRAM_STATUS
                            ELSE NULL
                        END) AS 'PrEP Cohort Status',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - DM' THEN 'Y' ELSE NULL END) AS 'DM Cohort',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - DM' THEN pop.FLAG_DATE
                            ELSE NULL
                        END) AS 'DM Cohort Date',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - DM' THEN pop.PROGRAM_STATUS
                            ELSE NULL
                        END) AS 'DM Cohort Status',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Anticoagulation' THEN 'Y'
                            ELSE NULL
                        END) AS 'Anticoagulation Cohort',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Anticoagulation' THEN pop.FLAG_DATE
                            ELSE NULL
                        END) AS 'Anticoagulation Cohort Date',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Anticoagulation' THEN pop.PROGRAM_STATUS
                            ELSE NULL
                        END) AS 'Anticoagulation Cohort Status',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist: Pre-DM' THEN 'Y' ELSE NULL END) AS 'Pre-DM Cohort',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist: Pre-DM' THEN pop.FLAG_DATE
                            ELSE NULL
                        END) AS 'Pre-DM Cohort Date',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist: Pre-DM' THEN pop.PROGRAM_STATUS
                            ELSE NULL
                        END) AS 'Pre-DM Cohort Status',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Miscellaneous' THEN 'Y'
                            ELSE NULL
                        END) AS 'Miscellaneous Cohort',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Miscellaneous' THEN pop.FLAG_DATE
                            ELSE NULL
                        END) AS 'Miscellaneous Cohort Date',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Miscellaneous' THEN pop.PROGRAM_STATUS
                            ELSE NULL
                        END) AS 'Miscellaneous Cohort Status',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Tobacco' THEN 'Y' ELSE NULL END) AS 'Tobacco Cohort',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Tobacco' THEN pop.FLAG_DATE
                            ELSE NULL
                        END) AS 'Tobacco Cohort Date',
                    MAX(CASE WHEN pop.CP_PROGRAM = 'SA64 Pharmacist - Tobacco' THEN pop.PROGRAM_STATUS
                            ELSE NULL
                        END) AS 'Tobacco Cohort Status',
                    MIN(pop.PROGRAM_STATUS) AS 'Active in Any Cohort',
                    pop.EARLIEST_ENROLLMENT,
                    d.PAT_NAME,
                    d.RACE,
                    d.BIRTH_SEX,
                    d.GENDER_IDENTITY,
                    d.ZIP,
                    d.ETHNICITY,
                    d.ASCVD_10_YR_SCORE,
                    d.AGE,
                    d.SMOKER,
                    d.PCP,
                    d.IDU,
                    d.PREFERRED_PHARMACY,
                    CASE WHEN v.APPT_STATUS IS NULL THEN NULL
                        ELSE v.PROC_CODE
                    END AS PROC_CODE,
                    CASE WHEN v.PROC_CODE IS NULL THEN 'Non-Visit (No Code)'
                        WHEN v.APPT_STATUS IS NULL THEN 'Non-Visit (No Code)'
                        WHEN v.PROC_CODE = '99211' THEN 'Office <10 Min (99211)'
                        WHEN v.PROC_CODE = 'TX016' THEN 'Non-billed (TX016)'
                        WHEN v.PROC_CODE = 'SmartPhrase' THEN 'Care Team Consult'
                        WHEN v.PROC_CODE = '99212' THEN 'Office 10-19 Min (99212)'
                        WHEN v.PROC_CODE = 'TA007' THEN 'Non-billed Visit (TA007)'
                        WHEN v.PROC_CODE = '99203' THEN 'Office 30-44 Min (99203)'
                        WHEN v.PROC_CODE = '99213' THEN 'Office 20-29 Min (99213)'
                        WHEN v.PROC_CODE = 'TX117' THEN 'Immz Only (TX117)'
                        WHEN v.PROC_CODE = '93793' THEN 'Warfarin (93793)'
                        WHEN v.PROC_CODE = '99214' THEN 'Office 30-39 (99214)'
                        WHEN v.PROC_CODE = 'TX023' THEN 'Lab Only (TX023)'
                        ELSE v.PROC_CODE
                    END AS 'PROCEDURE',
                    CASE WHEN v.APPT_STATUS IS NULL THEN v.PAT_ENC_CSN_ID
                        WHEN v.APPT_STATUS = 'Completed' THEN v.PAT_ENC_CSN_ID
                    END AS 'CP Touches',
                    CASE WHEN v.PROC_CODE IN ( '99201', '99211' ) THEN v.PAT_ENC_CSN_ID
                        ELSE NULL
                    END AS '99201/11',
                    CASE WHEN v.PROC_CODE IN ( '99202', '99212' ) THEN v.PAT_ENC_CSN_ID
                        ELSE NULL
                    END AS '99202/12',
                    CASE WHEN v.PROC_CODE IN ( '99203', '99213' ) THEN v.PAT_ENC_CSN_ID
                        ELSE NULL
                    END AS '99203/13',
                    CASE WHEN v.PROC_CODE IN ( '99204', '99214' ) THEN v.PAT_ENC_CSN_ID
                        ELSE NULL
                    END AS '99204/14',
                    CASE WHEN v.PROC_CODE <> '93793' THEN v.PAT_ENC_CSN_ID --Removing Warfarin visits
                        ELSE NULL
                    END AS 'CP Office Visits',
                    CASE WHEN v.PROC_CODE LIKE 'TX%' THEN v.PAT_ENC_CSN_ID
                        ELSE NULL
                    END AS 'Non-billable Visits',
                    v.PROC_NAME,
                    v.CONTACT_DATE,
                    v.PROV_NAME,
                    CASE WHEN v.PAT_ENC_CSN_ID IS NULL THEN 999999
                        ELSE v.PAT_ENC_CSN_ID
                    END AS PAT_ENC_CSN_ID,
                    v.APPT_STATUS,
                    v.VISIT_TYPE,
                    CASE WHEN v.TOTAL_CHG_AMOUNT IS NULL THEN 0.0
                        ELSE v.TOTAL_CHG_AMOUNT
                    END AS TOTAL_CHG_AMOUNT,
                    ABS(COALESCE(v.TOTAL_PAY_AMOUNT, 0.0)) TOTAL_PAY_AMOUNT,
                    v.VISIT_INSURANCE,
                    CASE --This needs to stay in since we need to answer a 1-to-many question at the pt level
                        WHEN d.FPL_PERCENTAGE IS NULL THEN '9. Unknown' --So that values are preferred over NULLs
                        WHEN d.FPL_PERCENTAGE < 100 THEN '1 <100'
                        WHEN d.FPL_PERCENTAGE < 140 THEN '2. 100 - 139'
                        WHEN d.FPL_PERCENTAGE < 201 THEN '3. 140 - 200'
                        WHEN d.FPL_PERCENTAGE < 251 THEN '4.201 - 250'
                        WHEN d.FPL_PERCENTAGE < 401 THEN '5. 251 - 400'
                        WHEN d.FPL_PERCENTAGE < 501 THEN '6. 401 - 500'
                        WHEN d.FPL_PERCENTAGE < 3000 THEN '7. >500'
                        ELSE '9. Unknown'
                    END AS fpl_percentage,
                    CASE WHEN d.fpl_Percentage IS NULL THEN '8. Unknown'
                        WHEN d.fpl_Percentage < 100 THEN '1. <100'
                        WHEN d.fpl_Percentage
                             BETWEEN 100 AND 139 THEN '2. 100 - 139'
                        WHEN d.fpl_Percentage
                             BETWEEN 140 AND 200 THEN '3. 140 - 200'
                        WHEN d.fpl_Percentage
                             BETWEEN 201 AND 250 THEN '4. 201 - 250'
                        WHEN d.fpl_Percentage
                             BETWEEN 251 AND 400 THEN '5. 251 - 400'
                        WHEN d.fpl_Percentage
                             BETWEEN 401 AND 500 THEN '6. 401 - 500'
                        WHEN d.fpl_Percentage > 500 THEN '7. >500'
                        ELSE '8. Unknown'
                    END AS fpl_percentage_levels,
                    SUBSTRING(v.DEPT_ABBREVIATION, 3, 2) 'STATE2',
                    CASE WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWAUKEE'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'KN' THEN 'KENOSHA'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'GB' THEN 'GREEN BAY'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'WS' THEN 'WAUSAU'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'AP' THEN 'APPLETON'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'EC' THEN 'EAU CLAIRE'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'LC' THEN 'LACROSSE'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'MD' THEN 'MADISON'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'BL' THEN 'BELOIT'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'BI' THEN 'BILLING'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'SL' THEN 'ST LOUIS'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'DN' THEN 'DENVER'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'AS' THEN 'AUSTIN'
                        WHEN SUBSTRING(v.DEPT_ABBREVIATION, 5, 2) = 'KC' THEN 'KANSAS CITY'
                        ELSE 'ERROR'
                    END AS CITY2
INTO #a
FROM #pop pop
    LEFT JOIN #Demo d ON d.PAT_ID = pop.PAT_ID
    LEFT JOIN #visits v ON v.PAT_ID = pop.PAT_ID
                           AND v.CONTACT_DATE < GETDATE()
GROUP BY pop.PAT_ID,
         pop.MRN,
         pop.EARLIEST_ENROLLMENT,
         d.PAT_NAME,
         d.RACE,
         d.BIRTH_SEX,
         d.GENDER_IDENTITY,
         d.ZIP,
         d.ETHNICITY,
         d.ASCVD_10_YR_SCORE,
         d.AGE,
         d.SMOKER,
         d.PCP,
         d.IDU,
         d.PREFERRED_PHARMACY,
         v.PROC_CODE,
         v.PROC_NAME,
         v.CONTACT_DATE,
         v.PROV_NAME,
         v.PAT_ENC_CSN_ID,
         v.APPT_STATUS,
         v.VISIT_TYPE,
         v.TOTAL_CHG_AMOUNT,
         v.TOTAL_PAY_AMOUNT,
         v.VISIT_INSURANCE,
         d.fpl_percentage,
         v.DEPT_ABBREVIATION,
         v.DEPARTMENT_NAME;

SELECT DISTINCT TOP 10000000 a.PAT_ID,
                             a.MRN,
                             a.[HTN Cohort],
                             CAST(a.[HTN Cohort Date] AS DATE) AS [HTN Cohort Date],
                             a.[HTN Cohort Status],
                             a.[PrEP Cohort],
                             CAST(a.[PrEP Cohort Date] AS DATE) AS [PrEP Cohort Date],
                             a.[PrEP Cohort Status],
                             a.[DM Cohort],
                             CAST(a.[DM Cohort Date] AS DATE) AS [DM Cohort Date],
                             a.[DM Cohort Status],
                             a.[Anticoagulation Cohort],
                             CAST(a.[Anticoagulation Cohort Date] AS DATE) AS [Anticoagulation Cohort Date],
                             a.[Anticoagulation Cohort Status],
                             a.[Pre-DM Cohort],
                             CAST(a.[Pre-DM Cohort Date] AS DATE) AS [Pre-DM Cohort Date],
                             a.[Pre-DM Cohort Status],
                             a.[Tobacco Cohort],
                             CAST(a.[Tobacco Cohort Date] AS DATE) AS [Tobacco Cohort Date],
                             a.[Tobacco Cohort Status],
                             a.[Miscellaneous Cohort],
                             CAST(a.[Miscellaneous Cohort Date] AS DATE) AS [Miscellaneous Cohort Date],
                             a.[Miscellaneous Cohort Status],
                             a.[Active in Any Cohort],
                             CAST(a.EARLIEST_ENROLLMENT AS DATE) AS EARLIEST_ENROLLMENT,
                             a.PAT_NAME,
                             a.RACE,
                             a.BIRTH_SEX,
                             a.GENDER_IDENTITY,
                             a.ZIP,
                             a.ETHNICITY,
                             a.ASCVD_10_YR_SCORE,
                             a.AGE,
                             a.SMOKER,
                             a.PCP,
                             a.IDU,
                             COALESCE(a3.CITY, a.CITY2) 'CITY',
                             COALESCE(a3.STATE, a.STATE2) 'STATE',
                             a.PREFERRED_PHARMACY,
                             a.PROC_CODE,
                             a.[PROCEDURE],
                             a.[CP Touches],
                             a.[99201/11],
                             a.[99202/12],
                             a.[99203/13],
                             a.[99204/14],
                             a.[CP Office Visits],
                             a.[Non-billable Visits],
                             a.PROC_NAME,
                             CAST(a.CONTACT_DATE AS DATE) AS CONTACT_DATE,
                             a.PROV_NAME,
                             a.PAT_ENC_CSN_ID,
                             a.APPT_STATUS,
                             a.VISIT_TYPE,
                             a.TOTAL_CHG_AMOUNT,
                             a.TOTAL_PAY_AMOUNT,
                             a.VISIT_INSURANCE,
                             a.fpl_percentage,
                             a.fpl_percentage_levels
FROM #a a
    LEFT JOIN #Attribution3 a3 ON a3.PAT_ID = a.PAT_ID;


DROP TABLE #pop2;
DROP TABLE #pop1;
DROP TABLE #pop;
DROP TABLE #visits;
DROP TABLE #Demo;
DROP TABLE #a;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;