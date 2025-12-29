/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT TOP 10000000 pev.PAT_ID,
                    pev.CONTACT_DATE LAST_OFFICE_VISIT,
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
                    CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN' THEN 'MAIN LOCATION'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'DR' THEN 'D&R'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'KE' THEN 'KEENEN'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'UC' THEN 'UNIVERSITY OF COLORADO'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'ON' THEN 'AUSTIN MAIN'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'TW' THEN 'AUSTIN OTHER'
                        ELSE 'ERROR'
                    END AS 'SITE',
                    CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
                        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
                        ELSE 'ERROR'
                    END AS 'LOS'
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -48, GETDATE())
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

SELECT TOP 10000000 id.IDENTITY_ID MRN,
                    p.PAT_NAME PATIENT,
                    orv.ORD_NUM_VALUE A1c,
                    orv.RESULT_DATE A1c_Date,
                    a3.CITY,
                    a3.STATE,
                    MIN(CASE WHEN flag.ACTIVE_C = 1 THEN 'ACTIVE' ELSE 'INACTIVE' END) AS COHORT_STATUS,
                    MAX(flag.ACCT_NOTE_INSTANT) COHORT_ENROLL_DATE,
                    ser.EXTERNAL_NAME PCP,
                    MAX(flag.LAST_UPDATE_INST) LAST_UPDATE_DATE,
                    zc.NAME COUNTY,
                    pa.ADDRESS
INTO #base
FROM Clarity.dbo.PATIENT_VIEW p
    LEFT JOIN Clarity.dbo.ZC_COUNTY zc ON zc.COUNTY_C = p.COUNTY_C
    LEFT JOIN Clarity.dbo.PAT_ADDRESS pa ON pa.PAT_ID = p.PAT_ID
                                            AND pa.LINE = 1
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.ORDER_PROC_VIEW opv ON p.PAT_ID = opv.PAT_ID
    INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag ON p.PAT_ID = flag.PATIENT_ID
    INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON cc.COMPONENT_ID = orv.COMPONENT_ID
WHERE flag.PAT_FLAG_TYPE_C = '640013'
      --AND id.IDENTITY_ID = '640011002'
      AND opv.ORDER_TYPE_C = 7
      AND orv.ORD_NUM_VALUE <> 9999999
      AND cc.COMMON_NAME = 'HEMOGLOBIN A1C'
      AND id.IDENTITY_TYPE_ID = '64'
GROUP BY id.IDENTITY_ID,
         p.PAT_NAME,
         orv.ORD_NUM_VALUE,
         orv.RESULT_DATE,
         a3.CITY,
         a3.STATE,
         ser.EXTERNAL_NAME,
         zc.NAME,
         pa.ADDRESS;

SELECT TOP 10000000 --to only get A1c on or after enrollement (can't do this in 1st step since it messes up the ACTIVE/INACTIVE for pts who have multiple enrollements)
       b.MRN,
       b.PATIENT,
       b.A1c,
       b.A1c_Date,
       b.CITY,
       b.STATE,
       b.COHORT_STATUS,
       b.COHORT_ENROLL_DATE,
       b.PCP,
       b.LAST_UPDATE_DATE,
       b.COUNTY,
       b.ADDRESS
INTO #base2
FROM #base b
WHERE b.A1c_Date >= b.COHORT_ENROLL_DATE;

SELECT TOP 10000000 b.MRN,
                    b.A1c,
                    b.A1c_Date,
                    ROW_NUMBER() OVER (PARTITION BY b.MRN ORDER BY b.A1c_Date DESC) AS ROW_NUM_DESC
INTO #FL
FROM #base2 b;

SELECT TOP 10000000 b.MRN,
                    b.PATIENT,
                    b.A1c,
                    b.A1c_Date,
                    b.CITY,
                    b.STATE,
                    b.COHORT_STATUS,
                    b.COHORT_ENROLL_DATE,
                    b.PCP,
                    b.COUNTY,
                    b.ADDRESS,
                    CASE WHEN b.COHORT_STATUS = 'INACTIVE' THEN b.LAST_UPDATE_DATE
                        ELSE NULL
                    END AS INACTIVE_DATE,
                    b.LAST_UPDATE_DATE,
                    CASE WHEN f.ROW_NUM_DESC = 1 THEN b.A1c
                        ELSE NULL
                    END AS LAST_A1c,
                    CASE WHEN f.ROW_NUM_DESC = 1 THEN b.A1c_Date
                        ELSE NULL
                    END AS LAST_A1c_Date
INTO #c
FROM #base b
    LEFT JOIN #FL f ON f.MRN = b.MRN
                       AND f.A1c_Date = b.A1c_Date;

SELECT TOP 10000000 --to only get A1c on or Before enrollement (can't do this in 1st step since it messes up the ACTIVE/INACTIVE for pts who have multiple enrollements)
       b.MRN,
       b.PATIENT,
       b.A1c,
       b.A1c_Date,
       b.CITY,
       b.STATE,
       b.COHORT_STATUS,
       b.COHORT_ENROLL_DATE,
       b.PCP,
       b.LAST_UPDATE_DATE,
       b.COUNTY,
       b.ADDRESS
INTO #base3
FROM #base b
WHERE b.A1c_Date <= b.COHORT_ENROLL_DATE;

SELECT TOP 10000000 b.MRN,
                    b.A1c,
                    b.A1c_Date,
                    ROW_NUMBER() OVER (PARTITION BY b.MRN ORDER BY b.A1c_Date DESC) AS ROW_NUM_DESC
INTO #LL
FROM #base3 b;

SELECT l.MRN, l.A1c, l.A1c_Date INTO #d FROM #LL l WHERE l.ROW_NUM_DESC = 1;

SELECT c.MRN,
       c.PATIENT,
       c.A1c,
       CAST(c.A1c_Date AS DATE) AS A1c_Date,
       c.CITY,
       c.STATE,
       c.COHORT_STATUS,
       c.COHORT_ENROLL_DATE,
       c.PCP,
       c.COUNTY,
       c.ADDRESS,
       CAST(c.INACTIVE_DATE AS DATE) AS INACTIVE_DATE,
       c.LAST_UPDATE_DATE,
       c.LAST_A1c,
       CAST(c.LAST_A1c_Date AS DATE) AS LAST_A1c_Date,
       CASE WHEN c.A1c_Date = l.A1c_Date THEN l.A1c
       END AS FIRST_A1c,
       CASE WHEN c.A1c_Date = l.A1c_Date THEN CAST(l.A1c_Date AS DATE)
       END AS FIRST_A1c_Date
FROM #c c
    INNER JOIN #d l ON l.MRN = c.MRN
--AND l.A1c_Date = c.A1c_Date

WHERE c.A1c_Date >= l.A1c_Date;

DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #base;
DROP TABLE #base2;
DROP TABLE #FL;
DROP TABLE #base3;
DROP TABLE #LL;
DROP TABLE #c;
DROP TABLE #d;
