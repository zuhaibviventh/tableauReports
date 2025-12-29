/*  Preferred Language – EPIC base population
    Requirements covered:
      - Only EPIC patients (base)
      - Provide EPIC_PREFERRED_LANGUAGE
      - Provide PATIENT_NAME (Lname, Fname from PAT_NAME) and DOB
      - Preserve STATE and Site using existing attribution parsing
*/
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#Attribution1') IS NOT NULL DROP TABLE #Attribution1;

SELECT
    pev.PAT_ID,
    pev.CONTACT_DATE AS LAST_OFFICE_VISIT,
    SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) AS STATE,
    CASE
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWAUKEE'
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
        ELSE dep.DEPT_ABBREVIATION
    END AS CityLabel,
    CASE
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'MN' THEN 'MAIN LOCATION'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'DR' THEN 'D&R'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'KE' THEN 'KEENEN'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'UC' THEN 'UNIVERSITY OF COLORADO'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'ON' THEN 'AUSTIN MAIN'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2) = 'TW' THEN 'AUSTIN OTHER'
        ELSE 'ERROR'
    END AS Site,
    CASE
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ('AD','PY','BH','MH') THEN 'BEHAVIORAL'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
        ELSE 'ERROR'
    END AS LOS
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
  AND pev.APPT_STATUS_C IN (2, 6);

IF OBJECT_ID('tempdb..#Attribution2') IS NOT NULL DROP TABLE #Attribution2;
SELECT
    a1.PAT_ID,
    a1.STATE,
    a1.CityLabel,
    a1.Site,
    a1.LOS,
    ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1;

IF OBJECT_ID('tempdb..#Attribution3') IS NOT NULL DROP TABLE #Attribution3;
SELECT
    a2.PAT_ID,
    a2.LOS,
    a2.CityLabel,
    a2.STATE
INTO #Attribution3
FROM #Attribution2 a2
WHERE a2.ROW_NUM_DESC = 1;

-- Final EPIC base result
SELECT
    id.IDENTITY_ID                               AS MRN,
    p.PAT_NAME                                   AS PATIENT_NAME,      -- Lname, Fname as stored in EPIC
    p.BIRTH_DATE                                 AS DOB,
    zl.NAME                                      AS EPIC_PREFERRED_LANGUAGE,
	ser.PROV_ID									 AS PCP_ID, -- 11/4/25 add PCP info
	ser.PROV_NAME								 AS PCP_NAME,
    CASE
        WHEN a3.STATE = 'MO' THEN 'Missouri'
        WHEN a3.STATE = 'TX' THEN 'Texas'
        WHEN a3.STATE = 'WI' THEN 'Wisconsin'
        WHEN a3.STATE = 'CO' THEN 'Colorado'
        ELSE a3.STATE
    END                                           AS STATE,
    a3.CityLabel                                  AS Site
FROM Clarity.dbo.IDENTITY_ID_VIEW id
INNER JOIN Clarity.dbo.PATIENT_VIEW p    ON p.PAT_ID = id.PAT_ID
left join Clarity.dbo.[CLARITY_SER] ser on p.CUR_PCP_PROV_ID = ser.PROV_ID
LEFT JOIN Clarity.dbo.ZC_LANGUAGE zl ON zl.LANGUAGE_C = p.LANGUAGE_C
INNER JOIN #Attribution3 a3         ON a3.PAT_ID = id.PAT_ID;
