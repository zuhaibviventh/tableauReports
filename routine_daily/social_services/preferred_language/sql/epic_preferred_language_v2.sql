/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Language Preferrences - Tableau
 Create Date: 1/24/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  

 Purpose:   

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 11/16/2023     Mitch       Adding Site

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#Attribution1') IS NOT NULL DROP TABLE #Attribution1;
SELECT pev.PAT_ID,
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
           ELSE dep.DEPT_ABBREVIATION
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
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -1, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );


IF OBJECT_ID('tempdb..#Attribution2') IS NOT NULL DROP TABLE #Attribution2;
SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1;


IF OBJECT_ID('tempdb..#Attribution3') IS NOT NULL DROP TABLE #Attribution3;
SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;

SELECT '' CLIENT_ID,
       id.IDENTITY_ID MRN,
       p.PAT_NAME,
       p.BIRTH_DATE,
       zl.NAME 'PREFERRED LANGUAGE',
       CASE WHEN a3.STATE = 'MO' THEN 'Missouri'
           WHEN a3.STATE = 'TX' THEN 'Texas'
           WHEN a3.STATE = 'WI' THEN 'Wisconsin'
           WHEN a3.STATE = 'CO' THEN 'Colorado'
           ELSE a3.STATE
       END AS STATE,
       a3.CITY 'Site'
FROM Clarity.dbo.IDENTITY_ID_VIEW id
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_LANGUAGE zl ON zl.LANGUAGE_C = p.LANGUAGE_C
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = id.PAT_ID;

