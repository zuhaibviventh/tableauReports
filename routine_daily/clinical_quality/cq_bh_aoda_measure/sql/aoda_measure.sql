/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	AODA Measures - Completing First 4 Visits(Star QI)
 Create Date:	7/17/2019
 Created By:	scogginsm
 System:		javelin.ochin.org
 Requested By:	Pam B

 Purpose:		To get pts who complete at least 4 appts after intake

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
 2/24/2022			Mitch				Major Revamp for Alteryx and Tableau
 2/24/2022			Mitch				Changing from 30 days to six weeks (42 days)
3/3/2022			Mitch				Per meeting with Pam B - Add H0005 to Day treatment, and H0022 to Outpt
3/3/2022			Mitch				Per Pam B - Removing check for provider type = 110 and adding dept check for AODA

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

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
           ELSE 'ERROR'
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
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'BEHAVIORAL';

SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;


/*-- GET ALL PATIENTS THAT MEET CRITERIA FOR INCLUSION --*/
SELECT perv.PAT_ID,
       perv.PAT_ENC_CSN_ID,
       ii.IDENTITY_ID AS 'MRN',
       p.BIRTH_DATE,
       p.PAT_NAME,
       perv.ENC_REASON_ID,
       eap.PROC_CODE LOS_PROC_CODE,
       perv.CONTACT_DATE,
       perv.ENC_REASON_NAME,
       pev.DEPARTMENT_ID,
       dep2.DEPARTMENT_NAME
INTO #T
FROM Clarity.dbo.PAT_ENC_RSN_VISIT_VIEW perv
    LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev ON perv.PAT_ENC_CSN_ID = pev.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.CLARITY_EAP eap ON pev.LOS_PRIME_PROC_ID = eap.PROC_ID
    LEFT JOIN Clarity.dbo.PATIENT_VIEW p ON perv.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.IDENTITY_ID_VIEW ii ON p.PAT_ID = ii.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_DEP_VIEW dep2 ON pev.DEPARTMENT_ID = dep2.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE perv.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND p.PAT_ID IS NOT NULL
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD'
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'WI';


--****************************************************
/*   MOST RECENT INTAKE VISIT DATE with CC of 1459  */
SELECT t.PAT_ID,
       MAX(t.CONTACT_DATE) AS 'LAST_INTAKE_VISIT_DATE'
INTO #Last_Intake
FROM #T t
WHERE t.ENC_REASON_ID = 1459
GROUP BY t.PAT_ID;

--****************************************************
/*   MOST RECENT INTAKE VISIT DATE  */
/*	But Intake was not in the last 42 days of the reporting period	*/
SELECT i.PAT_ID,
       i.LAST_INTAKE_VISIT_DATE,
       '1' AS Has_Intake
INTO #Intake_Minus30
FROM #Last_Intake i
WHERE i.LAST_INTAKE_VISIT_DATE <= (DATEADD(DAY, -43, GETDATE()));

--**********************************************************************
/*--  YES OR NO - 4 TREAMENTS OF ANY TYPE, WITHIN 42 DAYS OF LAST INTAKE VISIT --*/
SELECT V.PAT_ID,
       V.YN_VISITS 'Visits Within Six Weeks of Intake',
       CASE WHEN v.YN_VISITS > 3 THEN 'Yes'
       END AS 'FOUR_VISITS_SINCE_INTAKE'
INTO #4VIS_YN --DROP TABLE #4VIS_YN
FROM (SELECT I3.PAT_ID,
             COUNT(t.PAT_ENC_CSN_ID) AS 'YN_VISITS'
      FROM #T t
          LEFT JOIN #Last_Intake I3 ON t.PAT_ID = I3.PAT_ID
      WHERE t.CONTACT_DATE >= I3.LAST_INTAKE_VISIT_DATE
            AND t.CONTACT_DATE <= (DATEADD(DAY, 42, I3.LAST_INTAKE_VISIT_DATE))
      GROUP BY I3.PAT_ID

--HAVING 
--	COUNT(t.PAT_ENC_CSN_ID) > 3
) V;

--****************************************************
/*-- RETURNS MOST RECENT DAY TREATMENT, TRANSITION DATE, AND COUNT OF PATIENTS WHO RECEIVE DAY TREATMENTS  --*/
SELECT t.PAT_ID,
       i.LAST_INTAKE_VISIT_DATE,
       COUNT(t.PAT_ENC_CSN_ID) AS 'DAY_TREATMENTS',
       MIN(t.CONTACT_DATE) AS 'FIRST_DAY_TRTMNT',
       MAX(t.CONTACT_DATE) AS 'MOST_RECENT_DAY_TRTMNT'
INTO #Day_Treatment
FROM #T t
    LEFT JOIN #Last_Intake i ON t.PAT_ID = i.PAT_ID
WHERE t.LOS_PROC_CODE IN ( 'H2012', 'H0005' ) --Day Treatment 
      AND (t.CONTACT_DATE >= i.LAST_INTAKE_VISIT_DATE
           OR i.LAST_INTAKE_VISIT_DATE IS NULL)
GROUP BY t.PAT_ID,
         i.LAST_INTAKE_VISIT_DATE;

--***********************************************
/*  Number of completed Outpatient Visits Since Last Intake  */
SELECT t.PAT_ID,
       i.LAST_INTAKE_VISIT_DATE,
       COUNT(t.PAT_ENC_CSN_ID) AS 'COMP_OUT_VISITS',
       MIN(t.CONTACT_DATE) AS 'FIRST_OUTPATIENT_VISIT',
       MAX(t.CONTACT_DATE) AS 'LAST_OUTPATIENT_VISIT'
INTO #Outpatient
FROM #T t
    LEFT JOIN #Last_Intake i ON t.PAT_ID = i.PAT_ID
WHERE t.LOS_PROC_CODE IN ( 'H0005', 'H0022' )
      AND (t.CONTACT_DATE > i.LAST_INTAKE_VISIT_DATE OR i.LAST_INTAKE_VISIT_DATE IS NULL)
GROUP BY t.PAT_ID,
         i.LAST_INTAKE_VISIT_DATE;
--****************************************************

SELECT DISTINCT t.PAT_ID,
                t.MRN,
                t.PAT_NAME,
                t.BIRTH_DATE,
                i.LAST_INTAKE_VISIT_DATE,
                dt.FIRST_DAY_TRTMNT,
                dt.MOST_RECENT_DAY_TRTMNT,
                dt.DAY_TREATMENTS,
                o.FIRST_OUTPATIENT_VISIT,
                o.LAST_OUTPATIENT_VISIT,
                o.COMP_OUT_VISITS,
                COALESCE(V.FOUR_VISITS_SINCE_INTAKE, 'No') AS FOUR_VISITS_SINCE_INTAKE,
                64 AS SERVICE_AREA,
                t.DEPARTMENT_ID,
                t.DEPARTMENT_NAME,
                v.[Visits Within Six Weeks of Intake],
                ISNULL(IM.Has_Intake, '0') AS Complete_Denom,
                CASE WHEN V.FOUR_VISITS_SINCE_INTAKE = 'Yes'
                          AND IM.Has_Intake = 1 THEN 'Met'
                    ELSE 'Not Met'
                END AS MET_YN,
                CASE WHEN dt.FIRST_DAY_TRTMNT IS NOT NULL THEN '1'
                    ELSE '0'
                END AS Transition_Denom,
                CASE WHEN dt.MOST_RECENT_DAY_TRTMNT <= o.LAST_OUTPATIENT_VISIT THEN '1'
                    ELSE '0'
                END AS Transition_Num,
				flg.PAT_FLAG_TYPE_C
INTO #a
FROM #T t
    LEFT JOIN #Last_Intake i ON t.PAT_ID = i.PAT_ID
    LEFT JOIN #4VIS_YN V ON i.PAT_ID = V.PAT_ID
    LEFT JOIN #Intake_Minus30 IM ON t.PAT_ID = IM.PAT_ID
    LEFT JOIN #Day_Treatment dt ON t.PAT_ID = dt.PAT_ID
    LEFT JOIN #Outpatient o ON t.PAT_ID = o.PAT_ID
	LEFT JOIN ( SELECT DISTINCT flag.PATIENT_ID, flag.PAT_FLAG_TYPE_C
                             FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                             WHERE flag.ACTIVE_C = 1 -- AND flag.PAT_FLAG_TYPE_C = '640007
							 ) flg ON flg.PATIENT_ID = t.PAT_ID
	;

SELECT a.MRN,
       a.PAT_NAME 'Patient',
       CAST(a.BIRTH_DATE AS DATE) 'DOB',
       CAST(a.LAST_INTAKE_VISIT_DATE AS DATE) 'Intake Date',
       --,a.FIRST_DAY_TRTMNT
       --,a.MOST_RECENT_DAY_TRTMNT
       ISNULL(a.DAY_TREATMENTS, 0) + ISNULL(a.COMP_OUT_VISITS, 0) 'Total Visits in Last Twelve Months',
       --,a.FIRST_OUTPATIENT_VISIT
       --,a.LAST_OUTPATIENT_VISIT
       --,a.COMP_OUT_VISITS
       a.FOUR_VISITS_SINCE_INTAKE,
       a.DEPARTMENT_NAME,
       --,a.Complete_Denom
       a.MET_YN 'Four or More Completed Visits',
       --,a.Transition_Denom
       --,a.Transition_Num
       a3.CITY,
       a3.STATE,
       a.[Visits Within Six Weeks of Intake],
	   a.PAT_FLAG_TYPE_C
FROM #a a
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = a.PAT_ID
WHERE a.Complete_Denom = 1;


--*****************************************************
/*  --  DROP TEMP TABLES  -- */

DROP TABLE #T;
DROP TABLE #Outpatient;
DROP TABLE #Last_Intake;
DROP TABLE #Intake_Minus30;
DROP TABLE #Day_Treatment;
DROP TABLE #4VIS_YN;
DROP TABLE #a;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
