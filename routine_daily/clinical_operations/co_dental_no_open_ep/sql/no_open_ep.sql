/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:   Dental Episodes
 Create Date:   9/22/2020
 Created By:    ARCW\MScoggins
 System:        SQL-MKE-DEV-001
 Requested By:  Scott G 

 Purpose:       Pts seen in the last 3 months with and w/o An episode

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:       Changed By:         Change Description:
 ------------       -------------       ---------------------------------------------------


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
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -3, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'DENTAL';

SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE 
INTO #Attribution3 
FROM #Attribution2 a2 
WHERE a2.ROW_NUM_DESC = 1;

SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME,
       a3.CITY,
       a3.STATE,
       CASE WHEN ev.SUM_BLK_TYPE_ID IS NOT NULL THEN 'OPEN EPISODE'
           ELSE 'NONE'
       END AS DENTAL_EPISODE,
       apt.CONTACT_DATE,
       apt.EXTERNAL_NAME VISIT_PROVIDER
INTO #final
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN (SELECT pev.CONTACT_DATE,
                       ser.EXTERNAL_NAME,
                       pev.PAT_ID,
                       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
                FROM Clarity.dbo.PAT_ENC_VIEW pev
                    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') apt ON apt.PAT_ID = p.PAT_ID
                                                                                AND apt.ROW_NUM_DESC = 1
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON ev.PAT_LINK_ID = p.PAT_ID
                                             AND ev.STATUS_C = 1
                                             AND ev.SUM_BLK_TYPE_ID = 45
WHERE p.PAT_ID IN ( SELECT DISTINCT pev.PAT_ID
                    FROM Clarity.dbo.PAT_ENC_VIEW pev
                        INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                    WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                          AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
                          --AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'WI'
                          AND pev.CONTACT_DATE > DATEADD(MONTH, -3, GETDATE()))
      AND p.PAT_ID NOT IN 
		( SELECT DISTINCT 
			ev.PAT_LINK_ID 
		FROM Clarity.dbo.EPISODE_VIEW ev 
		WHERE 
			ev.STATUS_C <> 1 
			AND ev.SUM_BLK_TYPE_ID = 45 
		)
	  ;

SELECT f.MRN,
       f.PAT_NAME,
       f.CITY,
       f.STATE,
       f.DENTAL_EPISODE,
       CAST(f.CONTACT_DATE AS DATE) AS LAST_VISIT,
       f.VISIT_PROVIDER
FROM #final f
WHERE f.DENTAL_EPISODE = 'NONE';

DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #final;
