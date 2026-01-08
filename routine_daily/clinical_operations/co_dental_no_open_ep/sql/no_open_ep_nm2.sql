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
       dep.STATE,
       dep.CITY,
       dep.SITE,
       dep.SERVICE_TYPE,
       dep.SERVICE_LINE,
       dep.SUB_SERVICE_LINE
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -3, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.SERVICE_TYPE,
       a1.SERVICE_LINE,
       a1.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.SERVICE_LINE = 'DENTAL';

SELECT a2.PAT_ID,
       a2.SERVICE_TYPE,
       a2.SERVICE_LINE,
       a2.SUB_SERVICE_LINE, a2.CITY, a2.STATE
INTO #Attribution3 
FROM #Attribution2 a2 
WHERE a2.ROW_NUM_DESC = 1;

SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME,
       a3.CITY,
       a3.STATE,
       a3.SERVICE_TYPE,
       a3.SERVICE_LINE,
       a3.SUB_SERVICE_LINE,
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
                    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
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
                        LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
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
       f.SERVICE_TYPE,
       f.SERVICE_LINE,
       f.SUB_SERVICE_LINE,
       f.DENTAL_EPISODE,
       CAST(f.CONTACT_DATE AS DATE) AS LAST_VISIT,
       f.VISIT_PROVIDER
FROM #final f
WHERE f.DENTAL_EPISODE = 'NONE';

DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #final;
