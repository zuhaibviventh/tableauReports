/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Active Psychiatry Patients Retention
 Create Date:	12/24/2019
 Created By:	sharmaj
 System:		javelin.ochin.org
 Requested By:	

 Purpose:		

 Description: Denominator: Patients with a psych episode that’s been open 

			Criterion: Amount of time that’s passed between the most recent psych visit and today

			Numerator: Patients whose time from the “Criterion” < 6 months--- then Met else Not Met

 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------


**********************************************************************************************

*/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
       dep.STATE,
       dep.CITY,
       dep.SITE,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS 'LOS',
	   dep.SERVICE_TYPE,
	   dep.SERVICE_LINE, 
	   dep.SUB_SERVICE_LINE
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
	   a1.SERVICE_TYPE,
	   a1.SERVICE_LINE, 
	   a1.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'BEHAVIORAL';

SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE,a2.SERVICE_TYPE, a2.SERVICE_LINE, a2.SUB_SERVICE_LINE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;


/* Most recent psych visit/patient who has open and active episode for at least 6 months */
SELECT p.PAT_ID,
       p.PAT_NAME,
       id.IDENTITY_ID,
       ser.EXTERNAL_NAME PROV,
       CAST(pev.CONTACT_DATE AS DATE) Most_Recent_APPT,
       DATEDIFF(MONTH, (pev.CONTACT_DATE), GETDATE()) 'Months_since_Most_Recent_APPT',
       ROW_NUMBER() OVER (PARTITION BY p.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC,
       a3.city,
       a3.STATE,
	   a3.SERVICE_TYPE,
	   a3.SERVICE_LINE, 
	   a3.SUB_SERVICE_LINE,
       ser2.PROV_TYPE
INTO #a
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.EPISODE_VIEW AS ev2 ON p.PAT_ID = ev2.PAT_LINK_ID
    INNER JOIN Clarity.dbo.EPISODE_LINK_VIEW AS elv ON elv.EPISODE_ID = ev2.EPISODE_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW AS pev2 ON pev2.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW AS ser2 ON ser2.PROV_ID = pev2.VISIT_PROV_ID
WHERE pev.APPT_STATUS_C IN ( 2, 6 )
      AND pev.CONTACT_DATE >= DATEADD(M, -24, GETDATE())
      AND ev2.SUM_BLK_TYPE_ID = 221
      AND ev2.STATUS_C = 1
      AND ser.PROVIDER_TYPE_C IN ( '164', '136', '129' );


SELECT a.PAT_ID,
       a.PAT_NAME,
       a.IDENTITY_ID,
       a.PROV,
       a.Most_Recent_APPT,
       a.Months_since_Most_Recent_APPT,
       a.CITY,
       a.STATE,
	   a.SERVICE_TYPE,
	   a.SERVICE_LINE, 
	   a.SUB_SERVICE_LINE,
       a.PROV_TYPE
INTO #b
FROM #a a
WHERE a.ROW_NUM_DESC = 1;


SELECT b.pat_ID,
       b.PAT_NAME,
       b.IDENTITY_ID MRN,
       b.PROV,
       b.Most_Recent_APPT,
       b.Months_since_Most_Recent_APPT,
       CASE WHEN b.Months_since_Most_Recent_APPT < 6 THEN 'MET' --6 = fail
           ELSE 'NOT MET'
       END 'Psych_Retention',
       b.CITY,
       b.STATE,
	   b.SERVICE_TYPE 'Service Type',
	   b.SERVICE_LINE 'Service Line', 
	   b.SUB_SERVICE_LINE 'Sub-Service Line',
       b.PROV_TYPE
FROM #b b;


DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #a;
DROP TABLE #b;
