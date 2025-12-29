/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: RSR SBIRT Outcomes and Followup
 Create Date: 12/5/2018
 Created By:  scogginsm
 System:    javelin.ochin.org
 Requested By:  RSR

 Purpose:   To load AUDIT & DAST P/N and followup data to PE

 Description:
 
  BOE Folder Path: SA64 > TestResult-AUDIT-DAST


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 3/28/2019      Mitch       Added STL and RMC
 03/05/2020     Jaya        Updated department name to new name
 03/08/2020     Jaya        Updated AOrg
 9/29/2021      Mitch       Updating for Tableau use

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT id.IDENTITY_ID 'SCPMRN',
       id.PAT_ID,
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1013' THEN 'AUDIT' END) AS 'CLATestName',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1013' THEN ifmv.MEAS_VALUE END) AS 'CLATestResult',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1021' THEN ifmv.MEAS_VALUE END) AS 'CLAEnhancedMotivation',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1022' THEN ifmv.MEAS_VALUE END) AS 'CLANegotiatedPlan',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1023' THEN ifmv.MEAS_VALUE END) AS 'CLASeekTreatment',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1024' THEN ifmv.MEAS_VALUE END) AS 'CLAFollowupArranged',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1025' THEN ifmv.MEAS_VALUE END) AS 'CLABriefIntervention',
       emp.NAME 'CLATestCompletedBy',
       --,CONCAT(LEFT(STUFF(emp.NAME,1,CHARINDEX(',',emp.NAME + ','),''),CHARINDEX(' (',STUFF(emp.NAME,1,CHARINDEX(',',emp.NAME + ','),'') + ' (')-1),' ' ,LEFT(emp.NAME,CHARINDEX(',',REPLACE(emp.NAME,' (',', (') +',')-1)) AS 'CLATestCompletedBy'
       ifmv.RECORDED_TIME 'CLATestCompletedDate',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'WI' THEN 'Wisconsin'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'MO' THEN 'Missouri'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'CO' THEN 'Colorado'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'TX' THEN 'Texas'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2)
       END AS AOrg,
       'Client Services' APgm,
       'Final' 'CLATestStatus',
       'Keyword' 'CLATestResultType',
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
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KC' THEN 'KANSAS CITY'
           ELSE 'ERROR'
       END AS CITY
INTO #a
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.IP_FLWSHT_REC_VIEW ifrv ON p.PAT_ID = ifrv.PAT_ID
    LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS_VIEW ifmv ON ifrv.FSD_ID = ifmv.FSD_ID
    LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev ON ifrv.INPATIENT_DATA_ID = pev.INPATIENT_DATA_ID
    LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA ifgd ON ifmv.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
    LEFT JOIN Clarity.dbo.CLARITY_EMP_VIEW emp ON ifmv.TAKEN_USER_ID = emp.USER_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON emp.PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE ifmv.FLO_MEAS_ID IN ( '1013', '1021', '1022', '1023', '1024', '1025' )
      AND ifmv.RECORDED_TIME > '12/31/2018'
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) IN ( 'WI', 'CO', 'MO', 'TX' )
GROUP BY id.IDENTITY_ID,
         id.PAT_ID,
         emp.NAME,
         ifmv.RECORDED_TIME,
         dep.DEPT_ABBREVIATION
UNION
SELECT id.IDENTITY_ID 'SCPMRN',
       id.PAT_ID,
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1014' THEN 'DAST' END) AS 'CLATestName',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1014' THEN ifmv.MEAS_VALUE END) AS 'CLATestResult',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1021' THEN ifmv.MEAS_VALUE END) AS 'CLAEnhancedMotivation',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1022' THEN ifmv.MEAS_VALUE END) AS 'CLANegotiatedPlan',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1023' THEN ifmv.MEAS_VALUE END) AS 'CLASeekTreatment',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1024' THEN ifmv.MEAS_VALUE END) AS 'CLAFollowupArranged',
       MAX(CASE WHEN ifmv.FLO_MEAS_ID = '1025' THEN ifmv.MEAS_VALUE END) AS 'CLABriefIntervention',
       emp.NAME 'CLATestCompletedBy',
       --,CONCAT(LEFT(STUFF(emp.NAME,1,CHARINDEX(',',emp.NAME + ','),''),CHARINDEX(' (',STUFF(emp.NAME,1,CHARINDEX(',',emp.NAME + ','),'') + ' (')-1),' ' ,LEFT(emp.NAME,CHARINDEX(',',REPLACE(emp.NAME,' (',', (') +',')-1)) AS 'CLATestCompletedBy'
       ifmv.RECORDED_TIME 'CLATestCompletedDate',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'WI' THEN 'Wisconsin'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'MO' THEN 'Missouri'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'CO' THEN 'Colorado'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'TX' THEN 'Texas'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2)
       END AS AOrg,
       'Client Services' APgm,
       'Final' 'CLATestStatus',
       'Keyword' 'CLATestResultType',
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
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'CG' THEN 'CHICAGO'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'KC' THEN 'KANSAS CITY'
           ELSE 'ERROR'
       END AS CITY
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.IP_FLWSHT_REC_VIEW ifrv ON p.PAT_ID = ifrv.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON ifrv.INPATIENT_DATA_ID = pev.INPATIENT_DATA_ID
    LEFT JOIN Clarity.dbo.IP_FLWSHT_MEAS_VIEW ifmv ON ifrv.FSD_ID = ifmv.FSD_ID
    LEFT JOIN Clarity.dbo.IP_FLO_GP_DATA ifgd ON ifmv.FLO_MEAS_ID = ifgd.FLO_MEAS_ID
    LEFT JOIN Clarity.dbo.CLARITY_EMP_VIEW emp ON ifmv.TAKEN_USER_ID = emp.USER_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON emp.PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE ifmv.FLO_MEAS_ID IN ( '1014', '1021', '1022', '1023', '1024', '1025' )
      AND ifmv.RECORDED_TIME > '12/31/2018'
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) IN ( 'WI', 'CO', 'MO', 'TX' )
GROUP BY id.IDENTITY_ID,
         id.PAT_ID,
         emp.NAME,
         ifmv.RECORDED_TIME,
         dep.DEPT_ABBREVIATION;


SELECT a.SCPMRN,
       a.CLATestName,
       a.CLATestResult,
       CASE WHEN a.CLATestResult = 'Positive ' THEN 1
           WHEN a.CLATestResult = 'Positive' THEN 1
           ELSE 0
       END AS TR_NUMBER,
       CASE WHEN a.CLAEnhancedMotivation IS NULL THEN 'No'
           ELSE a.CLAEnhancedMotivation
       END AS CLAEnhancedMotivation,
       CASE WHEN a.CLANegotiatedPlan IS NULL THEN 'No'
           ELSE a.CLANegotiatedPlan
       END AS CLANegotiatedPlan,
       CASE WHEN a.CLASeekTreatment IS NULL THEN 'No'
           ELSE a.CLASeekTreatment
       END AS CLASeekTreatment,
       CASE WHEN a.CLAFollowupArranged IS NULL THEN 'No'
           ELSE a.CLAFollowupArranged
       END AS CLAFollowupArranged,
       CASE WHEN a.CLABriefIntervention IS NULL THEN 'No'
           ELSE a.CLABriefIntervention
       END AS CLABriefIntervention,
       a.CLATestCompletedBy,
       a.CLATestCompletedDate,
       a.AOrg,
       a.APgm,
       a.CLATestStatus,
       a.CLATestResultType,
       a.CITY,
       p.PAT_NAME PATIENT,
       CAST(CURRENT_TIMESTAMP AS DATE) AS TODAY,
       CAST(a.CLATestCompletedDate AS DATE) AS 'Screen Date'
FROM #a a
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = a.PAT_ID
WHERE a.CLATestResult IS NOT NULL

--ORDER BY 
--  a.SCPMRN
--  ,a.CLATestCompletedDate

;

DROP TABLE #a;