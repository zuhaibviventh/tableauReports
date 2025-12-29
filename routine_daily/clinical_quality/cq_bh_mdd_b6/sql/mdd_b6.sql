/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:   MDD Psych Patients with Antidepressant Script History B_6
 Create Date:   4/6/2022
 Created By:    ViventHealth\MScoggins
 System:        ANL-MKE-SVR-100
 Requested By:  

 Purpose:       

 Description:
 
 BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:       Changed By:         Change Description:
 ------------       -------------       ---------------------------------------------------
 1/27/2023          Mitch               Update inclusion criteria to include BH/MH episodes that were closed during the last 12 months
 1/27/2023          Mitch               Update the check for last visit to 12 months 
 1/27/2023          Mitch               Reduce "existing script" period from -1 to -5 years, to -1 to -2 years

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT TOP 1000000000 pev.PAT_ID,
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
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -18, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT TOP 1000000000 a1.PAT_ID,
                      a1.STATE,
                      a1.CITY,
                      a1.SITE,
                      a1.LOS,
                      ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'BEHAVIORAL';

SELECT TOP 1000000000 a2.PAT_ID,
                      a2.LOS,
                      a2.CITY,
                      a2.STATE
INTO #Attribution3
FROM #Attribution2 a2
WHERE a2.ROW_NUM_DESC = 1;

SELECT TOP 1000000000 id.IDENTITY_ID MRN,
                      p.PAT_NAME 'Patient',
                      pop.Psychiatrist,
                      COALESCE(pm.[Prescribed Before], 'N') 'Previous Antidepressant Rx',
                      pm.[Previous Rx Subclass],
                      nm.[Current Rx Subclass],
                      CASE WHEN pm.[Previous Rx Subclass] = nm.[Current Rx Subclass] THEN 'Y'
                          ELSE 'N'
                      END AS 'Same as Previous Script',
                      p.ZIP
INTO #a
FROM Clarity.dbo.IDENTITY_ID_VIEW id
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    --INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p4.PAT_ID = id.PAT_ID
    INNER JOIN (SELECT TOP 1000000000 --Pts with MDD Dx
                       plv.PAT_ID,
                       ser.PROV_NAME 'Psychiatrist',
                       ROW_NUMBER() OVER (PARTITION BY plv.PAT_ID ORDER BY ct.EFF_DATE DESC) AS ROW_NUM_DESC
                FROM Clarity.dbo.PROBLEM_LIST_VIEW plv
                    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = plv.PAT_ID
                    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                    INNER JOIN Clarity.dbo.PAT_PCP_VIEW ct ON plv.PAT_ID = ct.PAT_ID
                    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = ct.PCP_PROV_ID
                    INNER JOIN Clarity.dbo.EPISODE_VIEW ev ON plv.PAT_ID = ev.PAT_LINK_ID
                WHERE icd10.CODE IN ( 'F32.4', 'F32.5', 'F32.9', 'F33', 'F33.0', 'F33.1', 'F33.2', 'F33.3', 'F33.4', 'F33.41', 'F33.42', 'F33.9' ) -- Provided BY Caren E. (MDD list)
                      AND (plv.RESOLVED_DATE IS NULL --Active Dx
                           OR plv.RESOLVED_DATE > DATEADD(MONTH, -12, GETDATE()))
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'MH', 'BH', 'PY' )
                      AND pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
                      AND ct.RELATIONSHIP_C IN ( 1, 3 )
                      AND (ct.TERM_DATE IS NULL OR ct.TERM_DATE > DATEADD(MONTH, -12, GETDATE()))
                      AND ser.PROVIDER_TYPE_C IN ( '136', '164', '129' ) -- Psychiatrist/Psych NP
                      AND ev.SUM_BLK_TYPE_ID = 221
                      AND (ev.END_DATE IS NULL OR ev.END_DATE > DATEADD(MONTH, -12, GETDATE()))) pop ON pop.PAT_ID = id.PAT_ID
                                                                                                        AND pop.ROW_NUM_DESC = 1 --last added psych
    LEFT JOIN (SELECT TOP 1000000000 omv.PAT_ID,
                                     zps.NAME 'Current Rx Subclass',
                                     MAX(med.NAME) 'Current Rx Medication'
               FROM Clarity.dbo.ORDER_MED_VIEW omv
                   INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
                   INNER JOIN Clarity.dbo.ZC_PHARM_SUBCLASS zps ON zps.PHARM_SUBCLASS_C = med.PHARM_SUBCLASS_C
               WHERE med.PHARM_SUBCLASS_C IN ( 10378, 11109, 10966, 10966, 12356, 12513, 11111, 11108, 10967 /*Antidepressant*/ )
                     AND omv.ORDERING_DATE
                     BETWEEN DATEADD(MONTH, -12, GETDATE()) AND GETDATE()
                     AND omv.ORDER_STATUS_C = 2 -- Sent

               GROUP BY omv.PAT_ID,
                        zps.NAME) nm ON nm.PAT_ID = id.PAT_ID
    LEFT JOIN (SELECT TOP 1000000000 omv.PAT_ID,
                                     MAX('Y') 'Prescribed Before',
                                     zps.NAME 'Previous Rx Subclass',
                                     MAX(med.NAME) 'Previous Rx Medication'
               FROM Clarity.dbo.ORDER_MED_VIEW omv
                   INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
                   INNER JOIN Clarity.dbo.ZC_PHARM_SUBCLASS zps ON zps.PHARM_SUBCLASS_C = med.PHARM_SUBCLASS_C
               WHERE med.PHARM_SUBCLASS_C IN ( 10378, 11109, 10966, 10966, 12356, 12513, 11111, 11108, 10967 /*Antidepressant*/ )
                     AND omv.ORDERING_DATE
                     BETWEEN DATEADD(MONTH, -25, GETDATE()) AND DATEADD(MONTH, -13, GETDATE()) -- scripts between -1 and -2 years
                     AND omv.ORDER_STATUS_C = 2 -- Sent

               GROUP BY omv.PAT_ID,
                        zps.NAME) pm ON pm.PAT_ID = id.PAT_ID;

SELECT TOP 1000000000 a.MRN,
                      a.Patient,
                      a.Psychiatrist,
                      a.[Current Rx Subclass],
                      a.ZIP,
                      MAX(a.[Same as Previous Script]) 'Previous Rx for Subclass'
INTO #b
FROM #a a
WHERE a.[Current Rx Subclass] IS NOT NULL
GROUP BY a.MRN,
         a.Patient,
         a.Psychiatrist,
         a.[Current Rx Subclass],
         a.ZIP;

SELECT TOP 1000000000 b.MRN,
                      id.PAT_ID,
                      b.Patient,
                      b.Psychiatrist,
                      b.[Current Rx Subclass],
                      b.[Previous Rx for Subclass],
                      zps.PHARM_SUBCLASS_C,
                      MIN(omv.ORDERING_DATE) 'First RX Date',
                      b.ZIP
INTO #c
FROM #b b
    INNER JOIN Clarity.dbo.ZC_PHARM_SUBCLASS zps ON b.[Current Rx Subclass] = zps.NAME
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON b.MRN = id.IDENTITY_ID
    INNER JOIN Clarity.dbo.ORDER_MED_VIEW omv ON omv.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON med.MEDICATION_ID = omv.MEDICATION_ID
                                                     AND med.PHARM_SUBCLASS_C = zps.PHARM_SUBCLASS_C
WHERE b.[Previous Rx for Subclass] = 'N'
      AND omv.ORDERING_DATE > DATEADD(MONTH, -13, GETDATE())
GROUP BY b.MRN,
         b.Patient,
         b.Psychiatrist,
         b.[Current Rx Subclass],
         b.[Previous Rx for Subclass],
         zps.PHARM_SUBCLASS_C,
         id.PAT_ID,
         b.ZIP;

SELECT DISTINCT TOP 1000000000 c.MRN,
                               c.PAT_ID,
                               c.Patient,
                               c.Psychiatrist,
                               c.[Current Rx Subclass],
                               c.[First RX Date],
                               DATEADD(DAY, 90, c.[First RX Date]) 'Measurement Period End (90 days)',
                               DATEDIFF(DAY, c.[First RX Date], GETDATE()) 'Days Since First RX',
                               omv.ORDERING_DATE,
                               COALESCE(omv.MED_DIS_DISP_QTY, 30.0) 'Prescribed Dispense Quantity',
                               COALESCE(omv.REFILLS, '0') 'Total Refills Allowed',
                               DATEDIFF(MONTH, c.[First RX Date], omv.ORDERING_DATE) 'Month of Order',
                               pharm.PHARMACY_NAME,
                               c.ZIP
INTO #d
FROM #c c
    INNER JOIN Clarity.dbo.ORDER_MED_VIEW omv ON omv.PAT_ID = c.PAT_ID
    LEFT JOIN Clarity.dbo.RX_PHR pharm ON pharm.PHARMACY_ID = omv.PHARMACY_ID
    INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON med.MEDICATION_ID = omv.MEDICATION_ID
                                                     AND med.PHARM_SUBCLASS_C = c.PHARM_SUBCLASS_C
WHERE omv.ORDERING_DATE > DATEADD(MONTH, -13, GETDATE())
      AND omv.ORDERING_DATE <= DATEADD(DAY, 90, c.[First RX Date]);

SELECT TOP 1000000000 d.MRN,
                      d.PAT_ID,
                      d.Patient,
                      d.Psychiatrist,
                      d.[First RX Date],
                      d.[Prescribed Dispense Quantity] * (1 + d.[Total Refills Allowed]) 'Number of Doses',
                      d.[Measurement Period End (90 days)],
                      d.[Days Since First RX],
                      d.ZIP
INTO #e
FROM #d d;

SELECT TOP 1000000000 e.MRN,
                      e.PAT_ID,
                      e.Patient,
                      e.Psychiatrist,
                      e.[First RX Date],
                      e.[Number of Doses],
                      e.[Measurement Period End (90 days)],
                      CASE WHEN DATEDIFF(DAY, e.[First RX Date], e.[Measurement Period End (90 days)]) < e.[Days Since First RX] THEN
                               DATEDIFF(DAY, e.[First RX Date], e.[Measurement Period End (90 days)])
                          ELSE e.[Days Since First RX]
                      END AS 'Days on Meds',
                      e.ZIP
INTO #f
FROM #e e;
SELECT TOP 1000000000 f.MRN,
                      f.Patient,
                      f.Psychiatrist,
                      CAST(f.[First RX Date] AS DATE) AS 'First RX Date',
                      CAST(f.[Measurement Period End (90 days)] AS DATE) AS 'Measurement Period End (90 days)',
                      SUM(f.[Number of Doses]) 'Total Doses Prescribed',
                      f.[Days on Meds],
                      CASE WHEN f.[Measurement Period End (90 days)] < GETDATE() THEN 'Measurement Period Ended'
                          ELSE 'Still in Measurement Period'
                      END AS 'Measurement Period Complete',
                      a3.CITY,
                      a3.STATE,
                      f.ZIP
FROM #f f
    INNER JOIN #Attribution3 a3 ON f.PAT_ID = a3.PAT_ID
GROUP BY f.MRN,
         f.Patient,
         f.Psychiatrist,
         f.[First RX Date],
         f.[Measurement Period End (90 days)],
         f.[Days on Meds],
         a3.CITY,
         a3.STATE,
         f.ZIP;
DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #c;
DROP TABLE #d;
DROP TABLE #e;
DROP TABLE #f;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;