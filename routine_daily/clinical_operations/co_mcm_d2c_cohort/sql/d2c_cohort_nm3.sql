/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Medical - Data to Care Cohort
 Create Date: 3/25/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  Jess C

 Purpose:   Hardcoded list of MRNs to track the outcomes of Outreach pts

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT TOP 10000000 pev.PAT_ID,
                    pev.CONTACT_DATE LAST_OFFICE_VISIT,
                    dep.STATE,
                    dep.CITY,
                    dep.SERVICE_TYPE,
                    dep.SERVICE_LINE AS LOS,
                    dep.SUB_SERVICE_LINE,
                    dep.SITE
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -48, GETDATE())
      --AND pev.APPT_STATUS_C IN (2, 6)  --Since looking for pts with no compelted appts
      AND pev.ENC_TYPE_C NOT IN ( '32000', '119' ) --MyChart Encounters

;

SELECT TOP 10000000 a1.PAT_ID,
                    a1.STATE,
                    a1.CITY,
                    a1.SITE,
                    a1.LOS,
                    a1.SERVICE_TYPE,
                    a1.SUB_SERVICE_LINE,
                    ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT TOP 10000000 a2.PAT_ID,
                    a2.LOS,
                    a2.SERVICE_TYPE,
                    a2.SUB_SERVICE_LINE,
                    a2.SITE,
                    a2.CITY,
                    a2.STATE
INTO #Attribution3
FROM #Attribution2 a2
WHERE a2.ROW_NUM_DESC = 1;

SELECT TOP 1000000 id.IDENTITY_ID MRN,
                   p.PAT_NAME PATIENT,
                   ser.PROV_NAME 'PCP',
                   a3.CITY,
                   a3.STATE,
                   a3.LOS,
                   a3.SERVICE_TYPE,
                   a3.SUB_SERVICE_LINE,
                   a3.SITE,
                   vis.[VISIT PROVIDER] 'NEW VISIT PROVIDER',
                   vis.[VISIT DATE] AS 'NEW VISIT DATE',
                   vis.[VISIT DEPT] 'NEW VISIT DEPT',
                   CAST(CASE WHEN ser.PROV_NAME IS NULL THEN term.TERM_DATE END AS DATE) AS 'PCP TERM DATE',
                   CASE WHEN ser.PROV_NAME IS NULL THEN term.[PCP TERM REASON]
                   END AS 'PCP TERM REASON',
                   lvis.[VISIT DATE] AS 'LAST VISIT BEFORE OOC',
                   COALESCE(DATEDIFF(MONTH, lvis.[VISIT DATE], GETDATE()), -9999) 'MONTHS SINCE OOC',
                   CASE WHEN vis.[VISIT PROVIDER] IS NOT NULL THEN 'Back in Care'
                       WHEN ser.PROV_NAME IS NULL THEN 'PCP Termed'
                       ELSE 'Still OOC'
                   END AS 'DISPOSITION',
                   CASE WHEN vis.[VISIT PROVIDER] IS NOT NULL THEN 'Case Closed'
                       WHEN ser.PROV_NAME IS NULL THEN 'Case Closed'
                       ELSE 'Case Open'
                   END AS 'STATUS',
                   svis.[Next Any Appt] AS 'Next Any Appt',
                   svis.[Next Appt Prov],
                   spvis.[Next PCP Appt] AS 'Next PCP Appt',
                   spvis.[Next PCP Appt Prov]
FROM Clarity.dbo.IDENTITY_ID_VIEW id
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                                                  AND ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs and NPs
    LEFT JOIN (SELECT pev.PAT_ID,
                      ser.PROV_NAME 'VISIT PROVIDER',
                      CAST(pev.CONTACT_DATE AS DATE) 'VISIT DATE',
                      dep.DEPARTMENT_NAME 'VISIT DEPT',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
                     AND ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs and NPs

    ) vis ON vis.PAT_ID = id.PAT_ID
             AND vis.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT TOP 1000000 pev.PAT_ID,
                                  ser.PROV_NAME 'VISIT PROVIDER',
                                  CAST(pev.CONTACT_DATE AS DATE) 'VISIT DATE',
                                  dep.DEPARTMENT_NAME 'VISIT DEPT',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND pev.CONTACT_DATE < DATEADD(MONTH, -12, GETDATE())
                     AND pev.CONTACT_DATE > DATEADD(MONTH, -49, GETDATE())
                     AND ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs and NPs

    ) lvis ON lvis.PAT_ID = id.PAT_ID
              AND lvis.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT TOP 10000000 ct.PAT_ID,
                                   CAST(ct.TERMINATION_NEW_DT AS DATE) 'TERM_DATE',
                                   zcp.NAME 'PCP TERM REASON',
                                   ROW_NUMBER() OVER (PARTITION BY ct.PAT_ID ORDER BY ct.TERMINATION_NEW_DT DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.CARE_TEAM_EDIT_HX_VIEW ct
                   LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = ct.PROV_ID
                   LEFT JOIN Clarity.dbo.ZC_PCP_SWITCH_RSN zcp ON zcp.SWITCH_REASON_C = ct.SWITCH_REASON_NEW_C
               WHERE ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs and NPs
                     AND ct.TERMINATION_NEW_DT IS NOT NULL
                     AND ser.SERV_AREA_ID = 64) term ON term.PAT_ID = id.PAT_ID
                                                        AND term.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT TOP 1000000 pev.PAT_ID,
                                  CAST(pev.CONTACT_DATE AS DATE) AS 'Next Any Appt',
                                  ser.PROV_NAME 'Next Appt Prov',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled

    ) svis ON svis.PAT_ID = id.PAT_ID
              AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT TOP 1000000 pev.PAT_ID,
                                  CAST(pev.CONTACT_DATE AS DATE) AS 'Next PCP Appt',
                                  ser.PROV_NAME 'Next PCP Appt Prov',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND ser.PROV_ID <> '640178' --pulmonologist
                     AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs

    ) spvis ON spvis.PAT_ID = id.PAT_ID
               AND spvis.ROW_NUM_ASC = 1 -- First scheduled
WHERE id.IDENTITY_ID IN ( '640007103', '640001555', '640000518', '64001374', '640004292', '640000185', '640031551', '640011945', '640000693', '64000244',
                          '64000247', '640004327', '64000269', '640004033', '640015438', '640003854', '640003696', '640004231', '640003708', '640003800',
                          '640012674', '640006638', '64000427', '64000430', '640021706', '640004168', '64001230', '640007700', '640001768', '640005247',
                          '64000575', '640004763', '640004957', '640004407', '640017039', '640000987', '640001716', '64000738', '64000739', '640005020',
                          '64000766', '640015043', '64000787', '640001750', '64000789', '64000800', '640032304', '640000852', '640000392', '640030042',
                          '640000097', '64000966', '640018600', '64001005', '640000674', '64001038', '640003613', '64001084', '64001104', '64001105',
                          '640000248', '640001850', '64001135', '64001174', '640000206', '640004639', '640003787', '64001211', '64001239', '64001286',
                          '640001884', '64001308', '64001313', '640000128', '64001348', '640000612', '64001361', '640005834', '640019342', '64000028',
                          '640003558', '640005883', '640016616', '640001775', '640004548', '64000137', '64000161', '64000168', '64000169', '640011959',
                          '640004544', '64000212', '64000237', '640015111', '640001746', '640002580', '640003727', '64000457', '640000678', '640013477',
                          '64000505', '640001091', '640000516', '640000061', '640015075', '640003716', '640000067', '640010644', '640004020', '640000623',
                          '64000747', '640007685', '640016706', '640008595', '640008974', '640005233', '640000081', '640000323', '640004250', '640018110',
                          '640017603', '64000254', '64000984', '64001025', '640036940', '640016886', '640013815', '640004282', '64001099', '64001149',
                          '64001207', '640001673', '64001239', '640006310', '64001294', '640017167', '64001342', '640017992', '640004349' );
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;