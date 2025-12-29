/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Depressed Pts w PHQ9 Scores
 Create Date:	11/16/2018
 Created By:	scogginsm
 System:		javelin.ochin.org
 Requested By:	Internal Dahboard

 Purpose:		

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
 12/21/2018			Jaya				Adding Date parameters to Psych and MH active Pts.
 04/19/2019		    Jaya				Updating Active Patient logic to include Dx codes and LOS_PRIME_PROC_ID
 08/26/2019			Jaya				Adding UNION Operator to make code more efficent
 03/02/2020			Jaya				Updating name to new name
**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

-----First pass to get denominator of pts w/ Dx of depression and no Dx of Bi-polar, schitzo, etc ---------
SELECT TOP 1000000000 pev.PAT_ID,
                      pev.PAT_ENC_CSN_ID LAST_VISIT_ID,
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
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );
SELECT TOP 1000000000 a1.PAT_ID,
                      a1.LAST_VISIT_ID,
                      a1.STATE,
                      a1.CITY,
                      a1.LOS,
					  a1.SERVICE_TYPE,
					  a1.SERVICE_LINE, 
					  a1.SUB_SERVICE_LINE,
                      ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID, a1.LOS ORDER BY a1.LAST_VISIT_ID DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1;
SELECT TOP 1000000000 p.PAT_ID,
                      a2.LAST_VISIT_ID,
                      a2.STATE,
                      a2.CITY,
                      a2.LOS,
					  a2.SERVICE_TYPE,
					  a2.SERVICE_LINE, 
					  a2.SUB_SERVICE_LINE
INTO #Attribution3
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serm ON pev.VISIT_PROV_ID = serm.PROV_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE()) ----------------Active Medical
      AND a2.LOS = 'MEDICAL'
      AND p.CUR_PCP_PROV_ID LIKE '64%'
      AND pev.APPT_PRC_ID NOT IN ( '345', '346', '428', '505', '506' ) --Excluding PrEP to catch mis-coded pts
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                     8053, 8054, 8055, 8056 )
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND icd10.code IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
      AND a2.ROW_NUM_DESC = 1

--------------
UNION
SELECT TOP 1000000000 p.PAT_ID,
                      a2.LAST_VISIT_ID,
                      a2.STATE,
                      a2.CITY,
                      a2.LOS,
					  a2.SERVICE_TYPE,
					  a2.SERVICE_LINE, 
					  a2.SUB_SERVICE_LINE
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serm ON pev.VISIT_PROV_ID = serm.PROV_ID
WHERE serm.PROVIDER_TYPE_C IN ( '136', '164', '129' ) --------------Active Psych
      AND a2.LOS = 'BEHAVIORAL'
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ev.SUM_BLK_TYPE_ID = 221
      AND ev.STATUS_C = 1
      AND id.IDENTITY_TYPE_ID = 64
      AND pev.CONTACT_DATE >= DATEADD(d, -180, GETDATE())
      AND a2.ROW_NUM_DESC = 1

-------------------------
UNION
SELECT TOP 1000000000 p.PAT_ID,
                      a2.LAST_VISIT_ID,
                      a2.STATE,
                      a2.CITY,
                      a2.LOS,
					  a2.SERVICE_TYPE,
					  a2.SERVICE_LINE, 
					  a2.SUB_SERVICE_LINE
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serm ON pev.VISIT_PROV_ID = serm.PROV_ID
WHERE serm.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' ) -------------Active MH
      AND a2.LOS = 'BEHAVIORAL'
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ev.SUM_BLK_TYPE_ID = 221
      AND ev.STATUS_C = 1
      AND id.IDENTITY_TYPE_ID = 64
      AND pev.CONTACT_DATE >= DATEADD(d, -90, GETDATE())
      AND a2.ROW_NUM_DESC = 1;

SELECT TOP 1000000000 a3.PAT_ID,
                      p.PAT_NAME,
                      ser.PROV_NAME PCP,
                      a3.STATE,
                      a3.CITY,
					  a3.SERVICE_TYPE,
					  a3.SERVICE_LINE, 
					  a3.SUB_SERVICE_LINE,
                      id.IDENTITY_ID,
                      dep.DEPARTMENT_NAME,
                      meas.RECORDED_TIME LAST_PHQ,
                      CASE WHEN ISNUMERIC(meas.MEAS_VALUE) = 1 THEN meas.MEAS_VALUE
                          ELSE 0 -- Values of YES and NO apply only to the PHQ2 and can be normalized to < 9
                      END 'Value',
                      meas.MEAS_VALUE,
                      MAX(CASE -- These MAX Functions are important, do not take them out
                              WHEN ev.STATUS_C = 1
                                   AND ev.SUM_BLK_TYPE_ID = 221 THEN 'MH'
                              ELSE NULL
                          END) AS 'MH_EPISODE',
                      MAX(
                      CASE -- These MAX Functions are important, do not take them out
                          WHEN pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
                               AND p.CUR_PCP_PROV_ID LIKE '64%'
                               AND pev.APPT_PRC_ID NOT IN ( '345', '346', '428', '505', '506' ) --Excluding PrEP to catch mis-coded pts
                               AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048,
                                                              8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056 )
                               AND pev.APPT_STATUS_C IN ( 2, 6 )
                               AND a3.LOS = 'MEDICAL' THEN 'MED'
                          ELSE NULL
                      END) AS 'ACTIVE_MEDICAL_PT'
INTO #b
FROM #Attribution3 a3
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = a3.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON a3.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
                                                      AND icd10.CODE IN ( SELECT TOP 10000000 vic.ICD_CODES_LIST
                                                                          FROM Clarity.dbo.VCG_ICD_CODES vic
                                                                          WHERE vic.GROUPER_ID = '2100000117'
                                                                                AND vic.CODE_SET_C = 2 --ICD10
                                                   )
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON a3.LAST_VISIT_ID = pev.PAT_ENC_CSN_ID
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
    INNER JOIN Clarity.dbo.IP_FLWSHT_REC_VIEW ifrv ON p.PAT_ID = ifrv.PAT_ID --Per Angelique, this is the right join. We want all PHQs regardless of where they were done
    INNER JOIN Clarity.dbo.IP_FLWSHT_MEAS_VIEW meas ON ifrv.FSD_ID = meas.FSD_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
WHERE pev.PAT_ENC_CSN_ID IS NOT NULL -- This needs to be inthere to stop one weird case from RMC
      AND meas.FLO_MEAS_ID IN ( '1043', '1044', '3011', '3608' ) -- PHQ2 & 9	
      AND plv.PROBLEM_STATUS_C = 1 --Depression Dx is active on problem list
      AND plv.RESOLVED_DATE IS NULL --Depression Dx is not marked as resolved 

GROUP BY a3.PAT_ID,
         p.PAT_NAME,
         ser.PROV_NAME,
         a3.STATE,
         a3.CITY,
		 a3.SERVICE_TYPE,
		 a3.SERVICE_LINE, 
		 a3.SUB_SERVICE_LINE,
         id.IDENTITY_ID,
         dep.DEPARTMENT_NAME,
         meas.RECORDED_TIME,
         meas.MEAS_VALUE;

----------Second pass to order by pt, date and score and assign row numbers-------

SELECT TOP 1000000000 b.IDENTITY_ID,
                      b.PAT_ID,
                      b.PCP,
                      b.PAT_NAME,
                      b.LAST_PHQ,
                      b.Value,
                      b.MEAS_VALUE,
                      ROW_NUMBER() OVER (PARTITION BY b.PAT_ID ORDER BY b.LAST_PHQ DESC, b.Value DESC) AS ROW_NUM_DESC,
                      b.MH_EPISODE,
                      b.ACTIVE_MEDICAL_PT,
                      b.STATE,
                      b.CITY,
					  b.SERVICE_TYPE,
					  b.SERVICE_LINE, 
					  b.SUB_SERVICE_LINE
INTO #c
FROM #b b
WHERE b.PAT_ID NOT IN ( SELECT TOP 1000000000 plv.PAT_ID
                        FROM Clarity.dbo.PROBLEM_LIST_VIEW plv
                            INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
                        WHERE icd10.CODE LIKE 'F31.%'
                              OR icd10.CODE = 'F20.9'
                              OR icd10.CODE = 'F20.81'
                              OR icd10.CODE LIKE 'F23.%'
                              OR icd10.CODE = 'F25.0'
                              OR icd10.CODE = 'F25.1'
                              OR icd10.CODE = 'F34.0'
                              OR icd10.CODE = 'F06.33'
                              OR icd10.CODE = 'F06.34'
                              OR icd10.CODE = 'F21'
                              OR icd10.CODE = 'F22'
                              OR icd10.CODE = 'F06.0'
                              OR icd10.CODE = 'F06.1'
                              OR icd10.CODE = 'F06.2'
                              OR icd10.CODE = 'F28'
                              OR icd10.CODE = 'F29'
                                 AND plv.PROBLEM_STATUS_C = 1 --Depression Dx is active on problem list
                                 AND plv.RESOLVED_DATE IS NULL --Depression Dx is not marked as resolved
);

----Third pass to get most recent PHQ w/ highest score-----------
SELECT TOP 1000000000 c.IDENTITY_ID,
                      c.PAT_ID,
                      c.PCP,
                      c.PAT_NAME,
                      c.LAST_PHQ,
                      c.Value,
                      c.MEAS_VALUE,
                      c.MH_EPISODE,
                      c.ACTIVE_MEDICAL_PT,
                      c.STATE,
                      c.CITY,
					  c.SERVICE_TYPE,
					  c.SERVICE_LINE, 
					  c.SUB_SERVICE_LINE
INTO #d
FROM #c c
WHERE c.ROW_NUM_DESC = 1
ORDER BY c.Value;

SELECT TOP 1000000000 d.IDENTITY_ID,
                      d.PAT_ID,
                      d.PCP,
                      d.PAT_NAME,
                      d.LAST_PHQ,
                      d.Value,
                      CASE WHEN d.Value > 9 THEN 1
                          ELSE 0
                      END AS Greater_Than_9,
                      CASE WHEN d.Value < 10 THEN 1
                          ELSE 0
                      END AS Less_Than_10,
                      CASE WHEN d.Value > 9 THEN 'PHQ 10+'
                          ELSE 'PHQ < 9'
                      END AS PHQ_STATUS,
                      d.MEAS_VALUE,
                      d.MH_EPISODE,
                      d.ACTIVE_MEDICAL_PT,
                      CASE WHEN d.ACTIVE_MEDICAL_PT = 'MED'
                                AND d.MH_EPISODE IS NULL THEN 'MED'
                          WHEN d.ACTIVE_MEDICAL_PT IS NULL
                               AND d.MH_EPISODE = 'MH' THEN 'MH'
                          ELSE 'BOTH'
                      END AS LOS,
                      d.STATE,
                      d.CITY,
					  d.SERVICE_TYPE,
					  d.SERVICE_LINE, 
					  d.SUB_SERVICE_LINE,
                      'CLICK HERE FOR PATIENT DETAIL' 'CLICK HERE FOR PATIENT DETAIL'
INTO #a
FROM #d d;

SELECT TOP 1000000000 a.IDENTITY_ID,
                      a.PAT_ID,
                      a.PCP,
                      a.PAT_NAME,
                      a.LAST_PHQ,
                      a.Value,
                      a.Greater_Than_9,
                      a.Less_Than_10,
                      a.PHQ_STATUS,
                      a.MEAS_VALUE,
                      a.MH_EPISODE,
                      a.ACTIVE_MEDICAL_PT,
                      a.LOS,
                      a.STATE,
                      a.CITY,
					  a.SERVICE_TYPE 'Service Type',
					  a.SERVICE_LINE 'Service Line', 
					  a.SUB_SERVICE_LINE 'Sub-Service Line',
                      a.[CLICK HERE FOR PATIENT DETAIL],
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C IN ( '136', '164', '129' ) THEN ser.PROV_NAME END) AS 'PSYCHIATRY',
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' ) THEN ser.PROV_NAME
                          END) AS 'MH_TEAM_MEMBER'
FROM #a a
    LEFT JOIN Clarity.dbo.PAT_PCP_VIEW ct ON a.PAT_ID = ct.PAT_ID
                                             AND ct.RELATIONSHIP_C IN ( '1', '3' )
                                             AND ct.TERM_DATE IS NULL
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
GROUP BY a.IDENTITY_ID,
         a.PAT_ID,
         a.PCP,
         a.PAT_NAME,
         a.LAST_PHQ,
         a.Value,
         a.Greater_Than_9,
         a.Less_Than_10,
         a.PHQ_STATUS,
         a.MEAS_VALUE,
         a.MH_EPISODE,
         a.ACTIVE_MEDICAL_PT,
         a.LOS,
         a.STATE,
         a.CITY,
		 a.SERVICE_TYPE,
		 a.SERVICE_LINE, 
		 a.SUB_SERVICE_LINE,
         a.[CLICK HERE FOR PATIENT DETAIL];
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #c;
DROP TABLE #d;
