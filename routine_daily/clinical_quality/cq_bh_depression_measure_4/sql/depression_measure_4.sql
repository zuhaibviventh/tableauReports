/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Depression 4 - VLS for Depressed pts
 Create Date:	8/23/2018
 Created By:	sharmaj
 System:		javelin.ochin.org
 Requested By:	Internal Dashboard

 Purpose:	Depression 4 - Viral Supression Rate for Patients with Depression	

 Description: DENOM: Patients with active diagnosis of depression
				NUM: Met = Patients who are virally suppressed <200, Not met = Pts with VL 200+
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
 12/21/2018			Jaya				Adding Date parameters to Psych and MH active Pts.
 04/19/2019		    Jaya				Updating Active Patient logic to include Dx codes and LOS_PRIME_PROC_ID
 8/26/2019			Jaya				Adding UNION Operator to make code more efficent
 03/02/2020			Jaya				Updated name to new name
 11/4/2020			Mitch				Updating to new Component ID Logic and trying to improve overall performance
 1/29/2023			Mitch				Including BH Episodes closed in the last 12 months
 1/29/2023			Mitch				Removing 90 and 180-day lookback periods for BH providers
**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT TOP 1000000000 pev.PAT_ID,
                      pev.CONTACT_DATE last_visit_date,
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
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT TOP 1000000000 a1.PAT_ID,
                      a1.STATE,
                      a1.CITY,
                      a1.LOS,
                      ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID, a1.LOS ORDER BY a1.last_visit_date DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1;

; SELECT TOP 1000000000 p.PAT_ID
  INTO #pop
  FROM Clarity.dbo.PATIENT_VIEW p
      INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
      INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
      INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
      INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
      INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
      INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
      LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
  WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE()) ----------------Active Medical
        AND a2.LOS = 'MEDICAL'
        AND a2.ROW_NUM_DESC = 1
        AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                       8053, 8054, 8055, 8056 )
        AND pev.APPT_STATUS_C IN ( 2, 6 )
        AND icd10.code IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
        AND plv.RESOLVED_DATE IS NULL --Active Dx
        AND plv.PROBLEM_STATUS_C = 1 --Active Dx
        AND ser.SERV_AREA_ID = 64

  --------------
  UNION
  SELECT TOP 1000000000 p.PAT_ID
  FROM Clarity.dbo.PATIENT_VIEW p
      INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
      INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
      INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
      LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
      LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
      LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
  WHERE a2.LOS = 'BEHAVIORAL'
        AND a2.ROW_NUM_DESC = 1
        AND pev.APPT_STATUS_C IN ( 2, 6 )
        AND ev.SUM_BLK_TYPE_ID = 221
        AND (ev.END_DATE IS NULL OR ev.END_DATE > DATEADD(MONTH, -12, GETDATE()))
        AND id.IDENTITY_TYPE_ID = 64;

SELECT TOP 1000000000 a2.PAT_ID,
                      p.PAT_NAME,
                      ser.PROV_NAME PCP,
                      a2.STATE,
                      a2.CITY,
                      id.IDENTITY_ID,
                      dep.DEPARTMENT_NAME,
                      orv.ORD_VALUE,
                      orv.ORD_NUM_VALUE,
                      orv.RESULT_DATE LAST_LAB,
                      ROW_NUMBER() OVER (PARTITION BY opv.PAT_ID ORDER BY orv.RESULT_DATE DESC) AS ROW_NUM_DESC
INTO #Attribution3
FROM #Attribution2 a2
    INNER JOIN #pop pop ON pop.PAT_ID = a2.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = a2.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON a2.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
                                                      AND icd10.CODE IN ( SELECT TOP 10000000 vic.ICD_CODES_LIST
                                                                          FROM Clarity.dbo.VCG_ICD_CODES vic
                                                                          WHERE vic.GROUPER_ID = '2100000117'
                                                                                AND vic.CODE_SET_C = 2 --ICD10
                                                   )
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.ORDER_PROC_VIEW opv ON p.PAT_ID = opv.PAT_ID
    LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev ON opv.PAT_ENC_CSN_ID = pev.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
    INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON cc.COMPONENT_ID = orv.COMPONENT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
WHERE a2.ROW_NUM_DESC = 1
      AND plv.PROBLEM_STATUS_C = 1
      AND plv.RESOLVED_DATE IS NULL
      AND pev.PAT_ENC_CSN_ID IS NOT NULL -- This needs to be inthere to stop one weird case from RMC
      AND orv.RESULT_DATE >= DATEADD(MM, -12, GETDATE())
      AND cc.COMMON_NAME = 'HIV VIRAL LOAD';

SELECT TOP 1000000000 a3.IDENTITY_ID,
                      a3.PAT_ID,
                      a3.PCP,
                      a3.PAT_NAME,
                      a3.DEPARTMENT_NAME,
                      a3.STATE,
                      a3.CITY,
                      a3.ORD_VALUE,
                      a3.LAST_LAB,
                      CASE WHEN a3.ORD_NUM_VALUE <> 9999999 THEN CAST(a3.ORD_VALUE AS FLOAT)
                          WHEN a3.ORD_VALUE LIKE '>%' THEN 10000000
                          ELSE 0.0
                      END AS Result_Output
INTO #b
FROM #Attribution3 a3
WHERE a3.ROW_NUM_DESC = 1;

SELECT TOP 1000000000 b.IDENTITY_ID,
                      b.PAT_ID,
                      b.PAT_NAME,
                      b.PCP,
                      b.STATE,
                      b.CITY,
                      b.DEPARTMENT_NAME,
                      b.ORD_VALUE,
                      b.LAST_LAB,
                      b.Result_Output,
                      CASE WHEN b.Result_Output < 200 THEN 1
                          ELSE 0
                      END AS MET_YN,
                      'CLICK HERE FOR PATIENT DETAIL' 'CLICK HERE FOR PATIENT DETAIL'
INTO #c
FROM #b b;

SELECT TOP 1000000000 c.IDENTITY_ID,
                      c.PAT_ID,
                      c.PAT_NAME,
                      c.PCP,
                      MAX(c.MET_YN) MET_YN,
                      c.STATE,
                      c.CITY,
                      c.DEPARTMENT_NAME,
                      c.[CLICK HERE FOR PATIENT DETAIL],
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C IN ( '136', '164', '129' ) THEN ser.PROV_NAME END) AS 'PSYCHIATRY',
                      MAX(CASE WHEN ser.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' ) THEN ser.PROV_NAME
                          END) AS 'MH_TEAM_MEMBER',
                      c.ORD_VALUE
FROM #c c
    LEFT JOIN Clarity.dbo.PAT_PCP_VIEW ct ON c.PAT_ID = ct.PAT_ID
                                             AND ct.RELATIONSHIP_C IN ( '1', '3' )
                                             AND ct.TERM_DATE IS NULL
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
GROUP BY c.IDENTITY_ID,
         c.PAT_ID,
         c.PAT_NAME,
         c.PCP,
         c.STATE,
         c.CITY,
         c.DEPARTMENT_NAME,
         c.[CLICK HERE FOR PATIENT DETAIL],
         c.ORD_VALUE;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #b;
DROP TABLE #c;
DROP TABLE #pop;


