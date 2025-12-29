/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Depression 2 - Depressed Pts with Specific Dx
 Create Date:	8/23/2018
 Created By:	scogginsm
 System:		javelin.ochin.org
 Requested By:	Internal Dashboard

 Purpose:		Depression Measure 5 - Depressed Pts with Specific Dx

 Description:	DENOM: Pts w depression Active on their Current Problem List
				NUM: Met/Unmet is whether they have an active non-specific DX (ICD10 F32.8 or F32.9)
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
 11/6/2018			Mitch				Adding PCP and MH Team Members
 12/21/2018			Jaya				Adding Date parameters to Psych and MH active Pts.
 4/8/2019			Mitch				Adding MO
 04/19/2019			Jaya				Updating Active Patient logic to include Dx codes and LOS_PRIME_PROC_ID
 8/23/2019			Jaya				Adding UNION Operator to make code more efficent
 02/25/2020			Jaya				Updated to new department name
**********************************************************************************************

 */


SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

-----First pass to get denominator of pts w/ Dx of depression and no Dx of Bi-polar, schitzo, etc ---------
SELECT TOP 1000000000 pev.PAT_ID,
                      pev.DEPARTMENT_ID,
                      pev.PAT_ENC_CSN_ID LAST_VISIT_ID,
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
SELECT TOP 1000000 a1.PAT_ID,
                   a1.STATE,
                   a1.LOS,
                   a1.CITY,
                   ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID, a1.LOS ORDER BY a1.LAST_VISIT_ID DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1;
SELECT TOP 1000000 p.PAT_ID,
                   id.IDENTITY_ID,
                   a2.STATE,
                   a2.CITY,
                   a2.LOS
INTO #Attribution3
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE()) ----------------Active Medical
      AND p.CUR_PCP_PROV_ID LIKE '64%'
      AND pev.APPT_PRC_ID NOT IN ( '345', '346', '428', '505', '506' ) --Excluding PrEP to catch mis-coded pts
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                     8053, 8054, 8055, 8056 )
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND a2.ROW_NUM_DESC = 1
      AND a2.LOS = 'MEDICAL'
      AND icd10.code IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx

--------------
UNION
SELECT TOP 1000000000 p.PAT_ID,
                      id.IDENTITY_ID,
                      a2.STATE,
                      a2.CITY,
                      a2.LOS
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
WHERE ser.PROVIDER_TYPE_C IN ( '136', '164', '129' ) --------------Active Psych
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ev.SUM_BLK_TYPE_ID = 221
      AND ev.STATUS_C = 1
      AND id.IDENTITY_TYPE_ID = 64
      AND pev.CONTACT_DATE >= DATEADD(d, -180, GETDATE())
      AND a2.LOS = 'BEHAVIORAL'
      AND a2.ROW_NUM_DESC = 1

-------------------------
UNION
SELECT TOP 1000000000 p.PAT_ID,
                      id.IDENTITY_ID,
                      a2.STATE,
                      a2.CITY,
                      a2.LOS
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
WHERE ser.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' ) -------------Active MH
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ev.SUM_BLK_TYPE_ID = 221
      AND ev.STATUS_C = 1
      AND pev.CONTACT_DATE >= DATEADD(d, -90, GETDATE())
      AND a2.LOS = 'BEHAVIORAL'
      AND a2.ROW_NUM_DESC = 1;

SELECT TOP 1000000000 a3.IDENTITY_ID,
                      a3.PAT_ID,
                      a3.STATE,
                      a3.CITY,
                      a3.LOS,
                      p.PAT_NAME,
                      ser.PROV_NAME PCP,
                      CASE WHEN icd10.CODE IN ( 'F32.9', 'F32.8' ) THEN 0
                          ELSE 1
                      END AS MET_YN,
                      'CLICK HERE FOR PATIENT DETAIL' 'CLICK HERE FOR PATIENT DETAIL'
INTO #a4
FROM #Attribution3 a3
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON a3.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = a3.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
WHERE icd10.CODE IN ( SELECT TOP 100000000 vic.ICD_CODES_LIST
                      FROM Clarity.dbo.VCG_ICD_CODES vic
                      WHERE vic.GROUPER_ID = '2100000117'
                            AND vic.CODE_SET_C = 2 --ICD10
)
      AND plv.PROBLEM_STATUS_C = 1
      AND plv.RESOLVED_DATE IS NULL;


SELECT DISTINCT TOP 100000000 id.IDENTITY_ID,
                              a4.PAT_ID,
                              a4.PAT_NAME,
                              a4.PCP,
                              MAX(a4.MET_YN) MET_YN,
                              a4.STATE,
                              a4.CITY,
                              a4.[CLICK HERE FOR PATIENT DETAIL],
                              MAX(CASE WHEN ser2.PROVIDER_TYPE_C IN ( '136', '164', '129' ) THEN ser2.PROV_NAME END) AS 'PSYCHIATRY',
                              MAX(CASE WHEN ser2.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' ) THEN ser2.PROV_NAME
                                  END) AS 'MH_TEAM_MEMBER'
FROM #a4 a4
    LEFT JOIN Clarity.dbo.PAT_PCP_VIEW ct ON a4.PAT_ID = ct.PAT_ID
                                             AND ct.RELATIONSHIP_C IN ( '1', '3' )
                                             AND ct.TERM_DATE IS NULL
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser2 ON ser2.PROV_ID = ct.PCP_PROV_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = a4.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = a4.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = a4.PAT_ID
GROUP BY id.IDENTITY_ID,
         a4.PAT_ID,
         a4.PAT_NAME,
         a4.PCP,
         a4.STATE,
         a4.CITY,
         a4.[CLICK HERE FOR PATIENT DETAIL];


DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #a4;