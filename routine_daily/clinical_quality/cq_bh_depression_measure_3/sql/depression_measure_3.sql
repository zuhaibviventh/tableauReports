/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Depression 3 - Patients on antideprecent and or MH therapy
 Create Date:	8/23/2018
 Created By:	scogginsm
 System:		javelin.ochin.org
 Requested By:	Internal Dashboard

 Purpose:		Depression 3 - Patients with Depression who are on an Antidepressant or are being seen in Behavioral Health

 Description:	DENOM: Pts w depression Active on their Current Problem List,
				NUM: Met/Unmet if they are EITHER on a med or in MH Therapy
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
 12/21/2018			Jaya				Adding Date parameters to Psych and MH active Pts.
 4/8/2019			Mitch				Adding MO and updating attribution
 04/19/2019			Jaya				Updating Active Patient logic to include Dx codes and LOS_PRIME_PROC_ID
 6/19/2019			Mitch				Updating to exclude MO per Annual Work Plan
 8/20/2019			Jaya				Adding UNION Operator to make code more efficent
 10/31/2019			Mitch				Adding a check for an ICD10 code that indicates the pt is in MH care elsewhere (success cirterion)
 11/1/2019			Mitch				Adding a check against Current Meds that may not have been prescribed by us (success cirterion)
 11/1/2019			Mitch				Refactored the code for improved performance
 12/30/2019			Mitch				Updated meds section to look at the subclass table
 12/30/2019			Mitch				Added MO back in for 2020
 03/02/2020			Jaya				Updated name to new name
 8/4/2020			Mitch				Update to use INDICATION_OF_USE Instead of Pharmacy Subclass
 02/02/2021			Jaya				Added PA to the Provider_Type_C
**********************************************************************************************

 */


SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT TOP 1000000000 pev.PAT_ID,
                      pev.DEPARTMENT_ID,
                      dep.DEPARTMENT_NAME LAST_VISIT_DEPT,
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
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT TOP 1000000000 a1.PAT_ID,
                      a1.LAST_OFFICE_VISIT,
                      a1.LOS,
                      a1.CITY,
                      a1.STATE,
                      ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1;

SELECT TOP 1000000000 p.PAT_ID,
                      '' AS EPISODE,
                      a2.LAST_OFFICE_VISIT,
                      a2.LOS,
                      a2.CITY,
                      a2.STATE
INTO #Attribution3
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p4.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
WHERE ser.SERV_AREA_ID = 64
      AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
      AND pev.CONTACT_DATE > DATEADD(MM, -12, GETDATE()) --Visit in past year
      AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
      AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052,
                                     8053, 8054, 8055, 8056 ) -- Office Visits
      AND a2.LOS = 'MEDICAL' -- Visit was in a medical department
      AND icd10.CODE IN ( 'B20', 'Z21' ) --HIV and Asymptomatic HIV
      AND plv.RESOLVED_DATE IS NULL --Active Dx
      AND plv.PROBLEM_STATUS_C = 1 --Active Dx
      AND p4.PAT_LIVING_STAT_C = 1
      AND a2.ROW_NUM_DESC = 1
--------------
UNION
SELECT TOP 1000000000 p.PAT_ID,
                      CASE WHEN ev.STATUS_C = 1
                                AND ev.SUM_BLK_TYPE_ID = 221 THEN 1
                          ELSE 0
                      END AS EPISODE,
                      a2.LAST_OFFICE_VISIT,
                      a2.LOS,
                      a2.CITY,
                      a2.STATE
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE ser.PROVIDER_TYPE_C IN ( '136', '164', '129' ) --------------Active Psych
      AND a2.LOS = 'BEHAVIORAL'
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ev.SUM_BLK_TYPE_ID = 221
      AND ev.STATUS_C = 1
      AND pev.CONTACT_DATE >= DATEADD(d, -180, GETDATE())
      AND a2.ROW_NUM_DESC = 1
UNION
SELECT TOP 1000000000 p.PAT_ID,
                      CASE WHEN ev.STATUS_C = 1
                                AND ev.SUM_BLK_TYPE_ID = 221 THEN 1
                          ELSE 0
                      END AS EPISODE,
                      a2.LAST_OFFICE_VISIT,
                      a2.LOS,
                      a2.CITY,
                      a2.STATE
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN #Attribution2 a2 ON a2.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    LEFT JOIN Clarity.dbo.EPISODE_LINK_VIEW elv ON pev.PAT_ENC_CSN_ID = elv.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.EPISODE_VIEW ev ON elv.EPISODE_ID = ev.EPISODE_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
WHERE ser.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' ) -------------Active MH
      AND a2.LOS = 'BEHAVIORAL'
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ev.SUM_BLK_TYPE_ID = 221
      AND ev.STATUS_C = 1
      AND pev.CONTACT_DATE >= DATEADD(d, -90, GETDATE())
      AND a2.ROW_NUM_DESC = 1;

SELECT TOP 1000000000 att.PAT_ID,
                      att.LAST_OFFICE_VISIT,
                      att.LOS,
                      att.CITY,
                      att.STATE,
                      att.EPISODE
INTO #pop
FROM #Attribution3 att
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON att.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
                                                      AND icd10.CODE IN ( SELECT TOP 10000000 vic.ICD_CODES_LIST
                                                                          FROM Clarity.dbo.VCG_ICD_CODES vic
                                                                          WHERE vic.GROUPER_ID = '2100000117'
                                                                                AND vic.CODE_SET_C = 2 --ICD10
                                                   )
WHERE plv.PROBLEM_STATUS_C = 1
      AND plv.RESOLVED_DATE IS NULL;

SELECT TOP 1000000000 id.IDENTITY_ID,
                      pop.PAT_ID,
                      pop.STATE,
                      pop.CITY,
                      p.PAT_NAME,
                      ser.PROV_NAME PCP,
                      MAX(CASE WHEN pop.EPISODE = 1 THEN 1
                              WHEN ios.INDICATIONS_USE_ID IN ( 827, 848, 849, 883, 884, 3858, 3877 ) THEN 1
                              WHEN icd.CODE = 'Z78.9' THEN 1 --Only HIV+ pts so will not conflict w/ False Positive
                              ELSE 0
                          END) AS MET_YN,
                      'CLICK HERE FOR PATIENT DETAIL' 'CLICK HERE FOR PATIENT DETAIL'
INTO #a
FROM #pop pop
    LEFT JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv2 ON plv2.PAT_ID = pop.PAT_ID --LJ and additional copy of PLV to not conflict with IJ below
                                                    AND plv2.PROBLEM_STATUS_C = 1
                                                    AND plv2.RESOLVED_DATE IS NULL
    LEFT JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd ON icd.DX_ID = plv2.DX_ID
                                                   AND icd.CODE = 'Z78.9'
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = pop.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    LEFT JOIN Clarity.dbo.ORDER_MED_VIEW med ON p.PAT_ID = med.PAT_ID
                                                AND (med.END_DATE IS NULL OR med.END_DATE > GETDATE())
    LEFT JOIN CLARITY.dbo.INDICATIONS_OF_USE ios ON med.MEDICATION_ID = ios.MEDICATION_ID
                                                    AND ios.INDICATIONS_USE_ID IN ( 827, 848, 849, 883, 884, 3858, 3877 ) --meds indicated for use in depression

GROUP BY id.IDENTITY_ID,
         p.PAT_NAME,
         ser.PROV_NAME,
         pop.PAT_ID,
         pop.STATE,
         pop.CITY;

SELECT TOP 1000000000 --to add in care teams
       a.IDENTITY_ID,
       a.PAT_ID,
       a.PAT_NAME,
       a.PCP,
       MAX(a.MET_YN) MET_YN,
       a.STATE,
       a.CITY,
       a.[CLICK HERE FOR PATIENT DETAIL],
       MAX(CASE WHEN ser.PROVIDER_TYPE_C IN ( '136', '164', '129' ) THEN ser.PROV_NAME END) AS 'PSYCHIATRY',
       MAX(CASE WHEN ser.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' ) THEN ser.PROV_NAME
           END) AS 'MH_TEAM_MEMBER'
INTO #b
FROM #a a
    LEFT JOIN Clarity.dbo.PAT_PCP_VIEW ct ON a.PAT_ID = ct.PAT_ID
                                             AND ct.RELATIONSHIP_C IN ( '1', '3' )
                                             AND ct.TERM_DATE IS NULL
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ct.PCP_PROV_ID = ser.PROV_ID
GROUP BY a.IDENTITY_ID,
         a.PAT_NAME,
         a.PAT_ID,
         a.PCP,
         a.STATE,
         a.CITY,
         a.[CLICK HERE FOR PATIENT DETAIL];

SELECT TOP 1000000000 --to check current meds from outside orders based on meds
       b.IDENTITY_ID,
       b.PAT_ID,
       b.PAT_NAME,
       b.PCP,
       b.STATE,
       b.city,
       b.[CLICK HERE FOR PATIENT DETAIL],
       b.PSYCHIATRY,
       b.MH_TEAM_MEMBER,
       MAX(CASE WHEN b.MET_YN = 1 THEN 1
               WHEN ios.INDICATIONS_USE_ID IS NOT NULL THEN 1
               ELSE 0
           END) AS MET_YN
FROM #b b
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = b.PAT_ID
    LEFT JOIN Clarity.dbo.PAT_ENC_CURR_MEDS_VIEW cmv ON p.MEDS_LAST_REV_CSN = cmv.PAT_ENC_CSN_ID
                                                        AND cmv.IS_ACTIVE_YN = 'Y'
    LEFT JOIN CLARITY.dbo.INDICATIONS_OF_USE ios ON cmv.MEDICATION_ID = ios.MEDICATION_ID
                                                    AND ios.INDICATIONS_USE_ID IN ( 827, 848, 849, 883, 884, 3858, 3877 )
GROUP BY b.IDENTITY_ID,
         b.PAT_ID,
         b.PAT_NAME,
         b.PCP,
         b.MET_YN,
         b.STATE,
         b.CITY,
         b.[CLICK HERE FOR PATIENT DETAIL],
         b.PSYCHIATRY,
         b.MH_TEAM_MEMBER;


DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #pop;

