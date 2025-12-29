/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Medical 10 - Currently Use Tobacco
 Create Date:	8/23/2018
 Created By:	scogginsm
 System:		javelin.ochin.org
 Requested By:	Internal Dashboard

 Purpose:		Medical quality measure #10 - tobacco use status

 Description:	DENOM: Active HIV+ medical pts
				NUM:	Is current smoker
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
 1/17/2019			Jaya				To include STL
 8/2/2019			Mitch				Update for living pt check in the PATIENT_4 table
 02/24/2020			Jaya				Updated to new Department name
 02/02/2021			Jaya				Added PA to the Provider_Type_C
 3/17/2021			Mitch				Updating to use Registry and for Alteryx
 8/30/2021			Mitch				Adding Clinical Pharmacy FYI flag for Tableau filters
 2/3/2022			Mitch				Adding Pre-DM CP Flag
**********************************************************************************************

 */

SET ANSI_WARNINGS OFF;
SET NOCOUNT ON;

SELECT TOP 10000000 pev.PAT_ID,
                    pev.DEPARTMENT_ID,
                    dep.DEPARTMENT_NAME LAST_VISIT_DEPT,
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

SELECT TOP 10000000 a1.PAT_ID,
                    a1.LAST_VISIT_ID,
                    a1.LOS,
                    a1.CITY,
                    a1.STATE,
                    ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_VISIT_ID DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT TOP 10000000 id.IDENTITY_ID,
                    id.PAT_ID,
                    p.PAT_NAME,
                    CASE WHEN dm.SMOKING_USER_YN = 'Y' THEN 1
                        ELSE 0
                    END AS MET_YN,
                    a2.CITY,
                    a2.STATE,
                    ser.EXTERNAL_NAME PCP,
                    CASE WHEN flag.PAT_FLAG_TYPE_C = '640014' THEN 'YES'
                        ELSE 'NO'
                    END AS 'IN CLINICAL PHARM COHORT',
                    'CLICK HERE FOR PATIENT DETAIL' 'CLICK HERE FOR PATIENT DETAIL'
INTO #a
FROM Clarity.dbo.PATIENT_VIEW p
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN #Attribution2 a2 ON p.PAT_ID = a2.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.DM_WLL_ALL_VIEW dm ON dm.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag ON p.PAT_ID = flag.PATIENT_ID
                                                         AND flag.PAT_FLAG_TYPE_C = '640014' --Smoking Cohort
                                                         AND flag.ACTIVE_C = 1
WHERE a2.ROW_NUM_DESC = 1
      AND p.PAT_ID IN ( SELECT DISTINCT pev.PAT_ID
                        FROM Clarity.dbo.PATIENT_VIEW p
                            INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
                            INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
                            INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                            INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
                            INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                            INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
                            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
                        WHERE ser.SERV_AREA_ID = 64
                              AND ser.PROVIDER_TYPE_C IN ( '1', '9', '6', '113' ) -- Physicians and NPs, PAs
                              AND pev.CONTACT_DATE > DATEADD(MM, -12, GETDATE()) --Visit in past year
                              AND pev.APPT_STATUS_C IN ( 2, 6 ) --Visit was completed
                              AND pev.LOS_PRIME_PROC_ID IN ( 7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 7973, 7974, 8047, 8048,
                                                             8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056 ) -- Office Visits
                              AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
                              AND icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
                              AND plv.RESOLVED_DATE IS NULL --Active Dx
                              AND plv.PROBLEM_STATUS_C = 1 --Active Dx
                              AND p4.PAT_LIVING_STAT_C = 1 );

SELECT TOP 10000000 a.IDENTITY_ID MRN,
                    a.PAT_ID,
                    a.PAT_NAME PATIENT,
                    a.MET_YN AS SMOKER,
                    CASE WHEN a.MET_YN = 0 THEN 'Met'
                        ELSE 'Not Met'
                    END AS MET_YN,
                    a.CITY,
                    a.STATE,
                    a.PCP,
                    a.[IN CLINICAL PHARM COHORT],
                    svis.[Next Any Appt],
                    svis.[Next Appt Prov],
                    spvis.[Next PCP Appt],
                    spvis.[Next PCP Appt Prov]
FROM #a a
    LEFT JOIN (SELECT TOP 1000000 pev.PAT_ID,
                                  CAST(pev.CONTACT_DATE AS DATE) 'Next Any Appt',
                                  ser.PROV_NAME 'Next Appt Prov',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled

    ) svis ON svis.PAT_ID = a.PAT_ID
              AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT TOP 1000000 pev.PAT_ID,
                                  CAST(pev.CONTACT_DATE AS DATE) 'Next PCP Appt',
                                  ser.PROV_NAME 'Next PCP Appt Prov',
                                  ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND ser.PROV_ID <> '640178' --pulmonologist
                     AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs

    ) spvis ON spvis.PAT_ID = a.PAT_ID
               AND spvis.ROW_NUM_ASC = 1 -- First scheduled


;

DROP TABLE #a;
DROP TABLE #Attribution2;
DROP TABLE #Attribution1;
