/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Discharged SUD Patients with YN for Remission Dx
 Create Date:	4/5/2022
 Created By:	ViventHealth\MScoggins
 System:		ANL-MKE-SVR-100
 Requested By:	

 Purpose:		

 Description: Of the clients seen in AODA, 40% will have at least 1 remission diagnosis documented upon discharge/closing episode of care with AODA provider (WI)
				Demoninator Per Pam "Epic only recently gave us the ability to open SUD episodes, so that would be the best way.  We use SUD episode under BH/MH type."
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#episodes_info') IS NOT NULL DROP TABLE #episodes_info;
WITH
    all_episodes AS (
        SELECT EPISODE.PAT_LINK_ID AS PAT_ID,
               CAST(EPISODE.END_DATE AS DATE) AS END_DATE,
               CAST(EPISODE.START_DATE AS DATE) AS START_DATE,
               DATEDIFF(MONTH, EPISODE.START_DATE, EPISODE.END_DATE) AS MONTHS_IN_PROGRAM,
               ROW_NUMBER() OVER (PARTITION BY EPISODE.PAT_LINK_ID ORDER BY EPISODE.END_DATE DESC) AS ROW_NUM_DESC
        FROM Clarity.dbo.EPISODE_VIEW AS EPISODE
        WHERE EPISODE.SUM_BLK_TYPE_ID = 221
              AND EPISODE.NAME = 'SUD'
              AND EPISODE.END_DATE IS NOT NULL
    )
SELECT all_episodes.PAT_ID,
       all_episodes.END_DATE,
       all_episodes.START_DATE,
       all_episodes.MONTHS_IN_PROGRAM
INTO #episodes_info
FROM all_episodes
WHERE all_episodes.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#visit_dx') IS NOT NULL DROP TABLE #visit_dx;
SELECT PAT_ENC_DX.PAT_ID,
       MAX('Y') AS REMISSION_CODE_USED,
       MAX(SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2)) AS STATE,
       MAX(CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 5, 2)
               WHEN 'MK' THEN 'MILWAUKEE'
               WHEN 'KN' THEN 'KENOSHA'
               WHEN 'GB' THEN 'GREEN BAY'
               WHEN 'WS' THEN 'WAUSAU'
               WHEN 'AP' THEN 'APPLETON'
               WHEN 'EC' THEN 'EAU CLAIRE'
               WHEN 'LC' THEN 'LACROSSE'
               WHEN 'MD' THEN 'MADISON'
               WHEN 'BL' THEN 'BELOIT'
               WHEN 'BI' THEN 'BILLING'
               WHEN 'SL' THEN 'ST LOUIS'
               WHEN 'DN' THEN 'DENVER'
               WHEN 'AS' THEN 'AUSTIN'
               WHEN 'KC' THEN 'KANSAS CITY'
               WHEN 'CG' THEN 'CHICAGO'
               ELSE 'ERROR'
           END) AS CITY
INTO #visit_dx
FROM Clarity.dbo.PAT_ENC_DX_VIEW AS PAT_ENC_DX
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC ON PAT_ENC_DX.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG AS CLARITY_EDG ON PAT_ENC_DX.DX_ID = CLARITY_EDG.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON CLARITY_EDG.DX_ID = EDG_CURRENT_ICD10.DX_ID
    INNER JOIN Clarity.dbo.EPISODE_VIEW AS EPISODE ON PAT_ENC_DX.PAT_ID = EPISODE.PAT_LINK_ID
WHERE EDG_CURRENT_ICD10.CODE IN ( 'F10.11', 'F10.21', 'F11.11', 'F11.21', 'F12.11', 'F12.21', 'F13.11', 'F13.21', 'F14.11', 'F14.21', 'F15.11', 'F15.21',
                                  'F16.11', 'F16.21', 'F17.11', 'F17.21', 'F18.11', 'F18.21', 'F19.11', 'F19.11', 'F63.01' )
      AND PAT_ENC_DX.CONTACT_DATE <= EPISODE.END_DATE
GROUP BY PAT_ENC_DX.PAT_ID;


IF OBJECT_ID('tempdb..#department_info') IS NOT NULL DROP TABLE #department_info;
SELECT PAT_ENC.PAT_ID,
       MAX(SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2)) AS STATE,
       MAX(CASE SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 5, 2)
               WHEN 'MK' THEN 'MILWAUKEE'
               WHEN 'KN' THEN 'KENOSHA'
               WHEN 'GB' THEN 'GREEN BAY'
               WHEN 'WS' THEN 'WAUSAU'
               WHEN 'AP' THEN 'APPLETON'
               WHEN 'EC' THEN 'EAU CLAIRE'
               WHEN 'LC' THEN 'LACROSSE'
               WHEN 'MD' THEN 'MADISON'
               WHEN 'BL' THEN 'BELOIT'
               WHEN 'BI' THEN 'BILLING'
               WHEN 'SL' THEN 'ST LOUIS'
               WHEN 'DN' THEN 'DENVER'
               WHEN 'AS' THEN 'AUSTIN'
               WHEN 'KC' THEN 'KANSAS CITY'
               WHEN 'CG' THEN 'CHICAGO'
               ELSE 'ERROR'
           END) AS CITY
INTO #department_info
FROM Clarity.dbo.PAT_ENC_VIEW AS PAT_ENC
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
WHERE PAT_ENC.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE())
      AND SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 9, 2) = 'AD'
      AND PAT_ENC.APPT_STATUS_C IN ( 2, 6 )
GROUP BY PAT_ENC.PAT_ID;


SELECT IDENTITY_ID.IDENTITY_ID AS MRN,
       PATIENT.PAT_NAME AS [Patient],
       #episodes_info.END_DATE AS [SUD Discharge Date],
       #episodes_info.START_DATE AS [episode Start Date],
       #episodes_info.MONTHS_IN_PROGRAM AS [Months in Program],
       YEAR(#episodes_info.END_DATE) AS [Year of Discharge],
       COALESCE(#visit_dx.REMISSION_CODE_USED, 'N') AS [Remission Code Used],
       COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS [Race],
       COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS [Ethnicity],
       (DATEDIFF(MONTH, PATIENT.BIRTH_DATE, GETDATE()) / 12) AS [Age],
       COALESCE(ZC_GENDER_IDENTITY.NAME, ZC_SEX.NAME) AS [Gender Identity],
       COALESCE(#visit_dx.STATE, #department_info.STATE) AS [State],
       COALESCE(#visit_dx.CITY, #department_info.CITY) AS [City],
       PATIENT.ZIP,
	   flg.PAT_FLAG_TYPE_C
FROM Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
    INNER JOIN CLARITY.dbo.PATIENT_4 AS PATIENT_4 ON IDENTITY_ID.PAT_ID = PATIENT_4.PAT_ID
    LEFT JOIN ##patient_race_ethnicity ON PATIENT.PAT_ID = ##patient_race_ethnicity.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_GENDER_IDENTITY AS ZC_GENDER_IDENTITY ON PATIENT_4.GENDER_IDENTITY_C = ZC_GENDER_IDENTITY.GENDER_IDENTITY_C
    LEFT JOIN CLARITY.dbo.ZC_SEX AS ZC_SEX ON PATIENT.SEX_C = ZC_SEX.RCPT_MEM_SEX_C
    INNER JOIN #episodes_info ON IDENTITY_ID.PAT_ID = #episodes_info.PAT_ID
    LEFT JOIN #visit_dx ON IDENTITY_ID.PAT_ID = #visit_dx.PAT_ID
    LEFT JOIN #department_info ON IDENTITY_ID.PAT_ID = #department_info.PAT_ID
	LEFT JOIN ( SELECT DISTINCT flag.PATIENT_ID, PAT_FLAG_TYPE_C
                             FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                             WHERE flag.ACTIVE_C = 1 -- AND flag.PAT_FLAG_TYPE_C = '640007
							 ) flg ON flg.PATIENT_ID = PATIENT.PAT_ID
WHERE YEAR(#episodes_info.END_DATE) > 2021
      AND COALESCE(#visit_dx.STATE, #department_info.STATE) IS NOT NULL;

