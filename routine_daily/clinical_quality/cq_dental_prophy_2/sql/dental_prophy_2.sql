SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#excl') IS NOT NULL 
DROP TABLE #excl
;

SELECT --get pts with the endentulouos dx code
	plv.PAT_ID
	,MAX(5) 'K Code'
	,NULL 'D5110'
	,NULL 'D5120'

INTO #excl

FROM 
	Clarity.dbo.PROBLEM_LIST_VIEW AS plv
    INNER JOIN Clarity.dbo.CLARITY_EDG AS CLARITY_EDG ON plv.DX_ID = CLARITY_EDG.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON CLARITY_EDG.DX_ID = EDG_CURRENT_ICD10.DX_ID

WHERE 
	EDG_CURRENT_ICD10.CODE = 'K08.109' --"Complete loss of teeth, unspecified cause, unspecified class"
    AND plv.RESOLVED_DATE IS NULL --Active Dx
    AND plv.PROBLEM_STATUS_C = 1 --Active Dx

GROUP BY 
	plv.PAT_ID

UNION

SELECT 
	tdl.PAT_ID
	,NULL 'K Code'
	,MAX(1) 'D5110'
	,NULL 'D5120'

FROM 
	Clarity.dbo.CLARITY_TDL_TRAN_64_VIEW tdl 

WHERE
	tdl.CPT_CODE = 'D5110'
	AND tdl.DETAIL_TYPE NOT IN ( 50, 51 )

GROUP BY 
	tdl.PAT_ID

UNION

SELECT 
	tdl.PAT_ID
	,NULL 'K COde'
	,NULL 'D5110'
	,MAX(1) 'D5120'

FROM 
	Clarity.dbo.CLARITY_TDL_TRAN_64_VIEW tdl 

WHERE
	tdl.CPT_CODE = 'D5120'
	AND tdl.DETAIL_TYPE NOT IN ( 50, 51 )

GROUP BY 
	tdl.PAT_ID

;

IF OBJECT_ID('tempdb..#exclusion') IS NOT NULL 
DROP TABLE #exclusion
;

SELECT 
	e.PAT_ID
	,COALESCE(MAX(e.[K Code]), 0) 'K Code'
	,COALESCE(MAX(e.D5110), 0) 'D5110'
	,COALESCE(MAX(e.D5120), 0) 'D5120'
	,COALESCE(MAX(e.[K Code]), 0) + COALESCE(MAX(e.D5110), 0) + COALESCE(MAX(e.D5120), 0) 'Total Exclusion Score'

INTO #exclusion

FROM 
	#excl e

GROUP BY 
	e.PAT_ID

HAVING 
	COALESCE(MAX(e.[K Code]), 0) + COALESCE(MAX(e.D5110), 0) + COALESCE(MAX(e.D5120), 0) > 1

;

SELECT  pev.PAT_ID,
                     pev.DEPARTMENT_ID,
                     pev.PAT_ENC_CSN_ID LAST_VISIT_ID,
                     pev.CONTACT_DATE,
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
WHERE pev.CONTACT_DATE > DATEADD(MM, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND pev.PAT_ID IN ( SELECT DISTINCT  pev.PAT_ID
                          FROM Clarity.dbo.PAT_ENC_VIEW pev
                              INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                              INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
                          WHERE pev.CONTACT_DATE
                                BETWEEN DATEADD(MONTH, -12, GETDATE()) AND DATEADD(MONTH, -6, GETDATE()) --Smaller 'n' since we're looking only for pts who had a visit in the first 6 months
                                AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
                                AND ser.PROVIDER_TYPE_C = 108
                                AND pev.APPT_STATUS_C IN ( 2, 6 ));

SELECT  a1.PAT_ID,
                     a1.LAST_VISIT_ID,
                     a1.LOS,
                     a1.CITY,
                     a1.STATE,
                     ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.CONTACT_DATE DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
    INNER JOIN Clarity.dbo.EPISODE_VIEW ev ON ev.PAT_LINK_ID = a1.PAT_ID
WHERE a1.LOS = 'DENTAL'
      AND ev.SUM_BLK_TYPE_ID = 45
      AND ev.STATUS_C = 1;

SELECT  pev.PAT_ID,
                     CASE WHEN ser.PROVIDER_TYPE_C = '119' THEN 1
                         ELSE 0
                     END AS Prophy_Visit
INTO #a
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE pev.CONTACT_DATE
      BETWEEN DATEADD(MONTH, -13, GETDATE()) AND GETDATE()
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT'
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT  a.PAT_ID, SUM(a.Prophy_Visit) Prophy_Visits INTO #b FROM #a a GROUP BY a.PAT_ID;


SELECT  b.PAT_ID,
                     id.IDENTITY_ID MRN,
                     p.PAT_NAME PATIENT,
                     CASE WHEN b.Prophy_Visits > 1 THEN 1
                         ELSE 0
                     END AS Two_Plus_Prophies,
                     CASE WHEN b.Prophy_Visits > 1 THEN 'MET'
                         ELSE 'NOT MET'
                     END AS MET_YN,
                     att.STATE,
                     att.CITY,
                     p.HOME_PHONE,
                     p.WORK_PHONE,
                     MAX(oc.OTHER_COMMUNIC_NUM) CELL_PHONE
FROM #Attribution2 att --Since we're doing Denom work in the ATT
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON att.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    LEFT JOIN Clarity.dbo.OTHER_COMMUNCTN oc ON p.PAT_ID = oc.PAT_ID
                                                AND oc.OTHER_COMMUNIC_C = 1
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    LEFT JOIN #b b ON b.PAT_ID = att.PAT_ID
WHERE p4.PAT_LIVING_STAT_C = 1
      AND att.ROW_NUM_DESC = 1
      AND p.PAT_ID NOT IN ( SELECT DISTINCT -----These two subqueries are to identify edentulus pts for exclusion.------
                                   e.PAT_ID
                            FROM #Exclusion e )
GROUP BY b.PAT_ID,
         id.IDENTITY_ID,
         p.PAT_NAME,
         b.Prophy_Visits,
         att.CITY,
         att.STATE,
         p.HOME_PHONE,
         p.WORK_PHONE;

DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Exclusion;