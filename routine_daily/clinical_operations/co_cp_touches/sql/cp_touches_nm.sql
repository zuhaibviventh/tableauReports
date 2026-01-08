/**
 * javelin.ochin.org
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#visits_and_prov') IS NOT NULL DROP TABLE #visits_and_prov;
SELECT id.IDENTITY_ID MRN,
       pev.PAT_ENC_CSN_ID 'VISIT ID',
       p.PAT_NAME,
       CAST(pev.CONTACT_DATE AS DATE) AS CONTACT_DATE,
       ser.PROV_NAME,
       pev.ENC_TYPE_C,
       enc.NAME ENC_TYPE,
       dep.DEPARTMENT_NAME,
       CAST(GETDATE() AS DATE) AS TODAY,
       serpcp.EXTERNAL_NAME CURRENT_PCP,
       pev.APPT_PRC_ID
INTO #visits_and_prov
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.ZC_DISP_ENC_TYPE enc ON pev.ENC_TYPE_C = enc.DISP_ENC_TYPE_C
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
WHERE ser.PROVIDER_TYPE_C IN ('102', '104') --Updated 11/5/2024 per dan scales adding nutritionists.
      AND pev.CONTACT_DATE > '1/1/2018'
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND pev.ENC_TYPE_C NOT IN ( 9000, 9001, 109, 2010 )
      AND (pev.APPT_STATUS_C IS NULL OR pev.APPT_STATUS_C IN ( 2, 6 ))

UNION

SELECT iiv.IDENTITY_ID MRN,
       pev.PAT_ENC_CSN_ID 'VISIT ID',
       p.PAT_NAME,
       CAST(pev.CONTACT_DATE AS DATE) AS CONTACT_DATE,
       ce.NAME PROV_NAME,
       'SL' AS ENC_TYPE_C,
       CASE
			WHEN cs.SMARTPHRASE_ID = '1076999'THEN 'SmartPhrase' 
			ELSE 'Med Rec SmartPhrase' 
		END AS ENC_TYPE,
       dep.DEPARTMENT_NAME,
       CAST(GETDATE() AS DATE) AS TODAY,
       serpcp.EXTERNAL_NAME CURRENT_PCP,
       pev.APPT_PRC_ID
FROM Clarity.dbo.SMARTTOOL_LOGGER_VIEW sl
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON sl.CSN = pev.PAT_ENC_CSN_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW iiv ON pev.PAT_ID = iiv.PAT_ID
                                                   AND iiv.IDENTITY_TYPE_ID = 64
    INNER JOIN Clarity.dbo.CLARITY_EMP_VIEW ce ON sl.USER_ID = ce.USER_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ce.PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CL_SPHR cs ON sl.SMARTPHRASE_ID = cs.SMARTPHRASE_ID
                                         AND cs.SMARTPHRASE_ID IN ('1076999', '568339') --Clinical Pharmacy Touch
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
WHERE sl.LOG_DATE > '3/1/2021' --Later date since this smartphrase was only created in 2021 (just to speed up the query)

;


IF OBJECT_ID('tempdb..#cleanup') IS NOT NULL DROP TABLE #cleanup;
SELECT #visits_and_prov.MRN,
       #visits_and_prov.[VISIT ID],
       #visits_and_prov.PAT_NAME,
       #visits_and_prov.CONTACT_DATE,
       #visits_and_prov.PROV_NAME,
       #visits_and_prov.ENC_TYPE_C,
       #visits_and_prov.ENC_TYPE,
       #visits_and_prov.DEPARTMENT_NAME,
       #visits_and_prov.TODAY,
       #visits_and_prov.CURRENT_PCP,
       COALESCE(CLARITY_PRC.PRC_NAME, '*NO PROCEDURE USED') AS PRC_NAME,
       #visits_and_prov.APPT_PRC_ID,
       ROW_NUMBER() OVER (PARTITION BY [VISIT ID] ORDER BY ENC_TYPE_C DESC) AS ROW_NUM_DESC
INTO #cleanup
FROM #visits_and_prov
    LEFT JOIN Clarity.dbo.CLARITY_PRC AS CLARITY_PRC ON #visits_and_prov.APPT_PRC_ID = CLARITY_PRC.PRC_ID;


SELECT #cleanup.MRN,
       #cleanup.[VISIT ID],
       #cleanup.PAT_NAME,
       #cleanup.CONTACT_DATE,
       #cleanup.PROV_NAME,
       #cleanup.ENC_TYPE_C,
       #cleanup.ENC_TYPE,
       #cleanup.DEPARTMENT_NAME,
       #cleanup.TODAY,
       #cleanup.CURRENT_PCP,
       #cleanup.PRC_NAME
FROM #cleanup
WHERE ROW_NUM_DESC = 1;
