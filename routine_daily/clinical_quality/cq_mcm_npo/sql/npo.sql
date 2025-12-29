SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT id.IDENTITY_ID NEW_PT,
       1 AS RM_JOIN, --just to join in the date in the FROM Statement (put in SELECT)
       p.PAT_NAME,
       pev.CONTACT_DATE,
       pev.APPT_MADE_DATE,
       CLARITY_EMP.NAME AS APPT_CREATED_BY,
       ZC_APPT_STATUS.NAME AS APPT_STATUS,
       loc.LOC_NAME DEPARTMENT_NAME,
       ser.PROV_NAME VISIT_PROVIDER,
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
       SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) 'CITY',
       ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC,
       prc.PRC_NAME 'VISIT TYPE'
INTO #a
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
	INNER JOIN Clarity.dbo.CLARITY_LOC_VIEW loc ON dep.REV_LOC_ID = loc.LOC_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_PRC prc ON pev.APPT_PRC_ID = prc.PRC_ID
    INNER JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON pev.APPT_ENTRY_USER_ID = CLARITY_EMP.USER_ID
    INNER JOIN CLARITY.dbo.ZC_APPT_STATUS AS ZC_APPT_STATUS ON pev.APPT_STATUS_C = ZC_APPT_STATUS.APPT_STATUS_C
WHERE pev.APPT_STATUS_C IN ( 2, 6 )
      --AND YEAR(pev.CONTACT_DATE) = rm.REP_YEAR  --looks for the year of the last month (in WHERE)
      --AND MONTH(pev.CONTACT_DATE) = rm.REP_MONTH  --Looks for last month (in WHERE)
      AND pev.CONTACT_DATE > '12/31/2018' -- For Alteryx/Tableau
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND pev.APPT_PRC_ID IN ( 3, 319 )
      AND p.PAT_ID NOT IN ( SELECT DISTINCT flag.PATIENT_ID
                            FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                            WHERE flag.ACTIVE_C = 1
                                  AND (flag.PAT_FLAG_TYPE_C = '640005' --PrEP
                                       OR flag.PAT_FLAG_TYPE_C = '640008' --STI
                                       OR flag.PAT_FLAG_TYPE_C = '9800035' -- PEP
                                       OR flag.PAT_FLAG_TYPE_C = '640007' -- AODA HIV-
                                       OR flag.PAT_FLAG_TYPE_C = '640017') --False positive HIV test
);

SELECT DISTINCT iiv.IDENTITY_ID HAD_NPO,
                ce.NAME
INTO #b
FROM Clarity.dbo.SMARTTOOL_LOGGER_VIEW sl
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON sl.CSN = pev.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW iiv ON pev.PAT_ID = iiv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EMP_VIEW ce ON sl.USER_ID = ce.USER_ID
    INNER JOIN Clarity.dbo.CL_SPHR cs ON sl.SMARTPHRASE_ID = cs.SMARTPHRASE_ID
WHERE cs.SMARTPHRASE_ID IN ( 819742, 790929, 588647, 876608, 1068990 /*1302260 ---don't add this one yet as it was used by mistake*/ )
      AND sl.LOG_TIMESTAMP < '6/1/2022'
UNION
SELECT DISTINCT id.IDENTITY_ID HAD_NPO,
                emp.NAME
FROM Clarity.dbo.SMRTDTA_ELEM_DATA_VIEW sedv
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON sedv.CONTACT_SERIAL_NUM = pev.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EMP_VIEW emp ON sedv.CUR_VALUE_USER_ID = emp.USER_ID
    INNER JOIN Clarity.dbo.SMRTDTA_ELEM_VALUE_VIEW sev ON sedv.HLV_ID = sev.HLV_ID
WHERE sedv.ELEMENT_ID = 'SA64#012'
      AND pev.CONTACT_DATE > '5/31/2022' -- New SmartList went live 6/1/2022
      AND sev.SMRTDTA_ELEM_VALUE IN ( 'Successful Outreach and Completed NPO prior to Appt', 'Pt Attended Appointment and NPO Completed same day' );

SELECT a.NEW_PT 'NEW PT MRN',
       a.PAT_NAME 'PATIENT',
       a.CONTACT_DATE AS NEW_PT_VISIT_DATE,
       a.APPT_MADE_DATE,
       a.APPT_CREATED_BY,
       a.APPT_STATUS,
       CASE WHEN b.HAD_NPO IS NOT NULL THEN 'Y'
           ELSE 'N'
       END AS HAD_NPO,
       MIN(b.NAME) NPO_Provider, -- tHIS IS OK SINCE THE ONLY reason there would be two rows is if it's a yes
       a.DEPARTMENT_NAME,
       a.VISIT_PROVIDER,
       a.STATE,
       CASE
			WHEN a.CITY = 'GB' THEN 'Green Bay'
			WHEN a.CITY = 'SL' THEN 'St Louis'
			WHEN a.CITY = 'MK' THEN 'Milwaukee'
			WHEN a.CITY = 'KN' THEN 'Kenosha'
			WHEN a.CITY = 'MD' THEN 'Madison'
			WHEN a.CITY = 'AS' THEN 'Austin'
			WHEN a.CITY = 'KC' THEN 'Kansas City'
			WHEN a.CITY = 'DN' THEN 'Denver'
			WHEN a.CITY = 'CG' THEN 'Chicago'
			ELSE a.CITY
		END AS CITY,
       a.[VISIT TYPE] 'NEW PATIENT TYPE',
       COALESCE(npoact.[Outreach Attempt Count], 0) 'Outreach Attempt Count',
       COALESCE(npoact.[Outreach Attempt YN], 'N') 'Outreach Attempt YN',
       COALESCE(npoact.[Outreach Successful], 'N') 'Outreach Successful',
       npoact.LAST_OUTREACH_ATTEMPT,
       CASE WHEN npoact.[NPO Completed] IS NOT NULL THEN npoact.[NPO Completed]
           WHEN b.HAD_NPO IS NOT NULL THEN 'Unknown'
           ELSE 'No NPO'
       END AS 'NPO Type',
       CASE WHEN npoact.IDENTITY_ID IS NULL
                 AND b.HAD_NPO IS NULL THEN 'No Use'
           ELSE 'Smartphrase Used'
       END AS 'Any SmartPhrase/List Use'
FROM #a a
    LEFT JOIN #b b ON a.NEW_PT = b.HAD_NPO
    LEFT JOIN (SELECT id.IDENTITY_ID,
                      id.PAT_ID,
                      MAX(CAST(pev.CONTACT_DATE AS DATE)) AS LAST_OUTREACH_ATTEMPT,
                      SUM(CASE WHEN sev.SMRTDTA_ELEM_VALUE = 'Attempted Outreach and did not contact' THEN 1
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Successful Outreach and Completed NPO prior to Appt' THEN 1
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Successful Outreach but no NPO' THEN 1
                              ELSE 0
                          END) AS 'Outreach Attempt Count',
                      MAX(CASE WHEN sev.SMRTDTA_ELEM_VALUE = 'Attempted Outreach and did not contact' THEN 'Y'
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Successful Outreach and Completed NPO prior to Appt' THEN 'Y'
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Successful Outreach but no NPO' THEN 'Y'
                              ELSE 'N'
                          END) AS 'Outreach Attempt YN',
                      MAX(CASE WHEN sev.SMRTDTA_ELEM_VALUE = 'Pt Attended Appointment and NPO Completed same day' THEN NULL
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Attempted Outreach and did not contact' THEN 'N'
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Successful Outreach and Completed NPO prior to Appt' THEN 'Y'
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Successful Outreach but no NPO' THEN 'Y'
                              ELSE 'N'
                          END) AS 'Outreach Successful',
                      MIN(CASE WHEN sev.SMRTDTA_ELEM_VALUE = 'Pt Attended Appointment and NPO Completed same day' THEN 'NPO Same Day'
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Attempted Outreach and did not contact' THEN NULL
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Successful Outreach and Completed NPO prior to Appt' THEN 'NPO Before Appt'
                              WHEN sev.SMRTDTA_ELEM_VALUE = 'Successful Outreach but no NPO' THEN NULL
                              ELSE 'N'
                          END) AS 'NPO Completed'
               FROM Clarity.dbo.SMRTDTA_ELEM_DATA_VIEW sedv
                   INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON sedv.CONTACT_SERIAL_NUM = pev.PAT_ENC_CSN_ID
                   INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
                   INNER JOIN Clarity.dbo.SMRTDTA_ELEM_VALUE_VIEW sev ON sedv.HLV_ID = sev.HLV_ID
               WHERE sedv.ELEMENT_ID = 'SA64#012'
                     AND pev.CONTACT_DATE > '5/31/2022' -- New SmartList went live 6/1/2022
               GROUP BY id.IDENTITY_ID,
                        id.PAT_ID) npoact ON a.NEW_PT = npoact.IDENTITY_ID
WHERE a.ROW_NUM_DESC = 1
GROUP BY a.NEW_PT,
         a.PAT_NAME,
         a.CONTACT_DATE,
         a.APPT_MADE_DATE,
         a.APPT_CREATED_BY,
         b.HAD_NPO,
         a.DEPARTMENT_NAME,
         a.VISIT_PROVIDER,
         a.STATE,
         a.CITY,
         a.[VISIT TYPE],
         npoact.[Outreach Attempt Count],
         npoact.[Outreach Attempt YN],
         npoact.[Outreach Successful],
         npoact.[NPO Completed],
         npoact.IDENTITY_ID,
         a.APPT_STATUS,
         npoact.LAST_OUTREACH_ATTEMPT;

DROP TABLE #a;
DROP TABLE #b;
