SELECT id.IDENTITY_ID MRN,
       pev.PAT_ENC_CSN_ID 'VISIT ID',
       p.PAT_NAME,
       pev.CONTACT_DATE AS CONTACT_DATE,
       ser.EXTERNAL_NAME PROV_NAME,
       pev.ENC_TYPE_C,
       enc.NAME ENC_TYPE,
       dep.DEPARTMENT_NAME,
       CONVERT(NVARCHAR(30), GETDATE(), 101) AS TODAY,
       serpcp.EXTERNAL_NAME CURRENT_PCP
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.ZC_DISP_ENC_TYPE enc ON pev.ENC_TYPE_C = enc.DISP_ENC_TYPE_C
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
WHERE ser.PROVIDER_TYPE_C = 102
      AND pev.CONTACT_DATE > '1/1/2018'
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND pev.ENC_TYPE_C NOT IN ( 9000, 9001, 109, 2010 )
      AND (pev.APPT_STATUS_C IS NULL OR pev.APPT_STATUS_C IN ( 2, 6 ))
UNION
SELECT iiv.IDENTITY_ID MRN,
       pev.PAT_ENC_CSN_ID 'VISIT ID',
       p.PAT_NAME,
       pev.CONTACT_DATE,
       ser.EXTERNAL_NAME PROV_NAME,
       'SL' AS ENC_TYPE_C,
       'SmartPhrase' AS ENC_TYPE,
       dep.DEPARTMENT_NAME,
       CONVERT(NVARCHAR(30), GETDATE(), 101) AS TODAY,
       serpcp.EXTERNAL_NAME CURRENT_PCP
FROM Clarity.dbo.SMARTTOOL_LOGGER_VIEW sl
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON sl.CSN = pev.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW iiv ON pev.PAT_ID = iiv.PAT_ID
                                                   AND iiv.IDENTITY_TYPE_ID = 64
    INNER JOIN Clarity.dbo.CLARITY_EMP_VIEW ce ON sl.USER_ID = ce.USER_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ce.PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CL_SPHR cs ON sl.SMARTPHRASE_ID = cs.SMARTPHRASE_ID
                                         AND cs.SMARTPHRASE_ID = '1076999' --Clinical Pharmacy Touch
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
WHERE sl.LOG_DATE > '3/1/2021' --Later date since this smartphrase was only created in 2021 (just to speed up the query)

;