/* javelin.ochin.org */

SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME 'PATIENT',
       dep.DEPARTMENT_NAME,
       SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
       SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) 'CITY',
       CAST(pev.CONTACT_DATE AS DATE) 'Visit Date',
       ser.EXTERNAL_NAME 'Visit Provider',
       prc.PRC_NAME 'Visit Type',
       CAST(pev.APPT_MADE_DATE AS DATE) 'Date Appointment Created',
       DATEDIFF(DAY, GETDATE(), pev.CONTACT_DATE) 'Days Until Appt',
       DATEDIFF(DAY, pev.APPT_MADE_DATE, GETDATE()) 'Days Since Appt Created'
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_PRC prc ON pev.APPT_PRC_ID = prc.PRC_ID
WHERE pev.APPT_STATUS_C = 1
      AND pev.APPT_PRC_ID IN ( '3', '319' )
      AND pev.CONTACT_DATE > GETDATE() - 1;