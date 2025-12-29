/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT 
    id.IDENTITY_ID MRN
    ,p.PAT_NAME PATIENT
    ,ser.EXTERNAL_NAME PROVIDER
    ,SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE'
    ,CASE
        WHEN dep.DEPARTMENT_NAME = 'XXXVH AUS DENTAL' THEN 'VH AUS DENTAL'
        ELSE dep.DEPARTMENT_NAME
    END AS DEPARTMENT_NAME
    ,pev.CONTACT_DATE VISIT_DATE
    ,MIN(CASE --MIN to prefer Met over Unmet if code was one of the ones used)
        WHEN icd.CODE IS NOT NULL THEN 'MET'
        ELSE 'UNMET'
    END) AS PERIO_CODE_USED

FROM 
    Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.PAT_ENC_DX_VIEW dx ON dx.PAT_ENC_CSN_ID = pev.PAT_ENC_CSN_ID
    LEFT JOIN Clarity.dbo.CLARITY_EDG edg ON edg.DX_ID = dx.DX_ID
    LEFT JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd ON icd.DX_ID = edg.DX_ID
                    AND icd.CODE IN ('K05.221','K05.222', 'K05.223' /*Aggressive*/ , 'K05.321', 'K05.322', 'K05.323' /*Chronic*/, 'K05.00', 'K05.01', 'K05.10', 'K05.11' /*Gingivitis*/
                    , 'K05.311', 'K05.3122', 'K05.313' /*Localized, added 6/17/2022 per Dr. Abuzaineh*/  )

WHERE
    pev.APPT_STATUS_C IN (2, 6)
    AND ser.PROVIDER_TYPE_C = '119'
    AND pev.CONTACT_DATE > '12/31/2020' --So we don't get visits from before we started using these codes
    --AND pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())

GROUP BY 
    id.IDENTITY_ID 
    ,p.PAT_NAME 
    ,ser.EXTERNAL_NAME 
    ,dep.DEPT_ABBREVIATION
    ,dep.DEPARTMENT_NAME
    ,pev.CONTACT_DATE 

;
