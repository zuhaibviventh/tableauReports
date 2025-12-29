/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Monkeypox Dashboard - Treatment - Tableau
 Create Date: 8/5/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  

 Purpose:   

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

SELECT id.PAT_ID,
       id.IDENTITY_ID MRN,
       p.PAT_NAME 'Patient',
       CAST(med.[Script Date] AS DATE) AS 'Script Date',
       med.[Prescribed Medication],
       med.Prescriber,
       med.STATE,
       med.CITY,
       --,cp.LOG_DATE 'CP Consult Date'
       serpcp.PROV_NAME 'PCP'
FROM CLARITY.dbo.IDENTITY_ID_VIEW id
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
    INNER JOIN (SELECT omv.PAT_ID,
                       omv.ORDERING_DATE 'Script Date',
                       cm.NAME 'Prescribed Medication',
                       COALESCE(ser.PROV_NAME, emp.NAME) 'Prescriber',
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
                       ROW_NUMBER() OVER (PARTITION BY omv.PAT_ID ORDER BY omv.ORDERING_DATE DESC) AS ROW_NUM_DESC
                FROM CLARITY.dbo.ORDER_MED_VIEW omv
                    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON omv.LOGIN_DEP_ID = dep.DEPARTMENT_ID
                    INNER JOIN CLARITY.dbo.CLARITY_MEDICATION cm ON cm.MEDICATION_ID = omv.MEDICATION_ID
                    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON omv.AUTHRZING_PROV_ID = ser.PROV_ID
                    LEFT JOIN CLARITY.dbo.CLARITY_EMP emp ON omv.ORD_CREATR_USER_ID = emp.USER_ID
                WHERE omv.ORDERING_DATE > '6/1/2022'
                      AND (cm.NAME LIKE '%Tecovirimat%'
                           OR cm.NAME LIKE '%TPOXX%'
                           OR cm.NAME LIKE '%Cidofovir%'
                           OR cm.NAME LIKE '%Vistide%'
                           OR cm.NAME LIKE '%Tembexa%')) med ON med.PAT_ID = id.PAT_ID
                                                                AND med.ROW_NUM_DESC = 1;
--LEFT JOIN
--  (SELECT TOP 10000000
--    iiv.IDENTITY_ID MRN
--    ,pev.CONTACT_DATE
--    ,sl.LOG_DATE

--  FROM 
--    Clarity.dbo.SMARTTOOL_LOGGER_VIEW sl
--    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON sl.CSN = pev.PAT_ENC_CSN_ID
--    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW iiv ON pev.PAT_ID = iiv.PAT_ID
--                      AND iiv.IDENTITY_TYPE_ID = 64
--    INNER JOIN Clarity.dbo.CL_SPHR cs ON sl.SMARTPHRASE_ID = cs.SMARTPHRASE_ID
--                       AND cs.SMARTPHRASE_ID = '1076999' --Clinical Pharmacy Touch

--  WHERE
--    sl.LOG_DATE > '7/1/2022' --Later date since this smartphrase was only created in 2021 (just to speed up the query)
--  )cp ON cp.MRN = id.IDENTITY_ID
--      AND cp.LOG_DATE = med.[Script Date]
