/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:   Monkeypox Dashboard - Diagnoses - Tableau
 Create Date:   8/5/2022
 Created By:    ViventHealth\MScoggins
 System:        ANL-MKE-SVR-100
 Requested By:  

 Purpose:       

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:       Changed By:         Change Description:
 ------------       -------------       ---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT edx.PAT_ID,
       id.IDENTITY_ID MRN,
       p.PAT_NAME 'Patient',
       serpcp.PROV_NAME 'PCP',
       MIN(CAST(edx.CONTACT_DATE AS DATE)) 'MPX DX Date',
       COUNT(DISTINCT edx.CONTACT_DATE) 'MPX Visit Count',
       a3.STATE,
       a3.CITY
FROM CLARITY.dbo.PAT_ENC_DX_VIEW edx
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON edx.PAT_ID = id.PAT_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
    INNER JOIN (SELECT dep.PAT_ID,
                       dep.STATE,
                       dep.CITY,
                       dep.SITE
                FROM (SELECT pev.PAT_ID,
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
                                 ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 7, 2)
                             END AS 'SITE',
                             ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
                      FROM CLARITY.dbo.PAT_ENC_VIEW pev
                          INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                      WHERE pev.CONTACT_DATE > DATEADD(MONTH, -24, GETDATE())
                            AND pev.APPT_STATUS_C IN ( 2, 6 )) dep
                WHERE dep.ROW_NUM_DESC = 1) a3 ON a3.PAT_ID = edx.PAT_ID
WHERE edx.DX_ID IN ( 483922, 66270 ) --Monkeypox
      OR edx.DX_ID IN ( 66269, 484979, 242126, 969769, 237542, 66271, 237541 ) --Orthopoxvirus infection
GROUP BY edx.PAT_ID,
         id.IDENTITY_ID,
         p.PAT_NAME,
         serpcp.PROV_NAME,
         a3.STATE,
         a3.CITY;
