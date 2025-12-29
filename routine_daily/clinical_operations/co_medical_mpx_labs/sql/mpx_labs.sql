/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:   Monkeypox Dashboard - Tests - Tableau
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

SELECT opv.PAT_ID,
       id.IDENTITY_ID MRN,
       p.PAT_NAME 'Patient',
       serpcp.PROV_NAME 'PCP',
       MIN(UPPER(orv.ORD_VALUE)) 'Lab Result',
       CAST(opv.ORDERING_DATE AS DATE) AS ORDERING_DATE,
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
       END AS CITY
FROM CLARITY.dbo.ORDER_PROC_VIEW opv
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON pev.PAT_ENC_CSN_ID = opv.PAT_ENC_CSN_ID
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.ORDER_RESULTS_VIEW orv ON orv.ORDER_PROC_ID = opv.ORDER_PROC_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON opv.PAT_ID = id.PAT_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
WHERE orv.COMPONENT_ID IN ( SELECT TOP 1000000000 cc.COMPONENT_ID FROM CLARITY.dbo.CLARITY_COMPONENT cc WHERE cc.COMMON_NAME LIKE 'monkeypox%' )
GROUP BY opv.PAT_ID,
         id.IDENTITY_ID,
         opv.ORDERING_DATE,
         dep.DEPT_ABBREVIATION,
         p.PAT_NAME,
         serpcp.PROV_NAME;