SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT iv.PAT_ID,
       id.IDENTITY_ID MRN,
       p.PAT_NAME 'Patient',
       serpcp.PROV_NAME 'PCP',
       CAST(iv.IMMUNE_DATE AS DATE) 'Vaccine Date',
       COALESCE(iv.IMM_HISTORIC_ADM_YN, 'N') 'Historical',
       COALESCE(emp.NAME, iv.PHYSICAL_SITE) 'Given By',
       a3.STATE,
       a3.CITY
FROM CLARITY.dbo.IMMUNE_VIEW iv
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON iv.PAT_ID = id.PAT_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
    LEFT JOIN CLARITY.dbo.CLARITY_EMP emp ON iv.GIVEN_BY_USER_ID = emp.USER_ID
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
                WHERE dep.ROW_NUM_DESC = 1) a3 ON a3.PAT_ID = iv.PAT_ID
WHERE iv.IMMUNZATN_ID = 781 --SMALLPOX MONKEYPOX VACCINE (NATIONAL STOCKPILE)
      AND iv.IMMNZTN_STATUS_C = 1 --Given
;
