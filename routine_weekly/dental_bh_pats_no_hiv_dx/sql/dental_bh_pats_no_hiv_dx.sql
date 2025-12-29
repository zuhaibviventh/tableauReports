SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME PATIENT,
       ev.[PATIENT TYPE],
       CONVERT(NVARCHAR(30), svis.[Next Dental Appt], 101) AS 'Next Dental Appt',
       svis.[Next Dental Prov],
       CONVERT(NVARCHAR(30), spvis.[Next BH Appt], 101) AS 'Next BH Appt',
       spvis.[Next BH Appt Prov],
       attr.STATE,
       attr.CITY,
       LastBH.[Last BH Appt],
       LastBH.[Last BH Appt Prov],
       LastDen.[Last Dental Appt],
       LastDen.[Last Dental Prov]
FROM CLARITY.dbo.IDENTITY_ID_VIEW id
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    INNER JOIN (SELECT DISTINCT ev.PAT_LINK_ID PAT_ID,
                                CASE WHEN ev.SUM_BLK_TYPE_ID = 45 THEN 'DENTAL'
                                    WHEN ev.SUM_BLK_TYPE_ID = 221 THEN 'BH'
                                END AS 'PATIENT TYPE'
                FROM CLARITY.dbo.EPISODE_VIEW ev
                WHERE ev.SUM_BLK_TYPE_ID IN ( 45, 221 ) -- Dental and BH
                      AND ev.STATUS_C = 1) ev ON ev.PAT_ID = id.PAT_ID
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next Dental Appt',
                      ser.PROV_NAME 'Next Dental Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') svis ON svis.PAT_ID = id.PAT_ID
                                                                                AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next BH Appt',
                      ser.PROV_NAME 'Next BH Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'MH', 'BH', 'PY' )) spvis ON spvis.PAT_ID = id.PAT_ID
                                                                                                        AND spvis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT plv.PAT_ID
               FROM CLARITY.dbo.PROBLEM_LIST_VIEW plv
                   INNER JOIN CLARITY.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                   INNER JOIN CLARITY.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
               WHERE icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
                     AND plv.RESOLVED_DATE IS NULL --Active Dx
                     AND plv.PROBLEM_STATUS_C = 1 --Active Dx
    ) dx ON dx.PAT_ID = id.PAT_ID
    LEFT JOIN (SELECT pev.PAT_ID,
                      pev.CONTACT_DATE 'Last Visit',
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
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
                   LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE())
                     AND pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'MH', 'BH', 'PY', 'DT' )) attr ON attr.PAT_ID = id.PAT_ID
                                                                                                             AND attr.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Last Dental Appt',
                      ser.PROV_NAME 'Last Dental Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') LastDen ON LastDen.PAT_ID = id.PAT_ID
                                                                                   AND LastDen.ROW_NUM_DESC = 1 -- First scheduled
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Last BH Appt',
                      ser.PROV_NAME 'Last BH Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'MH', 'BH', 'PY' )) LastBH ON LastBH.PAT_ID = id.PAT_ID
                                                                                                         AND LastBH.ROW_NUM_DESC = 1 -- First scheduled

WHERE dx.PAT_ID IS NULL --To exclude pts who have an active HIV Dx on their problem list
      AND attr.STATE IS NOT NULL --Pts with no VH visits so episode set elsewhere
      AND id.PAT_ID NOT IN ( SELECT DISTINCT flag.PATIENT_ID
                             FROM CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW flag
                             WHERE flag.ACTIVE_C = 1
                                   AND flag.PAT_FLAG_TYPE_C = '640007' -- AODA HIV-
);