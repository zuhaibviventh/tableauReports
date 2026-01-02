SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME PATIENT,
       ev.[PATIENT TYPE],
       CONVERT(NVARCHAR(30), svis.[Next Dental Appt], 101) AS 'Next Dental Appt',
       svis.[Next Dental Prov],
       CONVERT(NVARCHAR(30), spvis.[Next BH Appt], 101) AS 'Next BH Appt',
       spvis.[Next BH Appt Prov],
       attr.STATE,
       attr.CITY,
       attr.SERVICE_TYPE as [Service Type],
	   attr.SERVICE_LINE as [Service Line],
	   attr.SUB_SERVICE_LINE as [Sub Service Line],
       LastBH.[Last BH Appt],
       LastBH.[Last BH Appt Prov],
       LastDen.[Last Dental Appt],
       LastDen.[Last Dental Prov]
FROM Clarity.dbo.IDENTITY_ID_VIEW id
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    INNER JOIN (SELECT DISTINCT ev.PAT_LINK_ID PAT_ID,
                                CASE WHEN ev.SUM_BLK_TYPE_ID = 45 THEN 'DENTAL'
                                    WHEN ev.SUM_BLK_TYPE_ID = 221 THEN 'BH'
                                END AS 'PATIENT TYPE'
                FROM Clarity.dbo.EPISODE_VIEW ev
                WHERE ev.SUM_BLK_TYPE_ID IN ( 45, 221 ) -- Dental and BH
                      AND ev.STATUS_C = 1) ev ON ev.PAT_ID = id.PAT_ID
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next Dental Appt',
                      ser.PROV_NAME 'Next Dental Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') svis ON svis.PAT_ID = id.PAT_ID
                                                                                AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next BH Appt',
                      ser.PROV_NAME 'Next BH Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'MH', 'BH', 'PY' )) spvis ON spvis.PAT_ID = id.PAT_ID
                                                                                                        AND spvis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT plv.PAT_ID
               FROM Clarity.dbo.PROBLEM_LIST_VIEW plv
                   INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
                   INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
               WHERE icd10.CODE IN ( 'B20', 'Z21', 'B97.35' ) --HIV and Asymptomatic HIV
                     AND plv.RESOLVED_DATE IS NULL --Active Dx
                     AND plv.PROBLEM_STATUS_C = 1 --Active Dx
    ) dx ON dx.PAT_ID = id.PAT_ID
    LEFT JOIN (SELECT pev.PAT_ID,
                      pev.CONTACT_DATE 'Last Visit',
                      dep.STATE,
                      dep.CITY,
                      dep.SERVICE_TYPE,
	                  dep.SERVICE_LINE,
	                  dep.SUB_SERVICE_LINE,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
                   LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE())
                     AND pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'MH', 'BH', 'PY', 'DT' )) attr ON attr.PAT_ID = id.PAT_ID
                                                                                                             AND attr.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Last Dental Appt',
                      ser.PROV_NAME 'Last Dental Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') LastDen ON LastDen.PAT_ID = id.PAT_ID
                                                                                   AND LastDen.ROW_NUM_DESC = 1 -- First scheduled
    LEFT JOIN (SELECT pev.PAT_ID,
                      CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Last BH Appt',
                      ser.PROV_NAME 'Last BH Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'AD', 'MH', 'BH', 'PY' )) LastBH ON LastBH.PAT_ID = id.PAT_ID
                                                                                                         AND LastBH.ROW_NUM_DESC = 1 -- First scheduled

WHERE dx.PAT_ID IS NULL --To exclude pts who have an active HIV Dx on their problem list
      AND attr.STATE IS NOT NULL --Pts with no VH visits so episode set elsewhere
      AND id.PAT_ID NOT IN ( SELECT DISTINCT flag.PATIENT_ID
                             FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                             WHERE flag.ACTIVE_C = 1
                                   AND flag.PAT_FLAG_TYPE_C = '640007' -- AODA HIV-
);