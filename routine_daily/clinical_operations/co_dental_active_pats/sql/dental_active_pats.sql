/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME PATIENT,
       ZC_COUNTY.NAME AS COUNTY_OF_RESIDENCE,
       CAST(v.CONTACT_DATE AS DATE) 'LAST VISIT',
       v.LAST_PROVIDER,
       v.STATE,
       v.DEPARTMENT_NAME,
       v.VISIT_NAME AS LAST_VISIT_ENCOUNTER_NAME,
       DATEDIFF(mm, v.CONTACT_DATE, GETDATE()) 'MONTHS SINCE SEEN',
       n.NEXT_PROVIDER,
       n.VISIT_NAME AS NEXT_VISIT_ENCOUNTER_NAME,
       CAST(n.CONTACT_DATE AS DATE) 'NEXT VISIT'
FROM CLARITY.dbo.IDENTITY_ID_VIEW id
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    INNER JOIN CLARITY.dbo.EPISODE_VIEW ev ON ev.PAT_LINK_ID = p.PAT_ID
    INNER JOIN CLARITY.dbo.ZC_COUNTY AS ZC_COUNTY ON p.COUNTY_C = ZC_COUNTY.COUNTY_C
    LEFT JOIN (SELECT pev.PAT_ID,
                      ser.PROV_NAME LAST_PROVIDER,
                      pev.CONTACT_DATE,
                      SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
                      dep.DEPARTMENT_NAME,
                      clarity_prc.PRC_NAME AS VISIT_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                   LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   INNER JOIN CLARITY.dbo.CLARITY_PRC AS clarity_prc ON pev.APPT_PRC_ID = clarity_prc.PRC_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 ) -- Arrived, completed
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') v ON id.PAT_ID = v.PAT_ID
                                                                             AND v.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT pev.PAT_ID,
                      ser.PROV_NAME NEXT_PROVIDER,
                      pev.CONTACT_DATE,
                      SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE',
                      dep.DEPARTMENT_NAME,
                      clarity_prc.PRC_NAME AS VISIT_NAME,
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM CLARITY.dbo.PAT_ENC_VIEW pev
                   INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                   LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
                   INNER JOIN CLARITY.dbo.CLARITY_PRC AS clarity_prc ON pev.APPT_PRC_ID = clarity_prc.PRC_ID
               WHERE pev.APPT_STATUS_C = 1 -- Scheduled
                     AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT') n ON id.PAT_ID = n.PAT_ID
                                                                             AND n.ROW_NUM_ASC = 1
WHERE ev.STATUS_C = 1
      AND ev.SUM_BLK_TYPE_ID = 45;
