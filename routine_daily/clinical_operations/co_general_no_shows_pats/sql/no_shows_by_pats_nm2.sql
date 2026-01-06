/* ANL-MKE-SVR-100 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

WITH
    /* Scheduled Visits */
    svis AS (
        SELECT pev.PAT_ID,
               CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next Any Appt',
               ser.PROV_NAME 'Next Appt Prov',
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW pev
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
        WHERE pev.APPT_STATUS_C = 1 --Scheduled
    ),
    /* Scheduled Physician, PA, and NP Visits */
    spvis AS (
        SELECT pev.PAT_ID,
               CONVERT(NVARCHAR(30), pev.CONTACT_DATE, 101) AS 'Next PCP Appt',
               ser.PROV_NAME 'Next PCP Appt Prov',
               ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
        FROM CLARITY.dbo.PAT_ENC_VIEW pev
            INNER JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
        WHERE pev.APPT_STATUS_C = 1 --Scheduled
              AND ser.PROV_ID <> '640178' --pulmonologist
              AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs
    )
SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME PATIENT,
       dep.STATE,
       dep.CITY,
       dep.SERVICE_TYPE,
       dep.SERVICE_LINE AS 'LOS',
       dep.SUB_SERVICE_LINE,
       ser.EXTERNAL_NAME PCP,
       servis.PROV_NAME 'VISIT PROVIDER',
       COUNT(pev.CONTACT_DATE) 'NO SHOWS',
       CAST(svis.[Next Any Appt] AS DATE) AS 'Next Any Appt',
       svis.[Next Appt Prov],
       CAST(spvis.[Next PCP Appt] AS DATE) AS 'Next PCP Appt',
       spvis.[Next PCP Appt Prov]
FROM CLARITY.dbo.PAT_ENC_VIEW pev --VISIT level Information
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = pev.PAT_ID --Pt-level informaiton
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID --contains MRN
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID -- Gives department names
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW servis ON pev.VISIT_PROV_ID = servis.PROV_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID -- Show's pts current PCP
    LEFT JOIN svis ON svis.PAT_ID = id.PAT_ID
                      AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN spvis ON spvis.PAT_ID = id.PAT_ID
                       AND spvis.ROW_NUM_ASC = 1 -- First scheduled
WHERE pev.CONTACT_DATE > DATEADD(MM, -12, CURRENT_TIMESTAMP) --Looks for visits scheduled for the last 12 months.
      AND pev.APPT_STATUS_C IN ( 4, 5, 7, 12, 13 ) -- No Show
GROUP BY id.IDENTITY_ID,
         p.PAT_ID,
         p.PAT_NAME,
         dep.DEPT_ABBREVIATION,
         ser.EXTERNAL_NAME,
         servis.PROV_NAME,
         svis.[Next Any Appt],
         svis.[Next Appt Prov],
         spvis.[Next PCP Appt],
         spvis.[Next PCP Appt Prov];
