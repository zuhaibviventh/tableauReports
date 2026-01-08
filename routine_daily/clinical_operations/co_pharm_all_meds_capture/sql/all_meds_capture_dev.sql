/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME PATIENT,
       (DATEDIFF(m, p.BIRTH_DATE, CURRENT_TIMESTAMP) / 12) AS PAT_AGE,
       omv.DESCRIPTION 'Rx Name',
       CAST(omv.ORDERING_DATE AS DATE) 'Order Date',
       rp.PHARMACY_NAME 'Pharmacy Name',
       dep.DEPARTMENT_NAME 'Department',
       ser.PROV_NAME 'Ordering Provider',
       CASE WHEN omv.PHARMACY_ID IN ( 86875, 58682, 77912, 98993 ) THEN 'Vivent Pharmacy'
           WHEN rp.PHARMACY_NAME LIKE '%VIVENT PHARMACY%' THEN 'Vivent Pharmacy'
           ELSE 'Other Pharmacy'
       END 'PHARMACY',
       dep.SITE,
       dep.State,
       dep.CITY,
       dep.SERVICE_TYPE,
       dep.SERVICE_LINE,
       dep.SUB_SERVICE_LINE,
       CASE WHEN prep.[PATIENT TYPE] = 'PrEP' THEN 'PrEP'
           ELSE 'HIV'
       END AS 'Patient Type',
       ztc.NAME PRIMARY_THERAPUTIC_CLASS,
       zpc.NAME PRIMARY_PHARMACY_CLASS,
       zps.NAME PRIMARY_PHARMACY_SUBCLASS,
       ser.PROV_TYPE 'PROVIDER TYPE'
FROM Clarity.dbo.ORDER_MED_VIEW omv
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON omv.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
    INNER JOIN Clarity.dbo.ZC_THERA_CLASS ztc ON ztc.THERA_CLASS_C = med.THERA_CLASS_C
    INNER JOIN Clarity.dbo.ZC_PHARM_CLASS zpc ON zpc.PHARM_CLASS_C = med.PHARM_CLASS_C
    INNER JOIN Clarity.dbo.ZC_PHARM_SUBCLASS zps ON zps.PHARM_SUBCLASS_C = med.PHARM_SUBCLASS_C
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON omv.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.RX_PHR rp ON omv.PHARMACY_ID = rp.PHARMACY_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON omv.AUTHRZING_PROV_ID = ser.PROV_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON omv.LOGIN_DEP_ID = dep.DEPARTMENT_ID
    LEFT JOIN (SELECT DISTINCT flag.PATIENT_ID,
                               'PrEP' AS 'PATIENT TYPE'
               FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
               WHERE flag.ACTIVE_C = 1
                     AND flag.PAT_FLAG_TYPE_C = '640005' --PrEP
    ) prep ON omv.PAT_ID = prep.PATIENT_ID
WHERE omv.ORDERING_DATE > '2019-01-01'
      AND omv.PHARMACY_ID IS NOT NULL
      AND omv.ORDER_CLASS_C <> 3 --No historical
      AND omv.ORDER_STATUS_C <> 4 -- No cancelled
      AND ser.SERV_AREA_ID = 64;