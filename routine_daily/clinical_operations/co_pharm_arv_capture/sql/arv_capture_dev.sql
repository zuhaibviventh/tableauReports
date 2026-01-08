/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#prep') IS NOT NULL DROP TABLE #prep;
SELECT DISTINCT flag.PATIENT_ID,
                'PrEP' AS 'PATIENT TYPE'
INTO #prep
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
WHERE flag.ACTIVE_C = 1
      AND flag.PAT_FLAG_TYPE_C = '640005'; --PrEP


SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME PATIENT,
       omv.DESCRIPTION 'Rx Name',
       omv.ORDERING_DATE 'Order Date',
       rp.PHARMACY_NAME 'Pharmacy Name',
       dep.DEPARTMENT_NAME 'Department',
       ser.PROV_NAME 'Ordering Provider',
       CASE WHEN omv.PHARMACY_ID IN ( 86875, 58682, 77912, 98993 ) THEN 'Vivent Pharmacy'
           WHEN rp.PHARMACY_NAME LIKE '%VIVENT PHARMACY%' THEN 'Vivent Pharmacy'
           ELSE 'Other Pharmacy'
       END 'Pharmacy',
       dep.Site,
       dep.STATE,
       dep.CITY,
       dep.SERVICE_TYPE,
       dep.SERVICE_LINE,
       dep.SUB_SERVICE_LINE,
       IIF(prep.[PATIENT TYPE] = 'PrEP', 'PrEP', 'HIV') AS 'PATIENT TYPE',
       GETDATE() AS UPDATE_DTTM
FROM Clarity.dbo.ORDER_MED_VIEW omv
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON omv.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
    INNER JOIN Clarity.dbo.INDICATIONS_OF_USE ios ON med.MEDICATION_ID = ios.MEDICATION_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON omv.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.RX_PHR rp ON omv.PHARMACY_ID = rp.PHARMACY_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON omv.AUTHRZING_PROV_ID = ser.PROV_ID
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON omv.LOGIN_DEP_ID = dep.DEPARTMENT_ID
    LEFT JOIN #prep prep ON omv.PAT_ID = prep.PATIENT_ID
WHERE YEAR(omv.ORDERING_DATE) > 2015
      AND ios.INDICATIONS_USE_ID IN ( 138, 3032, 4472 ) --HIV infection, HIV infection pre-exposure prophylaxis, prevention of HIV infection after exposure
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' --Medical Visits Only
      AND omv.PHARMACY_ID IS NOT NULL;
