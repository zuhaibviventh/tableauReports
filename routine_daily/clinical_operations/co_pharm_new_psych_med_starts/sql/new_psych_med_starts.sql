/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT pev.PAT_ID,
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
           ELSE 'ERROR'
       END AS 'SITE',
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS 'LOS'
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT a2.PAT_ID, a2.CITY, a2.STATE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;

SELECT id.IDENTITY_ID MRN,
       p.PAT_NAME PATIENT,
       serpcp.PROV_NAME 'PCP',
       rp.PHARMACY_NAME,
       med.NAME,
       medgrp.[Med Class],
       ser.PROV_NAME 'Prescriber',
       ser.PROV_TYPE 'Prescriber Type',
       omv.ORDERING_DATE 'First Rx in Last 12 Months',
       DATEDIFF(WEEK, (omv.ORDERING_DATE), GETDATE()) 'Weeks Since First Rx',
       COALESCE(pm.[Prescribed Before], 'N') 'Previously Prescribed',
       CASE WHEN odiv.ORDER_MED_ID IS NOT NULL THEN 'Yes'
           WHEN rp.PHARMACY_NAME LIKE 'VIVENT PHARM%'
                AND odiv.ORDER_MED_ID IS NULL THEN 'No'
           ELSE 'Unknown'
       END AS 'Dispensed',
       a3.CITY,
       a3.STATE,
       ROW_NUMBER() OVER (PARTITION BY id.PAT_ID, medgrp.[Med Class]
ORDER BY omv.ORDERING_DATE ASC) AS ROW_NUM_DESC
INTO #a
FROM #Attribution3 a3
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = a3.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
    INNER JOIN Clarity.dbo.ORDER_MED_VIEW omv ON omv.PAT_ID = a3.PAT_ID
    LEFT JOIN Clarity.dbo.ORDER_DISP_INFO_VIEW odiv ON odiv.ORDER_MED_ID = omv.ORDER_MED_ID
                                                       AND odiv.ORD_CNTCT_TYPE_C = 11 --Dispensed
    INNER JOIN Clarity.dbo.RX_PHR rp ON omv.PHARMACY_ID = rp.PHARMACY_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON omv.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON omv.AUTHRZING_PROV_ID = ser.PROV_ID
    LEFT JOIN (SELECT a33.PAT_ID,
                      med.PHARM_SUBCLASS_C,
                      'Y' 'Prescribed Before',
                      CASE WHEN med.PHARM_SUBCLASS_C = 11044 /*Anticonvulsant*/
                      THEN     'Anticonvulsant'
                          WHEN med.PHARM_SUBCLASS_C IN ( 10378, 11109, 10966, 10966, 12356, 12513, 11111, 11108, 10967 /*Antidepressant*/ ) THEN
                              'Antidepressant'
                          WHEN med.PHARM_SUBCLASS_C IN ( 11686, 11621, 10382, 10972, 10975, 11620, 11622, 12692, 12658 /*Antipsychotic*/ ) THEN 'Antipsychotic'
                          WHEN med.PHARM_SUBCLASS_C IN ( 12219, 10968 /*Bipolar Therapy Agent*/ ) THEN 'Bipolar Therapy Agent'
                      END AS 'Med Class'
               FROM #Attribution3 a33
                   INNER JOIN Clarity.dbo.ORDER_MED_VIEW omv ON omv.PAT_ID = a33.PAT_ID
                   INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
               WHERE med.PHARM_SUBCLASS_C IN ( 11044 /*Anticonvulsant*/, 10378, 11109, 10966, 10966, 12356, 12513, 11111, 11108, 10967,      /*Antidepressant*/
                                               11686, 11621, 10382, 10972, 10975, 11620, 11622, 12692, 12658 /*Antipsychotic*/, 12219, 10968 /*Bipolar Therapy Agents*/ )
                     AND omv.ORDERING_DATE
                     BETWEEN DATEADD(MONTH, -2, GETDATE()) AND DATEADD(MONTH, -13, GETDATE())
                     AND omv.ORDER_STATUS_C = 2 -- Sent
    ) pm ON pm.PAT_ID = a3.PAT_ID
            AND pm.PHARM_SUBCLASS_C = med.PHARM_SUBCLASS_C
    LEFT JOIN (SELECT CASE WHEN med.PHARM_SUBCLASS_C = 11044 /*Anticonvulsant*/
               THEN            'Anticonvulsant'
                          WHEN med.PHARM_SUBCLASS_C IN ( 10378, 11109, 10966, 10966, 12356, 12513, 11111, 11108, 10967 /*Antidepressant*/ ) THEN
                              'Antidepressant'
                          WHEN med.PHARM_SUBCLASS_C IN ( 11686, 11621, 10382, 10972, 10975, 11620, 11622, 12692, 12658 /*Antipsychotic*/ ) THEN 'Antipsychotic'
                          WHEN med.PHARM_SUBCLASS_C IN ( 12219, 10968 /*Bipolar Therapy Agent*/ ) THEN 'Bipolar Therapy Agent'
                      END AS 'Med Class',
                      med.PHARM_SUBCLASS_C
               FROM Clarity.dbo.CLARITY_MEDICATION med) medgrp ON medgrp.PHARM_SUBCLASS_C = med.PHARM_SUBCLASS_C
WHERE med.PHARM_SUBCLASS_C IN ( 11044 /*Anticonvulsant*/, 10378, 11109, 10966, 10966, 12356, 12513, 11111, 11108, 10967 /*Antidepressant*/, 11686, 11621,
                                10382, 10972, 10975, 11620, 11622, 12692, 12658 /*Antipsychotic*/, 12219, 10968 /*Bipolar Therapy Agents*/ )
      AND omv.ORDERING_DATE > DATEADD(MONTH, -1, GETDATE())
      AND omv.ORDER_STATUS_C = 2 -- Sent

;

SELECT a.MRN,
       a.PATIENT 'Patient',
       COALESCE(a.PCP, 'NO PCP') AS 'PCP',
       a.PHARMACY_NAME 'Pharmacy',
       CASE WHEN a.PHARMACY_NAME LIKE 'VIVENT PHARMACY%' THEN 'VIVENT PHARMACY'
           ELSE 'OTHER PHARMACY'
       END AS 'Pharmacy Group',
       a.NAME 'Medication',
       UPPER(a.[Med Class]) 'Med Class',
       a.Prescriber,
       UPPER(a.[Prescriber Type]) 'Prescriber Type',
       a.[First Rx in Last 12 Months], -- I changed this name in Tableau
       a.[Weeks Since First Rx],       -- Week filter two tailed slider
       UPPER(a.Dispensed) 'Dispensed',
       a.CITY 'City',
       a.STATE 'State'
FROM #a a
WHERE a.ROW_NUM_DESC = 1
      AND a.[Previously Prescribed] = 'N';

DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #a;
