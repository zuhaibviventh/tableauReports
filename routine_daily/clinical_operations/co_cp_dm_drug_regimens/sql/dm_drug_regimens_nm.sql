/**
 * ANL-MKE-SVR-100
 **/

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
       dep.STATE,
       dep.CITY,
       dep.SITE,
       dep.SERVICE_LINE AS LOS
       dep.SERVICE_TYPE,
       dep.SUB_SERVICE_LINE
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    LEFT JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
--AND pev.APPT_STATUS_C IN (2, 6) -- 1 pt not seen in a long time

;

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       a1.SERVICE_TYPE,
       a1.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE, a2.SERVICE_TYPE, a2.SUB_SERVICE_LINE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;

SELECT DISTINCT flag.PATIENT_ID PAT_ID,
                id.IDENTITY_ID,
                p.PAT_NAME,
                ser.PROV_NAME PCP
INTO #pop
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = flag.PATIENT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON flag.PATIENT_ID = id.PAT_ID
WHERE flag.ACTIVE_C = 1
      AND flag.PAT_FLAG_TYPE_C = '640013';
SELECT id.IDENTITY_ID,
       p.PAT_NAME,
       omv.PAT_ID,
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 10960, 10957, 12025 ) THEN 1 ELSE 0 END) AS 'Sulfonylureas',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 12270, 10099 ) THEN 1 ELSE 0 END) AS 'Meglitinides',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 12270, 12559, 10960, 11829, 12110, 12887, 10098, 12097 ) THEN 1
               ELSE 0
           END) AS 'Biguanide',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 11829, 12025, 12841, 10104 ) THEN 1 ELSE 0 END) AS 'Thiazolidinediones',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 2750 ) THEN 1 ELSE 0 END) AS 'Alpha Glucosidase Inhibitors',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 12084, 12602, 12394, 12481, 12110, 12887 ) THEN 1
               ELSE 0
           END) AS 'Dipeptidyl Peptidase IV (DPP-4) Inhibitors',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 12332 ) THEN 1 ELSE 0 END) AS 'Dopamine Agonists ',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 12559, 12602, 12486, 12887 ) THEN 1 ELSE 0 END) AS 'Sodium Glucose Transport Protein 2 (SGLT2) Inhibitors',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 12008, 12615 ) THEN 1 ELSE 0 END) AS 'Glucagon Like Peptide (GLP1) Agonists',
       MAX(CASE WHEN med.PHARM_SUBCLASS_C IN ( 12615, 12183, 12189, 12192, 12809, 12187, 10093, 11775, 11104, 12184, 12191, 12186 ) THEN 1
               ELSE 0
           END) AS 'Insulin Therapy'
INTO #med
FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
    LEFT JOIN Clarity.dbo.ORDER_MED_VIEW omv ON flag.PATIENT_ID = omv.PAT_ID --Must use this, cannot use the CUR_MED_ENC_LIST_VIEW 9or whatever it's called) since there is no way to get active meds from that.
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON omv.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON omv.PAT_ID = p.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
WHERE med.PHARM_SUBCLASS_C IN ( '10960', '10957', '12025', '12270', '10099', '12559', '11829', '12110', '12887', '10098', '12097', '12841', '10104', '2750',
                                '12084', '12602', '12394', '12481', '12332', '12486', '12008', '12615', '12183', '12189', '12192', '12809', '12187', '10093',
                                '11775', '11104', '12184', '12191', '12186' )
      --AND omv.ORDER_STATUS_C = 2 --The combo of this code and a NULL end date is what identifys an active med. (but is excluding outside orders)
      AND omv.END_DATE IS NULL
      AND flag.ACTIVE_C = 1
      AND flag.PAT_FLAG_TYPE_C = '640013'
GROUP BY id.IDENTITY_ID,
         p.PAT_NAME,
         omv.PAT_ID;

SELECT pop.PAT_ID,
       pop.IDENTITY_ID,
       pop.PAT_NAME,
       pop.PCP,
       a3.CITY,
       a3.STATE,
       a3.SERVICE_TYPE,
       a3.LOS,
       a3.SUB_SERVICE_LINE,
       COALESCE(med.Sulfonylureas, 0) Sulfonylureas,
       COALESCE(med.Meglitinides, 0) Meglitinides,
       COALESCE(med.Biguanide, 0) Biguanide,
       COALESCE(med.Thiazolidinediones, 0) Thiazolidinediones,
       COALESCE(med.[Alpha Glucosidase Inhibitors], 0) [Alpha Glucosidase Inhibitors],
       COALESCE(med.[Dipeptidyl Peptidase IV (DPP-4) Inhibitors], 0) [Dipeptidyl Peptidase IV (DPP-4) Inhibitors],
       COALESCE(med.[Dopamine Agonists ], 0) [Dopamine Agonists ],
       COALESCE(med.[Sodium Glucose Transport Protein 2 (SGLT2) Inhibitors], 0) [Sodium Glucose Transport Protein 2 (SGLT2) Inhibitors],
       COALESCE(med.[Glucagon Like Peptide (GLP1) Agonists], 0) [Glucagon Like Peptide (GLP1) Agonists],
       COALESCE(med.[Insulin Therapy], 0) [Insulin Therapy],
       COALESCE(
       med.Sulfonylureas + med.Meglitinides + med.Biguanide + med.Thiazolidinediones + med.[Alpha Glucosidase Inhibitors]
       + med.[Dipeptidyl Peptidase IV (DPP-4) Inhibitors] + med.[Dopamine Agonists ] + med.[Sodium Glucose Transport Protein 2 (SGLT2) Inhibitors]
       + med.[Glucagon Like Peptide (GLP1) Agonists] + med.[Insulin Therapy], 0) TOTAL
FROM #pop pop
    LEFT JOIN #med med ON med.PAT_ID = pop.PAT_ID
    LEFT JOIN #Attribution3 a3 ON a3.PAT_ID = pop.PAT_ID;

DROP TABLE #pop;
DROP TABLE #med;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
