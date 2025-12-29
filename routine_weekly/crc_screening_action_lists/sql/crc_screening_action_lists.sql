set nocount on;

SELECT  TOP 10000000
    pev.PAT_ID
    ,pev.DEPARTMENT_ID
    ,dep.DEPARTMENT_NAME LAST_VISIT_DEPT
    ,pev.CONTACT_DATE LAST_OFFICE_VISIT
    ,CASE
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
        ELSE 'ERROR'
    END AS LOS
    ,CASE
        WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWAUKEE'
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
        ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2)
    END AS CITY
    ,SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) 'STATE'

INTO #Attribution1

FROM 
    Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID

WHERE
    pev.CONTACT_DATE > DATEADD(MONTH, -12, GETDATE())
    AND pev.APPT_STATUS_C IN (2, 6)

;

SELECT  TOP 10000000
    a1.PAT_ID
    ,a1.LAST_OFFICE_VISIT
    ,a1.LOS
    ,a1.CITY
    ,a1.STATE
    ,ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC

INTO #Attribution2

FROM 
    #Attribution1 a1

WHERE
    a1.LOS = 'MEDICAL'

;

SELECT  TOP 10000000
    a2.PAT_ID
    ,a2.LAST_OFFICE_VISIT
    ,a2.LOS
    ,a2.CITY
    ,a2.STATE

INTO #Attribution3
     
FROM 
    #Attribution2 a2

WHERE
    a2.ROW_NUM_DESC = 1


;

---------------------------------------------------------------------------------------------------------------------------------
--GROUPERS
---------------------------------------------------------------------------------------------------------------------------------

SELECT   TOP 10000000
    LEFT(gi.EXTERNAL_GROUPER_ID,
             CHARINDEX('|', gi.EXTERNAL_GROUPER_ID) - 1) AS 'OID'
    ,gi.GROUPER_ID
    ,gi.GROUPER_NAME

INTO    #GrouperDefinitions

FROM    
    Clarity.dbo.GROUPER_ITEMS gi

WHERE   
    gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.108.12.1001' + '|eCQM Update 2018 EP-EC and EH' --Diagnosis: Malignant Neoplasm of Colon
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.526.3.1240' + '|eCQM Update 2018 EP-EC and EH' --Encounter, Performed: Annual Wellness Visit
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.101.12.1048' + '|eCQM Update 2018 EP-EC and EH' --Encounter, Performed: Face-to-Face Interaction
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.101.12.1016' + '|eCQM Update 2018 EP-EC and EH' --Encounter, Performed: Home Healthcare Services
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.101.12.1001' + '|eCQM Update 2018 EP-EC and EH' --Encounter, Performed: Office Visit
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.101.12.1025' + '|eCQM Update 2018 EP-EC and EH' --Encounter, Performed: Preventive Care Services - Established Office Visit, 18 and Up
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.101.12.1023' + '|eCQM Update 2018 EP-EC and EH' --Encounter, Performed: Preventive Care Services-Initial Office Visit, 18 and Up
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113762.1.4.1108.15' + '|eCQM Update 2018 EP-EC and EH' --Intervention, Order: Hospice care ambulatory | Intervention, Performed: Hospice care ambulatory
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.198.12.1011' + '|eCQM Update 2018 EP-EC and EH' --Laboratory Test, Performed: Fecal Occult Blood Test (FOBT)  AND FIT (One Year)
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.108.12.1039' + '|eCQM Update 2018 EP-EC and EH' --Laboratory Test, Performed: FIT DNA (Three Year)
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.108.12.1020' + '|eCQM Update 2018 EP-EC and EH' --Procedure, Performed: Colonoscopy
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.108.12.1038' + '|eCQM Update 2018 EP-EC and EH' --Procedure, Performed: CT Colonography
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.198.12.1010' + '|eCQM Update 2018 EP-EC and EH' --Procedure, Performed: Flexible Sigmoidoscopy
    OR gi.EXTERNAL_GROUPER_ID = '2.16.840.1.113883.3.464.1003.198.12.1019' + '|eCQM Update 2018 EP-EC and EH' --Procedure, Performed: Total Colectomy

;

--------------------------------------------------------------------------------------------------------------------  


SELECT   TOP 10000000
    CASE WHEN gcrl.COMPILED_CONTEXT = 'EDG'
        THEN gcrl.GROUPER_RECORDS_NUMERIC_ID
        ELSE NULL
    END AS 'DX_ID'
    , CASE WHEN gcrl.COMPILED_CONTEXT = 'EAP'
        THEN gcrl.GROUPER_RECORDS_NUMERIC_ID
        ELSE NULL
    END AS 'PROC_ID'

INTO    #Encounter_Codes

FROM    
    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl

WHERE   
    EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.101.12.1001', --EAP --Encounter: Office Visit
                        '2.16.840.1.113883.3.464.1003.101.12.1048', --EDG --Encounter: Face-to-Face 
                        '2.16.840.1.113883.3.464.1003.101.12.1025', --EAP --Encounter, Performed: Preventive Care Services - Established Office Visit, 18 and Up
                        '2.16.840.1.113883.3.464.1003.101.12.1023', --EAP --Encounter, Performed: Preventive Care Services-Initial Office Visit, 18 and Up
                        '2.16.840.1.113883.3.464.1003.101.12.1016', --EAP --Encounter, Performed: Home Healthcare Services
                        '2.16.840.1.113883.3.526.3.1240' --EAP --Encounter, Performed: Annual Wellness Visit
                        ) );

-----------------------------------------------------------------------------------------


SELECT TOP 10000000  
    CASE WHEN gcrl.COMPILED_CONTEXT = 'EAP'
        THEN gcrl.GROUPER_RECORDS_NUMERIC_ID
    END AS 'PROC_ID'
    , CASE WHEN gcrl.COMPILED_CONTEXT = 'HLX'
        THEN gcrl.GROUPER_RECORDS_VARCHAR_ID
    END AS 'ELEMENT_ID'

INTO    #Hospice_Codes

FROM    
    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
WHERE  
    EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN ( '2.16.840.1.113762.1.4.1108.15' --Intervention, Order: Hospice care ambulatory
                        ) )

;

--*************************************************************************************************
-- Epic grouper definitions for diagnoses, procedures and lab components
-- A pointer to the grouper record containing diagnoses that are part of the "Diagnosis active: colorectal cancer," "Diagnosis inactive: colorectal cancer," 
-- and "Diagnosis resolved: colorectal cancer" data elements


SELECT TOP 10000000  edg.DX_ID
INTO    #Colorectal_Cancer_DX
FROM    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
        INNER JOIN Clarity.dbo.CLARITY_EDG edg ON gcrl.GROUPER_RECORDS_NUMERIC_ID = edg.DX_ID
WHERE   EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.108.12.1001' --EAP --Diagnosis: Malignant Neoplasm of Colon
                        ) )
        AND gcrl.COMPILED_CONTEXT = 'EDG'
        ;

--*************************************************************************************************
-- Procedure records representing colectomy procedures that are part of the Procedure performed: total colectomy data element:


SELECT  TOP 10000000 eap.PROC_ID
      , eap.PROC_CODE
      , eap.PROC_NAME
INTO    #Total_Colectomy_Procedures
FROM    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
        INNER JOIN Clarity.dbo.CLARITY_EAP eap ON gcrl.GROUPER_RECORDS_NUMERIC_ID = eap.PROC_ID
WHERE   EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.198.12.1019' --EAP --Procedure, Performed: Total Colectomy
                        ) )
        AND gcrl.COMPILED_CONTEXT = 'EAP'
    ;


SELECT  TOP 10000000 edg.DX_ID
      , edg.DX_NAME
INTO    #Total_Colectomy_Diagnoses
FROM    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
        INNER JOIN Clarity.dbo.CLARITY_EDG edg ON gcrl.GROUPER_RECORDS_NUMERIC_ID = edg.DX_ID
WHERE   EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.198.12.1019' --EDG --Procedure, Performed: Total Colectomy
                        ) )
        AND gcrl.COMPILED_CONTEXT = 'EDG'
        ;
----------------------------------------------------


SELECT TOP 10000000  
    eap.PROC_ID
    ,eap.PROC_CODE
    ,eap.PROC_NAME
INTO    #Colonoscopy_Procedures
FROM    
    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
    INNER JOIN Clarity.dbo.CLARITY_EAP eap ON gcrl.GROUPER_RECORDS_NUMERIC_ID = eap.PROC_ID
WHERE   EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.108.12.1020' ) --Procedure, Performed: Colonoscopy
)
        AND gcrl.COMPILED_CONTEXT = 'EAP'
        ;


SELECT  TOP 10000000 gcrl.GROUPER_RECORDS_VARCHAR_ID AS 'ELEMENT_ID'
INTO    #Colonoscopy_HLX
FROM    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
WHERE   EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.108.12.1020' ) --Procedure, Performed: Colonoscopy
)
        AND gcrl.COMPILED_CONTEXT = 'HLX'
        ;

SELECT  TOP 10000000 eap.PROC_ID
      , eap.PROC_CODE
      , eap.PROC_NAME
INTO    #Colonography_Procedures
FROM    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
        INNER JOIN Clarity.dbo.CLARITY_EAP eap ON gcrl.GROUPER_RECORDS_NUMERIC_ID = eap.PROC_ID
WHERE   EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.108.12.1038' ) --Procedure, Performed: CT Colonography
)
        AND gcrl.COMPILED_CONTEXT = 'EAP'
        ;

SELECT  TOP 10000000 eap.PROC_ID
      , eap.PROC_CODE
      , eap.PROC_NAME
INTO    #Sigmoidoscopy_Procedures
FROM    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
        INNER JOIN Clarity.dbo.CLARITY_EAP eap ON gcrl.GROUPER_RECORDS_NUMERIC_ID = eap.PROC_ID
WHERE   EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.198.12.1010' ) --Procedure, Performed: Flexible Sigmoidoscopy
)
        AND gcrl.COMPILED_CONTEXT = 'EAP'
        ;

SELECT TOP 10000000  CASE WHEN gcrl.COMPILED_CONTEXT = 'LNC'
             THEN gcrl.GROUPER_RECORDS_NUMERIC_ID
        END AS LNC_ID
      , CASE WHEN gcrl.COMPILED_CONTEXT = 'LRR'
             THEN gcrl.GROUPER_RECORDS_NUMERIC_ID
        END AS Component_ID
INTO    #FOBT_Procedures
FROM    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl
WHERE   EXISTS ( SELECT  TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.198.12.1011' ) --Laboratory Test, Performed: Fecal Occult Blood Test (FOBT)
)
;

SELECT  TOP 10000000 
    CASE WHEN gcrl.COMPILED_CONTEXT = 'LNC'
       THEN gcrl.GROUPER_RECORDS_NUMERIC_ID
    END AS LNC_ID
    ,CASE WHEN gcrl.COMPILED_CONTEXT = 'LRR'
       THEN gcrl.GROUPER_RECORDS_NUMERIC_ID
     END AS Component_ID

INTO    #FITDNA_Procedures

FROM    Clarity.dbo.GROUPER_COMPILED_REC_LIST gcrl

WHERE   EXISTS ( SELECT TOP 10000000 *
                 FROM   #GrouperDefinitions gd
                 WHERE  gcrl.BASE_GROUPER_ID = gd.GROUPER_ID
                        AND gd.OID IN (
                        '2.16.840.1.113883.3.464.1003.108.12.1039' ) --Laboratory Test, Performed: FIT DNA
)
;


--*************************************************************************************************
/*******************************************************************************
Create temp table for denominator (active HIV+ pts 50-75 y.o during measurement year) + HX PCP LOGIC
********************************************************************************/



SELECT  TOP 10000000 pev.SERV_AREA_ID
      , pev.PAT_ID
      , pv.PAT_NAME
      , pv.BIRTH_DATE
      ,pv.CUR_PCP_PROV_ID
      , iiv.IDENTITY_ID
      , ser.PROV_ID
      , ser.EXTERNAL_NAME
     , dep.DEPARTMENT_NAME
     , COUNT(pev.PAT_ENC_CSN_ID) AS 'num_of_visits'
      , MAX(pev.CONTACT_DATE) AS 'last_medical_visit'
     
INTO   #a

FROM    
    Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON pev.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON edg.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW iiv ON pev.PAT_ID = iiv.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW pv ON pev.PAT_ID = pv.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = pv.CUR_PCP_PROV_ID
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON pv.PAT_ID = p4.PAT_ID

WHERE   
    pev.APPT_STATUS_C IN (2, 6)
    AND pev.LOS_PRIME_PROC_ID IN (7945, 7946, 7947, 7948, 7949, 7951, 7952, 7953, 7954, 7970, 7971, 7972, 
                                    7973, 7974, 8047, 8048, 8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056) -- Office Visits
    AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
    AND pev.CONTACT_DATE > DATEADD (YEAR,-1, GETDATE()) 
    AND ser.SERV_AREA_ID = 64
    AND pv.BIRTH_DATE >= DATEADD(YEAR, -76, GETDATE()) --Age less than 75 during measurement period
    AND pv.BIRTH_DATE < DATEADD(YEAR, -46, GETDATE()) --Age greater than or equal to 50 THE DAY BEFORE THE REPORTING PERIOD BEGINS
    AND icd10.CODE IN ('B20', 'Z21', 'B97.35') --HIV and Asymptomatic HIV
    AND plv.RESOLVED_DATE IS NULL --Active Dx
    AND plv.PROBLEM_STATUS_C = 1 --Active Dx
    AND p4.PAT_LIVING_STAT_C = 1

GROUP BY pev.SERV_AREA_ID
      , pev.PAT_ID
      , pv.PAT_NAME
      , pv.BIRTH_DATE
      , iiv.IDENTITY_ID
      , ser.PROV_ID
      , ser.EXTERNAL_NAME
      , dep.DEPARTMENT_NAME
      ,pv.CUR_PCP_PROV_ID
      
;

SELECT  TOP 10000000
    a.SERV_AREA_ID
    ,a.PAT_ID
    ,a.PAT_NAME
    ,a.BIRTH_DATE
    ,a.IDENTITY_ID
    ,a.PROV_ID
    ,a.EXTERNAL_NAME
    ,a.DEPARTMENT_NAME
    ,a.num_of_visits
    ,a.last_medical_visit
    ,a.CUR_PCP_PROV_ID
    ,att.CITY
    ,att.STATE

INTO  #Denominator

FROM 
    #a a
    INNER JOIN #Attribution3 att ON a.PAT_ID = att.PAT_ID

;

--*************************************************************************************************
-- Colonoscopy documented in surgical history, only cases where the exact date of the surgical Procedure is known.


SELECT  TOP 10000000 surg.PAT_ID
      , surg.PROC_ID
      , eap.PROC_CODE
      , eap.PROC_NAME
      , CAST(surg.SURGICAL_HX_DATE AS DATETIME) AS 'SURGICAL_HX_DATE'
INTO    #Surg_col_date
FROM    Clarity.dbo.SURGICAL_HX_VIEW surg
        INNER JOIN #Colonoscopy_Procedures csp ON surg.PROC_ID = csp.PROC_ID
        INNER JOIN #Denominator d ON surg.PAT_ID = d.PAT_ID
        INNER JOIN Clarity.dbo.CLARITY_EAP eap ON surg.PROC_ID = eap.PROC_ID
WHERE   ISDATE(surg.SURGICAL_HX_DATE) = 1

;

-- Valid date
--*************************************************************************************************


SELECT  TOP 10000000 scd.PAT_ID
      , scd.PROC_ID
      , scd.PROC_CODE
      , scd.PROC_NAME
      , scd.SURGICAL_HX_DATE
INTO    #Colonoscopy_SurgHx
FROM    #Surg_col_date scd
WHERE   scd.SURGICAL_HX_DATE <= GETDATE()

;

--*************************************************************************************************
-- Flexible sigmoidoscopy documented in surgical history, only cases where the exact date of the surgical Procedure is known.


SELECT  TOP 10000000 surg.PAT_ID
      , surg.PROC_ID
      , eap.PROC_CODE
      , eap.PROC_NAME
      , CAST(surg.SURGICAL_HX_DATE AS DATETIME) AS 'SURGICAL_HX_DATE'
INTO    #Sig_mod_date
FROM    Clarity.dbo.SURGICAL_HX_VIEW surg
        INNER JOIN #Sigmoidoscopy_Procedures csp ON surg.PROC_ID = csp.PROC_ID
        INNER JOIN #Denominator d ON surg.PAT_ID = d.PAT_ID
        INNER JOIN Clarity.dbo.CLARITY_EAP eap ON surg.PROC_ID = eap.PROC_ID
WHERE   ISDATE(surg.SURGICAL_HX_DATE) = 1

;

-- Valid date

--*************************************************************************************************


SELECT TOP 10000000  smd.PAT_ID
      , smd.PROC_ID
      , smd.PROC_CODE
      , smd.PROC_NAME
      , smd.SURGICAL_HX_DATE
INTO    #Sigmoidoscopy_SurgHx
FROM    #Sig_mod_date smd
WHERE   smd.SURGICAL_HX_DATE <= GETDATE()

;

--*************************************************************************************************
-- Colonoscopy orders completed: 
-- This is a very unlikely scenario, but still need to check to be consistent with the Epic build:


SELECT DISTINCT  TOP 10000000
        d.PAT_ID
      , op.ORDERING_DATE
      , 'Completed Colonoscopy Orders' AS 'MeetCriteria'
INTO    #Completed_Colonoscopy_Orders
FROM    #Denominator d
        INNER JOIN Clarity.dbo.ORDER_PROC_VIEW op ON d.PAT_ID = op.PAT_ID
        INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW ors ON op.ORDER_PROC_ID = ors.ORDER_PROC_ID -- Only Resulted Labs
        INNER JOIN #Colonoscopy_Procedures ppc ON op.PROC_ID = ppc.PROC_ID
WHERE   op.ORDER_STATUS_C = 5 -- Completed order 
        AND op.RESULT_TIME >= DATEADD(YEAR, -10, GETDATE())
        AND op.RESULT_TIME < GETDATE()

    ;

-- In the last 10 years


SELECT DISTINCT  TOP 10000000
        d.PAT_ID
      , op.ORDERING_DATE
      , 'Completed Colonography Orders' AS 'MeetCriteria'
INTO    #Completed_Colonography_Orders
FROM    #Denominator d
        INNER JOIN Clarity.dbo.ORDER_PROC_VIEW op ON d.PAT_ID = op.PAT_ID
        INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW ors ON op.ORDER_PROC_ID = ors.ORDER_PROC_ID -- Only Resulted Labs
        INNER JOIN #Colonography_Procedures ppc ON op.PROC_ID = ppc.PROC_ID
WHERE   op.ORDER_STATUS_C = 5 -- Completed order 
        AND op.RESULT_TIME >= DATEADD(YEAR, -5, GETDATE())
        AND op.RESULT_TIME < GETDATE()

;

-- In the last 4 years

--*************************************************************************************************
-- Flexible sigmoidoscopy orders completed by SA:
--  Another unlikely scenario, but still need to check to be consistent with the Epic build for MU and associated quick-guide:


SELECT DISTINCT      TOP 10000000
        d.PAT_ID
      , op.ORDERING_DATE
      , 'Completed Sigmoidoscopy Orders' AS 'MeetCriteria'
INTO    #Completed_Sigmoidoscopy_Orders
FROM    #Denominator d
        INNER JOIN Clarity.dbo.ORDER_PROC_VIEW op ON d.PAT_ID = op.PAT_ID
        INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW ors ON op.ORDER_PROC_ID = ors.ORDER_PROC_ID -- Only Resulted Labs
        INNER JOIN #Sigmoidoscopy_Procedures ppc ON op.PROC_ID = ppc.PROC_ID
WHERE   op.ORDER_STATUS_C = 5 -- Completed order 
        AND op.RESULT_TIME >= DATEADD(YEAR, -5, GETDATE())
        AND op.RESULT_TIME < GETDATE()

;

--  5 years or less from the end of the measurement period.

--*************************************************************************************************
-- FOBT result in the reporting period.

WITH    cte
          AS ( SELECT DISTINCT   TOP 10000000
                        d.PAT_ID
                      , op.ORDERING_DATE
                      , 'FOBT Order Result' AS 'MeetCriteria'
               FROM     #Denominator d
                        INNER JOIN Clarity.dbo.ORDER_PROC_VIEW op ON d.PAT_ID = op.PAT_ID
                        INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW ors ON op.ORDER_PROC_ID = ors.ORDER_PROC_ID -- Only Resulted Labs
                        INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON ors.COMPONENT_ID = cc.COMPONENT_ID
                        INNER JOIN #FOBT_Procedures ppc ON cc.DEFAULT_LNC_ID = ppc.LNC_ID
               WHERE    op.ORDER_STATUS_C = 5
                        AND -- Completed order 
                        op.RESULT_TIME >= DATEADD(YEAR, -1, GETDATE())
                        AND op.RESULT_TIME <= GETDATE()

               UNION
               
               SELECT DISTINCT   TOP 10000000
                        d.PAT_ID
                      , op.ORDERING_DATE
                      , 'FOBT Order Result' AS 'MeetCriteria'
               FROM     #Denominator d
                        INNER JOIN Clarity.dbo.ORDER_PROC_VIEW op ON d.PAT_ID = op.PAT_ID
                        INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW ors ON op.ORDER_PROC_ID = ors.ORDER_PROC_ID -- Only Resulted Labs
                        INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON ors.COMPONENT_ID = cc.COMPONENT_ID
                        INNER JOIN #FOBT_Procedures lrr ON cc.COMPONENT_ID = lrr.Component_ID
               WHERE    op.ORDER_STATUS_C = 5
                        AND -- Completed order 
                        op.RESULT_TIME >= DATEADD(YEAR, -1, GETDATE())
                        AND op.RESULT_TIME <= GETDATE()
             )
    SELECT DISTINCT      TOP 10000000
            cte.PAT_ID
          , cte.ORDERING_DATE
          , cte.MeetCriteria
    INTO    #FOBT_Resulted_Orders
    FROM    cte

;

--*************************************************************************************************
-- FOBT result in the reporting period.

WITH    cte
          AS ( SELECT DISTINCT       TOP 10000000
                        d.PAT_ID
                      , op.ORDERING_DATE
                      , 'FIT-DNA Order Result' AS 'MeetCriteria'
               FROM     #Denominator d
                        INNER JOIN Clarity.dbo.ORDER_PROC_VIEW op ON d.PAT_ID = op.PAT_ID
                        INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW ors ON op.ORDER_PROC_ID = ors.ORDER_PROC_ID -- Only Resulted Labs
                        INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON ors.COMPONENT_ID = cc.COMPONENT_ID
                        INNER JOIN #FITDNA_Procedures fpl ON cc.DEFAULT_LNC_ID = fpl.LNC_ID
               WHERE    op.ORDER_STATUS_C = 5   -- Completed order 
                        AND op.RESULT_TIME >= DATEADD(YEAR, -3, GETDATE())
                        AND op.RESULT_TIME < GETDATE()

               UNION
               
               SELECT DISTINCT   TOP 10000000
                        d.PAT_ID
                      , op.ORDERING_DATE
                      , 'FIT-DNA Order Result' AS 'MeetCriteria'
               FROM     #Denominator d
                        INNER JOIN Clarity.dbo.ORDER_PROC_VIEW op ON d.PAT_ID = op.PAT_ID
                        INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW ors ON op.ORDER_PROC_ID = ors.ORDER_PROC_ID -- Only Resulted Labs
                        INNER JOIN Clarity.dbo.CLARITY_COMPONENT cc ON ors.COMPONENT_ID = cc.COMPONENT_ID
                        INNER JOIN #FITDNA_Procedures lrr ON cc.COMPONENT_ID = lrr.Component_ID
               WHERE    op.ORDER_STATUS_C = 5   -- Completed order 
                        AND op.RESULT_TIME >= DATEADD(YEAR, -3, GETDATE())
                        AND op.RESULT_TIME < GETDATE()

                UNION

                SELECT DISTINCT TOP 10000000  ---------Insure1 FIT DNA Test
                    opv.PAT_ID
                    ,opv.ORDERING_DATE
                    ,'FIT-DNA Order Result' AS 'MeetCriteria'

                FROM 
                    Clarity.dbo.ORDER_PROC_VIEW opv
                    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON opv.PAT_ID = id.PAT_ID
                    --INNER JOIN Clarity.dbo.ORDER_RESULTS_VIEW orv ON orv.ORDER_PROC_ID = opv.ORDER_PROC_ID
                    INNER JOIN Clarity.dbo.CLARITY_EAP eap ON opv.PROC_ID = eap.PROC_ID

                WHERE
                    opv.RESULT_TIME >= DATEADD(YEAR, -3, GETDATE())
                        AND opv.RESULT_TIME < GETDATE()
                    AND eap.PROC_CODE = 'LV5578' ---------Insure1 FIT DNA Test
                    AND opv.ORDER_STATUS_C = 5 --Completed
             )
    SELECT DISTINCT  TOP 10000000
            cte.PAT_ID
          , cte.ORDERING_DATE
          , cte.MeetCriteria
    INTO    #FITDNA_Resulted_Orders
    FROM    cte

;
-- in measurement year
--Result Date changed from 2 to 3 per HEDIS KF

--*************************************************************************************************
-- Patients with completed Health Maintenance for COLON CANCER SCREENING COLONOSCOPY, COLON CANCER SCREENING SIGMOIDOSCOPY, or COLON CANCER SCREENING OCCULT BLOOD TEST WITH A STATUS OF 'DONE'



SELECT  TOP 10000000 DN.PAT_ID
      , HMH.HM_TOPIC_ID
      , HMH.HM_HX_DATE
INTO    #HM_PATS
FROM    Clarity.dbo.HM_HISTORY_VIEW HMH
        INNER JOIN #Denominator DN ON DN.PAT_ID = HMH.PAT_ID
WHERE   HMH.HM_TOPIC_ID IN ( 30, 31, 32, 75 )
        AND HMH.HM_TYPE_C IN (1, 4) --  HMO_STATUS_ID of 'Done'
        AND HMH.HM_HX_DATE IS NOT NULL

;

--*************************************************************************************************
-- Colonoscopy documented in the surgical history, within the last 10 years


SELECT  TOP 10000000 d.PAT_ID
      , csh.SURGICAL_HX_DATE
      , 'Colonoscopy in Surgical Hx' AS 'MeetCriteria'
INTO    #Numerator1
FROM    #Denominator d
        INNER JOIN #Colonoscopy_SurgHx csh ON d.PAT_ID = csh.PAT_ID
WHERE   csh.SURGICAL_HX_DATE >= DATEADD(YEAR, -10, GETDATE()) -- (Actual date must be known)
        AND csh.SURGICAL_HX_DATE < GETDATE()

UNION

-- Colonoscopy orders completed by SA: 
SELECT  TOP 10000000 c.PAT_ID
      , c.ORDERING_DATE
      , c.MeetCriteria
FROM    #Completed_Colonoscopy_Orders c

UNION

SELECT  TOP 10000000 c.PAT_ID
      , c.ORDERING_DATE
      , c.MeetCriteria
FROM    #Completed_Colonography_Orders c

UNION

--  Patients with HM Overrides for Colonoscopy within the last 10 years
SELECT TOP 10000000  HP.PAT_ID
      , HP.HM_HX_DATE
      , 'Colonoscopy HM' AS 'MeetCriteria'
FROM    #HM_PATS HP
WHERE   HP.HM_TOPIC_ID = 32
        AND HP.HM_HX_DATE >= DATEADD(YEAR, -10, GETDATE())
        AND HP.HM_HX_DATE < GETDATE()

UNION

--*************************************************************************************************
-- Flexible sigmoidoscopy conducted during the previous 5 years
SELECT  TOP 10000000 d.PAT_ID
      , sshx.SURGICAL_HX_DATE
      , 'Sigmoidoscopy in Surgical Hx' AS 'MeetCriteria'
FROM    #Denominator d
        INNER JOIN #Sigmoidoscopy_SurgHx sshx ON d.PAT_ID = sshx.PAT_ID
WHERE   sshx.SURGICAL_HX_DATE >= DATEADD(YEAR, -5, GETDATE()) -- Dx documented in the last 5 years
        AND sshx.SURGICAL_HX_DATE < GETDATE()

UNION

-- Sigmoidoscopy orders completed by SA:
SELECT  TOP 10000000 c.PAT_ID
      , c.ORDERING_DATE
      , c.MeetCriteria
FROM    #Completed_Sigmoidoscopy_Orders c

UNION

--  Patients with HM Overrides for Sigmoidoscopy within the last 5 years
SELECT  TOP 10000000 HP.PAT_ID
      , HP.HM_HX_DATE
      , 'Sigmoidoscopy HM' AS 'MeetCriteria'
FROM    #HM_PATS HP
WHERE   HP.HM_TOPIC_ID = 31
        AND HP.HM_HX_DATE >= DATEADD(YEAR, -5, GETDATE())
        AND HP.HM_HX_DATE < GETDATE()

UNION

SELECT  TOP 10000000 fobt.PAT_ID
      , fobt.ORDERING_DATE
      , fobt.MeetCriteria
FROM    #FOBT_Resulted_Orders fobt

UNION

--  Patients with HM Overrides for FOBT within the reporting period
SELECT  TOP 10000000 HP.PAT_ID
      , HP.HM_HX_DATE
      , 'FOBT HM' AS 'MeetCriteria'
FROM    #HM_PATS HP
WHERE   HP.HM_TOPIC_ID = 30
        AND ( HP.HM_HX_DATE >= DATEADD(YEAR, -1, GETDATE())
              AND HP.HM_HX_DATE < GETDATE()
            )

UNION

--  Patients with HM Overrides for FOBT within the reporting period
SELECT  TOP 10000000 HP.PAT_ID
      , HP.HM_HX_DATE
      , 'FIT DNA HM' AS 'MeetCriteria'
FROM    #HM_PATS HP
WHERE   HP.HM_TOPIC_ID = 75
        AND ( HP.HM_HX_DATE >= DATEADD(YEAR, -3, GETDATE())
              AND HP.HM_HX_DATE < GETDATE()
            )

UNION

SELECT  TOP 10000000 fro.PAT_ID
      , fro.ORDERING_DATE
      , fro.MeetCriteria
FROM    #FITDNA_Resulted_Orders fro

;

--*************************************************************************************************


SELECT TOP 10000000  m.PAT_ID
      , STUFF(( SELECT  TOP 10000000 ', ' + CAST(l.SURGICAL_HX_DATE AS VARCHAR(100))
                FROM    #Numerator1 l
                WHERE   m.PAT_ID = l.PAT_ID
                ORDER BY l.SURGICAL_HX_DATE ASC
              FOR
                XML PATH('')
              ), 1, 1, '') AS 'MeetsCriteriaDates'
      , STUFF(( SELECT  TOP 10000000 ', ' + l.MeetCriteria
                FROM    #Numerator1 l
                WHERE   m.PAT_ID = l.PAT_ID
                ORDER BY l.MeetCriteria ASC
              FOR
                XML PATH('')
              ), 1, 1, '') AS 'MeetsCriteria'
INTO    #Numerator
FROM    #Numerator1 m
GROUP BY m.PAT_ID

;

--*************************************************************************************************
-- Exclusions:
-- Total colectomy documented in surgical history:


SELECT  TOP 10000000 den.PAT_ID
      , shx.SURGICAL_HX_DATE
      , 'Colectomy in Surgical Hx' AS 'Exclusion_Reason'
INTO    #ColectomySurgHx
FROM    #Denominator den
        INNER JOIN Clarity.dbo.SURGICAL_HX_VIEW shx ON den.PAT_ID = shx.PAT_ID
        INNER JOIN #Total_Colectomy_Procedures cpc ON shx.PROC_ID = cpc.PROC_ID

;

-- total colectomy Diagnosis in Encounter Diagnosis, Problem list, or Medical history


SELECT TOP 10000000  *
      , ROW_NUMBER() OVER ( PARTITION BY sub.PAT_ID ORDER BY sub.Record_Date ) AS [Rank]
INTO    #ColectomyDx
FROM    ( SELECT   TOP 10000000  den.PAT_ID
                  , pedx.CONTACT_DATE AS Record_Date
                  , 'Colectomy Encounter DX' AS 'Exclusion_Reason'
          FROM      #Denominator den
                    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = den.PAT_ID
                    INNER JOIN Clarity.dbo.PAT_ENC_DX_VIEW pedx ON pedx.PAT_ENC_CSN_ID = pev.PAT_ENC_CSN_ID
                    INNER JOIN #Total_Colectomy_Diagnoses coldx ON coldx.DX_ID = pedx.DX_ID
          WHERE     pev.CONTACT_DATE >= DATEADD(YEAR, -1, GETDATE())     -- Visit Dx within reporting period
                    AND pev.CONTACT_DATE < GETDATE()

    -----------------
          UNION ALL
    -----------------
          SELECT   TOP 10000000  mhx.PAT_ID
                  , try_cast(mhx.MEDICAL_HX_DATE as datetime) AS 'Record_Date'
                  , 'Colectomy in Med History' AS 'Exclusion_Reason'
          FROM      ( SELECT    mhx.PAT_ID
                              , MAX(mhx.PAT_ENC_DATE_REAL) AS 'PEDR'
                      FROM      Clarity.dbo.MEDICAL_HX_VIEW mhx
                      GROUP BY  mhx.PAT_ID
                    ) sub
                    INNER JOIN Clarity.dbo.MEDICAL_HX_VIEW mhx ON sub.PAT_ID = mhx.PAT_ID
                                                          AND mhx.PAT_ENC_DATE_REAL = sub.PEDR
                    INNER JOIN #Total_Colectomy_Diagnoses coldx ON mhx.DX_ID = coldx.DX_ID
          WHERE     ISDATE(mhx.MEDICAL_HX_DATE) = 1
                    AND LEN(mhx.MEDICAL_HX_DATE) > 4


    -------------------
          UNION ALL
    -----------------
          SELECT  TOP 10000000   den.PAT_ID
                  , ISNULL(pl.NOTED_DATE, pl.DATE_OF_ENTRY) AS 'Record_Date'
                  , 'Colectomy Problem List Diagnosis' AS 'Exclusion_Reason'
          FROM      #Denominator den
                    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW pl ON den.PAT_ID = pl.PAT_ID
                    INNER JOIN #Total_Colectomy_Diagnoses coldx ON pl.DX_ID = coldx.DX_ID
          WHERE     ( pl.PROBLEM_STATUS_C = 1
                      OR ( pl.PROBLEM_STATUS_C = 2
                           AND ( pl.RESOLVED_DATE IS NULL
                                 OR pl.RESOLVED_DATE >= GETDATE()
                               )
                         )
                    )
                    AND ISNULL(pl.NOTED_DATE, pl.DATE_OF_ENTRY) <= den.last_medical_visit
        ) sub

;

-- or COLON CANCER documented documented in Medical Hx


SELECT  TOP 10000000 den.PAT_ID
      , mhx.MEDICAL_HX_DATE
      , 'Colon Cancer in Medical Hx' AS 'Exclusion_Reason'
INTO    #ColonCancerMedHx
FROM    #Denominator den
        INNER JOIN Clarity.dbo.MEDICAL_HX_VIEW mhx ON den.PAT_ID = mhx.PAT_ID
        INNER JOIN ( SELECT TOP 10000000 mhv.PAT_ID
                          , MAX(mhv.PAT_ENC_DATE_REAL) AS 'PEDR'
                     FROM   Clarity.dbo.MEDICAL_HX_VIEW mhv
                     GROUP BY mhv.PAT_ID
                   ) mhx_mr ON mhx.PAT_ID = mhx_mr.PAT_ID
                               AND mhx.PAT_ENC_DATE_REAL = mhx_mr.PEDR
        INNER JOIN #Colorectal_Cancer_DX hdc ON mhx.DX_ID = hdc.DX_ID

;

SELECT  TOP 10000000 d.PAT_ID
      , COALESCE(pl.NOTED_DATE, pl.DATE_OF_ENTRY) AS 'Exclusion_Date'
      , 'Colon Cancer in Problem List' AS 'Exclusion_Reason'
INTO    #ColonCancerPL
FROM    #Denominator d
        INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW pl ON d.PAT_ID = pl.PAT_ID
        INNER JOIN #Colorectal_Cancer_DX pldx ON pl.DX_ID = pldx.DX_ID
WHERE   pl.PROBLEM_STATUS_C IN ( 1, 2 ) -- Active or Resolved
        AND ISNULL(pl.NOTED_DATE, pl.DATE_OF_ENTRY) < GETDATE()

;

-- or Dx of colorectal CANCER documented as A visit Dx


SELECT  TOP 10000000 d.PAT_ID
      , dx.CONTACT_DATE AS 'Diagnosis_Date'
      , 'Colon Cancer in Visit Dx' AS 'Exclusion_Reason'
INTO    #ColonCancerVisitDx
FROM    #Denominator d
        INNER JOIN Clarity.dbo.PAT_ENC_DX_VIEW dx ON d.PAT_ID = dx.PAT_ID -- Visit DXs
        INNER JOIN #Colorectal_Cancer_DX vdx ON dx.DX_ID = vdx.DX_ID
WHERE   dx.CONTACT_DATE < GETDATE()

;

SELECT  TOP 10000000 csh.PAT_ID
      , csh.SURGICAL_HX_DATE AS 'Exclusion_Date'
      , csh.Exclusion_Reason
INTO    #ColectomyCancer_Exclusion1
FROM    #ColectomySurgHx csh

UNION

SELECT TOP 10000000  PAT_ID
      , CONVERT(VARCHAR(10), cdx.Record_Date, 101)
      , Exclusion_Reason
FROM    #ColectomyDx cdx
WHERE   cdx.Rank = 1

UNION

SELECT  TOP 10000000 ccm.PAT_ID
      , ccm.MEDICAL_HX_DATE
      , ccm.Exclusion_Reason
FROM    #ColonCancerMedHx ccm

UNION

SELECT TOP 10000000  ccp.PAT_ID
      , CONVERT(VARCHAR(10), ccp.Exclusion_Date, 101)
      , ccp.Exclusion_Reason
FROM    #ColonCancerPL ccp

UNION

SELECT TOP 10000000  ccvd.PAT_ID
      , CONVERT(VARCHAR(10), ccvd.Diagnosis_Date, 101)
      , ccvd.Exclusion_Reason
FROM    #ColonCancerVisitDx ccvd

UNION

SELECT  TOP 10000000 d.PAT_ID
      , CONVERT(VARCHAR(10), opv.ORDERING_DATE, 101)
      , 'Hospice Order'
FROM    #Denominator d
        INNER JOIN Clarity.dbo.ORDER_PROC_VIEW opv ON d.PAT_ID = opv.PAT_ID
        INNER JOIN #Hospice_Codes hc ON opv.PROC_ID = hc.PROC_ID
WHERE   opv.ORDERING_DATE >= DATEADD(YEAR, -1, GETDATE())
        AND opv.ORDERING_DATE < GETDATE()
        AND ( opv.ORDER_STATUS_C IS NULL
              OR opv.ORDER_STATUS_C <> 4
            )

UNION

SELECT  TOP 10000000 d.PAT_ID
      , CONVERT(VARCHAR(10), pev.CONTACT_DATE, 101)
      , 'Hospice SmartData'
FROM    #Denominator d
        INNER JOIN Clarity.dbo.SMRTDTA_ELEM_DATA_VIEW sedv ON d.PAT_ID = sedv.PAT_LINK_ID
        INNER JOIN Clarity.dbo.SMRTDTA_ELEM_VALUE_VIEW sevv ON sedv.HLV_ID = sevv.HLV_ID
        INNER JOIN #Hospice_Codes hc ON sedv.ELEMENT_ID = hc.ELEMENT_ID
        LEFT JOIN Clarity.dbo.HNO_INFO_VIEW hno ON ( hno.NOTE_ID = sedv.RECORD_ID_VARCHAR
                                         AND sedv.CONTEXT_NAME = 'NOTE'
                                       )
                                       OR ( hno.NOTE_ID = sedv.SRC_NOTE_ID
                                            AND sedv.CONTEXT_NAME = 'ENCOUNTER'
                                          )
        LEFT JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ENC_CSN_ID = hno.PAT_ENC_CSN_ID
WHERE   pev.CONTACT_DATE >= DATEADD(YEAR, -1, GETDATE())
        AND pev.CONTACT_DATE < GETDATE()
        AND sevv.SMRTDTA_ELEM_VALUE IN ( 'Y', '1' )

;

-- For patients with multiple exclusion data sources, create comma delimited list:


SELECT  m.PAT_ID
      , STUFF(( SELECT TOP 10000000  ',' + l.Exclusion_Reason
                FROM    #ColectomyCancer_Exclusion1 l
                WHERE   m.PAT_ID = l.PAT_ID
                ORDER BY l.Exclusion_Reason ASC
              FOR
                XML PATH('')
              ), 1, 1, '') AS 'Exclusion_Reasons'
      , STUFF(( SELECT TOP 10000000  ',' + l.Exclusion_Date
                FROM    #ColectomyCancer_Exclusion1 l
                WHERE   m.PAT_ID = l.PAT_ID
                ORDER BY l.Exclusion_Date ASC
              FOR
                XML PATH('')
              ), 1, 1, '') AS 'Exclusion_Dates'
INTO    #ColectomyCancer_Exclusion
FROM    #ColectomyCancer_Exclusion1 m
GROUP BY m.PAT_ID;

;

--*************************************************************************************************
-- Outcome:


SELECT DISTINCT      TOP 10000000
      d.IDENTITY_ID AS 'MRN'
      ,d.PAT_ID
      , d.PAT_NAME
      , CONVERT(nvarchar(30), d.BIRTH_DATE, 101) AS BIRTH_DATE
      , d.num_of_visits
      , CONVERT(nvarchar(30), d.last_medical_visit  , 101) AS last_medical_visit
      , num.MeetsCriteria
      , num.MeetsCriteriaDates
      , cex.Exclusion_Reasons
      , cex.Exclusion_Dates
      , CASE WHEN cex.PAT_ID IS NOT NULL THEN 'Excluded'
             WHEN num.PAT_ID IS NOT NULL THEN 'Met'
             ELSE 'Not Met'
        END AS 'Outcome'
      , d.EXTERNAL_NAME AS 'pcp_name'
      , CONVERT(NVARCHAR, DATEADD(YEAR, -1, GETDATE()), 101) AS 'Report_Period_Begin'
      , CONVERT(NVARCHAR, ( GETDATE() - 1 ), 101) AS 'Report_Period_End'
      ,d.CITY
      ,d.STATE

INTO #c

FROM    
    #Denominator d
    LEFT JOIN #Numerator num ON d.PAT_ID = num.PAT_ID
    LEFT JOIN #ColectomyCancer_Exclusion cex ON d.PAT_ID = cex.PAT_ID
    LEFT JOIN #HM_PATS hm ON d.PAT_ID = hm.PAT_ID

WHERE
    cex.Exclusion_Reasons IS NULL --Take this out to see excluded pts.

;

SELECT  TOP 10000000
    c.MRN
    ,c.PAT_ID
    ,c.PAT_NAME
    ,c.BIRTH_DATE
    ,c.num_of_visits
    ,c.last_medical_visit
    ,c.MeetsCriteria
    ,c.MeetsCriteriaDates
    ,c.Exclusion_Reasons
    ,c.Exclusion_Dates
    ,c.Outcome
    ,c.pcp_name
    ,c.Report_Period_Begin
    ,c.Report_Period_End
    ,c.CITY
    ,c.STATE
    ,CASE 
        WHEN c.Outcome = 'Not Met' AND r.START_DATE IS NOT NULL THEN r.START_DATE
        WHEN c.Outcome = 'Not Met' AND opv.ORDERING_DATE IS NOT NULL THEN opv.ORDERING_DATE
        ELSE NULL
    END AS COLONOSCOPY_REFERRAL_DATE


INTO #d

FROM 
    #c c
    LEFT JOIN Clarity.dbo.REFERRAL_VIEW r ON c.PAT_ID = r.PAT_ID
                        AND r.RFL_TYPE_C = 118
                        AND r.RFL_STATUS_C IN (1, 2, 3, 6, 7, 104, 105, 112, 117, 118, 119, 120)
    LEFT JOIN Clarity.dbo.ORDER_PROC_VIEW opv ON c.PAT_ID = opv.PAT_ID
                        AND opv.PROC_ID IN (
                                            SELECT DISTINCT
                                                cps.PROC_ID 
                                            FROM 
                                                #Colonoscopy_Procedures cps
                                            )

;

SELECT  TOP 10000000
    d.MRN
    ,d.PAT_ID
    ,d.PAT_NAME
    ,d.BIRTH_DATE
    ,d.num_of_visits
    ,d.last_medical_visit
    ,d.MeetsCriteria
    ,d.MeetsCriteriaDates
    ,d.Exclusion_Reasons
    ,d.Exclusion_Dates
    ,d.Outcome
    ,d.pcp_name
    ,d.Report_Period_Begin
    ,d.Report_Period_End
    ,d.CITY
    ,d.STATE
    ,d.COLONOSCOPY_REFERRAL_DATE
    ,ROW_NUMBER() OVER (PARTITION BY d.PAT_ID ORDER BY d.COLONOSCOPY_REFERRAL_DATE DESC) AS ROW_NUM_DESC 

INTO #e

FROM 
    #d d

;

SELECT  TOP 10000000
    e.MRN
    ,e.PAT_ID
    ,e.PAT_NAME
    ,e.BIRTH_DATE
    ,(DATEDIFF(m, e.BIRTH_DATE, GETDATE()) / 12) AGE
    ,e.last_medical_visit
    ,e.MeetsCriteria
    ,e.MeetsCriteriaDates
    ,e.Exclusion_Reasons
    ,e.Exclusion_Dates
    ,e.Outcome
    ,e.pcp_name PCP
    ,e.Report_Period_End
    ,e.CITY
    ,e.STATE
    ,CONVERT(nvarchar(30), e.COLONOSCOPY_REFERRAL_DATE, 101) AS  'COLONOSCOPY REFERRAL DATE'

INTO #f

FROM 
    #e e

WHERE
    e.ROW_NUM_DESC = 1

;

SELECT  TOP 10000000--To get active pts in Clinical Pharmacy Cohorts
    flag.PATIENT_ID PAT_ID
    ,MAX(CASE
        WHEN f.NAME IS NOT NULL THEN 'Y'
    END) AS 'ACTIVE_CP_COHORT'


INTO #fyi

FROM 
    Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
    INNER JOIN Clarity.dbo.ZC_BPA_TRIGGER_FYI f ON flag.PAT_FLAG_TYPE_C = f.BPA_TRIGGER_FYI_C


WHERE
    --flag.PAT_FLAG_TYPE_C IN ('640011', '640012', '640013') ---HTN, DM, AntiCoag
    f.name LIKE 'SA64 Pharmacist%'
    AND flag.ACTIVE_C = 1 -- Only currently actives

GROUP BY 
    flag.PATIENT_ID

;

SELECT DISTINCT TOP 10000000
    f.MRN
    ,f.PAT_NAME
    ,f.BIRTH_DATE
    ,f.AGE
    ,f.last_medical_visit
    ,f.MeetsCriteria
    ,f.MeetsCriteriaDates
    ,f.Exclusion_Reasons
    ,f.Exclusion_Dates
    ,f.Outcome
    ,f.PCP
    ,f.Report_Period_End
    ,f.CITY
    ,f.STATE
    ,f.[COLONOSCOPY REFERRAL DATE]
    ,CASE
        WHEN f.Outcome = 'Not Met' AND opv.PROC_ID = 63002 THEN opv.ORDERING_DATE
        ELSE NULL
    END AS FOBT_ORDER_DATE
    ,CASE
        WHEN f.Outcome = 'Not Met' AND opv.PROC_ID = 180985 THEN opv.ORDERING_DATE
        ELSE NULL
    END AS FitDNA_ORDER_DATE
    ,CASE
        WHEN fyi.ACTIVE_CP_COHORT = 'Y' THEN 'Y'
        ELSE 'N'
    END AS 'CLINICAL PHARMACY COHORT'
    ,zpr.NAME 'RACE'
    ,svis.[Next Any Appt]
    ,svis.[Next Appt Prov]
    ,spvis.[Next PCP Appt]
    ,spvis.[Next PCP Appt Prov]

FROM 
    #f f
    LEFT JOIN Clarity.dbo.ORDER_PROC_VIEW opv ON opv.PAT_ID = f.PAT_ID
                    AND opv.PROC_ID IN ( 63002, 180985) --IN ('LS652' (proc ID 63002), 'LV5046' (proc ID 180985))
                    AND opv.ORDERING_DATE > DATEADD(YEAR, -3, GETDATE())
    LEFT JOIN #fyi fyi ON fyi.PAT_ID = f.PAT_ID
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON pr.PAT_ID = f.PAT_ID
                            AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON zpr.PATIENT_RACE_C = pr.PATIENT_RACE_C
    LEFT JOIN
        (SELECT  TOP 1000000
            pev.PAT_ID
            ,id.IDENTITY_ID
            ,CONVERT(nvarchar(30), pev.CONTACT_DATE, 101) AS 'Next Any Appt'
            ,ser.PROV_NAME 'Next Appt Prov'
            ,ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC

            FROM 
            Clarity.dbo.PAT_ENC_VIEW pev
            INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID

        WHERE
            pev.APPT_STATUS_C = 1 --Scheduled

        ) svis ON svis.IDENTITY_ID = f.MRN
                AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN
        (SELECT  TOP 1000000
            pev.PAT_ID
            ,id.IDENTITY_ID
            ,CONVERT(nvarchar(30), pev.CONTACT_DATE, 101) AS  'Next PCP Appt'
            ,ser.PROV_NAME 'Next PCP Appt Prov'
            ,ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC

            FROM 
            Clarity.dbo.PAT_ENC_VIEW pev
            INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON pev.PAT_ID = id.PAT_ID
            INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID

        WHERE
            pev.APPT_STATUS_C = 1 --Scheduled
            AND ser.PROVIDER_TYPE_C IN ('1', '6', '9', '113') -- Physicians, PAs and NPs

        ) spvis ON spvis.IDENTITY_ID = f.MRN
                AND spvis.ROW_NUM_ASC = 1 -- First scheduled

;

DROP TABLE #Attribution1
DROP TABLE #Attribution2
DROP TABLE #Attribution3
DROP TABLE #a
DROP TABLE #c
DROP TABLE #d
DROP TABLE #e
DROP TABLE #f
DROP TABLE #GrouperDefinitions
DROP TABLE #Encounter_Codes
DROP TABLE #Hospice_Codes
DROP TABLE #Colorectal_Cancer_DX
DROP TABLE #Total_Colectomy_Procedures
DROP TABLE #Total_Colectomy_Diagnoses
DROP TABLE #Colonoscopy_Procedures
DROP TABLE #Colonoscopy_HLX
DROP TABLE #Colonography_Procedures
DROP TABLE #Sigmoidoscopy_Procedures
DROP TABLE #FOBT_Procedures
DROP TABLE #FITDNA_Procedures
DROP TABLE #Denominator
DROP TABLE #Surg_col_date
DROP TABLE #Colonoscopy_SurgHx
DROP TABLE #Sig_mod_date
DROP TABLE #Sigmoidoscopy_SurgHx
DROP TABLE #Completed_Colonoscopy_Orders
DROP TABLE #Completed_Colonography_Orders
DROP TABLE #Completed_Sigmoidoscopy_Orders
DROP TABLE #FOBT_Resulted_Orders
DROP TABLE #FITDNA_Resulted_Orders
DROP TABLE #HM_PATS
DROP TABLE #Numerator1
DROP TABLE #Numerator
DROP TABLE #ColectomySurgHx
DROP TABLE #ColectomyDx
DROP TABLE #ColonCancerMedHx
DROP TABLE #ColonCancerPL
DROP TABLE #ColonCancerVisitDx
DROP TABLE #ColectomyCancer_Exclusion1
DROP TABLE #ColectomyCancer_Exclusion
DROP TABLE #fyi
