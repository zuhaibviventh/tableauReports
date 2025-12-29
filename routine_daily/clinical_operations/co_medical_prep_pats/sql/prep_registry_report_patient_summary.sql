/*---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Create Date	:	5/27/2016
Created By	:	Guma Olupot
Requested By:	SA 64 ARCW RSS
JIRA		:	HDREP-12395

Purpose		:   RSS request for evaluating PrEP Patients

Parameters:       
				@Reporting_Period VARCHAR(254)
				@Start_Date AS DATETIME
				@End_Date AS DATETIME



Modification History
Change Date:		Changed By:					Change Description:
------------------	-----------------			---------------------------------------------------
06-06-2016			Guma Olupot					Initial Creation
08-10-2017			Guma Olupot					Changed basis of Evaluated population to be dependent on FYI Flags per jira RPRT-655 comment on 08-08-2017.
12-11-2017			Guma Olupot					EAPPS-28768
5-15-2018			Kasey Fouquet				Changed Block 1: hardcoded two flags per RPRT-6691 and removed @FYI_FLAG_ID's parameter.
10/17/2018			Mitch Scoggins				Changed Block 1: Updated former [PrEP D&R] column to 'SITE' and using a CASE statement to put [PrEP D&R], Keenan or 'Onsite' into that column.
10/17/2018			Mitch Scoggins				Updated FINAL SELECT Active pt logic to check for Current PCP AND  ((current Truvada) OR (Visit within the last 4 months))
10/17/2018			Mitch Scoggins				Changed Block 2: Added DESC as the sort order in the ROW_NUMBER function so the dates were sorting properly
10/17/2018			Mitch Scoggins				Changed Block 5: Limited med list to Truvada only, and switched the evaluation period from > 90 dyas to < 120
10/1/2020			Mitch						Changed Block 5: Updating to include Descovy - Actually, using the INDICATIONS_OF_USE table to capture any HIV med for future-proofing. This is per PMI w// Leslie and Mitch on 9/30/2020
10/1/2020			Mitch						Changed Block 4: Was excluding some types of medical visits. Switched to looking at departments, not specific visit types.
10/1/2020			Mitch						Updated pharmacy list and did lots of code cleanup (e.g., including 'Clarity.' in dbo names
10/1/2020			Mitch						Removed the ufn for age calculation so we can run this on our internal Clarity
11/10/2020			Mitch						Based on discussion w/ Leslie, removing all labs except "HIV Antigen/Antibody" (PMI 11/6/2020)
11/10/2020			Mitch						Updated Guma's phenominally annoying coding style to be ANSI compliant
12/7/2020			Mitch						Total rewrite for effeciency, accuracy and to run on our server
7/14/2021			Mitch						Adding Gender Identity, ZIP code, and (if possible) appt type in-person vs. virtual. Request of Dan Scales.
2023-03-22          Benzon                      Reportable in Python

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

/*---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
DEPT PARAM
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
SELECT DISTINCT dep.DEPARTMENT_ID,
                dep.DEPARTMENT_NAME,
                dep.DEPT_ABBREVIATION
INTO #DEPT
FROM CLARITY.dbo.CLARITY_DEP_VIEW dep
WHERE dep.SERV_AREA_ID = '64'
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD';

/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
BLOCK 1: [FYI Flag]
[PAT_LEVEL]
Collects note with most recent FYI flag
Only SELECT patients with current active FYI flag
Not Constrained by date the flag was associated with patient
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
SELECT Pass1.PAT_ID,
       MIN(CASE WHEN Pass1.[PrEP D&R] = 'PrEP D&R' THEN 'D&R' ELSE 'Onsite' END) AS 'SITE',
       MAX(Pass1.[SA64 PrEP]) AS 'SA64 PrEP'
INTO #FYI_Patients
FROM (SELECT DISTINCT fyv.PATIENT_ID 'PAT_ID',
                      CASE WHEN fyv.PAT_FLAG_TYPE_C = '640005' THEN 'SA64 PrEP'
                      END AS 'SA64 PrEP',
                      CASE WHEN fyv.PAT_FLAG_TYPE_C = '640006' THEN 'PrEP D&R'
                      END AS 'PrEP D&R'
      FROM CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW fyv
      WHERE fyv.PAT_FLAG_TYPE_C IN ( '640005', '640006' )
            AND fyv.ACTIVE_C = '1') Pass1
GROUP BY Pass1.PAT_ID;

/*--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
BLOCK 2: [SMART DATA ELEMENTS] 
PAT LEVEL
NUANCE : 
1. SDE Capture Context is imperative e.g Notes vs Encounters. We need to UNION queries to capture CSN of each visit and get Visit Date
2. Multiple SDE's can be entered for a single patient on the same encounter. Logic will need to sort for the most recent entry.
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
SELECT pec.HLV_ID AS 'HLV_Record_ID',
       p.PAT_ID,
       p.PAT_NAME 'PATIENT_NAME',
       pe_enc.PAT_ENC_CSN_ID 'pat_enc_csn_id',
       pe_enc.CONTACT_DATE 'VISIT_DATE',
       pec.CUR_VALUE_DATETIME 'SDE_EDIT_DATE',
       pec.ELEMENT_ID 'SDE_ID',
       sev.SMRTDTA_ELEM_VALUE 'SDE_VALUE',
       sev.LINE 'SDE_VALUE_LINE',
       dep.SERV_AREA_ID 'ServAreaID'
INTO #pass1
FROM CLARITY.dbo.SMRTDTA_ELEM_DATA_VIEW pec
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = pec.PAT_LINK_ID
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pe_enc ON pec.CONTACT_SERIAL_NUM = pe_enc.PAT_ENC_CSN_ID
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pe_enc.DEPARTMENT_ID
    LEFT JOIN CLARITY.dbo.SMRTDTA_ELEM_VALUE_VIEW sev ON sev.HLV_ID = pec.HLV_ID
WHERE pec.ELEMENT_ID IN ( 'SA64#004', 'SA64#005' )
      AND pe_enc.CONTACT_DATE > DATEADD(MONTH, -24, GETDATE())
UNION
SELECT pec.HLV_ID 'HLV_Record_ID',
       p.PAT_ID 'PAT_Z_ID',
       p.PAT_NAME 'PATIENT_NAME',
       pe_note.PAT_ENC_CSN_ID 'pat_enc_csn_id',
       pe_note.CONTACT_DATE 'VISIT_DATE',
       pec.CUR_VALUE_DATETIME 'SDE_EDIT_DATE',
       pec.ELEMENT_ID 'SDE_ID',
       sev.SMRTDTA_ELEM_VALUE 'SDE_VALUE',
       sev.LINE 'SDE_VALUE_LINE',
       dep.SERV_AREA_ID 'ServAreaID'
FROM CLARITY.dbo.SMRTDTA_ELEM_DATA_VIEW pec
    INNER JOIN CLARITY.dbo.HNO_INFO_VIEW hno ON pec.RECORD_ID_VARCHAR = hno.NOTE_ID
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pe_note ON hno.PAT_ENC_CSN_ID = pe_note.PAT_ENC_CSN_ID
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pe_note.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = pec.PAT_LINK_ID
    LEFT JOIN CLARITY.dbo.SMRTDTA_ELEM_VALUE_VIEW sev ON sev.HLV_ID = pec.HLV_ID
WHERE pec.ELEMENT_ID IN ( 'SA64#004', 'SA64#005' )
      AND pe_note.CONTACT_DATE > DATEADD(MONTH, -24, GETDATE());

SELECT pass1.HLV_Record_ID,
       pass1.PAT_ID,
       pass1.PATIENT_NAME,
       pass1.pat_enc_csn_id,
       pass1.VISIT_DATE,
       pass1.SDE_EDIT_DATE,
       pass1.SDE_ID,
       pass1.SDE_VALUE,
       pass1.SDE_VALUE_LINE,
       pass1.ServAreaID,
       ROW_NUMBER() OVER (PARTITION BY pass1.PAT_ID,
                                       pass1.SDE_ID
                          ORDER BY pass1.SDE_EDIT_DATE DESC, /* Most recent date */
                                   pass1.SDE_VALUE_LINE DESC --Last line if more than one in a visit, but this is very rare (like once)
       ) AS 'Most_Recent_SDE_EDIT_PER_PAT'
INTO #pass2
FROM #pass1 pass1;

SELECT pass2.HLV_Record_ID,
       pass2.PAT_ID,
       pass2.PATIENT_NAME,
       pass2.pat_enc_csn_id,
       pass2.VISIT_DATE,
       pass2.SDE_EDIT_DATE,
       pass2.SDE_ID,
       pass2.SDE_VALUE,
       pass2.SDE_VALUE_LINE,
       pass2.ServAreaID,
       pass2.Most_Recent_SDE_EDIT_PER_PAT
INTO #pass3
FROM #pass2 pass2
WHERE pass2.Most_Recent_SDE_EDIT_PER_PAT = 1;

SELECT pass3.PAT_ID,
       pass3.PATIENT_NAME,
       MAX(
       CASE WHEN pass3.SDE_ID = 'SA64#005'
                 AND pass3.SDE_VALUE = 'Indicated for PrEP and Not Retained in care, due to:  {SA64 Not Retained Reason:18954}' THEN
                'Indicated for PrEP and Not Retained in care'
           WHEN pass3.SDE_ID = 'SA64#005' THEN pass3.SDE_VALUE
       END) AS 'Last_PrEP_Retention_Selection',
       CONVERT(VARCHAR(10), MAX(CASE WHEN pass3.SDE_ID = 'SA64#005' THEN pass3.VISIT_DATE END), 101) AS 'Last_PrEP_Retention_Selection_Visit_Date',
       MAX(CASE WHEN pass3.SDE_ID = 'SA64#004' THEN pass3.SDE_VALUE END) AS 'Last_PrEP_Non_Retention_Reason',
       CONVERT(VARCHAR(10), MAX(CASE WHEN pass3.SDE_ID = 'SA64#004' THEN pass3.VISIT_DATE END), 101) AS 'Last_PrEP_Non_Retention_Reason_Visit_Date',
       pass3.ServAreaID
INTO #SmartData_Pat_Level
FROM #pass3 pass3
GROUP BY pass3.PAT_ID,
         pass3.PATIENT_NAME,
         pass3.ServAreaID;

/*-------------------------------------------------------------------------------------------------------------------------------------------------------
BLOCK 3: [LAST VISIT & NEXT VISIT & PCP]
[PAT_LEVEL]
---------------------------------------------------------------------------------------------------------------------------------------------------------*/
SELECT p.PAT_ID,
       ISNULL(ser.PROV_ID, 'No Current PCP') 'CURRENT_PCP_ID',
       ISNULL(ser.PROV_NAME, 'No Current PCP') 'CURRENT_PCP_VAME',
       MAX(MAx_Enx.CONTACT_DATE) 'Last_Visit',
       MIN(Next_Enc.CONTACT_DATE) 'Next_Visit'
INTO #PCP_LAST_VISIT_INFO
FROM #FYI_Patients fyi
    INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = fyi.PAT_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
                                                  AND id.IDENTITY_TYPE_ID = '64'
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    LEFT JOIN (SELECT enc.PAT_ID,
                      MAX(enc.CONTACT_DATE) 'CONTACT_DATE'
               FROM CLARITY.dbo.PAT_ENC_VIEW enc
                   LEFT JOIN CLARITY.dbo.X_ERRONEOUS_ENCOUNTERS_VIEW EE ON enc.PAT_ENC_CSN_ID = EE.PAT_ENC_CSN_ID
               WHERE enc.APPT_STATUS_C IN ( 2, 6 )
                     AND EE.PAT_ENC_CSN_ID IS NULL
                     -- AND enc.SERV_AREA_ID = '64'
                     AND enc.CONTACT_DATE > DATEADD(MONTH, -24, GETDATE())
               GROUP BY enc.PAT_ID) MAx_Enx ON MAx_Enx.PAT_ID = p.PAT_ID
    LEFT JOIN (SELECT ev.PAT_ID,
                      MIN(ev.CONTACT_DATE) 'CONTACT_DATE'
               FROM CLARITY.dbo.PAT_ENC_VIEW ev
               WHERE ev.CONTACT_DATE > GETDATE()
               --AND ev.SERV_AREA_ID = '64'
               GROUP BY ev.PAT_ID) Next_Enc ON Next_Enc.PAT_ID = p.PAT_ID
GROUP BY p.PAT_ID,
         ser.PROV_ID,
         ser.PROV_NAME;

/*---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
BLOCK 4: [Denominator]
[ENC_LEVEL]
:Patients with at least one Medical Visit where on record where the status is arrived or completed
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
SELECT a.PAT_ID,
       a.PAT_ENC_CSN_ID,
       a.CONTACT_DATE AS 'MR_Contact_Date',
       a.DEPARTMENT_NAME AS 'MR_Contact_DEPT_NAME',
       a.DEPARTMENT_NAME,
       a.SERV_AREA_ID,
	   a.STATE,
			 a.CITY,
			 a.SERVICE_TYPE,
			a.SERVICE_LINE,
			 a.SUB_SERVICE_LINE
INTO #Encounter_Pop
FROM (SELECT pn.PAT_ID,
             pn.PAT_ENC_CSN_ID,
             ROW_NUMBER() OVER (PARTITION BY pn.PAT_ID, pn.PAT_ENC_CSN_ID ORDER BY pn.PAT_ID DESC) 'Enc_Row',
             pn.CONTACT_DATE,
             pn.COVERAGE_ID,
             c.PAYOR_ID,
             epm.PAYOR_NAME,
             CASE WHEN pn.APPT_PRC_ID IN ( '345', '3' ) THEN 'PREP_NEW_PATIENT_VISIT'
                 WHEN pn.APPT_PRC_ID IN ( '346', '1', '52', '56', '104', '219', '733' ) THEN 'PREP_FOLLOW_UP_VISIT'
                 ELSE 'OTHER_POTENTIAL_PREP_VISIT'
             END AS 'PREP_VISIT_TYPE',
             dep.DEPARTMENT_ID,
             dep.DEPARTMENT_NAME,
             dep2.SERV_AREA_ID,
			 ANALYTICS_DEP.STATE,
			 ANALYTICS_DEP.CITY,
			 ANALYTICS_DEP.SERVICE_TYPE,
			 ANALYTICS_DEP.SERVICE_LINE,
			 ANALYTICS_DEP.SUB_SERVICE_LINE
      FROM CLARITY.dbo.PAT_ENC_VIEW pn
          INNER JOIN #FYI_Patients fyi ON fyi.PAT_ID = pn.PAT_ID
          INNER JOIN #DEPT dep ON dep.DEPARTMENT_ID = pn.DEPARTMENT_ID
          INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep2 ON dep2.DEPARTMENT_ID = dep.DEPARTMENT_ID
		  INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping AS ANALYTICS_DEP ON dep2.DEPARTMENT_ID = ANALYTICS_DEP.DEPARTMENT_ID
          INNER JOIN CLARITY.dbo.PATIENT_VIEW p ON p.PAT_ID = pn.PAT_ID
          INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
                                                        AND id.IDENTITY_TYPE_ID = '64'
          LEFT JOIN CLARITY.dbo.X_ERRONEOUS_ENCOUNTERS_VIEW EE ON pn.PAT_ENC_CSN_ID = EE.PAT_ENC_CSN_ID
          LEFT JOIN CLARITY.dbo.COVERAGE c ON pn.COVERAGE_ID = c.COVERAGE_ID
          LEFT JOIN CLARITY.dbo.CLARITY_EPM epm ON c.PAYOR_ID = epm.PAYOR_ID
      WHERE pn.CONTACT_DATE
            BETWEEN DATEADD(DAY, -365, GETDATE()) AND GETDATE()
            AND dep2.SERV_AREA_ID = '64'
            AND pn.APPT_STATUS_C IN ( 2, 6 ) /*Only include Appointments marked Complete or Arrived*/
            AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
            AND EE.PAT_ENC_CSN_ID IS NULL) a
WHERE a.Enc_Row = 1;

/*-------------------------------------------------------------------------------------------------------------------------------------------------------
BLOCK 5 : 
TRUVADA MEDICATION
Patients with a Truvada Med Order before the Report End Date and who have been prescribed Truvada for a span of more than 3 months based on Start &  End times for Medication
---------------------------------------------------------------------------------------------------------------------------------------------------------*/
SELECT DISTINCT cur.PAT_ID
INTO #Current_Truvada_Pop
FROM CLARITY.dbo.PAT_ENC_CURR_MEDS_VIEW cur
    LEFT JOIN CLARITY.dbo.ORDER_MED_VIEW omv ON omv.ORDER_MED_ID = cur.CURRENT_MED_ID
    LEFT JOIN CLARITY.dbo.CLARITY_MEDICATION med ON omv.MEDICATION_ID = med.MEDICATION_ID
    LEFT JOIN CLARITY.dbo.INDICATIONS_OF_USE ios ON med.MEDICATION_ID = ios.MEDICATION_ID
WHERE ios.INDICATIONS_USE_ID IN ( 138, 3032, 4472 ) --HIV infection, HIV infection pre-exposure prophylaxis,prevention of HIV infection after exposure
      AND cur.Y_SERV_AREA_ID = 64
      AND DATEDIFF(DAY, omv.ORDERING_DATE, GETDATE()) <= 120 --Covers 90-day fills too.
GROUP BY cur.PAT_ID;

/*---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 BLOCK 6: [LAB DATA]
 [PA_LEVEL]
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
SELECT opv.PAT_ID,
       res.RESULT_DATE,
       res.ORD_VALUE,
       ROW_NUMBER() OVER (PARTITION BY opv.PAT_ID ORDER BY res.RESULT_DATE DESC) AS ROW_NUM_DESC
INTO #a
FROM CLARITY.dbo.ORDER_PROC_VIEW opv --INNER JOIN #FYI_Patients fyi ON fyi.PAT_ID = opv.PAT_ID
    INNER JOIN CLARITY.dbo.ORDER_RESULTS_VIEW res ON res.ORDER_PROC_ID = opv.ORDER_PROC_ID
    INNER JOIN CLARITY.dbo.CLARITY_COMPONENT cc ON res.COMPONENT_ID = cc.COMPONENT_ID
WHERE opv.ORDERING_DATE > DATEADD(MONTH, -24, GETDATE())
      AND cc.COMMON_NAME IN ( 'HIV1Ab/HIV2AB/ HIV Ag', 'HIV-1/2 AB/AG. 4TH GEN.', 'HIV ANTIGEN-ANTIBODY INTERPRETATION' );

SELECT a.PAT_ID,
       a.RESULT_DATE 'Last HIV Lab Date',
       a.ORD_VALUE 'Last HIV Lab Result'
INTO #labs
FROM #a a
WHERE a.ROW_NUM_DESC = 1;

/*-------------------------------------------------------------------------------------------------------------------------------------------------------
BLOCK 8: [COMBINED PAT LEVEL DATA]
---------------------------------------------------------------------------------------------------------------------------------------------------------*/
SELECT a.PAT_ID,
       a.SERV_AREA_ID 'SERV_AREA_ID',
       a.MR_Contact_Date 'MR_Contact_Date',
       a.MR_Contact_DEPT_NAME 'MR_Contact_DEPT_NAME',
	   a.STATE,
	   a.CITY,
	   a.SERVICE_TYPE,
	   a.SERVICE_LINE,
	   a.SUB_SERVICE_LINE
INTO #HIV_AND_ENC_Combined_Data_Pat_Level_Summary
FROM (SELECT d.PAT_ID,
             d.PAT_ENC_CSN_ID,
             ROW_NUMBER() OVER (PARTITION BY d.PAT_ID ORDER BY d.MR_Contact_Date DESC) 'Enc_Row',
             d.MR_Contact_Date,
             d.MR_Contact_DEPT_NAME,
             d.SERV_AREA_ID,
			 d.STATE,
			 d.CITY,
			 d.SERVICE_TYPE,
			 d.SERVICE_LINE,
			 d.SUB_SERVICE_LINE
      FROM #Encounter_Pop d) a
WHERE a.Enc_Row = 1;

/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FINAL SELECT: 
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
SELECT p.PAT_NAME 'Patient_Name',
       CASE WHEN truv.PAT_ID IS NOT NULL THEN 1
           ELSE 0
       END AS 'Current_Truvada_Med',
       id.IDENTITY_ID 'MRN',
       CAST(p.BIRTH_DATE AS DATE) 'DOB',
       (DATEDIFF(m, p.BIRTH_DATE, GETDATE()) / 12) 'Current_Age',
       zcs.ABBR 'Gender',
       zpr.NAME 'Patient_Race',
       zeg.NAME 'Ethnicity',
       CAST(labs.[Last HIV Lab Date] AS DATE) [Last HIV Lab Date],
       CONVERT(VARCHAR, (labs.[Last HIV Lab Result]), 101) [Last HIV Lab Result],
       hivpat.MR_Contact_DEPT_NAME,
	   hivpat.STATE,
	   hivpat.CITY,
	   hivpat.SERVICE_TYPE 'Service Type',
	   hivpat.SERVICE_LINE 'Service Line',
	   hivpat.SUB_SERVICE_LINE 'Sub Service Line',
       CAST(pcp.Last_Visit AS DATE) AS Last_Visit,
       fyip.[SA64 PrEP],
       fyip.SITE,
       smt.Last_PrEP_Retention_Selection,
       CAST(smt.Last_PrEP_Retention_Selection_Visit_Date AS DATE) AS Last_PrEP_Retention_Selection_Visit_Date,
       CASE WHEN smt.Last_PrEP_Retention_Selection = 'Indicated for PrEP and Not Retained in care' THEN smt.Last_PrEP_Non_Retention_Reason
           ELSE NULL
       END AS Last_PrEP_Non_Retention_Reason,
       CASE WHEN smt.Last_PrEP_Retention_Selection = 'Indicated for PrEP and Not Retained in care' THEN
                CAST(smt.Last_PrEP_Non_Retention_Reason_Visit_Date AS DATE)
           ELSE NULL
       END AS Last_PrEP_Non_Retention_Reason_Visit_Date,
       CAST(pcp.Next_Visit AS DATE) Next_Visit,
       pcp.CURRENT_PCP_VAME,
       CASE WHEN (pcp.CURRENT_PCP_VAME <> 'No Current PCP'
                  AND (hivpat.MR_Contact_Date > DATEADD(MONTH, -4, GETDATE()))
                  OR truv.PAT_ID IS NOT NULL
                  OR pcp.Next_Visit IS NOT NULL) THEN 'ACTIVE'
           ELSE 'INACTIVE'
       END AS 'PAT_PREP_STATUS',
       CAST(GETDATE() - 1 AS DATE) 'Report_Period_End',
       p.ZIP CURRENT_ZIP_CODE,
       zgi.NAME GENDER_IDENTITY,
       DATEDIFF(MONTH, pcp.Last_Visit, GETDATE()) 'MONTHS SINCE SEEN'
INTO #Final_Records
FROM CLARITY.dbo.PATIENT_VIEW p
    INNER JOIN CLARITY.dbo.PATIENT_4 p4 ON p4.PAT_ID = p.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_GENDER_IDENTITY zgi ON zgi.GENDER_IDENTITY_C = p4.GENDER_IDENTITY_C
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
                                                  AND id.IDENTITY_TYPE_ID = '64'
    INNER JOIN #HIV_AND_ENC_Combined_Data_Pat_Level_Summary hivpat ON hivpat.PAT_ID = p.PAT_ID
    LEFT JOIN #labs labs ON labs.PAT_ID = p.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_ETHNIC_GROUP zeg ON p.ETHNIC_GROUP_C = zeg.ETHNIC_GROUP_C
    LEFT JOIN CLARITY.dbo.PATIENT_RACE pr ON p.PAT_ID = pr.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN CLARITY.dbo.ZC_PATIENT_RACE zpr ON pr.PATIENT_RACE_C = zpr.PATIENT_RACE_C
    LEFT JOIN #Current_Truvada_Pop truv ON truv.PAT_ID = p.PAT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SA sa ON sa.SERV_AREA_ID = hivpat.SERV_AREA_ID
    LEFT JOIN #PCP_LAST_VISIT_INFO pcp ON pcp.PAT_ID = p.PAT_ID
    LEFT JOIN CLARITY.dbo.X_PAT_PCP_NO_TERM_DATE_VIEW xpcp ON xpcp.PAT_ID = p.PAT_ID
                                                              AND xpcp.PCP_TYPE_C = '1'
    LEFT JOIN [#SmartData_Pat_Level] smt ON smt.PAT_ID = p.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_SEX zcs ON zcs.RCPT_MEM_SEX_C = p.SEX_C
    LEFT JOIN #FYI_Patients fyip ON fyip.PAT_ID = p.PAT_ID;

SELECT * FROM #Final_Records

--WHERE
--	#Final_Records.MRN = '640019279'
;

/*---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CLEANUP
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
DROP TABLE #Encounter_Pop;
DROP TABLE #labs;
DROP TABLE #Final_Records;
DROP TABLE #Current_Truvada_Pop;
DROP TABLE #DEPT;
DROP TABLE #HIV_AND_ENC_Combined_Data_Pat_Level_Summary;
DROP TABLE #FYI_Patients;
DROP TABLE #PCP_LAST_VISIT_INFO;
DROP TABLE #SmartData_Pat_Level;
DROP TABLE #pass1;
DROP TABLE #pass2;
DROP TABLE #pass3;
DROP TABLE #a;
