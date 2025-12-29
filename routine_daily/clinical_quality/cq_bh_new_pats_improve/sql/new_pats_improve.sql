/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: BH-Depression - New Patients and Symptom Remission (6 Mo)
 Create Date: 3/31/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  

 Purpose:   

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 1/27/2023      Mitch       Update inclusion criteria to include BH/MH episodes that were closed during the last 12 months
 1/27/2023      Mitch       Update the check for last visit to 12 months for both Psych and MHT
 3/05/2024      Benzon      Added patient race and ethnicity

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


SELECT pev.PAT_ID,
       pev.CONTACT_DATE LAST_OFFICE_VISIT,
       dep.STATE,
       dep.CITY,
       dep.SITE,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
           ELSE 'ERROR'
       END AS 'LOS',
       dep.SERVICE_TYPE,
       dep.SERVICE_LINE,
       dep.SUB_SERVICE_LINE
INTO #Attribution1
FROM Clarity.dbo.PAT_ENC_VIEW pev
    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -36, GETDATE())
      AND pev.APPT_STATUS_C IN ( 2, 6 );

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       a1.SERVICE_TYPE,
       a1.SERVICE_LINE,
       a1.SUB_SERVICE_LINE,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS IN ( 'BEHAVIORAL' );

SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE,a2.SERVICE_TYPE, a2.SERVICE_LINE,a2.SUB_SERVICE_LINE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;


SELECT DISTINCT a3.PAT_ID,
                id.IDENTITY_ID MRN,
                p.PAT_NAME PATIENT,
                serpcp.PROV_NAME PCP,
                a3.STATE,
                a3.CITY,
                a3.SERVICE_TYPE,
                a3.SERVICE_LINE,
                a3.SUB_SERVICE_LINE,
                --,icd10.CODE 'DEPRESSION DX'
                CASE WHEN icd10.CODE IN ( 'F32.4', 'F32.5', 'F32.9', 'F33', 'F33.0', 'F33.1', 'F33.2', 'F33.3', 'F33.4', 'F33.41', 'F33.42', 'F33.9' ) -- Provided BY Caren E. (MDD list)
                THEN     'Y'
                    ELSE 'N'
                END AS 'MAJOR DEPRESSIVE DISORDER',
                bh.[BHMH EPISODE START],
                CASE WHEN DATEDIFF(MONTH, bh.[BHMH EPISODE START], GETDATE()) < 19 THEN 'Y'
                    ELSE 'N'
                END AS 'EPISODE NEW IN MEASUREMENT PERIOD',
                ctp.PSYCHIATRY 'PSYCHIATRIST',
                ctm.MH_TEAM_MEMBER 'MH THERAPIST',
                p.ZIP,
                bh.[Episode Status]
INTO #a
FROM #Attribution3 a3
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = a3.PAT_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW plv ON a3.PAT_ID = plv.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_EDG edg ON plv.DX_ID = edg.DX_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 icd10 ON plv.DX_ID = icd10.DX_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
    INNER JOIN (SELECT DISTINCT ev.PAT_LINK_ID PAT_ID,
                                ev.START_DATE 'BHMH EPISODE START',
                                CASE WHEN ev.STATUS_C = 1 THEN 'Open'
                                    ELSE 'Closed'
                                END AS 'Episode Status',
                                ROW_NUMBER() OVER (PARTITION BY ev.PAT_LINK_ID ORDER BY ev.START_DATE ASC) AS ROW_NUM_ASC -- To get First Episode if more than one

                FROM Clarity.dbo.EPISODE_VIEW ev
                    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = ev.PAT_LINK_ID
                    INNER JOIN ANALYTICS.TRANSFORM.DepartmentMapping dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                WHERE ev.SUM_BLK_TYPE_ID = 221
                      --AND ev.STATUS_C = 1 -- Want inactive pts too
                      AND ev.START_DATE
                      BETWEEN DATEADD(MONTH, -18, GETDATE()) AND DATEADD(MONTH, -6, GETDATE()) --Episodes that have been open for at least six months
                                                                                               --AND (ev.END_DATE IS NULL
                                                                                               -- OR ev.END_DATE > DATEADD(MONTH, -18, GETDATE())) --Do not need this since chacking start date
                                                                                               --AND pev.APPT_STATUS_C IN (2, 6)
                      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'MH', 'BH', 'PY' )) bh ON bh.PAT_ID = a3.PAT_ID
    LEFT JOIN (SELECT ct.PAT_ID,
                      ROW_NUMBER() OVER (PARTITION BY ct.PAT_ID ORDER BY ct.EFF_DATE DESC) AS ROW_NUM_DESC,
                      ser.PROV_NAME 'PSYCHIATRY'
               FROM Clarity.dbo.PAT_PCP_VIEW ct
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = ct.PCP_PROV_ID
               WHERE ct.RELATIONSHIP_C IN ( 1, 3 )
                     AND (ct.TERM_DATE IS NULL OR ct.TERM_DATE > DATEADD(MONTH, -12, GETDATE()))
                     AND ser.PROVIDER_TYPE_C IN ( '136', '164', '129' )) ctp ON ctp.PAT_ID = a3.PAT_ID
                                                                                AND ctp.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT ct.PAT_ID,
                      ROW_NUMBER() OVER (PARTITION BY ct.PAT_ID ORDER BY ct.EFF_DATE DESC) AS ROW_NUM_DESC,
                      ser.PROV_NAME 'MH_TEAM_MEMBER'
               FROM Clarity.dbo.PAT_PCP_VIEW ct
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON ser.PROV_ID = ct.PCP_PROV_ID
               WHERE ct.RELATIONSHIP_C IN ( 1, 3 )
                     AND (ct.TERM_DATE IS NULL OR ct.TERM_DATE > DATEADD(MONTH, -12, GETDATE()))
                     AND ser.PROVIDER_TYPE_C NOT IN ( '136', '164', '129' )) ctm ON ctm.PAT_ID = a3.PAT_ID
                                                                                    AND ctm.ROW_NUM_DESC = 1
WHERE icd10.CODE IN ( SELECT vic.ICD_CODES_LIST FROM Clarity.dbo.VCG_ICD_CODES vic WHERE vic.GROUPER_ID = '2100000117' AND vic.CODE_SET_C = 2 --ICD10
)
      AND bh.ROW_NUM_ASC = 1;

--This step is just to remove the 'N's for pts who have both Y and N for MDD
SELECT a.PAT_ID,
       a.MRN,
       a.PATIENT,
       COALESCE(a.PCP, 'Non Medical Pt') AS 'PCP',
       a.STATE,
       a.CITY,
       a.SERVICE_TYPE,
       a.SERVICE_LINE,
       a.SUB_SERVICE_LINE,
       MAX(a.[MAJOR DEPRESSIVE DISORDER]) 'MAJOR DEPRESSIVE DISORDER', --To pick Y for pts who have both Y and N
       a.[BHMH EPISODE START],
       a.[EPISODE NEW IN MEASUREMENT PERIOD],
       a.PSYCHIATRIST,
       a.[MH THERAPIST],
       a.ZIP,
       a.[Episode Status]
INTO #b
FROM #a a
GROUP BY a.PAT_ID,
         a.MRN,
         a.PATIENT,
         a.PCP,
         a.STATE,
         a.CITY,
         a.SERVICE_TYPE,
         a.SERVICE_LINE,
         a.SUB_SERVICE_LINE,
         a.[BHMH EPISODE START],
         a.[EPISODE NEW IN MEASUREMENT PERIOD],
         a.PSYCHIATRIST,
         a.[MH THERAPIST],
         a.ZIP,
         a.[Episode Status];


SELECT b.PAT_ID,
       b.MRN,
       b.PATIENT,
       b.PCP,
       b.STATE,
       b.CITY,
       b.SERVICE_TYPE,
       b.SERVICE_LINE,
       b.SUB_SERVICE_LINE,
       b.[MAJOR DEPRESSIVE DISORDER],
       b.[BHMH EPISODE START],
       b.[EPISODE NEW IN MEASUREMENT PERIOD],
       b.PSYCHIATRIST,
       b.[MH THERAPIST],
       phq.[PHQ9 DATE],
       phq.[PHQ9 Score] 'PHQ9 SCORE',
       DATEDIFF(MONTH, b.[BHMH EPISODE START], phq.[PHQ9 DATE]) 'MONTHS SINCE PROGRAM START',
       ABS(DATEDIFF(DAY, b.[BHMH EPISODE START], phq.[PHQ9 DATE])) 'DAYS SINCE PROGRAM START',
       ABS(DATEDIFF(DAY, (DATEADD(MONTH, 6, b.[BHMH EPISODE START])), phq.[PHQ9 DATE])) 'DAYS SINCE PROGRAM FU DATE',
       b.ZIP,
       b.[Episode Status]
INTO #c
FROM #b b
    LEFT JOIN (SELECT ifrv.PAT_ID,
                      ifrv.RECORD_DATE 'PHQ9 DATE',
                      MAX(meas.MEAS_VALUE) 'PHQ9 Score' -- Highest Score per PHQ Day
               FROM Clarity.dbo.IP_FLWSHT_REC_VIEW ifrv
                   INNER JOIN Clarity.dbo.IP_FLWSHT_MEAS_VIEW meas ON ifrv.FSD_ID = meas.FSD_ID
               WHERE meas.FLO_MEAS_ID IN ( '1043', '1044', '3011', '3608' ) -- PHQ2 & 9
                     AND ifrv.RECORD_DATE > DATEADD(MONTH, -20, GETDATE()) --minus 20 to catch two months prior to start of measurement period
               GROUP BY ifrv.PAT_ID,
                        ifrv.RECORD_DATE) phq ON phq.PAT_ID = b.PAT_ID;


SELECT c.PAT_ID,
       c.MRN,
       c.PATIENT,
       c.PCP,
       c.STATE,
       c.CITY,
       c.SERVICE_TYPE,
       c.SERVICE_LINE,
       c.SUB_SERVICE_LINE,
       c.[MAJOR DEPRESSIVE DISORDER],
       c.[BHMH EPISODE START],
       c.[EPISODE NEW IN MEASUREMENT PERIOD],
       c.[Episode Status],
       c.PSYCHIATRIST,
       c.[MH THERAPIST],
       c.[PHQ9 DATE],
       c.[PHQ9 SCORE],
       c.[MONTHS SINCE PROGRAM START],
       c.[DAYS SINCE PROGRAM START],
       c.ZIP,
       ROW_NUMBER() OVER (PARTITION BY c.PAT_ID ORDER BY c.[DAYS SINCE PROGRAM START] ASC) AS ROW_NUM_ASC_Start
INTO #init
FROM #c c
WHERE c.[MONTHS SINCE PROGRAM START] IN ( 0, 1, 2 );

SELECT c.PAT_ID,
       c.MRN,
       c.PATIENT,
       c.PCP,
       c.STATE,
       c.CITY,
       c.SERVICE_TYPE,
       c.SERVICE_LINE,
       c.SUB_SERVICE_LINE,
       c.[MAJOR DEPRESSIVE DISORDER],
       c.[BHMH EPISODE START],
       c.[EPISODE NEW IN MEASUREMENT PERIOD],
       c.[Episode Status],
       c.PSYCHIATRIST,
       c.[MH THERAPIST],
       c.[PHQ9 DATE],
       c.[PHQ9 SCORE],
       c.[MONTHS SINCE PROGRAM START],
       c.[DAYS SINCE PROGRAM START],
       c.[DAYS SINCE PROGRAM FU DATE],
       ROW_NUMBER() OVER (PARTITION BY c.PAT_ID ORDER BY c.[DAYS SINCE PROGRAM FU DATE] ASC) AS ROW_NUM_ASC_FU
INTO #d
FROM #c c
WHERE c.[MONTHS SINCE PROGRAM START] IN ( 5, 6, 7 );

SELECT DISTINCT c.PAT_ID,
                c.MRN,
                c.PATIENT,
                c.PCP,
                c.STATE,
                c.CITY,
                c.SERVICE_TYPE,
                c.SERVICE_LINE,
                c.SUB_SERVICE_LINE,
                c.[MAJOR DEPRESSIVE DISORDER],
                c.[BHMH EPISODE START],
                c.[EPISODE NEW IN MEASUREMENT PERIOD],
                c.[Episode Status],
                c.PSYCHIATRIST,
                c.[MH THERAPIST],
                init.[PHQ9 DATE] 'Initial PHQ Date',
                init.[PHQ9 SCORE] 'Initial PHQ Score',
                fu.[PHQ9 DATE] 'Follow-up PHQ Date',
                fu.[PHQ9 SCORE] 'Follow-up PHQ9 Score',
                CASE WHEN init.[PHQ9 SCORE] IS NULL THEN NULL
                    WHEN fu.[PHQ9 SCORE] IS NULL THEN NULL
                    WHEN init.[PHQ9 SCORE] = 0 THEN fu.[PHQ9 SCORE]
                    WHEN fu.[PHQ9 SCORE] = 0 THEN 0 - init.[PHQ9 SCORE]
                    ELSE CAST(fu.[PHQ9 SCORE] AS DECIMAL(7, 2)) / CAST(init.[PHQ9 SCORE] AS DECIMAL(7, 2))
                END AS 'Change',
                c.ZIP,
                COALESCE(##patient_race_ethnicity.RACE_CATEGORY, 'Unknown') AS RACE_CATEGORY,
                COALESCE(##patient_race_ethnicity.ETHNICITY_CATEGORY, 'Unknown') AS ETHNICITY_CATEGORY
INTO #final
FROM #c c
    LEFT JOIN #init init ON init.PAT_ID = c.PAT_ID
                            AND init.ROW_NUM_ASC_Start = 1
    LEFT JOIN #d fu ON fu.PAT_ID = c.PAT_ID
                       AND fu.ROW_NUM_ASC_FU = 1
    LEFT JOIN ##patient_race_ethnicity ON c.PAT_ID = ##patient_race_ethnicity.PAT_ID;


SELECT f.PAT_ID,
       f.MRN,
       f.PATIENT,
       f.PCP,
       f.STATE 'State',
       f.CITY 'City',
       f.SERVICE_TYPE 'Service Type',
       f.SERVICE_LINE 'Service Line',
       f.SUB_SERVICE_LINE 'Sub-Service Line',
       f.[MAJOR DEPRESSIVE DISORDER],
       COALESCE(CAST(f.[BHMH EPISODE START] AS DATE), '1900-01-01') AS 'BHMH EPISODE START',
       f.[EPISODE NEW IN MEASUREMENT PERIOD],
       f.[Episode Status],
       f.PSYCHIATRIST,
       f.[MH THERAPIST],
       COALESCE(CAST(f.[Initial PHQ Date] AS DATE), '1900-01-01') AS 'Initial PHQ Date',
       COALESCE(f.[Initial PHQ Score], 0) AS 'Initial PHQ Score',
       COALESCE(CAST(f.[Follow-up PHQ Date] AS DATE), '1900-01-01') AS 'Follow-up PHQ Date',
       COALESCE(f.[Follow-up PHQ9 Score], 0) AS 'Follow-up PHQ9 Score',
       f.Change,
       f.ZIP,
       f.RACE_CATEGORY,
       f.ETHNICITY_CATEGORY,
       CASE WHEN f.Change IS NULL THEN 'Unmeasurable'
           WHEN f.Change <= 0.5 THEN 'Met'
           ELSE 'Not Met'
       END AS 'Outcome'
FROM #final f
WHERE f.[EPISODE NEW IN MEASUREMENT PERIOD] = 'Y';

DROP TABLE #Attribution3;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #c;
DROP TABLE #init;
DROP TABLE #d;
DROP TABLE #final;
