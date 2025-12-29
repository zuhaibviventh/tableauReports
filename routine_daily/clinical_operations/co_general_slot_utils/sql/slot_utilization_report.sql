/*
Project:            Operations Report - Tableau
Query:              slot_utilization_report.sql
Description:        To show how providers are do with filling their schedules.

Author:             Mitch Scoggins
System:             javelin.ochin.org
Dependencies:       None.
Inclusion Criteria: 
Exclusion Criteria:
Timeframe:
Comments:           This report will need maintanence when new providers or new departments are added.
                    LOGIC FOR COUNTING SLOTS AND APPOINTMENTS
                     Case                               Counted In Slots    Counted in Appts
                    Opening = 1, Unavailable = 0, Held = 0 and Appt = 0 Y   N
                    Opening = 1, Unavailable = 1, Held = 0 and Appt = 0 N   N
                    Opening = 1, Unavailable = 0, Held = 1 and Appt = 0 N   N
                    Opening = 1, Unavailable = 0, Held = 0 and Appt 1+  Y   Y
                    Opening = 1, Unavailable = 0, Held = 0, Appt 1+ & Outside Template = Y  ??  Y
                
                    Opening = 0, Unavailable = 0, Held = 0 and Appt = 0 N   N
                    Opening = 0, Unavailable = 0, Held = 0 and Appt 1+  N   Y
                    Opening = 0, Unavailable = 1, Held = 0 and Appt 1+  N   Y
                    Opening = 0, Unavailable = 0, Held = 1 and Appt 1+  N   Y
                    Opening = 0, Unavailable = 0, Held = 0,  Appt 1+ & Outside Template = Y ??  Y
                
                    If there is an opening, it is counted toward slots unless unavailable or held.      
                    Regardless of any other condition, book appointments count in the quantity booked into that slot.
                    Requested by Leslie.
                    Created on 81/25/2021
*/

SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

/* Slot Count */
SELECT ser.PROV_NAME,
       av.PROV_ID,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWUAKEE'
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
       END AS DEPARTMENT_NAME,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2)
       END AS 'LOS',
       av.DEPARTMENT_ID,
       CASE WHEN ser.PROV_TYPE = 'Counselor' THEN 'AODA Counselor'
           WHEN ser.PROV_TYPE = 'Certified Clinical Medical Assistant' THEN 'Medical Assistant'
           WHEN ser.PROV_TYPE = 'Nutritionist' THEN 'Nutritionist/Dietitian'
           WHEN ser.PROV_TYPE = 'Dietitian' THEN 'Nutritionist/Dietitian'
           WHEN ser.PROVIDER_TYPE_C IN ( '136', '164' ) THEN 'Psychiatrist/Psych NP'
           WHEN ser.PROV_TYPE IN ( 'Licensed Professional Counselor', 'Licensed Clinical Social Worker', 'Licensed Master Social Worker',
                                   'Licensed Marriage and Family Therapist' ) THEN 'Psychotherapist'
           ELSE ser.PROV_TYPE
       END AS PROV_TYPE,
       av.ORG_REG_OPENINGS Slots,
       av.SLOT_LENGTH Slot_Minutes,
       av.SLOT_BEGIN_TIME,
       ser.PROVIDER_TYPE_C
INTO #First_Temp_Table
FROM Clarity.dbo.AVAILABILITY_VIEW av
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON av.PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON av.DEPARTMENT_ID = dep.DEPARTMENT_ID
WHERE av.SLOT_BEGIN_TIME
      BETWEEN '1/1/2021' AND DATEADD(DAY, -2, GETDATE())
      AND ser.SERV_AREA_ID = 64
      AND ser.ACTIVE_STATUS_C = 1 -- Active Providers
      AND av.APPT_NUMBER = 0
      AND av.UNAVAILABLE_RSN_C IS NULL
      AND av.TIME_HELD_YN <> 'Y'
      AND ser.PROVIDER_TYPE_C NOT IN ( '0', '125', '158', '157', '156', '121', '222', '137' )
      AND ser.PROV_ID NOT IN ( '6499991', '640712', '6400096' ) --ARCW Fix and Test Physican, EOE
      AND ser.PROV_NAME NOT LIKE '%DBLBK' --dental doublebooks schedules
;

/* Booked Slots Count (OCHIN excludes canceled) */
SELECT ser.PROV_NAME,
       av.PROV_ID,
       av.DEPARTMENT_ID,
       av.NUM_APTS_SCHEDULED,
       av.SLOT_LENGTH Appt_Minutes,
       pev.PAT_ENC_CSN_ID,
       av.SLOT_BEGIN_TIME,
       CASE WHEN ser.PROV_TYPE = 'Counselor' THEN 'AODA Counselor'
           WHEN ser.PROV_TYPE = 'Certified Clinical Medical Assistant' THEN 'Medical Assistant'
           WHEN ser.PROV_TYPE = 'Nutritionist' THEN 'Nutritionist/Dietitian'
           WHEN ser.PROV_TYPE = 'Dietitian' THEN 'Nutritionist/Dietitian'
           WHEN ser.PROVIDER_TYPE_C IN ( '136', '164' ) THEN 'Psychiatrist/Psych NP'
           WHEN ser.PROV_TYPE IN ( 'Licensed Professional Counselor', 'Licensed Clinical Social Worker', 'Licensed Master Social Worker',
                                   'Licensed Marriage and Family Therapist' ) THEN 'Psychotherapist'
           ELSE ser.PROV_TYPE
       END AS PROV_TYPE,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 5, 2) = 'MK' THEN 'MILWUAKEE'
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
       END AS DEPARTMENT_NAME,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD' THEN 'MEDICAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'DT' THEN 'DENTAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'CM' THEN 'CASE MANAGEMENT'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'RX' THEN 'PHARMACY'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'AD' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'PY' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'BH' THEN 'BEHAVIORAL'
           WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MH' THEN 'BEHAVIORAL'
           ELSE SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2)
       END AS 'LOS'
INTO #Second_Temp_Table
FROM Clarity.dbo.AVAILABILITY_VIEW av
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON pev.PAT_ENC_CSN_ID = av.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON av.DEPARTMENT_ID = dep.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON av.PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = av.PAT_ID
WHERE av.SLOT_BEGIN_TIME
      BETWEEN '1/1/2021' AND DATEADD(DAY, -2, GETDATE())
      AND ser.SERV_AREA_ID = 64
      AND ser.ACTIVE_STATUS_C = 1 -- Active Providers
      AND av.APPT_NUMBER > 0 /*Slots w appt booked only*/
      AND pev.APPT_STATUS_C <> 3
      AND ser.PROVIDER_TYPE_C NOT IN ( '0', '125', '158', '157', '156', '121', '222', '137' )
      AND ser.PROV_ID NOT IN ( '6499991', '640712', '6400096' ) --ARCW Fix and Test Physican, EOE
      AND ser.PROV_NAME NOT LIKE '%DBLBK' --dental doublebook schedules
;

SELECT CASE WHEN ftt.PROV_NAME IS NULL THEN stt.PROV_NAME ELSE ftt.PROV_NAME END AS 'PROVIDER',
       CASE WHEN ftt.PROV_ID IS NULL THEN stt.PROV_ID ELSE ftt.PROV_ID END AS 'PROV_ID',
       CASE WHEN ftt.DEPARTMENT_NAME IS NULL THEN stt.DEPARTMENT_NAME ELSE ftt.DEPARTMENT_NAME END AS DEPARTMENT,
       CASE WHEN ftt.LOS IS NULL THEN stt.LOS ELSE ftt.LOS END AS LOS,
       CASE WHEN ftt.DEPARTMENT_ID IS NULL THEN stt.DEPARTMENT_ID ELSE ftt.DEPARTMENT_ID END AS DEPT_ID,
       CASE WHEN ftt.PROV_TYPE IS NULL THEN stt.PROV_TYPE ELSE ftt.PROV_TYPE END AS 'PROVIDER TYPE',
       CASE WHEN ftt.Slots IS NULL THEN 0 ELSE ftt.Slots END AS 'APPOINTMENT SLOTS',
       CASE WHEN ftt.Slot_Minutes IS NULL THEN 0 ELSE ftt.Slot_Minutes END AS 'SLOT MINUTES',
       CASE WHEN ftt.SLOT_BEGIN_TIME IS NULL THEN stt.SLOT_BEGIN_TIME ELSE ftt.SLOT_BEGIN_TIME END AS 'APPOINTMENT TIME',
       CASE WHEN stt.NUM_APTS_SCHEDULED IS NULL THEN 0
           WHEN stt.NUM_APTS_SCHEDULED > 1 THEN 1 -- This is since each appt gets it's own line so can sum for total appts
           ELSE stt.NUM_APTS_SCHEDULED
       END AS 'NUM APPTS SCHEDULED',
       CASE WHEN stt.Appt_Minutes IS NULL THEN 0 ELSE stt.Appt_Minutes END AS 'APPOINTMENT MINUTES',
       stt.PAT_ENC_CSN_ID
FROM #First_Temp_Table ftt
    FULL OUTER JOIN #Second_Temp_Table stt ON stt.PROV_ID = ftt.PROV_ID
                                              AND stt.SLOT_BEGIN_TIME = ftt.SLOT_BEGIN_TIME
                                              AND stt.DEPARTMENT_ID = ftt.DEPARTMENT_ID;

DROP TABLE #First_Temp_Table;
DROP TABLE #Second_Temp_Table;
