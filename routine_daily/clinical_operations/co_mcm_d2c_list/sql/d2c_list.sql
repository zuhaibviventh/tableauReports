/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name: Medical - Data to Care (Tableau)
 Create Date: 2/4/2022
 Created By:  ViventHealth\MScoggins
 System:    ANL-MKE-SVR-100
 Requested By:  

 Purpose:   

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:   Changed By:     Change Description:
 ------------   -------------   ---------------------------------------------------
 2/4/2022       Mitch           Per Jess C. - updating 12 OOC to only check for appts with PCPs

**********************************************************************************************

 */

SET ANSI_WARNINGS OFF;
SET NOCOUNT ON;

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
WHERE pev.CONTACT_DATE > DATEADD(MONTH, -48, GETDATE())
      --AND pev.APPT_STATUS_C IN (2, 6)  --Since looking for pts with no compelted appts
      AND pev.ENC_TYPE_C NOT IN ( '32000', '119' ) --MyChart Encounters

;

SELECT a1.PAT_ID,
       a1.STATE,
       a1.CITY,
       a1.SITE,
       a1.LOS,
       ROW_NUMBER() OVER (PARTITION BY a1.PAT_ID ORDER BY a1.LAST_OFFICE_VISIT DESC) AS ROW_NUM_DESC
INTO #Attribution2
FROM #Attribution1 a1
WHERE a1.LOS = 'MEDICAL';

SELECT a2.PAT_ID, a2.LOS, a2.CITY, a2.STATE INTO #Attribution3 FROM #Attribution2 a2 WHERE a2.ROW_NUM_DESC = 1;

----To get pts not seen in the last 12 months-----

SELECT p.PAT_ID,
       p.PAT_NAME,
       p.CUR_PCP_PROV_ID,
       MAX(pev.CONTACT_DATE) Last_Visit
INTO #Third_Temp_Table
FROM Clarity.dbo.PATIENT_VIEW p
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON p.PAT_ID = pev.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
WHERE p.CUR_PCP_PROV_ID LIKE '64%'
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) = 'MD'
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs and NPs
GROUP BY p.PAT_ID,
         p.CUR_PCP_PROV_ID,
         p.PAT_NAME;


SELECT DISTINCT ttt.PAT_ID,
                'Not Seen in 12 mo' 'Reason'
INTO #Not_Seen_12mo
FROM #Third_Temp_Table ttt
WHERE ttt.Last_Visit < DATEADD(MM, -12, GETDATE());


---To get pts whose PCP was termed with the reason LTFU---------
SELECT p.PAT_ID,
       'LTFU' 'Reason',
       pcp.TERM_DATE LTFU_DATE
INTO #LTFU
FROM Clarity.dbo.PAT_PCP_VIEW pcp
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pcp.PCP_PROV_ID = ser.PROV_ID
                                                   AND ser.SERV_AREA_ID = 64
    LEFT JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = pcp.PAT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON id.PAT_ID = p.PAT_ID
                                                  AND id.IDENTITY_TYPE_ID = 64
WHERE pcp.TERM_DATE > DATEADD(MONTH, -12, GETDATE())
      AND p.CUR_PCP_PROV_ID IS NULL
      AND id.IDENTITY_TYPE_ID = 64
      AND pcp.SWITCH_REASON_C = 151;


SELECT PAT_ID, Reason, LTFU_DATE INTO #Forth_Temp_Table FROM #LTFU
UNION
SELECT PAT_ID, Reason, NULL AS LTFU_DATE FROM #Not_Seen_12mo;

------To add useful info to the Data gathered in the previous statements------------

SELECT id.IDENTITy_ID,
       p.PAT_ID,
       p.PAT_NAME,
       p.BIRTH_DATE,
       ftt.Reason,
       ser.PROV_NAME 'Current PCP',
       zpr.NAME RACE,
       sex.NAME SEX,
       zeg.NAME ETHNICITY,
       p.CUR_PRIM_LOC_ID,
       CASE WHEN ftt.LTFU_DATE IS NULL THEN '1900-01-01'
           ELSE ftt.LTFU_DATE
       END AS LTFU_DATE
INTO #a
FROM #Forth_Temp_Table ftt
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON ftt.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_SEX sex ON p.SEX_C = sex.RCPT_MEM_SEX_C
    LEFT JOIN Clarity.dbo.ZC_ETHNIC_GROUP zeg ON zeg.ETHNIC_GROUP_C = p.ETHNIC_GROUP_C
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON ftt.PAT_ID = id.PAT_ID
    LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON p.CUR_PCP_PROV_ID = ser.PROV_ID
    LEFT JOIN Clarity.dbo.PATIENT_RACE race ON p.PAT_ID = race.PAT_ID
                                               AND race.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON zpr.PATIENT_RACE_C = race.PATIENT_RACE_C
WHERE p.PAT_ID NOT IN ( SELECT DISTINCT flag.PATIENT_ID
                        FROM Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag
                        WHERE flag.ACTIVE_C = 1
                              AND (flag.PAT_FLAG_TYPE_C = '640005' --PrEP
                                   OR flag.PAT_FLAG_TYPE_C = '640008' --STI
                                   OR flag.PAT_FLAG_TYPE_C = '9800035' -- PEP
                                   OR flag.PAT_FLAG_TYPE_C = '640007') -- AODA HIV-
);

SELECT a.IDENTITY_ID,
       a.PAT_ID,
       a.PAT_NAME,
       a.BIRTH_DATE,
       a.Reason,
       a.[Current PCP],
       a.RACE,
       a.SEX,
       a.ETHNICITY,
       a.CUR_PRIM_LOC_ID,
       CAST(a.LTFU_DATE AS DATE) AS LTFU_DATE,
       CAST(CASE WHEN orv.ORD_VALUE IS NULL THEN NULL
           WHEN ISNUMERIC(orv.ORD_VALUE) = 1 THEN orv.ORD_VALUE
           WHEN orv.ORD_VALUE LIKE '>%' THEN 10000000.0
           ELSE 0.0
       END AS INT) AS 'Viral Load',
       ROW_NUMBER() OVER (PARTITION BY opv.PAT_ID ORDER BY orv.RESULT_DATE DESC) AS ROW_NUM_DESC
INTO #b
FROM #a a
    LEFT JOIN Clarity.dbo.ORDER_PROC_VIEW opv ON a.PAT_ID = opv.PAT_ID
    LEFT JOIN Clarity.dbo.ORDER_RESULTS_VIEW orv ON opv.ORDER_PROC_ID = orv.ORDER_PROC_ID
                                                    AND orv.COMPONENT_ID IN ( SELECT DISTINCT cc.COMPONENT_ID
                                                                              FROM CLARITY.dbo.CLARITY_COMPONENT cc
                                                                              WHERE cc.COMMON_NAME = 'HIV VIRAL LOAD' )
                                                    AND orv.ORD_VALUE NOT IN ( 'Delete', 'See comment' )
                                                    AND orv.RESULT_DATE >= DATEADD(MM, -48, GETDATE());


SELECT b.IDENTITY_ID MRN,
       --,b.PAT_ID
       b.PAT_NAME,
       CAST(b.BIRTH_DATE AS DATE) AS 'BIRTH DATE',
       b.Reason,
       b.[Current PCP] 'CURRENT PCP',
       b.RACE,
       b.SEX,
       b.ETHNICITY,
       loc.LOC_NAME 'PRIMARY LOCATION',
       COALESCE(b.[Viral Load], -9999) AS 'Viral Load',
       b.LTFU_DATE,
       a3.CITY,
       a3.STATE,
       lvis.[VISIT DATE] AS 'LAST VISIT BEFORE OOC',
       COALESCE(DATEDIFF(MONTH, lvis.[VISIT DATE], GETDATE()), -9999) 'MONTHS SINCE OOC',
       svis.[Next Any Appt] AS 'Next Any Appt',
       svis.[Next Appt Prov],
       spvis.[Next PCP Appt] AS 'Next PCP Appt',
       spvis.[Next PCP Appt Prov]
FROM #b b
    LEFT JOIN Clarity.dbo.CLARITY_LOC loc ON b.CUR_PRIM_LOC_ID = loc.LOC_ID
    INNER JOIN #Attribution3 a3 ON a3.PAT_ID = b.PAT_ID
    LEFT JOIN (SELECT pev.PAT_ID,
                      ser.PROV_NAME 'VISIT PROVIDER',
                      CAST(pev.CONTACT_DATE AS DATE) 'VISIT DATE',
                      dep.DEPARTMENT_NAME 'VISIT DEPT',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE DESC) AS ROW_NUM_DESC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                     AND pev.CONTACT_DATE < DATEADD(MONTH, -12, GETDATE())
                     AND pev.CONTACT_DATE > DATEADD(MONTH, -49, GETDATE())
                     AND ser.PROVIDER_TYPE_C IN ( 1, 6, 9, 113 ) -- Physicians, PAs and NPs

    ) lvis ON lvis.PAT_ID = b.PAT_ID
              AND lvis.ROW_NUM_DESC = 1
    LEFT JOIN (SELECT pev.PAT_ID,
                      CAST(pev.CONTACT_DATE AS DATE) AS 'Next Any Appt',
                      ser.PROV_NAME 'Next Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled

    ) svis ON svis.PAT_ID = b.PAT_ID
              AND svis.ROW_NUM_ASC = 1 -- First scheduled
    LEFT JOIN (SELECT pev.PAT_ID,
                      CAST(pev.CONTACT_DATE AS DATE) AS 'Next PCP Appt',
                      ser.PROV_NAME 'Next PCP Appt Prov',
                      ROW_NUMBER() OVER (PARTITION BY pev.PAT_ID ORDER BY pev.CONTACT_DATE ASC) AS ROW_NUM_ASC
               FROM Clarity.dbo.PAT_ENC_VIEW pev
                   INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
               WHERE pev.APPT_STATUS_C = 1 --Scheduled
                     AND ser.PROV_ID <> '640178' --pulmonologist
                     AND ser.PROVIDER_TYPE_C IN ( '1', '6', '9', '113' ) -- Physicians, PAs and NPs

    ) spvis ON spvis.PAT_ID = b.PAT_ID
               AND spvis.ROW_NUM_ASC = 1 -- First scheduled
WHERE b.ROW_NUM_DESC = 1;

DROP TABLE #Forth_Temp_Table;
DROP TABLE #Not_Seen_12mo;
DROP TABLE #LTFU;
DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #Attribution1;
DROP TABLE #Attribution2;
DROP TABLE #Attribution3;
DROP TABLE #Third_Temp_Table;

