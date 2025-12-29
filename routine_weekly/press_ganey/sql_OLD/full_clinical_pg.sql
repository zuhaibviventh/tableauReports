SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#full_cohort') IS NOT NULL DROP TABLE #full_cohort;
WITH
    unions AS (
        SELECT ##clinical_visits.* FROM ##clinical_visits
        UNION
        SELECT ##pharmacy_visits.* FROM ##pharmacy_visits
    )
SELECT unions.* INTO #full_cohort FROM unions;


IF OBJECT_ID('tempdb..#b') IS NOT NULL DROP TABLE #b;
SELECT a.SERV_TYPE,
       a.CLIENT_ID,
       a.PAT_LAST_NAME,
       a.PAT_FIRST_NAME,
       a.PAT_MIDDLE_NAME,
       a.ADD_LINE_1,
       a.ADD_LINE_2,
       CASE --Need to update this when texting goes live to allow lines with with only a phone number
           WHEN a.ADD_LINE_1 = ''
                AND a.EMAIL_ADDRESS = ''
                AND a.HOME_PHONE = '' THEN 0
           ELSE 1
       END AS SURVEY,
       a.CITY,
       a.STATE,
       a.ZIP,
       a.HOME_PHONE,
       a.UNIQUE_VISIT_ID,
       a.SVC_DATE,
       a.BIRTH_DATE,
       a.PAYOR,
       a.SEX,
       a.MED_REC,
       a.VISIT_PROV_ID,
       a.PROV_NAME,
       a.PROV_TYPE,
       a.DEPARTMENT_ID,
       a.DEPARTMENT_NAME,
       a.DEPT_ABBREVIATION,
       a.EMAIL_ADDRESS,
       a.LANGUAGE,
       MAX(a.PrEP) PrEP,
       MAX(a.Site) 'Site',
       CASE WHEN a.RACE IS NULL THEN 'Unknown'
           WHEN a.RACE = '' THEN 'Unknown'
           WHEN a.RACE = 'Not Collected or Unknown' THEN 'Unknown'
           WHEN a.RACE = 'Patient Refused' THEN 'Unknown'
           ELSE a.RACE
       END AS RACE,
       a.EOR
INTO #b
FROM #full_cohort a
GROUP BY a.SERV_TYPE,
         a.CLIENT_ID,
         a.PAT_LAST_NAME,
         a.PAT_FIRST_NAME,
         a.PAT_MIDDLE_NAME,
         a.ADD_LINE_1,
         a.ADD_LINE_2,
         a.CITY,
         a.PAYOR,
         a.STATE,
         a.LANGUAGE,
         a.ZIP,
         a.HOME_PHONE,
         a.UNIQUE_VISIT_ID,
         a.SVC_DATE,
         a.BIRTH_DATE,
         a.SEX,
         a.MED_REC,
         a.VISIT_PROV_ID,
         a.PROV_NAME,
         a.PROV_TYPE,
         a.DEPARTMENT_ID,
         a.DEPARTMENT_NAME,
         a.DEPT_ABBREVIATION,
         a.EMAIL_ADDRESS,
         a.EOR,
         a.RACE;


/* The order of columns in the test file must be the same as in the final file, so be wary of sending results from SQL directly */
SELECT DISTINCT b.SERV_TYPE,
                b.CLIENT_ID,
                b.PAT_LAST_NAME,
                b.PAT_FIRST_NAME,
                b.PAT_MIDDLE_NAME,
                b.ADD_LINE_1,
                b.ADD_LINE_2,
                b.CITY,
                b.STATE,
                b.ZIP,
                b.PAYOR,
                b.HOME_PHONE,
                b.UNIQUE_VISIT_ID,
                b.SVC_DATE,
                b.BIRTH_DATE,
                b.SEX,
                b.MED_REC,
                b.VISIT_PROV_ID,
                CASE WHEN b.PROV_NAME = 'BAKER, DAVID' THEN 'NICO BAKER'
                    WHEN b.PROV_NAME = 'HARPER IV, JAMES NICHOLOUS' THEN 'NICK HARPER'
                    ELSE b.PROV_NAME
                END AS PROV_NAME,
                b.PROV_TYPE,
                b.DEPARTMENT_ID,
                b.DEPARTMENT_NAME,
                CASE WHEN SUBSTRING(b.DEPT_ABBREVIATION, 3, 2) = 'MO' THEN 'MISSOURI'
                    WHEN SUBSTRING(b.DEPT_ABBREVIATION, 3, 2) = 'CO' THEN 'COLORADO'
                    WHEN SUBSTRING(b.DEPT_ABBREVIATION, 3, 2) = 'WI' THEN 'WISCONSIN'
                    WHEN SUBSTRING(b.DEPT_ABBREVIATION, 3, 2) = 'TX' THEN 'TEXAS'
                    WHEN SUBSTRING(b.DEPT_ABBREVIATION, 3, 2) = 'IL' THEN 'ILLINOIS'
                    ELSE 'ERROR'
                END AS Service_State,
                b.EMAIL_ADDRESS,
                b.LANGUAGE,
                b.PrEP,
                b.Site,
                b.RACE,
                b.EOR
FROM #b b
WHERE b.SURVEY = 1;
