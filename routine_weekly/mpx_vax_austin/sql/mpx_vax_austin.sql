SELECT id.IDENTITY_ID AS Patient_Id,
       CONVERT(NVARCHAR(30), p.BIRTH_DATE, 101) AS Patient_DOB,
       p.CITY AS Patient_Address_City,
       zs.ABBR AS Patient_Address_State,
       LEFT(p.ZIP, 5) AS Patient_Address_Zipcode,
       zc.NAME AS Patient_Address_County,
       COALESCE(zbs.NAME, sex.NAME) AS Patient_Sex_at_Birth,
       CASE 
           WHEN zgi.NAME IS NULL THEN 'Prefer not to answer'
           WHEN zgi.NAME IS NULL THEN 'Prefer not to answer'
           WHEN zgi.NAME = 'Male' THEN 'Male'
           WHEN zgi.NAME = 'Female' THEN 'Female'
           WHEN zgi.NAME = 'Questioning' THEN 'Questioning'
           WHEN zgi.NAME = 'Transgender Female / Male-to-Female' THEN 'Transgender female'
           WHEN zgi.NAME = 'Transgender Male / Female-to-Male' THEN 'Transgender Male'
           WHEN zgi.NAME = 'Non-binary/genderqueer' THEN 'Genderqueer'
           ELSE 'Prefer not to answer'
       END AS Patient_Gender_Identity,
       CASE 
           WHEN zpr.NAME IS NULL THEN 'Prefer not to answer'
           WHEN zpr.NAME IN ( 'American Indian', 'Alaskan Native' ) THEN 'American Indian or Alaska Native'
           WHEN zpr.NAME LIKE 'Black/African American%' THEN 'Black or African American'
           WHEN zpr.NAME = 'Asian' THEN 'Asian'
           WHEN zpr.NAME IN ( 'Native Hawaiian', 'Pacific Islander' ) THEN 'Native Hawaiian or Other Pacific Islander'
           WHEN zpr.NAME = 'White' THEN 'White'
           ELSE 'Prefer not to answer'
       END AS Patient_Race,
       CASE 
           WHEN zeg.NAME IS NULL THEN 'Prefer not to answer'
           WHEN zeg.NAME = 'Hispanic or Latino/a' THEN 'Yes'
           WHEN zeg.NAME = 'Non-Hispanic or Latino/a' THEN 'No'
           ELSE 'Prefer not to answer'
       END AS Patient_Ethnicity,
       'Vivent Health' AS Vaccine_Provider,
       'Vivent Health Austin' AS Vaccine_Provider_Site,
       78752 AS Vaccine_Provider_Site_Zipcode,
       'Travis' AS Vaccine_Provider_Site_County,
       CONVERT(NVARCHAR(30), iv.IMMUNE_DATE, 101) AS Vaccination_Date,
       'Dose' + ' ' + COALESCE(dose.QUEST_ANSWER, '1') AS Dose_Number,
       'Jynneos' AS Vaccine_manufacturer,
       iv.LOT AS Vaccine_lot_number
FROM Clarity.dbo.IMMUNE_VIEW iv
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON iv.IMM_CSN = pev.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON iv.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON p.PAT_ID = id.PAT_ID
    --LEFT JOIN Clarity.dbo.CLARITY_SER_VIEW serpcp ON p.CUR_PCP_PROV_ID = serpcp.PROV_ID
    --LEFT JOIN Clarity.dbo.CLARITY_EMP emp ON iv.GIVEN_BY_USER_ID = emp.USER_ID
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON pr.PAT_ID = iv.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON zpr.PATIENT_RACE_C = pr.PATIENT_RACE_C
    LEFT JOIN Clarity.dbo.ZC_ETHNIC_GROUP zeg ON zeg.ETHNIC_GROUP_C = p.ETHNIC_GROUP_C
    LEFT JOIN Clarity.dbo.ZC_STATE zs ON zs.STATE_C = p.STATE_C
    LEFT JOIN Clarity.dbo.ZC_COUNTY zc ON zc.COUNTY_C = p.COUNTY_C
    INNER JOIN Clarity.dbo.PATIENT_4 p4 ON p4.PAT_ID = iv.PAT_ID
    LEFT JOIN Clarity.dbo.ZC_SEX_ASGN_AT_BIRTH zbs ON zbs.SEX_ASGN_AT_BIRTH_C = p4.SEX_ASGN_AT_BIRTH_C
    LEFT JOIN Clarity.dbo.ZC_SEX sex ON p.SEX_C = sex.RCPT_MEM_SEX_C
    LEFT JOIN Clarity.dbo.ZC_GENDER_IDENTITY zgi ON zgi.GENDER_IDENTITY_C = p4.GENDER_IDENTITY_C
    LEFT JOIN Clarity.dbo.CL_QANSWER_QA dose ON iv.IMM_ANSWER_ID = dose.ANSWER_ID
                                                AND dose.QUEST_ID = '155000'
WHERE iv.IMMUNZATN_ID = 781 --SMALLPOX MONKEYPOX VACCINE (NATIONAL STOCKPILE)
      AND iv.IMMNZTN_STATUS_C = 1 --Given
      AND (iv.IMM_HISTORIC_ADM_YN IS NULL OR iv.IMM_HISTORIC_ADM_YN = 'N') --Not given elsewhere
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 3, 2) = 'TX'
      AND iv.IMMUNE_DATE > DATEADD(DAY, -9, GETDATE())
--AND iv.IMMUNE_DATE BETWEEN '10/2/2022' AND '10/8/2022'
;
