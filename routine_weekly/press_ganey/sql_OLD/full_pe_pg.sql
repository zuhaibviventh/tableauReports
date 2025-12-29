/* 10.200.180.16 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

--To get clients who are literate in Spanish. This will get everyone who is Spanish Literate even if they are also English literate
SELECT MAX(IIF(lang.SCPLanguageProficiency = 'Spanish', 1, 0)) AS LANGUAGE,
       lang.SQLID
INTO #Language
FROM vivent.dbo.vwClient_Profile_SCPLanguageProficiency lang
GROUP BY lang.SQLID;


SELECT CASE WHEN sp.ServiceType LIKE 'Food%' THEN 'FOOD'
           WHEN sp.ServiceType LIKE 'Issue%Check%' THEN 'HOUSING'
           WHEN sp.ServiceType IN ( 'Assistance with ADAP', 'Assistance with ADAP / IPSP', 'Assistance with ADAP/IPSP Enrollment',
                                    'Assistance with Affordable Care Act', 'Assistance with Affordable Care Act Enrollment',
                                    'Assistance with BadgerCare Enrollment', 'Assistance with BadgerCare Plus', 'Assistance with COBRA',
                                    'Assistance with COBRA Enrollment', 'Assistance with Community Waiver enrollment', 'Assistance with Community Waivers',
                                    'Assistance with Connect for Health Colorado', 'Assistance with Dental Insurance',
                                    'Assistance with Dental Insurance enrollmen', 'Assistance with Dental Insurance enrollment',
                                    'Assistance with Employer/Private Insurance', 'Assistance with Employer/Private Insurance Enrollment',
                                    'Assistance with Employer-Sponsored Coverage', 'Assistance with Family Planning Waiver',
                                    'Assistance with Family Planning Waiver Enrollment', 'Assistance with FoodShare', 'Assistance with FoodShare Enrollment',
                                    'Assistance with MAPP', 'Assistance with MAPP enrollment', 'Assistance with Med D extra help enrollment',
                                    'Assistance with Medicaid', 'Assistance with Medicaid Enrollment', 'Assistance with Medicare',
                                    'Assistance with Medicare A & B', 'Assistance with Medicare A & B Enrollment', 'Assistance with Medicare C',
                                    'Assistance with Medicare C Enrollment', 'Assistance with Medicare C/D', 'Assistance with Medicare C/D Enrollment',
                                    'Assistance with Medicare D', 'Assistance with Medicare D Enrollment', 'Assistance with Medicare Supplement Plan',
                                    'Assistance with Medicare Supplement Plan Enrollment', 'Assistance with Private Insurance',
                                    'Assistance with Private Insurance Enrollment', 'Assistance with QMB/SLMB enrollment', 'Assistance with SSD / SSI',
                                    'Assistance with SSD/SSI Enrollment', 'Made: Client Contact Clinic', 'Made: Client Contact Corrections',
                                    'Made: Client Contact Correspondence', 'Made: Client Contact E-Mail from a client - made contact',
                                    'Made: Client Contact Home', 'Made: Client Contact Hospital', 'Made: Client Contact Office',
                                    'Made: Client Contact Other Visit', 'Made: Client Contact Telephone',
                                    'Made: Client Contact Text Message from a client - made contact', 'Made: CM Client Contact Clinic',
                                    'Made: CM Client Contact Office', 'Made: CM Client Contact Telephone',
                                    'Made: Contact Minor''s Parent or Guardian Correspondence', 'Made: Contact Minor''s Parent or Guardian Telephone',
                                    'Made: Contact Minor''s Parent or Guardian Visit' ) THEN 'CM'
           WHEN sp.ServiceType LIKE 'Legal%'
                AND sp.Form = 'Service Provided' THEN 'LEGAL'
           ELSE 'MOO'
       END AS SERV_TYPE,
       33118 CLIENT_ID,
       vcp.ClientProfileID,
       vcp.SCPClientLast PAT_LAST_NAME,
       vcp.SCPClientFirst PAT_FIRST_NAME,
       IIF(vcp.SCPClientMI = '', NULL, vcp.SCPClientMI) AS PAT_MIDDLE_NAME,
       CASE WHEN vcp.SCPStreetAddress1 LIKE 'Homeless%' THEN NULL
           WHEN vcp.SCPSendMail IS NULL
                OR vcp.SCPSendMail = 'Yes' THEN vcp.SCPStreetAddress1 ---is mailing to NULLS ok?
           ELSE NULL
       END AS ADD_LINE_1,
       CASE WHEN vcp.SCPStreetAddress1 LIKE 'Homeless%' THEN NULL
           WHEN vcp.SCPStreetAddress2 = '' THEN NULL
           WHEN vcp.SCPSendMail IS NULL
                OR vcp.SCPSendMail = 'Yes' THEN vcp.SCPStreetAddress2
           ELSE NULL
       END AS ADD_LINE_2,
       vcp.SCPCity CITY,
       vcp.SCPState STATE,
       LEFT(vcp.SCPZip, 5) ZIP,
       /* 
       need to add the check for it being ok to contact by phone
       -- Can't do phone numbers due to unstructured data. Cheryl is proposing a new field "Text number" masked to xxx-xxx-xxxx
       */
       CASE WHEN vcp.SCPClientPhone1Type = 'Cell Phone' THEN vcp.SCPClientPhone1Number
           WHEN vcp.SCPClientPhone2Type = 'Cell Phone' THEN vcp.SCPClientPhone2Number
           WHEN vcp.SCPClientPhone3Type = 'Cell Phone' THEN vcp.SCPClientPhone3Number
       END AS HOME_PHONE,
       sp.ActivityDocumentID UNIQUE_VISIT_ID,
       CONVERT(NVARCHAR(30), sp.AActivityDate, 101) AS SVC_DATE,
       CONVERT(NVARCHAR(30), vcp.SCPDateOfBirth, 101) AS BIRTH_DATE,
       CASE WHEN vcp.SCPGender = 'Male' THEN '1'
           WHEN vcp.SCPGender = 'Female' THEN '2'
           ELSE 'M'
       END AS GENDER,
       CASE WHEN vcp.SCPMRN = '' THEN vcp.SCPClientID
           WHEN vcp.SCPMRN IS NOT NULL THEN vcp.SCPMRN
           ELSE vcp.SCPClientID
       END AS MED_REC,
       IIF(sp.ServiceType LIKE 'Food%', 'the Food Pantry staff', sp.ServiceProvider) AS PROV_NAME,
       --using client county since service county is often blank for housing clients
       CASE WHEN sp.AOrg = 'Vivent Health Colorado' THEN 'Denver'
           WHEN sp.ProgramOfficeLocation = 'St. Louis' THEN 'St. Louis'
           WHEN sp.ProgramOfficeLocation = 'Kansas City' THEN 'Kansas City'
           WHEN sp.AOrg = 'Vivent Health Texas' THEN 'Austin'
           WHEN sp.AOrg = 'Vivent Health Illinois' THEN 'Chicago'
           WHEN sp.ProgramOfficeLocation IS NOT NULL THEN sp.ProgramOfficeLocation
           WHEN vcp.SCPCounty IN ( 'Calumet', 'Fond du Lac', 'Green Lake', 'Marquette', 'Outagamie', 'Sheboygan', 'Waupaca', 'Waushara', 'Winnebago' ) THEN
               'Appleton'
           WHEN vcp.SCPCounty IN ( 'Barron', 'Buffalo', 'Burnett', 'Chippewa', 'Clark', 'Dunn', 'Eau Claire', 'Pepin', 'Pierce', 'Polk', 'Rusk', 'St. Croix' ) THEN
               'Eau Claire'
           WHEN vcp.SCPCounty IN ( 'Brown', 'Door', 'Kewaunee', 'Manitowoc', 'Marinette', 'Menominee', 'Oconto', 'Shawano' ) THEN 'Green Bay'
           WHEN vcp.SCPCounty IN ( 'Jefferson', 'Kenosha', 'Racine', 'Walworth' ) THEN 'Kenosha'
           WHEN vcp.SCPCounty IN ( 'Jackson', 'La Crosse', 'Monroe', 'Trempealeau', 'Vernon' ) THEN 'La Crosse'
           WHEN vcp.SCPCounty IN ( 'Adams', 'Columbia', 'Crawford', 'Dane', 'Dodge', 'Grant', 'Iowa', 'Juneau', 'Lafayette', 'Richland', 'Sauk' ) THEN
               'Madison'
           WHEN vcp.SCPCounty IN ( 'Milwaukee', 'Ozaukee', 'Washington', 'Waukesha' ) THEN 'Milwaukee'
           WHEN vcp.SCPCounty IN ( 'Ashland', 'Bayfield', 'Douglas', 'Iron', 'Sawyer', 'Washburn' ) THEN 'Superior'
           WHEN vcp.SCPCounty IN ( 'Florence ', 'Forest', 'Langlade', 'Lincoln', 'Marathon', 'Oneida', 'Portage', 'Price', 'Taylor', 'Vilas', 'Wood' ) THEN
               'Wausau'
           WHEN vcp.SCPCounty IN ( 'Rock', 'Green' ) THEN 'Beloit'
           ELSE 'Other'
       END AS DEPARTMENT_ID,
       CASE WHEN sp.AOrg = 'Vivent Health Colorado' THEN 'Denver'
           WHEN sp.ProgramOfficeLocation = 'St. Louis' THEN 'St. Louis'
           WHEN sp.ProgramOfficeLocation = 'Kansas City' THEN 'Kansas City'
           WHEN sp.AOrg = 'Vivent Health Texas' THEN 'Austin'
           WHEN sp.AOrg = 'Vivent Health Illinois' THEN 'Chicago'
           WHEN sp.ProgramOfficeLocation IS NOT NULL THEN sp.ProgramOfficeLocation
           WHEN vcp.SCPCounty IN ( 'Calumet', 'Fond du Lac', 'Green Lake', 'Marquette', 'Outagamie', 'Sheboygan', 'Waupaca', 'Waushara', 'Winnebago' ) THEN
               'Appleton'
           WHEN vcp.SCPCounty IN ( 'Barron', 'Buffalo', 'Burnett', 'Chippewa', 'Clark', 'Dunn', 'Eau Claire', 'Pepin', 'Pierce', 'Polk', 'Rusk', 'St. Croix' ) THEN
               'Eau Claire'
           WHEN vcp.SCPCounty IN ( 'Brown', 'Door', 'Kewaunee', 'Manitowoc', 'Marinette', 'Menominee', 'Oconto', 'Shawano' ) THEN 'Green Bay'
           WHEN vcp.SCPCounty IN ( 'Jefferson', 'Kenosha', 'Racine', 'Walworth' ) THEN 'Kenosha'
           WHEN vcp.SCPCounty IN ( 'Jackson', 'La Crosse', 'Monroe', 'Trempealeau', 'Vernon' ) THEN 'La Crosse'
           WHEN vcp.SCPCounty IN ( 'Adams', 'Columbia', 'Crawford', 'Dane', 'Dodge', 'Grant', 'Iowa', 'Juneau', 'Lafayette', 'Richland', 'Sauk' ) THEN
               'Madison'
           WHEN vcp.SCPCounty IN ( 'Milwaukee', 'Ozaukee', 'Washington', 'Waukesha' ) THEN 'Milwaukee'
           WHEN vcp.SCPCounty IN ( 'Ashland', 'Bayfield', 'Douglas', 'Iron', 'Sawyer', 'Washburn' ) THEN 'Superior'
           WHEN vcp.SCPCounty IN ( 'Florence ', 'Forest', 'Langlade', 'Lincoln', 'Marathon', 'Oneida', 'Portage', 'Price', 'Taylor', 'Vilas', 'Wood' ) THEN
               'Wausau'
           WHEN vcp.SCPCounty IN ( 'Rock', 'Green' ) THEN 'Beloit'
           ELSE 'Other'
       END AS DEPARTMENT_NAME,
       REVERSE(SUBSTRING(REVERSE(sp.AOrg), 1, CHARINDEX(' ', REVERSE(sp.AOrg)) - 1)) AS Service_State,
       IIF(vcp.SCPPermissionEmail = 'Yes', vcp.SCPEmailAddr, NULL) AS EMAIL_ADDRESS,
       CASE WHEN l.LANGUAGE = 1 THEN 1
           WHEN vcp.SCPLanguagePreferredWritten = 'Spanish' THEN 1
           ELSE 0
       END AS LANGUAGE,
       vcp.SCPRaceCat RACE,
       '$' EOR
INTO #a
FROM vivent.dbo.vwService_Activity_All sp
    INNER JOIN vivent.dbo.vwClient_Profile_All vcp ON vcp.ClientProfileID = sp.ClientProfileID
    --INNER JOIN vivent.dbo.vwUser_Profile vup ON sp.ACreateBy = vup.UserName
    LEFT JOIN #Language l ON l.SQLID = vcp.SQLID
WHERE sp.AActivityDate >= DATEADD(DAY, -8, GETDATE()) --If run on Monday this will go back to Saturday. If Run on Tues, will go to Sunday. If Wed, then Monday
      AND sp.AActivityDate <= DATEADD(DAY, -2, GETDATE()) --If run on Monday this will go back to Saturday. If Run on Tues, will go to Sunday. If Wed, then Monday
      AND (sp.ServiceType IN ( 'Food Pantry Delivery', 'Food Pantry - Emergency', 'Food Pantry Usage', 'Nutritional Counseling', 'Food Pantry Delivery',
                               'Food Pantry Usage', 'Housing Assistance', 'Made: Case Conferencing In Person', 'Assistance with ADAP',
                               'Assistance with ADAP / IPSP', 'Assistance with ADAP/IPSP Enrollment', 'Assistance with Affordable Care Act',
                               'Assistance with Affordable Care Act Enrollment', 'Assistance with BadgerCare Enrollment', 'Assistance with BadgerCare Plus',
                               'Assistance with COBRA', 'Assistance with COBRA Enrollment', 'Assistance with Community Waiver enrollment',
                               'Assistance with Community Waivers', 'Assistance with Connect for Health Colorado', 'Assistance with Dental Insurance',
                               'Assistance with Dental Insurance enrollmen', 'Assistance with Dental Insurance enrollment',
                               'Assistance with Employer/Private Insurance', 'Assistance with Employer/Private Insurance Enrollment',
                               'Assistance with Employer-Sponsored Coverage', 'Assistance with Family Planning Waiver',
                               'Assistance with Family Planning Waiver Enrollment', 'Assistance with FoodShare', 'Assistance with FoodShare Enrollment',
                               'Assistance with MAPP', 'Assistance with MAPP enrollment', 'Assistance with Med D extra help enrollment',
                               'Assistance with Medicaid', 'Assistance with Medicaid Enrollment', 'Assistance with Medicare', 'Assistance with Medicare A & B',
                               'Assistance with Medicare A & B Enrollment', 'Assistance with Medicare C', 'Assistance with Medicare C Enrollment',
                               'Assistance with Medicare C/D', 'Assistance with Medicare C/D Enrollment', 'Assistance with Medicare D',
                               'Assistance with Medicare D Enrollment', 'Assistance with Medicare Supplement Plan',
                               'Assistance with Medicare Supplement Plan Enrollment', 'Assistance with Private Insurance',
                               'Assistance with Private Insurance Enrollment', 'Assistance with QMB/SLMB enrollment', 'Assistance with SSD / SSI',
                               'Assistance with SSD/SSI Enrollment', 'Made: Client Contact Clinic', 'Made: Client Contact Corrections',
                               'Made: Client Contact Correspondence', 'Made: Client Contact E-Mail from a client - made contact', 'Made: Client Contact Home',
                               'Made: Client Contact Hospital', 'Made: Client Contact Office', 'Made: Client Contact Other Visit',
                               'Made: Client Contact Telephone', 'Made: Client Contact Text Message from a client - made contact',
                               'Made: CM Client Contact Clinic', 'Made: CM Client Contact Office', 'Made: CM Client Contact Telephone',
                               'Made: Contact Minor''s Parent or Guardian Correspondence', 'Made: Contact Minor''s Parent or Guardian Telephone',
                               'Made: Contact Minor''s Parent or Guardian Visit', 'Legal' )
           OR sp.ServiceType LIKE 'Issue%Check%')
      AND vcp.SCPMRN NOT IN ( '640003964', '640005483', '640000069', '640001127', '640000386', '640007491', '64000540' ) -- Opt outs
      AND vcp.SCPDeathDate IS NULL
      AND (sp.DeleteFlag = 'N' OR sp.DeleteFlag IS NULL);

SELECT CASE WHEN a.SERV_TYPE = 'CM'
                 AND a.Service_State = 'Wisconsin' THEN 'SP0301'
           WHEN a.SERV_TYPE = 'FOOD'
                AND a.Service_State = 'Wisconsin' THEN 'SP0201'
           WHEN a.SERV_TYPE = 'HOUSING'
                AND a.Service_State = 'Wisconsin' THEN 'SP0401'
           WHEN a.SERV_TYPE = 'LEGAL'
                AND a.Service_State = 'Wisconsin' THEN 'SP0501'
           WHEN a.SERV_TYPE = 'CM'
                AND a.Service_State = 'Colorado' THEN 'SP0302'
           WHEN a.SERV_TYPE = 'FOOD'
                AND a.Service_State = 'Colorado' THEN 'SP0202'
           WHEN a.SERV_TYPE = 'HOUSING'
                AND a.Service_State = 'Colorado' THEN 'SP0402'
           WHEN a.SERV_TYPE = 'LEGAL'
                AND a.Service_State = 'Colorado' THEN 'SP0502'
           WHEN a.SERV_TYPE = 'CM'
                AND a.Service_State = 'Missouri' THEN 'SP0103'
           WHEN a.SERV_TYPE = 'FOOD'
                AND a.Service_State = 'Missouri' THEN 'SP0203'
           WHEN a.SERV_TYPE = 'HOUSING'
                AND a.Service_State = 'Missouri' THEN 'SP0403'
           WHEN a.SERV_TYPE = 'LEGAL'
                AND a.Service_State = 'Missouri' THEN 'SP0503'
           WHEN a.SERV_TYPE = 'CM'
                AND a.Service_State = 'Texas' THEN 'SP0304'
           WHEN a.SERV_TYPE = 'FOOD'
                AND a.Service_State = 'Texas' THEN 'SP0204'
           WHEN a.SERV_TYPE = 'HOUSING'
                AND a.Service_State = 'Texas' THEN 'SP0404'
           WHEN a.SERV_TYPE = 'LEGAL'
                AND a.Service_State = 'Texas' THEN 'SP0504'
           WHEN a.SERV_TYPE = 'CM'
                AND a.Service_State = 'Illinois' THEN 'SP0305'
           WHEN a.SERV_TYPE = 'FOOD'
                AND a.Service_State = 'Illinois' THEN 'SP0205'
           WHEN a.SERV_TYPE = 'HOUSING'
                AND a.Service_State = 'Illinois' THEN 'SP0405'
           WHEN a.SERV_TYPE = 'LEGAL'
                AND a.Service_State = 'Illinois' THEN 'SP0505'
       END AS SERV_TYPE,
       a.CLIENT_ID,
       a.ClientProfileID,
       a.PAT_LAST_NAME,
       a.PAT_FIRST_NAME,
       a.PAT_MIDDLE_NAME,
       a.ADD_LINE_1,
       a.ADD_LINE_2,
       a.CITY,
       a.STATE,
       a.ZIP,
       IIF(a.HOME_PHONE LIKE '___-___-____', a.HOME_PHONE, NULL) AS HOME_PHONE,
       a.UNIQUE_VISIT_ID,
       a.SVC_DATE,
       a.BIRTH_DATE,
       a.GENDER,
       a.MED_REC,
       a.PROV_NAME,
       a.DEPARTMENT_ID,
       a.DEPARTMENT_NAME,
       a.Service_State,
       CASE WHEN a.EMAIL_ADDRESS NOT LIKE '% %' THEN a.EMAIL_ADDRESS --to remove emails with spaces
           ELSE NULL
       END AS EMAIL_ADDRESS,
       a.LANGUAGE,
       a.RACE,
       a.EOR
INTO #b
FROM #a a
WHERE a.ADD_LINE_1 IS NOT NULL
      OR a.EMAIL_ADDRESS IS NOT NULL
      OR a.HOME_PHONE IS NOT NULL;


--To check enrollment status
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
                b.HOME_PHONE,
                b.UNIQUE_VISIT_ID,
                b.SVC_DATE,
                b.BIRTH_DATE,
                b.GENDER,
                b.MED_REC,
                b.PROV_NAME,
                b.DEPARTMENT_ID,
                b.DEPARTMENT_NAME,
                b.Service_State,
                b.EMAIL_ADDRESS,
                b.LANGUAGE,
                b.RACE,
                b.EOR,
                MAX(CASE WHEN b.SERV_TYPE LIKE 'SP02%' THEN 1    --- food pantry  since enrollment not required
                        WHEN b.SERV_TYPE LIKE 'SP05%' THEN 1     --- Legal since enrollment not required
                        WHEN pe.ClientProfileID IS NOT NULL
                             AND b.SERV_TYPE LIKE 'SP03%' THEN 1 -- Enrolled in Case Mgmnt
                        WHEN b.SERV_TYPE = 'SP0404' THEN 1       -- All TX housing services as if client is enrolled
                        WHEN vh.ClientProfileID IS NOT NULL
                             AND b.SERV_TYPE LIKE 'SP04%' THEN 1 -- Enrolled in Housing
                        ELSE 0
                    END) AS ENROLLED
INTO #c
FROM #b b
    LEFT JOIN vivent.dbo.vwProgram_Enrollments_All2016 pe ON b.ClientProfileID = pe.ClientProfileID
                                                             AND pe.DeleteFlag <> 'Y'
                                                             AND (pe.EndDate >= DATEADD(DAY, -8, GETDATE()) OR pe.EndDate IS NULL) --Not dis-enrolled
                                                             AND (pe.Program LIKE '%Case Management%' OR pe.Program LIKE 'Linkage to Care%')
    LEFT JOIN vivent.dbo.vwProgram_Enrollment_Housing vh ON b.ClientProfileID = vh.ClientProfileID
                                                            AND vh.CLAProgramName IN ( 'Ryan White STRMU Extension', 'Ryan White', 'HaRTSS', 'SCHIP', 'SHOPWA',
                                                                                       'CHOPWA' )
                                                            AND vh.DeleteFlag <> 'Y'
                                                            AND (vh.CLAProgramDateEnd > DATEADD(DAY, -8, GETDATE()) --Dis-enrolled after start
                                                                 OR vh.CLAProgramDateEnd IS NULL) --Not dis-enrolled

WHERE b.SERV_TYPE IS NOT NULL
      AND b.SERV_TYPE <> 'MOO'
GROUP BY b.SERV_TYPE,
         b.CLIENT_ID,
         b.PAT_LAST_NAME,
         b.PAT_FIRST_NAME,
         b.PAT_MIDDLE_NAME,
         b.ADD_LINE_1,
         b.ADD_LINE_2,
         b.CITY,
         b.STATE,
         b.ZIP,
         b.HOME_PHONE,
         b.UNIQUE_VISIT_ID,
         b.SVC_DATE,
         b.BIRTH_DATE,
         b.GENDER,
         b.MED_REC,
         b.PROV_NAME,
         b.DEPARTMENT_ID,
         b.DEPARTMENT_NAME,
         b.Service_State,
         b.EMAIL_ADDRESS,
         b.LANGUAGE,
         b.RACE,
         b.EOR;


SELECT c.SERV_TYPE,
       c.CLIENT_ID,
       c.PAT_LAST_NAME,
       c.PAT_FIRST_NAME,
       c.PAT_MIDDLE_NAME,
       c.ADD_LINE_1,
       c.ADD_LINE_2,
       c.CITY,
       c.STATE,
       c.ZIP,
       c.HOME_PHONE,
       c.UNIQUE_VISIT_ID,
       c.SVC_DATE,
       c.BIRTH_DATE,
       c.GENDER,
       c.MED_REC,
       c.PROV_NAME,
       c.DEPARTMENT_ID,
       c.DEPARTMENT_NAME,
       c.Service_State,
       c.EMAIL_ADDRESS,
       c.LANGUAGE,
       c.RACE,
       '' SUB_SERVICE,
       c.EOR
FROM #c c
WHERE c.ENROLLED = 1
      OR c.SERV_TYPE LIKE 'SP04%' --Since some housing may not be enrolled
;

DROP TABLE #a;
DROP TABLE #b;
DROP TABLE #c;
DROP TABLE #language;
