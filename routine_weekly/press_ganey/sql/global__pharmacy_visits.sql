SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

-- ERROR: 410000050

IF OBJECT_ID('tempdb..##pharmacy_visits') IS NOT NULL DROP TABLE ##pharmacy_visits;
SELECT CASE WHEN o.DISPENSE_PHR_ID IN ( 410000015, 410000016 ) THEN 'SP0101' --WI RX
           WHEN o.DISPENSE_PHR_ID = 410000017 THEN 'SP0102'                  --CO RX
           WHEN o.DISPENSE_PHR_ID IN ( 410000018, 410000021 ) THEN 'SP0103'  --MO RX
           WHEN o.DISPENSE_PHR_ID = 410000019 THEN 'SP0104'                  --TX RX
           WHEN o.DISPENSE_PHR_ID = 410000100 THEN 'SP0105'                  ---Add Chicago Phrmacy if/when opened
           ELSE 'ERROR'
       END AS SERV_TYPE,
       '33118' CLIENT_ID,
       CASE WHEN suff.NAME IS NULL THEN p.PAT_LAST_NAME
           ELSE p.PAT_LAST_NAME + ' ' + suff.NAME
       END AS PAT_LAST_NAME, --to attach Jr, Sr, III, etc.
       p.PAT_FIRST_NAME,
       p.PAT_MIDDLE_NAME,
       dep.DEPT_ABBREVIATION,
       CASE WHEN pa1.ADDRESS IS NULL THEN ''
           WHEN pa1.ADDRESS IN ( '1212 57th', '1212 57TH  ST', '1212 57TH ST', '1212 57TH STREET', '445 S ADAMS', '445 S ADAMS ST', '445 S Adams Street',
                                 '445 S. ADAMS', '445 S. Adams St.', '445 S. Adams St. ', '445 S. Adams Street', '445 South Adams Street', '4545 E 9th Ave',
                                 '4545 E 9th Ave Ste. 120', '4545 E 9TH AVE UNIT 120', '4545 E 9th Ave, Ste. 120', '4545 E. 9th Ave, Ste 120',
                                 '4545 EAST 9TH AVE SUITE 120', '600 williamson st', '600 WILLIAMSON ST ', '600 WILLIAMSON ST STE E ',
                                 '600 WILLIAMSON ST STE H', '600 williamson st ste H ', '600 WILLIAMSON STREET', '820 N Plainkton Ave ', '820 N PLAKINTON AVE',
                                 '820 N Planinton Ave', '820 N PLANKINTON', '820 N PLANKINTON ', '820 N Plankinton Ave', '820 N PLANKINTON AVE ',
                                 '820 N Plankinton Ave.', '820 N Planktinton Ave', '820 n Plankton ave', '820 N. Plainkinton', '820 N. PLANKINTON',
                                 '820 N. Plankinton Ae', '820 N. Plankinton Ave', '820 N. Plankinton Ave,', '820 N. Plankinton Ave.',
                                 '820 N. Plankinton Ave. ', '820 N. Plankinton Avenue', '820 N.Plakinton Ave.', '820 Plankinton Ave', '820  N PLANKINTON AVE',
                                 '820 North Plankinton Avenue', '2653 Locust Street' /*STL*/, '#Homeless', '#NOADRESS', 'CONFIDENTIAL', 'homeless',
                                 'No Address', 'NO MAIL', 'No updated address', 'Unknown', 'UPDATE', 'UPDATED' ) THEN ''
           ELSE pa1.ADDRESS
       END AS ADD_LINE_1,
       pa2.ADDRESS ADD_LINE_2,
       p.CITY,
       zs.ABBR STATE,
       p.ZIP,
       CASE WHEN oc.OTHER_COMMUNIC_NUM = 'NONE' THEN ''
           WHEN p4.SEND_SMS_YN = 'Y' THEN oc.OTHER_COMMUNIC_NUM --Pts who have consented for SMSs
           ELSE ''
       END AS HOME_PHONE,
       o.ORDER_MED_ID 'UNIQUE_VISIT_ID',
       o.CONTACT_DATE SVC_DATE,
       p.BIRTH_DATE,
       CASE --using legal sex. Shoudl we use Gender ID instead?
           WHEN p.SEX_C = '1' THEN '2'
           WHEN p.SEX_C = '2' THEN '1'
           ELSE 'M'
       END AS SEX,
       id.IDENTITY_ID MED_REC,
       '' AS VISIT_PROV_ID,
       '' AS PROV_NAME,
       'Pharmacist' AS PROV_TYPE,
       '' AS PAYOR,
       CASE WHEN o.DISPENSE_PHR_ID = 410000015 THEN '64001004'
           WHEN o.DISPENSE_PHR_ID = 410000016 THEN '64011004'
           WHEN o.DISPENSE_PHR_ID = 410000017 THEN '64012005'
           WHEN o.DISPENSE_PHR_ID = 410000018 THEN '64013004'
           WHEN o.DISPENSE_PHR_ID = 410000019 THEN '64017004'
		   WHEN o.DISPENSE_PHR_ID = 410000100 THEN '64020003'
           WHEN o.DISPENSE_PHR_ID = 410000021 THEN '64019004'
       END AS DEPARTMENT_ID,
       CASE WHEN o.DISPENSE_PHR_ID = 410000015 THEN 'Pharmacy Milwaukee'
           WHEN o.DISPENSE_PHR_ID = 410000016 THEN 'Pharmacy Madison'
           WHEN o.DISPENSE_PHR_ID = 410000017 THEN 'Pharmacy Denver'
           WHEN o.DISPENSE_PHR_ID = 410000018 THEN 'Pharmacy St Louis'
           WHEN o.DISPENSE_PHR_ID = 410000019 THEN 'Pharmacy Austin'
		   WHEN o.DISPENSE_PHR_ID = 410000100 THEN 'Pharmacy Chicago'
           WHEN o.DISPENSE_PHR_ID = 410000021 THEN 'Pharmacy Kansas City'
       END AS 'DEPARTMENT_NAME',
       CASE WHEN p.EMAIL_ADDRESS IS NULL THEN ''
           WHEN p.EMAIL_ADDRESS IN ( 'noemail@noemail.com', 'no-email@noemail.com', 'no@noemail.com', 'noemail@nomail.com', 'none@nne.com',
                                     'nomail@nomail.com', 'none@none.com', 'noemail@noemail.org', 'none@none.comq', 'noemal@noemail.com',
                                     '#Noemail@Noemail.com', 'nne@none.com', 'nnoemail@noemail.com', 'no@email.com', 'no@noe.com', 'no@o.com',
                                     'noe@noemail.com', 'noeail@noemail.com', 'noeamail@nomail.com', 'noemail.com@nomail.com', 'Noemail@arcw.org',
                                     'noemail@email.com', 'noemail@email.org', 'noemail@gamil.com', 'noemail@gmail.com', 'noemail@google.com',
                                     'Noemail@mail.com', 'noemail@msn.com', 'noemail@noe3mail.com', 'noemail@noeamil.com', 'noemail@noemai.com',
                                     'noemail@noemailc.com', 'noemail@noemal.com', 'no-email@noemal.com', 'noemail@noemeail.com', 'noemail@noemial.com',
                                     'NOEMAIL@NOMAIL.CM', 'noe-mail@nomail.com', 'noemail@nomail.ocm', 'noemail@nomailc.om', 'Noemail@nomaill.com',
                                     'noemail@nomial.com', 'noemailonfile@gmail.com', 'noemial@gmail.com', 'noemial@nomaile.com', 'noemil@gmail.com',
                                     'nomail@email.com', 'nomail@mail.com', 'nomail@nomaile.com', 'nomeail@nomail.com', 'non@none.com', 'none@noe.com',
                                     'none@noemail.com', 'none@none.coom', 'none@noneemail.com', 'none@nonne.com', '#nomail@nomail.com', 'noemai@noemail.com',
                                     'no@no.com', 'noemail@noeamial.com', 'noemail@noeemail.com', 'noemail@noemail.coom', 'noemail@normail.com',
                                     'noemaill@noemail.com', 'nomadnathan2790@yahoo.com', 'nomail@nomial.com', 'nomial@nomial.com', 'noone@noone.com' ) THEN ''
           WHEN p.EMAIL_ADDRESS LIKE '#%' THEN ''
           ELSE p.EMAIL_ADDRESS
       END AS EMAIL_ADDRESS,
       CASE WHEN flag.PAT_FLAG_TYPE_C = '640005'
                 AND flag.ACTIVE_C = 1 THEN 'Yes'
           ELSE 'No'
       END AS PrEP,
       'CLINIC' AS Site,
       CASE WHEN p.LANGUAGE_C = '3' THEN 1                   --  Spanish
           WHEN p.LANGUAGE_C = '16' THEN 3                   --  Russian
           WHEN p.LANGUAGE_C = '4' THEN 4                    --  German
           WHEN p.LANGUAGE_C = '10' THEN 5                   --  Italian
           WHEN p.LANGUAGE_C = '12' THEN 6                   --  Polish
           WHEN p.LANGUAGE_C = '9' THEN 7                    --  Greek
           WHEN p.LANGUAGE_C = '15' THEN 8                   --  Portuguese
           WHEN p.LANGUAGE_C IN ( '13', '76', '46' ) THEN 12 --  Chinese
           WHEN p.LANGUAGE_C = '69' THEN 13                  --  Vietnamese
           WHEN p.LANGUAGE_C = '2' THEN 20                   --  French
           WHEN p.LANGUAGE_C = '27' THEN 21                  --  Creole French
           WHEN p.LANGUAGE_C = '21' THEN 22                  --  Arabic
           WHEN p.LANGUAGE_C = '92' THEN 24                  --  Marshallese
           WHEN p.LANGUAGE_C = '55' THEN 25                  --  Samoan
           WHEN p.LANGUAGE_C = '37' THEN 26                  --  Hmong
           WHEN p.LANGUAGE_C = '58' THEN 27                  --  Somali
           WHEN p.LANGUAGE_C = '14' THEN 28                  --  Japanese
           WHEN p.LANGUAGE_C = '42' THEN 29                  --  Korean
           WHEN p.LANGUAGE_C = '60' THEN 30                  --  Tagalog
           WHEN p.LANGUAGE_C = '160' THEN 31                 --  Armenian
           WHEN p.LANGUAGE_C = '75' THEN 34                  --  Cambodian
           WHEN p.LANGUAGE_C = '84' THEN 36                  --  French Creole Haitian
           WHEN p.LANGUAGE_C = '36' THEN 36                  --  Haitian
           WHEN p.LANGUAGE_C = '11' THEN 37                  --  Hebrew
           WHEN p.LANGUAGE_C = '35' THEN 38                  --  Hindi
           WHEN p.LANGUAGE_C = '68' THEN 39                  --  Urdu
           WHEN p.LANGUAGE_C = '71' THEN 40                  --  Yiddish
           WHEN p.LANGUAGE_C = '39' THEN 42                  --  Indonesian
           WHEN p.LANGUAGE_C = '44' THEN 43                  --  Laotian
           WHEN p.LANGUAGE_C = '93' THEN 44                  --  Malay
           WHEN p.LANGUAGE_C = '57' THEN 45                  --  Swahili
           WHEN p.LANGUAGE_C = '61' THEN 46                  --  Thai
           WHEN p.LANGUAGE_C = '25' THEN 50                  --  Bosnian
           WHEN p.LANGUAGE_C = '56' THEN 51                  --  Serbian
           WHEN p.LANGUAGE_C = '26' THEN 52                  --  Croatian
           WHEN p.LANGUAGE_C = '80' THEN 52                  --  SerboCroatian
           WHEN p.LANGUAGE_C = '161' THEN 53                 --  Turkish
           WHEN p.LANGUAGE_C = '52' THEN 54                  --  Punjabi
           WHEN p.LANGUAGE_C = '54' THEN 55                  --  Romanian
           WHEN p.LANGUAGE_C = '87' THEN 56                  --  Ilocano
           WHEN p.LANGUAGE_C = '19' THEN 57                  --  Albanian
           WHEN p.LANGUAGE_C = '31' THEN 59                  --  Farsi
           WHEN p.LANGUAGE_C = '22' THEN 60                  --  Bengali
           ELSE 0                                            -- ENGLISH
       END AS LANGUAGE,
       zpr.NAME RACE,
       '$' EOR
INTO ##pharmacy_visits
FROM Clarity.dbo.ORDER_DISP_INFO_VIEW o
    INNER JOIN Clarity.dbo.RX_PHR phr ON o.DISPENSE_PHR_ID = phr.PHARMACY_ID
    INNER JOIN Clarity.dbo.ORDER_MED_VIEW omv ON o.ORDER_MED_ID = omv.ORDER_MED_ID
    INNER JOIN Clarity.dbo.PAT_ENC_VIEW pev ON omv.PAT_ENC_CSN_ID = pev.PAT_ENC_CSN_ID
    INNER JOIN Clarity.dbo.PATIENT_VIEW p ON pev.PAT_ID = p.PAT_ID
    LEFT JOIN Clarity.dbo.PATIENT_RACE pr ON p.PAT_ID = pr.PAT_ID
                                             AND pr.LINE = 1
    LEFT JOIN Clarity.dbo.ZC_PATIENT_RACE zpr ON pr.PATIENT_RACE_C = zpr.PATIENT_RACE_C
    LEFT JOIN Clarity.dbo.PAT_ADDRESS pa1 ON p.PAT_ID = pa1.PAT_ID
                                             AND pa1.line = 1
    LEFT JOIN Clarity.dbo.PAT_ADDRESS pa2 ON p.PAT_ID = pa2.PAT_ID
                                             AND pa2.LINE = 2
    LEFT JOIN Clarity.dbo.PATIENT_4 p4 ON p.PAT_ID = p4.PAT_ID
    LEFT JOIN Clarity.dbo.OTHER_COMMUNCTN oc ON p.PAT_ID = oc.PAT_ID
                                                AND oc.OTHER_COMMUNIC_C = '1'
    LEFT JOIN Clarity.dbo.ZC_STATE zs ON p.STATE_C = zs.STATE_C
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag ON pev.PAT_ID = flag.PATIENT_ID
    LEFT JOIN Clarity.dbo.ZC_PAT_NAME_SUFFIX suff ON suff.PAT_NAME_SUFFIX_C = p.PAT_NAME_SUFFIX_C
                                                     AND p.PAT_NAME_SUFFIX_C IN ( 0, 1, 2, 3, 4, 5, 1000, 1001, 1002 )
WHERE pev.CONTACT_DATE >= DATEADD(DAY, -8, GETDATE()) --If run on Monday this will go back to Saturday. If Run on Tues, will go to Sunday. If Wed, then Monday
      AND pev.CONTACT_DATE <= DATEADD(DAY, -2, GETDATE()) --If run on Monday this will go back to Saturday. If Run on Tues, will go to Sunday. If Wed, then Monday
      AND p4.PAT_LIVING_STAT_C = 1
      AND id.IDENTITY_ID NOT IN ( SELECT ##survey_opt_outs.IDENTITY_ID FROM ##survey_opt_outs ) -- Survey opt outs
      AND phr.PHARMACY_NAME LIKE 'VIVENT PHARMACY%'
      AND o.FILL_STATUS_C = 80 -- Dispensed
;

SELECT 1 FROM ##pharmacy_visits; -- CHANGE THE * BACK TO 1 BEFORE YOU PUT THIS BACK
