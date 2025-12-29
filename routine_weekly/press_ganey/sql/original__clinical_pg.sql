/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:   Press Ganey Extract of Visits
 Create Date:   7/1/2018
 Created By:    scogginsm
 System:        javelin.ochin.org
 Requested By:  Patient Satisfaction Survey Team

 Purpose:       To pull a list of all clinical visits that happened last week yo send to Press Ganey

 Description:
 
  BOE Folder Path: SA64 > Press Ganey


 *****  Modification History *****

 Change Date:       Changed By:         Change Description:
 ------------       -------------       ---------------------------------------------------
 8/1/2018           Mitch Scoggins      Added a CASE statement to change DAVID BAKER to NICO BAKER
 8/15/2019          Mitch Scoggins      Added pharmacy visits for when they go live. STILL TO DO, add RMC pharmacy code
 8/20/2018          Mitch Scoggins      Only allow phone numbers in when p4.SEND_SMS_YN = 'Y' to only inlcude phones for pts who are OK with texts. This is in prep for the dat we're allowed to text pts
                                        Converted NULL in PAYOR to 'SELF PAY'
 4/16/2019          Mitch               Adding Service_State column for easier reporting
 5/3/2019           Mitch               Updated the Service_State logic to handle the STL Pharmacy data
 12/31/2019         Mitch               Renaming sites based on Dept IDs so we can have site names before the Epic change
 12/31/2019         Mitch               Updaiting SITE logic to look at Dept IDs instead of FYI flags
 1/16/2020          Mitch               Updated last name to include suffix if pt has one
 6/5/2020           Mitch               Alteryx
 8/10/2020          Mitch               Adding Denver Dental and the Austin ones I forgot
 9/15/2021          Mitch               Updating to use pharmacy dispense info for pharmacy surveys
 2/5/2023           Mitch               Dropping Temp Tables inline too (for Alteryx)
 2/19/2024          Benzon Salazar      Python
**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#a') IS NOT NULL DROP TABLE #a;
SELECT CASE WHEN pev.DEPARTMENT_ID IN ( 64001001, 64002001, 64003001, 64011001, 64014001, 64015001 ) THEN 'MD0101'                              -- WI Medical
           WHEN pev.DEPARTMENT_ID = 64012002 THEN 'MD0102'                                                                                      -- CO Medical
           WHEN pev.DEPARTMENT_ID IN ( 64012004, 64016001 ) THEN 'OY0102'                                                                       -- CO BH
           WHEN pev.DEPARTMENT_ID = 64012003 THEN 'DS0102'                                                                                      -- CO Dental
           WHEN pev.DEPARTMENT_ID IN ( 64008001, 64009001, 64011005 ) THEN 'DS0101'                                                             -- WI Dental
           WHEN pev.DEPARTMENT_ID IN ( 64001005, 64002004, 64003004, 64004004, 64005004, 64006004, 64007004, 64010004, 64011006 ) THEN 'OY0101' -- WI Psych & MH
           WHEN pev.DEPARTMENT_ID IN ( 64001006, 64003005, 64011007 ) THEN 'OY0101'                                                             -- WI AODA
           WHEN pev.DEPARTMENT_ID IN ( 64001004, 64011004 ) THEN 'SP0101'                                                                       --WI RX
           WHEN pev.DEPARTMENT_ID = 64012005 THEN 'SP0102'                                                                                      --CO RX
           WHEN pev.DEPARTMENT_ID = 64013004 THEN 'SP0103'                                                                                      --STL RX
           WHEN pev.DEPARTMENT_ID = 64013001 THEN 'MD0103'                                                                                      --STL Med
           WHEN pev.DEPARTMENT_ID = 64017001 THEN 'MD0104'                                                                                      -- TX Medical
           WHEN pev.DEPARTMENT_ID IN ( 64017002, 64017003, 64017005 ) THEN 'OY0104'                                                             -- TX BH
           WHEN pev.DEPARTMENT_ID IN ( 64018001, 64017006 ) THEN 'DS0104'                                                                       -- TX Dental
           WHEN pev.DEPARTMENT_ID = 64017004 THEN 'SP0104'                                                                                      --TX RX
           WHEN pev.DEPARTMENT_ID = 64013006 THEN 'DS0103'                                                                                      --STL Dental
           WHEN pev.DEPARTMENT_ID = 64013005 THEN 'OY0103'                                                                                      --STL BH
                                                                                                                                                --WHEN pev.DEPARTMENT_ID = 64013002 THEN
                                                                                                                                                --  '' --STL AODA
           WHEN pev.DEPARTMENT_ID = 64019001 THEN 'MD0103'                                                                                      --KC MD
           WHEN pev.DEPARTMENT_ID = 64019004 THEN 'SP0103'                                                                                      --KC RX
           WHEN pev.DEPARTMENT_ID IN ( 64019002, 64019005 ) THEN 'OY0103'                                                                       --KC BH
           WHEN pev.DEPARTMENT_ID = 64020001 THEN 'OY0105'                                                                                      --Chicago BH
           WHEN pev.DEPARTMENT_ID = 64019006 THEN 'DS0104'                                                                                      --KC Dental

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
       pev.PAT_ENC_CSN_ID 'UNIQUE_VISIT_ID',
       pev.CONTACT_DATE SVC_DATE,
       p.BIRTH_DATE,
       CASE --using legal sex. Shoudl we use Gender ID instead?
           WHEN p.SEX_C = '1' THEN '2'
           WHEN p.SEX_C = '2' THEN '1'
           ELSE 'M'
       END AS SEX,
       id.IDENTITY_ID MED_REC,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) <> 'RX' THEN pev.VISIT_PROV_ID
           ELSE NULL
       END AS VISIT_PROV_ID,
       CASE WHEN SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) <> 'RX' THEN ser.PROV_NAME
           ELSE NULL
       END AS PROV_NAME,
       ser.PROV_TYPE,
       CASE WHEN zfc.NAME IS NULL THEN 'SELF PAY'
           ELSE zfc.NAME
       END AS PAYOR,
       pev.DEPARTMENT_ID,
       CASE WHEN pev.DEPARTMENT_ID = 64005001 THEN 'Medical Appleton'
           WHEN pev.DEPARTMENT_ID = 64005004 THEN 'Mental Health Appleton'
           WHEN pev.DEPARTMENT_ID = 64010004 THEN 'Mental Health Beloit'
           WHEN pev.DEPARTMENT_ID = 64015001 THEN 'Medical Milwaukee'
           WHEN pev.DEPARTMENT_ID = 64006004 THEN 'Mental Health Eau Claire'
           WHEN pev.DEPARTMENT_ID = 64003005 THEN 'Mental Health Green Bay'
           WHEN pev.DEPARTMENT_ID = 64009001 THEN 'Dental Green Bay'
           WHEN pev.DEPARTMENT_ID = 64003001 THEN 'Medical Green Bay'
           WHEN pev.DEPARTMENT_ID = 64003004 THEN 'Mental Health Green Bay'
           WHEN pev.DEPARTMENT_ID = 64002001 THEN 'Medical Kenosha'
           WHEN pev.DEPARTMENT_ID = 64002004 THEN 'Mental Health Kenosha'
           WHEN pev.DEPARTMENT_ID = 64014001 THEN 'Medical Milwaukee'
           WHEN pev.DEPARTMENT_ID = 64007004 THEN 'Mental Health LaCrosse'
           WHEN pev.DEPARTMENT_ID = 64001006 THEN 'Mental Health Milwaukee'
           WHEN pev.DEPARTMENT_ID = 64008001 THEN 'Dental Milwaukee'
           WHEN pev.DEPARTMENT_ID = 64001001 THEN 'Medical Milwaukee'
           WHEN pev.DEPARTMENT_ID = 64001005 THEN 'Mental Health Milwaukee'
           WHEN pev.DEPARTMENT_ID = 64001004 THEN 'Pharmacy Milwaukee'
           WHEN pev.DEPARTMENT_ID = 64011007 THEN 'Mental Health Madison'
           WHEN pev.DEPARTMENT_ID = 64011005 THEN 'Dental Madison'
           WHEN pev.DEPARTMENT_ID = 64011001 THEN 'Medical Madison'
           WHEN pev.DEPARTMENT_ID = 64011006 THEN 'Mental Health Madison'
           WHEN pev.DEPARTMENT_ID = 64011004 THEN 'Pharmacy Madison'
           WHEN pev.DEPARTMENT_ID = 64013002 THEN 'Mental Health St Louis'
           WHEN pev.DEPARTMENT_ID = 64013006 THEN 'Dental St Louis'
           WHEN pev.DEPARTMENT_ID = 64013001 THEN 'Medical St Louis'
           WHEN pev.DEPARTMENT_ID = 64013005 THEN 'Mental Health St Louis'
           WHEN pev.DEPARTMENT_ID = 64013004 THEN 'Pharmacy St Louis'
           WHEN pev.DEPARTMENT_ID = 64004001 THEN 'Medical Wausau'
           WHEN pev.DEPARTMENT_ID = 64004004 THEN 'Mental Health Wausau'
           WHEN pev.DEPARTMENT_ID = 64012001 THEN 'Mental Health Denver'
           WHEN pev.DEPARTMENT_ID = 64012003 THEN 'Dental Denver'
           WHEN pev.DEPARTMENT_ID = 64012002 THEN 'Medical Denver'
           WHEN pev.DEPARTMENT_ID = 64012004 THEN 'Mental Health Denver'
           WHEN pev.DEPARTMENT_ID = 64012005 THEN 'Pharmacy Denver'
           WHEN pev.DEPARTMENT_ID = 64016001 THEN 'Mental Health Denver'
           WHEN pev.DEPARTMENT_ID IN ( 64018001, 64017006 ) THEN 'Dental Austin'
           WHEN pev.DEPARTMENT_ID = 64017001 THEN 'Medical Austin'
           WHEN pev.DEPARTMENT_ID = 64017004 THEN 'Pharmacy Austin'
           WHEN pev.DEPARTMENT_ID IN ( 64017002, 64017003, 64017005 ) THEN 'Mental Health Austin'
           WHEN pev.DEPARTMENT_ID = 64019001 THEN 'Medical Kansas City'
           WHEN pev.DEPARTMENT_ID = 64019004 THEN 'Pharmacy Kansas City'
           WHEN pev.DEPARTMENT_ID IN ( 64019002, 64019005 ) THEN 'Mental Health Kansas City'
           WHEN pev.DEPARTMENT_ID = 64020001 THEN 'Mental Health Chicago'
           WHEN pev.DEPARTMENT_ID = 64019006 THEN 'Dental Kansas City'
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
       CASE WHEN pev.DEPARTMENT_ID = 64015001 THEN 'D and R'
           WHEN pev.DEPARTMENT_ID = 64014001 THEN 'Keneen'
           ELSE 'CLINIC'
       END AS Site,
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
INTO #a
FROM Clarity.dbo.PAT_ENC_VIEW pev
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
                                                AND oc.OTHER_COMMUNIC_C = 1
    LEFT JOIN Clarity.dbo.ZC_STATE zs ON p.STATE_C = zs.STATE_C
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW id ON p.PAT_ID = id.PAT_ID
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    INNER JOIN Clarity.dbo.CLARITY_DEP_VIEW dep ON pev.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag ON pev.PAT_ID = flag.PATIENT_ID
    LEFT JOIN Clarity.dbo.COVERAGE c ON pev.COVERAGE_ID = c.COVERAGE_ID
    LEFT JOIN Clarity.dbo.CLARITY_EPM epm ON c.PAYOR_ID = epm.PAYOR_ID
    LEFT JOIN Clarity.dbo.ZC_FINANCIAL_CLASS zfc ON epm.FINANCIAL_CLASS = zfc.FINANCIAL_CLASS
    LEFT JOIN Clarity.dbo.ZC_PAT_NAME_SUFFIX suff ON suff.PAT_NAME_SUFFIX_C = p.PAT_NAME_SUFFIX_C
                                                     AND p.PAT_NAME_SUFFIX_C IN ( 0, 1, 2, 3, 4, 5, 1000, 1001, 1002 )
WHERE pev.CONTACT_DATE >= DATEADD(DAY, -8, GETDATE()) --If run on Monday this will go back to Saturday. If Run on Tues, will go to Sunday. If Wed, then Monday
      AND pev.CONTACT_DATE <= DATEADD(DAY, -2, GETDATE()) --If run on Monday this will go back to Saturday. If Run on Tues, will go to Sunday. If Wed, then Monday
      AND p4.PAT_LIVING_STAT_C = 1
      AND id.IDENTITY_ID NOT IN ( %s ) -- Survey opt outs
      AND SUBSTRING(dep.DEPT_ABBREVIATION, 9, 2) IN ( 'MD', 'DT', 'BH', 'MH', 'PY', '' )
      AND pev.APPT_STATUS_C IN ( 2, 6 )
      AND ser.PROV_ID NOT IN ( '640178', '640566' ) --BONNE, VALERIE JEAN, Jack Keegan
      AND ser.PROVIDER_TYPE_C IN ( '10', '117', '134', '136', '129', '164', '110', '1', '6', '9', '108', '177', '178' )
UNION
SELECT CASE WHEN o.DISPENSE_PHR_ID IN ( 410000015, 410000016 ) THEN 'SP0101' --WI RX
           WHEN o.DISPENSE_PHR_ID = 410000017 THEN 'SP0102'                  --CO RX
           WHEN o.DISPENSE_PHR_ID IN ( 410000018, 410000021 ) THEN 'SP0103'  --MO RX
           WHEN o.DISPENSE_PHR_ID = 410000019 THEN 'SP0104'                  --TX RX
                                                                             ---Add Chicago Phrmacy if/when opened
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
       NULL AS VISIT_PROV_ID,
       NULL AS PROV_NAME,
       'Pharmacist' AS PROV_TYPE,
       NULL AS PAYOR,
       CASE WHEN o.DISPENSE_PHR_ID = 410000015 THEN '64001004'
           WHEN o.DISPENSE_PHR_ID = 410000016 THEN '64011004'
           WHEN o.DISPENSE_PHR_ID = 410000017 THEN '64012005'
           WHEN o.DISPENSE_PHR_ID = 410000018 THEN '64013004'
           WHEN o.DISPENSE_PHR_ID = 410000019 THEN '64017004'
           WHEN o.DISPENSE_PHR_ID = 410000021 THEN '64019004'
       END AS DEPARTMENT_ID,
       CASE WHEN o.DISPENSE_PHR_ID = 410000015 THEN 'Pharmacy Milwaukee'
           WHEN o.DISPENSE_PHR_ID = 410000016 THEN 'Pharmacy Madison'
           WHEN o.DISPENSE_PHR_ID = 410000017 THEN 'Pharmacy Denver'
           WHEN o.DISPENSE_PHR_ID = 410000018 THEN 'Pharmacy St Louis'
           WHEN o.DISPENSE_PHR_ID = 410000019 THEN 'Pharmacy Austin'
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
      AND id.IDENTITY_ID NOT IN ( %s ) -- Survey opt outs
      AND phr.PHARMACY_NAME LIKE 'VIVENT PHARMACY%'
      AND o.FILL_STATUS_C = 80 -- Dispensed
;

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
FROM #a a
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
