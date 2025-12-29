/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	HCV Testing Data for Wisconsin
 Create Date:	6/1/2022
 Created By:	Mitch Scoggins/Vivent/WI
 System:		pe.viventhealth.org
 Requested By:	Kristen G

 Purpose:		Hep C Testing Data for the State. Weekly delivery.

 Description:	
 

 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

 SELECT 
	CONVERT(nvarchar(30), ts.ACreateDate, 101) AS 'Date Added'
	,CONVERT(nvarchar(30), ts.AActivityDate, 101) AS 'Date Updated'
	,ts.CLAProgramOfficeLocation 'Provider Creating'
	,ts.CPRProvider	'User Creating'
	,CASE
		WHEN ts.ConfirmatoryHCVTestResult IS NOT NULL THEN ts.ConfirmatoryHCVTestResult
		WHEN ts.HCVTestType = 'Blood Draw' THEN ts.HCVRapidTestResult --Blood draw values still stored in this column if its done as the first step.
		WHEN ts.ConfirmatoryHCVTestResult IS NULL THEN 'Not Performed'
		ELSE ts.ConfirmatoryHCVTestResult	
	END AS 'Confirmatory test result'
	,ts.SexWithFemale 'Female'
	,CASE
		WHEN ts.HCVTestType = 'Rapid Test' THEN ts.HCVRapidTestResult
		WHEN ts.HCVTestType IS NULL THEN ts.HCVRapidTestResult
		ELSE 'Not Performed'
	END AS 'HCV rapid test result'
	,ts.SexWithMale	'Male'
	,ts.SexWithNonBinary	'Non-binary' ----This is sex with
	,CONVERT(nvarchar(30), ts.TestDate, 101) AS 'Start Date'-----Per Kristen, this is the date of the test
	,CASE
		WHEN ts.TransType = 'Female to Male' THEN 'Yes'
		ELSE 'No'
	END AS 'Transgender Female to Male'
	,CASE
		WHEN ts.TransType = 'Male to Female'THEN 'Yes'
		ELSE 'No'
	END AS 	'Transgender Male to Female'
	,CASE
		WHEN ts.ConfirnatoryHCVTestResultDate IS NOT NULL THEN 'Yes'
		ELSE 'No'
	END AS 'Was confirmatory (RNA) specimen drawn?'
	,ts.SCPClientFirst	'First Name'
	,ts.SCPClientLast	'Last Name'
	,ts.SCPClientMI	'Middle Name'
	,ts.SCPStreetAddress1 + ' ' + (COALESCE(ts.SCPStreetAddress2,''))	'Address'
	,ts.SCPState	'State'
	,LEFT(ts.SCPZip, 5)	'Zip Code'
	,ts.SCPCity	'City'
	,ts.SCPCounty	'County'
	,ts.SCPGender	'Current Gender Identity'
	,ts.SCPEthnicity	'Ethnicity'
	,race.SCPRaceAll	'Race'
	,CONVERT(nvarchar(30), ts.SCPDateOfBirth, 101) AS 	'Date of Birth'
	,ts.IncarceratedEver	'Been incarcerated in jail or prison ever in your lifetime?'
	,ts.ClientLinked	'Client was linked to Hepatitis C treatment provider'
	,ts.ConfirmatoryHCVTestResultProvided	'Confirmatory results provided to client?'
	,CONVERT(nvarchar(30), ts.TestDate, 101)	'End Date' --End of what? Per Kristen, this is meaningless
	,NULL /*ts.Notes*/	'General notes section' --this is multi-line and causing a problem with the export
	,ts.ConfirmatoryHCVTestResultNotProvided	'If confirmatory test results were not provided to client, why?'
	,CASE
		WHEN ts.ConfirmatorySpecimenDrawn = 'No' THEN ts.ConfirmSpecimenNotDrawn	
	END AS 'If confirmatory test was not drawn, why?'
	,ts.HCVTestResultNotProvided	'If results were not provided, why?'
	,ts.JailWhereTestDone	'If test was completed in a jail, list name of jail'
	,ts.LinkedProviderName	'If yes, what provider was the client linked with?' 
	,ts.Inject6Months	'Injected drugs in the past 6 months?'
	,ts.InjectDrugsEver	'Injected drugs ever in your lifetime?'
	,CASE
		WHEN ts.OpioidStudyID IS NOT NULL THEN 'Yes'
		ELSE 'No'
	END AS 'Opioid SNT (Greater WI Only)'
	,ts.SexWithNonOther	'Other' 
	,ts.HCVTestResultProvided	'Results provided to client?'
	,ts.SharedIntranasalEquipEver	'Shared equipment used to snort drugs ever in your lifetime?'
	,ts.SharedSmokeEquipEver	'Shared equipment used to smoke drugs ever in your lifetime?'
	,ts.SharedInjectEquipEver	'Shared injection drug use equipment in the past 6 months?'
	,ts.SharedEquipmentEver	'Shared injection drug use equipment ever in your lifetime?'
	--,REPLACE(RIGHT(ts.SiteName, charindex('-', REVERSE(ts.SiteName), charindex('-', REVERSE(ts.SiteName))+1)-2), ' (ARCW)', '') 	'Test Location'
	,CASE				
		WHEN ps.SiteType =	'F01.01 - Clinical - Inpatient hospital F02.12- Clinical -TB clinic'	THEN	'Outreach'
		WHEN ps.SiteType =	'F02.19 - Clinical - Substance abuse treatment facility'	THEN	'AODA Tx'
		WHEN ps.SiteType =	'F02.51 - Clinical - Community health center'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F03%'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F08%'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F09%'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F10%'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F11%'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F12%'	THEN	'Jail'
		WHEN ps.SiteType LIKE	'F13%'	THEN	'Outreach'
		WHEN ps.SiteType =	'F04.05 - Non-clinical - HIV testing site'	THEN	'Office'
		WHEN ps.SiteType =	'F06.02 - Non-clinical - Community setting - School/educational facility'	THEN	'Outreach'
		WHEN ps.SiteType =	'F06.03 - Non-clinical - Community setting - Church/mosque/synagogue/temple'	THEN	'Outreach'
		WHEN ps.SiteType =	'F06.04 - Non-clinical - Community Setting - Shelter/transitional housing'	THEN	'Outreach'
		WHEN ps.SiteType =	'F06.05 - Non-clinical - Community setting - Commercial facility'	THEN	'Outreach'
		WHEN ps.SiteType =	'F06.07 - Non-clinical - Community setting - Bar/club/adult entertainment'	THEN	'Outreach'
		WHEN ps.SiteType =	'F06.08 - Non-clinical - Community setting - Public area'	THEN	'Outreach'
		WHEN ps.SiteType =	'F06.12 - Non-clinical - Community setting - Individual residence'	THEN	'Outreach'
		WHEN ps.SiteType =	'F06.88 - Non-clinical - Community setting - Other'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F07%'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F14%'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F15%'	THEN	'Outreach'
		WHEN ps.SiteType LIKE	'F40%'	THEN	'Van'
		WHEN ps.SiteType LIKE	'F50%'	THEN	'Home'
		WHEN ps.SiteType LIKE	'F88%'	THEN	'Outreach'
	END AS 'Test Location'
	--,ps.SiteType
	,ts.OpioidStudyID	'Opioid SNT study ID (Greater WI only)'
	,ts.BestPhoneContact	'Telephone'
	,SUBSTRING(ts.BestPhoneContact, 2, 3)	'Telephone Area Code'
	,ts.SCPEmailAddr	'Email'

FROM
	Vivent.dbo.vwTesting_Session ts
	LEFT JOIN vivent.dbo.vwTesting_Session_SCPRaceAll race ON race.SQLID = ts.SQLID	
						AND race.IDX = 0
	INNER JOIN vivent.dbo.vwPrevention_Site ps ON ps.SiteName = ts.SiteName

WHERE
	ts.AOrg = 'Vivent Health Wisconsin'
	AND COALESCE(ts.DeleteFlag, 'N') = 'N'
	AND ts.TestType IN ('HIV/STI & HCV', 'HCV-only')
	--AND ts.EncounterCompleteDate BETWEEN @Start_Date AND @End_Date  -------------------These are NULL for tests before 6/13
	AND ts.Status = 'Encounter Complete'
	AND ts.EncounterCompleteDate BETWEEN DATEADD(DAY, -9, GETDATE()) AND DATEADD(DAY, -2, GETDATE()) 

;
