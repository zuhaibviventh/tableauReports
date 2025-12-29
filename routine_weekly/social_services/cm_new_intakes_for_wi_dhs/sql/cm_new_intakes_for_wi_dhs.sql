/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	CM New Intakes for WI DHS
 Create Date:	7/19/2022
 Created By:	Mitch Scoggins/Vivent/WI
 System:		pe.viventhealth.org
 Requested By:	WI DHS

 Purpose:		Identify newly enrolled CM clients who haven't been enrolled in the last 3 years and who've not had a VH medical appt

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------


**********************************************************************************************

 */
 
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

 SELECT
	cp.SCPClientFirst 'Client First Name'
	,cp.SCPClientLast 'Client Last Name'
	,al.SCPAliases 'AKA/Alias'
	,CONVERT(nvarchar(30), cp.SCPDateOfBirth, 101) AS DOB
	,COALESCE(cp.SCPBirthGender, 'Unknown') 'Sex Assigned at Birth'
	,cp.SCPGender 'Gender Identity'
	,CASE
		WHEN cp.SCPRaceCat IS NULL THEN 'Unknown'
		WHEN cp.SCPRaceCat = '' THEN 'Unknown'
		ELSE cp.SCPRaceCat
	END AS 'Race'
	,COALESCE(cp.SCPEthnicity, 'Unknown') 'Ethnicity'
	,pr.SCPProvider 'Case Manager'
	,CONVERT(nvarchar(30), pe.StartDate, 101) AS  'Date of Intake'
	,CONVERT(nvarchar(30), cp.CMACondHIVDateDiagnose, 101) AS  'Date of HIV Dx'
	,pe.CLAProgramOfficeLocation 'Intake Office'
	,'Grouper' Grouper
	,CONVERT(nvarchar(30), GETDATE(), 101) AS 'FileDate'
 
FROM 
	vivent.dbo.vwClient_Profile_All cp
	INNER JOIN vivent.dbo.vwProvider_Relationship pr ON pr.ClientProfileID = cp.ClientProfileID
	INNER JOIN vivent.dbo.vwProgram_Enrollments_All2016 pe ON cp.ClientProfileID = pe.ClientProfileID
	LEFT JOIN vivent.dbo.vwClient_Profile_SCPAliases al ON al.SQLID = cp.SQLID
						AND al.IDX = 0
					
WHERE
	pe.Program IN ( 'Brief Services', 'Linkage to Care', 'Outreach Case Management', 'Early Intervention Services',
	'Community Case Management', 'Clinic Case Management', 'Access, Adherence & Monitoring',
	'Supportive Case Management')
	AND pr.SCPProviderRelationship IN ('Case Manager Intern', 'EIS Case Manager', 'Early Intervention Services Case Manager', 'Community Case Manager', 
							'Clinic Case Manager', 'Brief Services Worker', 'Linkage to Care Case Manager', 'Vivent Health Case Manager Intern', 
							'Outreach Case Manager', 'Supportive Case Manager' ) 
	AND	pe.Status = 'Admitted'
	AND pr.DeleteFlag = 'N'
	AND pr.SCPProviderDateEnd IS NULL
	AND pe.StartDate > DATEADD(DAY, -9, GETDATE())
	AND pe.AORG = 'Vivent Health Wisconsin'
	AND pe.ClientProfileID NOT IN
		(SELECT 
			pe.ClientProfileID

			FROM 
			vivent.dbo.vwProgram_Enrollments_All2016 pe
					
		WHERE
			pe.Program IN ( 'Brief Services', 'Linkage to Care', 'Outreach Case Management', 'Early Intervention Services',
			'Community Case Management', 'Clinic Case Management', 'Access, Adherence & Monitoring',
			'Supportive Case Management')
			AND pe.DeleteFlag = 'N'
			AND pe.StartDate BETWEEN DATEADD(MONTH, -36, GETDATE()) AND DATEADD(DAY, -9, GETDATE())
			AND pe.AORG = 'Vivent Health Wisconsin'
		)
	--AND pe.ClientProfileID NOT IN
	--	(SELECT
	--	 	a.ClientProfileID

	--	 FROM 
	--	 	vivent.dbo.vwAppointment a

	--	WHERE
	--		a.DeleteFlag = 'N'
	--		AND a.CLNApptDate > DATEADD(MONTH, -24, GETDATE())
	--		AND a.CLNApptSubject IN ('HIV Medical Care','Medical Care - ARCW')
	--		AND a.ACreateBy LIKE 'Provide%'
	--	)