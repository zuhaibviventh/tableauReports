/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Project 7 - 340B/MOU Eligible Clients
 Create Date:	10/5/2022
 Created By:	Sam Clay/Vivent/WI
 System:		pe.vi venthealth.org
 Requested By:	

 Purpose:		

 Description:
 

 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
 12/12/2022			Mitch				Adding AAM (and 340B even though it's not being used) as inclusion and as output
 12/12/2022			Mitch				Adding AAM service Provided to inclusion and MOU Columns
 12/12/2022			Mitch				Updated the output to include 'Access, Adherence & Monitoring' Services Provided under “Last MOU Service”
 12/12/2022			Mitch				Added a column for the last scanned medical note (within the last 18 months) marked as 340B document subtype
 12/12/2022			Mitch				Added a column for the last scanned medical note (within the last 18 months) regardless of document subtype
 12/14/2022			Mitch				Limiting Services provided (per Dan S) to sp.CPRServiceProvided IN ('MOU Access, Adherence and Monitoring Contact', 'MOU Case Management Contact', 'MOU Insurance Coordination Contact', 'AAM Assessment')
 1/3/2022			Mitch				sc.DocumentDateObtained is often NULL updating scanned docs to look for COALESCE(sc.DocumentDate, sc.ACreateDate) instead
**********************************************************************************************
*/

/*Step One: Determine who should be in the report. This not only includes eligible clients, but also includes clients who might potentially
			be eligible. So this list also requires some follow-up.*/
			
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

SELECT	DISTINCT
	crit.ClientProfileID
	,COUNT (1) AS CritCount

INTO	#Eligible

FROM
	(	SELECT	DISTINCT --Clients enrolled in certain programs
			pe.ClientProfileID
		
		FROM
			Vivent.dbo.vwProgram_Enrollment pe
		
		WHERE
			pe.DeleteDate IS NULL
			AND pe.CLAProgramName IN ('Access, Adherence & Monitoring', 'MOU', '340B')
			AND pe.CLAProgramDateStart < GETDATE()
			AND ISNULL (pe.CLAProgramDateEnd, DATEADD (DAY, 1, GETDATE())) > GETDATE()

		UNION ALL

		SELECT	DISTINCT --Clients who've received certain services
			sp.ClientProfileID

		FROM
			Vivent.dbo.vwService_Provided sp
		WHERE
			sp.DeleteDate IS NULL
			AND sp.CPRServiceProvided IN ('MOU Access, Adherence and Monitoring Contact', 'MOU Case Management Contact', 'MOU Insurance Coordination Contact', 'AAM Assessment')
			AND sp.CPRServiceFrom BETWEEN DATEADD (MONTH, -14, GETDATE()) AND GETDATE()

		UNION ALL

		SELECT	DISTINCT --Clients for whom a 340B Medical Note has been scanned
			sc.ClientProfileID
		FROM
			Vivent.dbo.vwScan sc
		WHERE
			sc.DeleteDate IS NULL
			AND sc.DocumentSubType LIKE '%340%'
			AND COALESCE(sc.DocumentDate, sc.ACreateDate) BETWEEN DATEADD(MONTH, -18, GETDATE()) AND GETDATE()
	) crit
	GROUP BY
		crit.ClientProfileID
;
/*Step Two: Attaching columns of data to eligible clients*/

SELECT --Getting clients who are currently enrolled in certain programs
	pe.ClientProfileID
	,MIN(pe.CLAProgramDateStart) CLAProgramDateStart
	,NULL CLAProgramDateEnd

INTO #ProgramEnrollment

FROM
	Vivent.dbo.vwProgram_Enrollment pe

WHERE
	pe.DeleteDate IS NULL
	AND pe.CLAProgramName IN ('Access, Adherence & Monitoring', 'MOU', '340B')
	AND pe.CLAProgramDateStart < GETDATE()
	AND ISNULL (pe.CLAProgramDateEnd, DATEADD (DAY, 1, GETDATE())) > GETDATE()  --This is just left over from when the report has parameters on it.

GROUP BY 
	pe.ClientProfileID
;

SELECT --Getting latest service provided from the last 14 months, of specific services. 
	sp.ClientProfileID
	,sp.CPRProvider
	,sp.CPRServiceFrom AS LastServiceDate
	,CASE
		WHEN sp.CPRServiceProvided LIKE 'MOU Access%'
		THEN 'AAM'
		WHEN sp.CPRServiceProvided LIKE 'MOU Case%'
		THEN 'MCM'
		WHEN sp.CPRServiceProvided LIKE 'MOU Insurance%'
		THEN 'Ins Coor'
		WHEN sp.CPRServiceProvided LIKE 'AAM%' THEN sp.CPRServiceProvided
		ELSE NULL
	END AS ContactType
	,ROW_NUMBER () OVER (PARTITION BY sp.ClientProfileID ORDER BY	sp.CPRServiceFrom DESC ,sp.CPRServiceProvided ASC) AS RowNum

INTO	#ServiceProvided

FROM
	Vivent.dbo.vwService_Provided sp

WHERE
	sp.DeleteDate IS NULL
	AND sp.CPRServiceProvided IN ('MOU Access, Adherence and Monitoring Contact', 'MOU Case Management Contact', 'MOU Insurance Coordination Contact', 'AAM Assessment')
	AND sp.CPRServiceFrom BETWEEN DATEADD (MONTH, -14, GETDATE()) AND GETDATE()
	
;

SELECT --Latest service provided from the last 14 months from a larger list of services from a list provided by Dan S, to be used to see if the CMs are documenting correction
	sp.ClientProfileID
	,sp.CPRProvider
	,sp.CPRServiceFrom AS 'Last Any Service Date'
	,sp.CPRServiceProvided 'Last Any Service'
	,ROW_NUMBER () OVER (PARTITION BY sp.ClientProfileID ORDER BY sp.CPRServiceFrom DESC ,sp.CPRServiceProvided ASC	) AS RowNum

INTO	#AnyServiceProvided

FROM
	Vivent.dbo.vwService_Provided sp

WHERE
	sp.DeleteDate IS NULL
	AND sp.CPRServiceFrom BETWEEN DATEADD (MONTH, -14, GETDATE()) AND GETDATE()
	AND sp.CPRServiceCategory IN ( 'Access, Adherence and Monitoring', 'Community Case Management', 'Emergency Financial Assistance', 'Health Insurance Premium and Cost Sharing Assistance for Low-Income Individuals (HIP/CSA)', 'Insurance Coordination', 'Medical Case Management (MCM), including Treatment Adherence Services', 'Medical Transportation', 'MOU' )

;

SELECT --Most recent ROI
	roi.ClientProfileID
	,roi.CSRReleaseTo
	,roi.CSReleaseDate
	,roi.CSREndDate
	,ROW_NUMBER () OVER (PARTITION BY roi.ClientProfileID ORDER BY roi.CSReleaseDate DESC) AS RowNum

INTO	#ReleaseOfInformation

FROM
	Vivent.dbo.vwRelease_Of_Information roi

WHERE
	roi.MOURelated = 'Yes'
	AND roi.DeleteDate IS NULL
	AND roi.ClientProfileID IN ( SELECT ClientProfileID FROM #Eligible )
;

SELECT --340B-related medical note
	sc.ClientProfileID
	,sc.DocumentDate 'Last Valid 340B Medical Note Scan'
	,ROW_NUMBER() OVER (PARTITION BY sc.ClientProfileID ORDER BY COALESCE(sc.DocumentDate, sc.ACreateDate) DESC) AS ROW_NUM_DESC

INTO #340b_mn

FROM
	Vivent.dbo.vwScan sc

WHERE
	sc.DeleteDate IS NULL
	AND sc.DocumentSubType LIKE '%340%'
	AND COALESCE(sc.DocumentDate, sc.ACreateDate) BETWEEN DATEADD(MONTH, -18, GETDATE()) AND GETDATE()
;

SELECT --Any medical note to check to see if a CM put one in and forgot to mark it as 340B
	sc.ClientProfileID
	,sc.DocumentDate 'Last Valid Any Medical Note Scan'
	,ROW_NUMBER() OVER (PARTITION BY sc.ClientProfileID ORDER BY COALESCE(sc.DocumentDate, sc.ACreateDate) DESC) AS ROW_NUM_DESC

INTO #any_mn

FROM
	Vivent.dbo.vwScan sc

WHERE
	sc.DeleteDate IS NULL
	AND sc.DocumentType = 'Medical Documents'
	AND COALESCE(sc.DocumentDate, sc.ACreateDate) BETWEEN DATEADD(MONTH, -18, GETDATE()) AND GETDATE()

;
/*Step Three: Assemble the data above into a final result set*/

SELECT DISTINCT
	cp.SCPClientID 'PE Client ID'
	,cp.SCPClientFirst 'First Name'
	,cp.SCPClientLast 'Last Name'
	,cp.SCPMRN 'Epic MRN'
	,cp.SCPState 'State of Residence'
	,cp.SCPCity 'City of Residence'
	,pe.CLAProgramDateStart 'Program Enrollment Date'
	,pe.CLAProgramDateEnd 'Enrollment End Date'
	,sp.LastServiceDate 'Last MOU Service'
	,sp.ContactType 'Last MOU Type'
	,sp.CPRProvider 'Last MOU Service Provider'
	,spa.CPRProvider 'Last Any Service Provider'
	,mn.[Last Valid 340B Medical Note Scan]
	,am.[Last Valid Any Medical Note Scan]
	,spa.[Last Any Service Date]
	,spa.[Last Any Service]
	,roi.CSREndDate 'ROI End Date'
	,roi.CSRReleaseTo 'ROI To'
	,CASE
		WHEN elg.CritCount = 3	THEN 'Y'
		ELSE 'N'
	END AS FullyEligible
	,CONVERT(nvarchar(30), GETDATE(), 101) AS 'Today'

FROM
	Vivent.dbo.vwClient_Profile_all cp
	INNER JOIN #Eligible elg ON cp.ClientProfileID = elg.ClientProfileID
	LEFT OUTER JOIN #ProgramEnrollment pe ON cp.ClientProfileID = pe.ClientProfileID
	LEFT OUTER JOIN #ServiceProvided sp ON cp.ClientProfileID = sp.ClientProfileID
										AND sp.RowNum = 1
	LEFT OUTER JOIN #AnyServiceProvided spa ON cp.ClientProfileID = spa.ClientProfileID
										AND spa.RowNum = 1
	LEFT OUTER JOIN #ReleaseOfInformation roi ON cp.ClientProfileID = roi.ClientProfileID
										AND roi.RowNum = 1
	LEFT JOIN #340b_mn mn ON mn.ClientProfileID = cp.ClientProfileID
										AND mn.ROW_NUM_DESC = 1
	LEFT JOIN #any_mn am ON am.ClientProfileID = cp.ClientProfileID
										AND am.ROW_NUM_DESC = 1

;

DROP TABLE #ReleaseOfInformation;
DROP TABLE #AnyServiceProvided;
DROP TABLE #ServiceProvided;
DROP TABLE #ProgramEnrollment;
DROP TABLE #Eligible;
DROP TABLE #340b_mn;
DROP TABLE #any_mn;