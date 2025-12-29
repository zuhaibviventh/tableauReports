/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	austin_client_wo_tct_id
 Create Date:	3/3/2024
 Created By:	Reporting Account/VIVENT
 System:		10.200.180.16
 Requested By:	Lourdes P

 Description:
 

 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

 --------Identity Clients with TCT IDs and Write to Temp
 IF OBJECT_ID('tempdb..#a') IS NOT NULL									
 DROP TABLE #a;

SELECT DISTINCT
	cp.ClientProfileID
	,tct.TCTClientID

INTO #a

FROM
	Vivent.dbo.vwClient_Profile cp
	CROSS APPLY ---More effecient than join (at least in this case)
		(SELECT	TOP 1
			*
		FROM
			Vivent.dbo.vwTCT_Client_Link t
	
		WHERE
			t.ClientProfileID = cp.ClientProfileID
			AND ISNULL (t.DeleteFlag, '') <> 'Y'
	
		ORDER BY
			t.EffectiveDate DESC
			,t.LinkDate DESC
			,t.ACreateDate DESC
			,t.SQLID
		) tct

--ORDER BY
--	cp.SCPClientID

;

SELECT
	cp.SCPClientID 'Client ID'
	,cp.SCPClientFirst 'First Name'
	,cp.SCPClientLast 'Last Name'
	--,a.TCTClientID
	,COALESCE(cp.ClientType, 'MISSING - Please set in PE') 'Client Type'
	,COALESCE(cp.SCPeUCI, 'EUCI Only For RW Clients') 'EUCI'

FROM 
	vivent.dbo.vwClient_Profile cp
	LEFT JOIN #a a ON a.ClientProfileID = cp.ClientProfileID
	INNER JOIN
		(SELECT DISTINCT ----Client receiving Services in Austin
		 	sa.ClientProfileID
			
		 FROM 
		 	vivent.dbo.vwService_Activity_All sa

		WHERE
			sa.AOrg = 'Vivent Health Texas'
			AND COALESCE(sa.DeleteFlag, 'N') = 'N'
			AND sa.ServiceDate > '11/1/2023'
			AND COALESCE(sa.ServiceCategory, 'x') IN ('x','Early Intervention Services (EIS)','Housing','Supportive Case Management Services (Non-MCM)','Medical Nutrition Therapy',
								'Health Insurance Premium and Cost Sharing Assistance for Low-Income Individuals (HIP/CSA)','Food Bank/Home Delivered Meals',
								'Medical Case Management (MCM), including Treatment Adherence Services','Insurance Coordination','Housing Navigation','Emergency Financial Assistance',
								'Referral')

		UNION

		SELECT 
			a.ClientProfileID

		FROM 
			vivent.dbo.vwAppointment a 

		WHERE
			COALESCE(a.DeleteFlag, 'N') = 'N'
			AND a.CLNApptDate > '11/1/2023'
			AND a.AOrg = 'Vivent Health Texas'
			AND a.CLNApptStatus = 'Kept'
			AND a.CLNEpicFillerID IS NOT NULL --Appt interfaced from Epic

		) serv ON serv.ClientProfileID = cp.ClientProfileID

WHERE
	a.TCTClientID IS NULL
	AND COALESCE(cp.ClientType, 'x') IN ('x', 'Ryan White')