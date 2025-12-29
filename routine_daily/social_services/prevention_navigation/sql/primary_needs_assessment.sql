/* 10.200.180.16 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

-- you cannot participate in sessions without being enrolled
WITH
    prev_provider AS (
    SELECT Provider_Relationship.ClientProfileID,
           Provider_Relationship.SCPProvider AS PreventionNavigator,
           ROW_NUMBER() OVER (PARTITION BY Provider_Relationship.ClientProfileID
                              ORDER BY Provider_Relationship.SCPProviderDateStart DESC) AS ROW_NUM_DESC
    FROM Vivent.dbo.vwProvider_Relationship AS Provider_Relationship
    WHERE Provider_Relationship.SCPProviderRelationship = 'Prevention Navigator'
          AND COALESCE(Provider_Relationship.DeleteFlag, 'N') = 'N'
)
SELECT Client_Profile.SCPClientID,
       Client_Profile.SCPClientFirst,
       Client_Profile.SCPClientLast,
       CASE WHEN Program_Enrollment.AOrg = 'Vivent Health Colorado' THEN 'Colorado'
           WHEN Program_Enrollment.AOrg = 'Vivent Health Wisconsin' THEN 'Wisconsin'
           WHEN Program_Enrollment.AOrg = 'Vivent Health Missouri' THEN 'Missouri'
           WHEN Program_Enrollment.AOrg = 'Vivent Health Texas' THEN 'Texas'
           ELSE 'ERROR'
       END AS Service_State,
       Program_Enrollment.CLAProgramOfficeLocation,
       --COALESCE(prev_provider.PreventionNavigator, 'None Assigned') AS PreventionNavigator,
	   CASE
	   		WHEN Prevention_Navigation_Session.ACreateBy LIKE '%/%' 
	   		THEN LEFT(Prevention_Navigation_Session.ACreateBy, CHARINDEX('/', Prevention_Navigation_Session.ACreateBy) - 1) 
	   		ELSE COALESCE(Prevention_Navigation_Session.ACreateBy, prev_provider.PreventionNavigator, 'None Assigned')
	   	END AS 'PreventionNavigator',
       Prevention_Navigation_Session.SessionNumber,
       Prevention_Navigation_Session.PNAFoodSecurity AS [Food Security],
       Prevention_Navigation_Session.PNAFoodSecurityRank AS [Food Security Rank],
       Prevention_Navigation_Session.PNATransportation AS [Transportation],
       Prevention_Navigation_Session.PNATransportationRank AS [Transportation Rank],
       Prevention_Navigation_Session.PNASleepHousing AS [Sleep/Housing],
       Prevention_Navigation_Session.PNASleepHousingRank AS [Sleep/Housing Rank],
       Prevention_Navigation_Session.PNAInsurance AS [Insurance],
       Prevention_Navigation_Session.PNAInsuranceRank AS [Insurance Rank],
       Prevention_Navigation_Session.PNAHealthcare AS [Healthcare],
       Prevention_Navigation_Session.PNAHealthcareRank AS [Healthcare Rank],
       Prevention_Navigation_Session.PNAMentalHealth AS [Mental Health],
       Prevention_Navigation_Session.PNAMentalHealthRank AS [Mental Health Rank],
       Prevention_Navigation_Session.PNAIncome AS [Income],
       Prevention_Navigation_Session.PNAIncomeRank AS [Income Rank],
       Prevention_Navigation_Session.PNARelationships AS [Relationships],
       Prevention_Navigation_Session.PNARelationshipsRank AS [Relationships Rank],
       Prevention_Navigation_Session.PNALegal AS [Legal],
       Prevention_Navigation_Session.PNALegalRank AS [Legal Rank],
       Prevention_Navigation_Session_Text.PNAComments AS [PNA Comments],
       GETDATE() AS UPDATE_DTTM
FROM Vivent.dbo.vwProgram_Enrollment AS Program_Enrollment
    INNER JOIN Vivent.dbo.vwClient_Profile AS Client_Profile ON Program_Enrollment.ClientProfileID = Client_Profile.ClientProfileID
    LEFT JOIN prev_provider ON Program_Enrollment.ClientProfileID = prev_provider.ClientProfileID
                               AND prev_provider.ROW_NUM_DESC = 1
    LEFT JOIN Vivent.dbo.vwPrevention_Navigation_Session AS Prevention_Navigation_Session ON Program_Enrollment.ClientProfileID = Prevention_Navigation_Session.ClientProfileID
                                                                                             AND Program_Enrollment.EnrollmentUniqueID = Prevention_Navigation_Session.EnrollmentUniqueID
    LEFT JOIN Vivent.dbo.vwPrevention_Navigation_Session_Text AS Prevention_Navigation_Session_Text ON Prevention_Navigation_Session.SQLID = Prevention_Navigation_Session_Text.SQLID
WHERE Program_Enrollment.CLAProgramName = 'Prevention Navigation'
      AND COALESCE(Program_Enrollment.DeleteFlag, 'N') = 'N'
      AND Client_Profile.ClientType <> 'Test Client'
	  AND Client_Profile.SCPClientLast NOT LIKE 'test-%';