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
       Prevention_Navigation_Session.MLAHIVHCV AS [(HIV/HCV) At this time: do you have a goal of lowering your risk of getting HIV or HCV?],
       Prevention_Navigation_Session.MLAHIVHCVRank AS [(HIV/HCV) What number represents where you are at on the motivational ladder?],
       Prevention_Navigation_Session.MLAOverdose AS [(Overdose) At this time: do you have a goal of lowering your risk of overdosing?],
       Prevention_Navigation_Session.MLAOverdoseRank AS [(Overdose) What number represents where you are at on the motivational ladder?],
       Prevention_Navigation_Session.MLAHepC AS [(HepC) At this time: do you have a goal of accessing treatment for Hepatitis C?],
       Prevention_Navigation_Session.MLAHepCRank AS [(HepC) What number represents where you are at on the motivational ladder?],
       Prevention_Navigation_Session.MLASubstanceUseTreatment AS [(SubstanceUseTreatment) At this time: do you have a goal of accessing substance use treatment?],
       Prevention_Navigation_Session.MLASubstanceUseTreatmentRank AS [(SubstanceUseTreatment) What number represents where you are at on the motivational ladder?],
       Prevention_Navigation_Session.MLASubstanceUseReduction AS [(SubstanceUseReduction) At this time: do you have a goal of reducing the amount of substances you use?],
       Prevention_Navigation_Session.MLASubstanceUseReductionRank AS [(SubstanceUseReduction) What number represents where you are at on the motivational ladder?],
       Prevention_Navigation_Session.MLASubstanceUseCessation AS [(SubstanceUseCessation) At this time: do you have a goal of stopping using substances completely?],
       Prevention_Navigation_Session.MLASubstanceUseCessationRank AS [(SubstanceUseCessation) What number represents where you are at on the motivational ladder?],
       Prevention_Navigation_Session.MLASubstanceUseAcceptance AS [(SubstanceUseAcceptance) At this time: do you have a goal of being more accepting of yourself regarding your substance use?],
       Prevention_Navigation_Session.MLASubstanceUseAcceptanceRank AS [(SubstanceUseAcceptance) What number represents where you are at on the motivational ladder?],
       Prevention_Navigation_Session.MLAStableHousing AS [(StableHousing) At this time: do you have a goal of accessing stable housing?],
       Prevention_Navigation_Session.MLAStableHousingRank AS [(StableHousing) What number represents where you are at on the motivational ladder?],
	   Prevention_Navigation_Session_Text.MLAComments AS [MLA Comments],
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
      AND Client_Profile.ClientType <> 'Test Client';
