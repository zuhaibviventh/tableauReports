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
       Prevention_Navigation_Session.IRAHIVAssessnent AS [Did you discuss the HIV assessment with the client?],
       Prevention_Navigation_Session.IRAHIVTransmission AS [How confident is the client in their knowledge of HIV is transmitted?],
       Prevention_Navigation_Session.IRAHIVTested AS [How often does the client get tested for HIV?],
       Prevention_Navigation_Session.IRAHIVCondomUse AS [How often does the client use condoms when sexually active?],
       Prevention_Navigation_Session.IRAHIVPrEPKnowledge AS [How often is the client in their knowledge of PrEP?],
       Prevention_Navigation_Session.IRAHIVObtainPrEP AS [How confident is the client in their ability to obtain PrEP?],
       Prevention_Navigation_Session.IRAHCVAssessnent AS [Did you discuss the HCV assessment with the client?],
       Prevention_Navigation_Session.IRAHCVStatus AS [Does the client know their HCV status?],
       Prevention_Navigation_Session.IRAHCVTransmission AS [How confident is the client in their knowledge of how HCV is transmitted?],
       Prevention_Navigation_Session.IRAHCVSharePersonalHygiene AS [How often does the client share razors, toothbrushes, and/or nail clippers?],
       Prevention_Navigation_Session.IRAHCVTested AS [How often does the client get tested for HCV?],
       Prevention_Navigation_Session.IRAHCVTreatment AS [How confident is the client in accessing HCV treatment, if needed?],
       Prevention_Navigation_Session.IRAHCVSharePeriphernalia AS [How often does the client share straws or pipes for snorting and/or smoking?],
       Prevention_Navigation_Session.IRAInjectAssessment AS [Did you discuss the Safer Injection assessment with the client?],
       Prevention_Navigation_Session.IRAInjectUnusedSyringe AS [How often does the client use an un-used syringe and works when injecting?],
       Prevention_Navigation_Session.IRAInjectRotateSite AS [How often does the client rotate injection sites?],
       Prevention_Navigation_Session.IRAInjectSharps AS [How often does the client use a sharps container or return syringes to a syringe service program?],
       Prevention_Navigation_Session.IRAInjectUnusedSupplies AS [How confident is the client in accessing un-used supplies from a syringe service program or other source?],
       Prevention_Navigation_Session.IRAInjectSyringes AS [How comfortable is the client in accessing services via a syringe service program?],
       Prevention_Navigation_Session.IRAInjectPrepareInject AS [How confident is the client in preparing and injecting without the help of others?],
       Prevention_Navigation_Session.IRAInfection AS [How often in the past 6 months does the client report having a skin or soft tissue infection when related to injecting?],
       Prevention_Navigation_Session_IRAInfectionTreatment.IRAInfectionTreatment AS [What treatment methods were used with the client's most recent skin or soft tissue infection?],
       Prevention_Navigation_Session.IRAOpioidOverdose AS [Did you discuss the Opioid Overdose assessment with the client?],
       Prevention_Navigation_Session.OOAUse AS [How often does the client report using drugs in the past week?],
       Prevention_Navigation_Session.OOAOverdoses AS [How many times has the client overdosed in the past six months?],
       Prevention_Navigation_Session.OOARecognizeOverdose AS [How confident is the client of their ability to recognize signs of an overdose?],
       Prevention_Navigation_Session.OOANaloxoneUse AS [How confident is the client in how to use naloxone?],
       Prevention_Navigation_Session.OOACarryingNaloxone AS [How often does the client report carrying naloxone or having it nearby during use?],
       Prevention_Navigation_Session.OOARescueBreathing AS [How confident is the client in their ability to perform rescue breathing on someone else?],
       Prevention_Navigation_Session.OOAUsing911 AS [How often does the client report calling 911 when witnessing or responding to an overdose?],
       Prevention_Navigation_Session.OOAGoodSamaritanLaw AS [How confident is the client in the protections afforded to them under the Good Samaritan Law?],
       Prevention_Navigation_Session.OOADrugChecking AS [How often does the client use drug checking methods like test strips?],
       Prevention_Navigation_Session.OOATaste AS [How often does the client taste their shot before injecting?],
       Prevention_Navigation_Session.OOAUseAlone AS [How often does the client use alone?],
       Prevention_Navigation_Session.IRASubstanceUseTreatment AS [Did you discuss the Substance Use Treatment assessment with the client?],
       Prevention_Navigation_Session.SUTASubstanceUseTreatmentOptions AS [How confident is the client in their knowledge of what substance use treatment options are available in their area?],
       Prevention_Navigation_Session.SUTASubstanceUseTreatment AS [How confident is the client in accessing their preferred method of substance use treatment if they wanted to?],
       Prevention_Navigation_Session.SUTALikelyTreatment AS [How likely is the client to seek out their preferred substance use treatment method if they wanted to?],
       Prevention_Navigation_Session.SUTAUnableToAccessTreatment AS [In the past year has the client been unable to access their preferred substance use treatment method when they tried to?],
       Prevention_Navigation_Session.IRAStigma AS [Did you discuss the Stigma assessment with the client?],
       Prevention_Navigation_Session.SAAvoidHealthcare AS [How likely is the client to avoid healthcare due to their substance use?],
       Prevention_Navigation_Session.SAPrimaryCareProvider AS [How likely is the client to tell their primary care provider about their substance use?],
       Prevention_Navigation_Session.SAAvoidSocial AS [How likely is the client to avoid social situations due to their substance use?],
       Prevention_Navigation_Session.SAAvoidFamily AS [How likely is the client to avoid family and friends due to their substance use?],
       Prevention_Navigation_Session.SAHowIsolated AS [How isolated does the client's substance use make them feel?],
       Prevention_Navigation_Session_Text.IRAComments,
       GETDATE() AS UPDATE_DTTM
FROM Vivent.dbo.vwProgram_Enrollment AS Program_Enrollment
    INNER JOIN Vivent.dbo.vwClient_Profile AS Client_Profile ON Program_Enrollment.ClientProfileID = Client_Profile.ClientProfileID
    LEFT JOIN prev_provider ON Program_Enrollment.ClientProfileID = prev_provider.ClientProfileID
                               AND prev_provider.ROW_NUM_DESC = 1
    LEFT JOIN Vivent.dbo.vwPrevention_Navigation_Session AS Prevention_Navigation_Session ON Program_Enrollment.ClientProfileID = Prevention_Navigation_Session.ClientProfileID
                                                                                             AND Program_Enrollment.EnrollmentUniqueID = Prevention_Navigation_Session.EnrollmentUniqueID
    LEFT JOIN Vivent.dbo.vwPrevention_Navigation_Session_IRAInfectionTreatment AS Prevention_Navigation_Session_IRAInfectionTreatment ON Prevention_Navigation_Session.SQLID = Prevention_Navigation_Session_IRAInfectionTreatment.SQLID
    LEFT JOIN Vivent.dbo.vwPrevention_Navigation_Session_Text AS Prevention_Navigation_Session_Text ON Prevention_Navigation_Session.SQLID = Prevention_Navigation_Session_Text.SQLID
WHERE Program_Enrollment.CLAProgramName = 'Prevention Navigation'
      AND COALESCE(Program_Enrollment.DeleteFlag, 'N') = 'N'
      AND Client_Profile.ClientType <> 'Test Client';
