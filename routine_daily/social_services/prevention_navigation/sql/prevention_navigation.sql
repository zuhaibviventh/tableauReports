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
       Client_Profile.SCPRaceCat,
       Client_Profile.SCPEthnicity,
       Client_Profile.SCPGender AS GenderIdentity,
       COALESCE(REPLACE(Client_Profile.SCPZip, '-', ''), NULL) AS ZipCode,
       Program_Enrollment.CLAProgramStatus,
       CASE WHEN Program_Enrollment.AOrg = 'Vivent Health Colorado' THEN 'Colorado'
           WHEN Program_Enrollment.AOrg = 'Vivent Health Wisconsin' THEN 'Wisconsin'
           WHEN Program_Enrollment.AOrg = 'Vivent Health Missouri' THEN 'Missouri'
           WHEN Program_Enrollment.AOrg = 'Vivent Health Texas' THEN 'Texas'
           ELSE Program_Enrollment.AOrg
       END AS Service_State,
       Program_Enrollment.CLAProgramOfficeLocation,
       COALESCE(prev_provider.PreventionNavigator, 'None Assigned') AS PreventionNavigator,
	    --CASE
	    --    WHEN Prevention_Navigation_Session.ACreateBy LIKE '%/%' 
	    --    THEN LEFT(Prevention_Navigation_Session.ACreateBy, CHARINDEX('/', Prevention_Navigation_Session.ACreateBy) - 1) 
	    --    ELSE COALESCE(Prevention_Navigation_Session.ACreateBy, prev_provider.PreventionNavigator, 'None Assigned')
	    --END AS 'PreventionNavigator',
       CASE
		WHEN Prevention_Navigation_Session.ACreateBy LIKE '%/%' THEN SUBSTRING(Prevention_Navigation_Session.ACreateBy, 1, CHARINDEX('/', Prevention_Navigation_Session.ACreateBy) - 1) 
		WHEN Prevention_Navigation_Session.ACreateBy LIKE 'Import: %' THEN STUFF(Prevention_Navigation_Session.ACreateBy, 1, 8, '')
		ELSE Prevention_Navigation_Session.ACreateBy
		END AS ProviderName,
       CAST(Program_Enrollment.CLAProgramDateStart AS DATE) AS CLAProgramDateStart,
       CAST(Program_Enrollment.CLAProgramDateEnd AS DATE) AS CLAProgramDateEnd,
       CAST(Prevention_Navigation_Session.ACreateDate AS DATE) AS SessionCreateDate,
       -- Date when the session is marked Complete
       COALESCE(CAST(Prevention_Navigation_Session.EncounterDate AS DATE),CAST(Prevention_Navigation_Session.ACreateDate AS DATE), '1901-01-01') as DateCompleted,
       COALESCE(DATEDIFF(DAY, Prevention_Navigation_Session.EncounterDate, Prevention_Navigation_Session.ACreateDate), -999) AS DayToDataEntry,
       -- The difference between the [the Session Date] and [the date the session was last edited in PE or the date the session was marked complete]
       COALESCE(DATEDIFF(DAY, Prevention_Navigation_Session.ACreateDate, Prevention_Navigation_Session.DateCompleted), -999) AS DataEntryLag,
       Prevention_Navigation_Session.SessionNumber,
       Prevention_Navigation_Session.Status,
       GETDATE() AS UPDATE_DTTM
FROM Vivent.dbo.vwProgram_Enrollment AS Program_Enrollment
    INNER JOIN Vivent.dbo.vwClient_Profile AS Client_Profile ON Program_Enrollment.ClientProfileID = Client_Profile.ClientProfileID
    LEFT JOIN prev_provider ON Program_Enrollment.ClientProfileID = prev_provider.ClientProfileID
                               AND prev_provider.ROW_NUM_DESC = 1
    LEFT JOIN Vivent.dbo.vwPrevention_Navigation_Session AS Prevention_Navigation_Session ON Program_Enrollment.ClientProfileID = Prevention_Navigation_Session.ClientProfileID
                                                                                             AND Program_Enrollment.EnrollmentUniqueID = Prevention_Navigation_Session.EnrollmentUniqueID
WHERE Program_Enrollment.CLAProgramName = 'Prevention Navigation'
      AND COALESCE(Program_Enrollment.DeleteFlag, 'N') = 'N'
      AND Client_Profile.ClientType <> 'Test Client';


