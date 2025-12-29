SET ANSI_WARNINGS OFF;
SET NOCOUNT ON;

WITH
    contacts AS (
        SELECT Contact.ClientProfileID,
               SUM(1) AS DEPENDENT_COUNT
        FROM vivent.dbo.vwContact AS Contact
        WHERE Contact.DeleteFlag = 'N'
              AND Contact.SCPDependent = 'Yes'
        GROUP BY Contact.ClientProfileID
    )
SELECT Client_Profile.SCPMRN AS EPIC_MRN,
       CAST(MAX(Service_Provided.AActivityDate) AS DATE) AS LATEST_FOOD_PANTRY_SERVICE_DT
FROM Vivent.dbo.vwService_Provided AS Service_Provided
    INNER MERGE JOIN vivent.dbo.vwClient_Profile AS Client_Profile ON Service_Provided.ClientProfileID = Client_Profile.ClientProfileID
    LEFT MERGE JOIN contacts ON Service_Provided.ClientProfileID = contacts.ClientProfileID
WHERE Service_Provided.CPRServiceCategory LIKE 'Food%'
      AND Service_Provided.AActivityDate > '2018-12-31'
      AND Service_Provided.DeleteFlag = 'N'
      AND Service_Provided.AActivityDate !> GETDATE()
GROUP BY Client_Profile.SCPMRN;
