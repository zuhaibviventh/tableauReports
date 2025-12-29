/* 10.200.180.16 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#id') IS NOT NULL									
DROP TABLE #id;

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Condom' AS Item_Type,
    Prevention_Supplies.PSCondomDistFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSCondomDistribution AS Distribution_Count

INTO #id

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSCondomDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'HygienKit' AS Item_Type,
    Prevention_Supplies.PSHygieneKitDistFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSHygieneKitDistribution AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSHygieneKitDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Naloxone (Injection)' AS Item_Type,
    Prevention_Supplies.PSInjectNaloxoneDistFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSInjectNaloxoneDistribution AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSInjectNaloxoneDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Lube' AS Item_Type,
    Prevention_Supplies.PSLubeDistFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSLubeDistribution AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSLubeDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Naloxone (Nasal)' AS Item_Type,
    Prevention_Supplies.PSNasalNaloxoneDistFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSNasalNaloxoneDistribution AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSNasalNaloxoneDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'SaferSex Kit' AS Item_Type,
    Prevention_Supplies.PSSaferSexKitDistFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSSaferSexKitDistribution AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSSaferSexKitDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Smoke' AS Item_Type,
    Prevention_Supplies.PSSmokeFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSSmokeDistroCount AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSSmokeDistroCount IS NOT NULL

UNION ALL

SELECT 
Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Syringes Distributed' AS Item_Type,
    Prevention_Supplies.PSSyringeDistFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSSyringeDistribution AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSSyringeDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Syringes Collected' AS Item_Type,
    NULL AS Dist_Funding_Source,
    Prevention_Supplies.PSSyringesCollected AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSSyringesCollected IS NOT NULL

UNION ALL

SELECT	
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Wound Care' AS Item_Type,
    Prevention_Supplies.PSWoundCareFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSWoundCareDistribution AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSWoundCareDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Xylazine' AS Item_Type,
    Prevention_Supplies.PSXylazineDistFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSXylazineDistribution AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSXylazineDistribution IS NOT NULL

UNION ALL

SELECT 
	Prevention_Supplies.DistributionID AS Event_ID,
    COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
    'Fentanyl Strips' AS Item_Type,
    Prevention_Supplies.PSFentanylTrainingFundingSrc AS Dist_Funding_Source,
    Prevention_Supplies.PSFentanylTestStripsDistributed AS Distribution_Count

FROM 
	ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies

WHERE 
	COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'
    AND Prevention_Supplies.PSFentanylTestStripsDistributed IS NOT NULL
    
IF OBJECT_ID('tempdb..#item_distributions') IS NOT NULL 
DROP TABLE #item_distributions;

SELECT 
	id.Event_ID,
    id.ZipCode,
    id.Item_Type,
    id.Dist_Funding_Source,
    id.Distribution_Count

INTO #item_distributions

FROM #id id
;


SELECT 
	i.Event_ID,
    i.ZipCode,
    i.Item_Type,
    CASE WHEN i.Dist_Funding_Source IS NULL AND i.Distribution_Count IS NOT NULL THEN 'Unfunded (Vivent Health Funding)'
        ELSE i.Dist_Funding_Source
    END AS Dist_Funding_Source,
    i.Distribution_Count,
    Prevention_Supplies.PSEncounterElection,
    Prevention_Supplies.PSPrevDistType,
    CASE
		WHEN Prevention_Supplies.PSServiceDate < '1/1/2007' THEN CAST(Prevention_Supplies.ACreateDate AS DATE) --When date is way old, use entry date.
		WHEN Prevention_Supplies.PSServiceDate < GETDATE() THEN CAST(Prevention_Supplies.PSServiceDate AS DATE) 
		ELSE CAST(Prevention_Supplies.ACreateDate AS DATE) --Using the entry date when the service date is in the future.
	END AS PSServiceDate,
    CASE
		WHEN Prevention_Supplies.AOrg = 'Vivent Health Colorado' THEN 'Denver'
		ELSE Prevention_Supplies.PSProgramOfficeLocation
	END AS Program_Office_Location,
	UPPER(REVERSE(substring(reverse(Prevention_Supplies.AOrg),1, charindex(' ', reverse(Prevention_Supplies.AOrg)) -1))) AS Service_State,
    --CASE WHEN Prevention_Supplies.AOrg = 'Vivent Health Colorado' THEN 'COLORADO'
    --    WHEN Prevention_Supplies.AOrg = 'Vivent Health Wisconsin' THEN 'WISCONSIN'
    --    WHEN Prevention_Supplies.AOrg = 'Vivent Health Missouri' THEN 'MISSOURI'
    --    WHEN Prevention_Supplies.AOrg = 'Vivent Health Texas' THEN 'TEXAS'
    --    ELSE 'ERROR'
    --END AS Service_State,
	CASE
		WHEN Prevention_Supplies.ACreateBy LIKE '%/%' THEN SUBSTRING(Prevention_Supplies.ACreateBy, 1, CHARINDEX('/', Prevention_Supplies.ACreateBy) - 1) 
		WHEN Prevention_Supplies.ACreateBy LIKE 'Import: %' THEN STUFF(Prevention_Supplies.ACreateBy, 1, 8, '')
		ELSE Prevention_Supplies.ACreateBy
	END AS Provider_Name,
    Prevention_Supplies.PSSiteName,
	COALESCE(Prevention_Supplies.PSFirstVisit, 'Not Answered') 'First Visit YN'

FROM 
	#item_distributions i
    INNER JOIN ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies ON i.Event_ID = Prevention_Supplies.DistributionID

WHERE
	i.Distribution_Count >0	
;
