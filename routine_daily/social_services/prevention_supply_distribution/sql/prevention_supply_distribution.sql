/* 10.200.180.16 */
/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	Prevention Supply Distribution
 Create Date:	8/20/2025
 Created By:	Reporting Account/VIVENT
 System:		pe.viventhealth.org
 Requested By:	

 Description:Prevention Supply Distribution Table
 

 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------
 8/20/2025          Mitch & Ben	        added Site County column 

**********************************************************************************************

 */

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#all_proc') IS NOT NULL									
DROP TABLE #all_proc;

        SELECT Client_Profile_all.SCPClientID,
               Prevention_Supplies.DistributionID AS Event_ID,
               CASE WHEN Client_Profile_all.SCPDateOfBirth IS NOT NULL THEN (DATEDIFF(MONTH, Client_Profile_all.SCPDateOfBirth, Prevention_Supplies.PSServiceDate) / 12)
                   ELSE Prevention_Supplies_SCPAliases.SCPAge
               END AS SCPClientAge,
               CASE 
					WHEN Prevention_Supplies_SCPRaceAll.IDX > 0 THEN 'More than one race'
					WHEN Prevention_Supplies_SCPRaceAll.SCPRaceAll IS NULL THEN 'Unknown'
					WHEN Prevention_Supplies_SCPRaceAll.SCPRaceAll = '' THEN 'Unknown'
					--WHEN Prevention_Supplies_SCPRaceAll.SCPRaceAll IN ('Pacific Islander', 'Native Hawaiian') THEN 'Native Hawaiian or Pacific Islander' --Per Kristen G. do not merge these
					--WHEN Prevention_Supplies_SCPRaceAll.SCPRaceAll IN ('Native American/American Indian', 'Alaskan American', 'American Indian or Alaska Native' --Per Kristen G. do not merge these
					--													,'American Indian, Alaska Native, or Indigenous', 'Native American') THEN 'American Indian or Alaska Native'
					ELSE Prevention_Supplies_SCPRaceAll.SCPRaceAll
               END AS Race,
               CASE 
					WHEN Prevention_Supplies.SCPEthnicity IS NULL THEN 'Unknown'
					WHEN Prevention_Supplies.SCPEthnicity = '' THEN 'Unknown'
                   ELSE Prevention_Supplies.SCPEthnicity
               END AS Ethnicity,
               Prevention_Supplies.SCPGender,
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
			END AS  Program_Office_Location,
               COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
               UPPER(REVERSE(SUBSTRING(REVERSE(Prevention_Supplies.AOrg),1, CHARINDEX(' ', REVERSE(Prevention_Supplies.AOrg)) -1))) AS 'Service_State',
			   --CASE WHEN Prevention_Supplies.AOrg = 'Vivent Health Colorado' THEN 'COLORADO'
      --             WHEN Prevention_Supplies.AOrg = 'Vivent Health Wisconsin' THEN 'WISCONSIN'
      --             WHEN Prevention_Supplies.AOrg = 'Vivent Health Missouri' THEN 'MISSOURI'
      --             WHEN Prevention_Supplies.AOrg = 'Vivent Health Texas' THEN 'TEXAS'
      --             ELSE 'ERROR'
      --         END AS Service_State,
               CASE
				WHEN Prevention_Supplies.ACreateBy LIKE '%/%' THEN SUBSTRING(Prevention_Supplies.ACreateBy, 1, CHARINDEX('/', Prevention_Supplies.ACreateBy) - 1) 
				WHEN Prevention_Supplies.ACreateBy LIKE 'Import: %' THEN STUFF(Prevention_Supplies.ACreateBy, 1, 8, '')
			   ELSE Prevention_Supplies.ACreateBy
				END AS Provider_Name,
               CASE WHEN Prevention_Supplies.PSCondomDistFundingSrc IS NULL
                         AND Prevention_Supplies.PSCondomDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSCondomDistFundingSrc
               END AS CondomDist_Funding_Source,
               COALESCE(Prevention_Supplies.PSCondomDistribution, -999) AS Condom_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSHygieneKitDistFundingSrc IS NULL
                         AND Prevention_Supplies.PSHygieneKitDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSHygieneKitDistFundingSrc
               END AS HygieneKitDist_Funding_Source,
               COALESCE(Prevention_Supplies.PSHygieneKitDistribution, -999) AS HygieneKit_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSInjectNaloxoneDistFundingSrc IS NULL
                         AND Prevention_Supplies.PSInjectNaloxoneDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSInjectNaloxoneDistFundingSrc
               END AS InjectNaloxoneDist_Funding_Source,
               COALESCE(Prevention_Supplies.PSInjectNaloxoneDistribution, -999) AS InjectNaloxone_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSLubeDistFundingSrc IS NULL
                         AND Prevention_Supplies.PSLubeDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSLubeDistFundingSrc
               END AS LubeDist_Funding_Source,
               COALESCE(Prevention_Supplies.PSLubeDistribution, -999) AS Lube_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSNasalNaloxoneDistFundingSrc IS NULL
                         AND Prevention_Supplies.PSNasalNaloxoneDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSNasalNaloxoneDistFundingSrc
               END AS NasalNaloxoneDist_Funding_Source,
               COALESCE(Prevention_Supplies.PSNasalNaloxoneDistribution, -999) AS NasalNaloxone_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSSaferSexKitDistFundingSrc IS NULL
                         AND Prevention_Supplies.PSSaferSexKitDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSSaferSexKitDistFundingSrc
               END AS SaferSexKitDist_Funding_Source,
               COALESCE(Prevention_Supplies.PSSaferSexKitDistribution, -999) AS SaferSexKit_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSSmokeFundingSrc IS NULL
                         AND Prevention_Supplies.PSSmokeDistroCount >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSSmokeFundingSrc
               END AS Smoke_Funding_Source,
               COALESCE(Prevention_Supplies.PSSmokeDistroCount, -999) AS Smoke_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSSyringeDistFundingSrc IS NULL
                         AND Prevention_Supplies.PSSyringeDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSSyringeDistFundingSrc
               END AS SyringeDist_Funding_Source,
               COALESCE(Prevention_Supplies.PSSyringeDistribution, -999) AS Syringe_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSWoundCareFundingSrc IS NULL
                         AND Prevention_Supplies.PSWoundCareDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSWoundCareFundingSrc
               END WoundCare_Funding_Source,
               COALESCE(Prevention_Supplies.PSWoundCareDistribution, -999) AS WoundCare_Distribution_Count,
               CASE WHEN Prevention_Supplies.PSXylazineDistFundingSrc IS NULL
                         AND Prevention_Supplies.PSXylazineDistribution >0 THEN 'Unfunded (Vivent Health Funding)'
                   ELSE Prevention_Supplies.PSXylazineDistFundingSrc
               END AS XylazineDist_Funding_Source,
               COALESCE(Prevention_Supplies.PSXylazineDistribution, -999) AS Xylazine_Distribution_Count,
               COALESCE(Prevention_Supplies.PSFentanylTraining, -999) AS PSFentanylTraining,
               COALESCE(Prevention_Supplies.PSFentanylTrainingAttendees, -999) AS PSFentanylTrainingAttendees,
               COALESCE(Prevention_Supplies.PSFentanylTrainingsHeld, -999) AS PSFentanylTrainingsHeld,
               COALESCE(Prevention_Supplies.PSInjectNaloxone911, -999) AS PSInjectNaloxone911,
               COALESCE(Prevention_Supplies.PSInjectNaloxoneDosesUsed, -999) AS PSInjectNaloxoneDosesUsed,
               COALESCE(Prevention_Supplies.PSInjectNaloxoneHospital, -999) AS PSInjectNaloxoneHospital,
               COALESCE(Prevention_Supplies.PSInjectNaloxoneTraining, -999) AS PSInjectNaloxoneTraining,
               COALESCE(Prevention_Supplies.PSInjectNaloxoneTrainingAttendees, -999) AS PSInjectNaloxoneTrainingAttendees,
               COALESCE(Prevention_Supplies.PSInjectNaloxoneTrainingsHeld, -999) AS PSInjectNaloxoneTrainingsHeld,
               COALESCE(Prevention_Supplies.PSInjectNaloxoneUsed, -999) AS PSInjectNaloxoneUsed, --Peer Saves
               COALESCE(Prevention_Supplies.PSNasalNaloxone911, -999) AS PSNasalNaloxone911,
               COALESCE(Prevention_Supplies.PSFentanylNegative, -999) AS PSFentanylNegative,
               COALESCE(Prevention_Supplies.PSFentanylPositive, -999) AS PSFentanylPositive,
               COALESCE(Prevention_Supplies.PSFentanylTestStripsDistributed, -999) AS PSFentanylTestStripsDistributed,
               COALESCE(Prevention_Supplies.PSNasalNaloxoneDistribution, -999) AS PSNasalNaloxoneDistribution,
               COALESCE(Prevention_Supplies.PSNasalNaloxoneDosesUsed, -999) AS PSNasalNaloxoneDosesUsed,
               COALESCE(Prevention_Supplies.PSNasalNaloxoneUsed, -999) AS PSNasalNaloxoneUsed,  --Peer Saves
               COALESCE(Prevention_Supplies.PSNasalNaloxoneHospital, -999) AS PSNasalNaloxoneHospital,
               COALESCE(Prevention_Supplies.PSNasalNaloxoneTraining, -999) AS PSNasalNaloxoneTraining,
               COALESCE(Prevention_Supplies.PSNasalNaloxoneTrainingAttendees, -999) AS PSNasalNaloxoneTrainingAttendees,
               COALESCE(Prevention_Supplies.PSNasalNaloxoneTrainingsHeld, -999) AS PSNasalNaloxoneTrainingsHeld,
               COALESCE(Prevention_Supplies.PSQtyPickupFor, -999) AS PSQtyPickupFor,
               COALESCE(Prevention_Supplies.PSReferralsGiven, -999) AS PSReferralsGiven,
               COALESCE(Prevention_Supplies.PSSmokeDistroCount, -999) AS PSSmokeDistroCount,
               COALESCE(Prevention_Supplies.PSSmokePeopleCount, -999) AS PSSmokePeopleCount,
               COALESCE(Prevention_Supplies.PSSmokeTrainingAttendees, -999) AS PSSmokeTrainingAttendees,
               COALESCE(Prevention_Supplies.PSSmokeTrainingsHeld, -999) AS PSSmokeTrainingsHeld,
               COALESCE(Prevention_Supplies.PSSyringesCollected, -999) AS PSSyringesCollected,
               COALESCE(Prevention_Supplies.PSXylazineNegative, -999) AS PSXylazineNegative,
               COALESCE(Prevention_Supplies.PSXylazinePositive, -999) AS PSXylazinePositive,
               COALESCE(Prevention_Supplies.PSXylazineTraining, -999) AS PSXylazineTraining,
               COALESCE(Prevention_Supplies.PSXylazineTrainingAttendees, -999) AS PSXylazineTrainingAttendees,
               COALESCE(Prevention_Supplies.PSXylazineTrainingsHeld, -999) AS PSXylazineTrainingsHeld,
			   COALESCE(Prevention_Supplies.PSSharpsContainers, -999) AS PSSharpsContainers,
			   COALESCE(Prevention_Supplies.PSSmokeCrackKitCount, -999) AS PSSmokeCrackKitCount,
			   COALESCE(Prevention_Supplies.PSSmokeMethKitCount, -999) AS PSSmokeMethKitCount,
			   COALESCE(Prevention_Supplies.PSSmokeSafeCount, -999) AS PSSmokeSafeCount,
			   COALESCE(Prevention_Supplies.PSSmokeSafeSnortCount, -999) AS PSSmokeSafeSnortCount,
			   COALESCE(Prevention_Supplies.PSSterileWater, -999) AS PSSterileWater,
			   COALESCE(Prevention_Supplies.PSSyringe29Gauge, -999) AS PSSyringe29Gauge,
			   COALESCE(Prevention_Supplies.PSSyringe31Gauge, -999) AS PSSyringe31Gauge,
               Prevention_Supplies.PSSyringeExchange,
			   Prevention_Supplies.PSNasalNaloxoneDistTo,
			   Prevention_Supplies.PSInjectNaloxoneDistTo,
			   Prevention_Supplies.PSSiteName,
			   r_raw.SCPRaceAll 'Raw_Race_Value',
               COALESCE(Prevention_Supplies.PSQtyPickupFor, -999) AS NumPeopleHelped,
			   CASE
					WHEN Prevention_Supplies.SCPIncome IS NULL THEN 'Unknown'
					WHEN Prevention_Supplies.SCPIncome = '75000' THEN '$75,000 +'
				END AS 'Income',
               CURRENT_TIMESTAMP AS UPDATE_DTTM,
			   COALESCE(Prevention_Supplies.PSFirstVisit, 'Not Answered') 'First Visit YN',
			   psite.SiteCounty
			   

		INTO #all_proc
        FROM ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies
            LEFT JOIN ODS.PROVIDE.vwClient_Profile_all AS Client_Profile_all ON Prevention_Supplies.SCPClientID = Client_Profile_all.SCPClientID
			INNER JOIN ODS.PROVIDE.vwPrevention_Site AS psite ON psite.SiteName = Prevention_Supplies.PSSiteName
			LEFT JOIN
				(SELECT 
					r.SQLID
					,r.IDX
					,r.SCPRaceAll

				FROM 
					ODS.PROVIDE.vwPrevention_Supplies_SCPRaceAll r
					INNER JOIN
						(SELECT
							r.SQLID
							,MAX(r.IDX) 'Maxline'

						FROM 
							ODS.PROVIDE.vwPrevention_Supplies_SCPRaceAll r
						GROUP BY 
							r.SQLID
						)race ON race.SQLID = r.SQLID
								AND r.IDX = race.Maxline
				) Prevention_Supplies_SCPRaceAll ON Prevention_Supplies.SQLID = Prevention_Supplies_SCPRaceAll.SQLID
            LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_SCPAliases AS Prevention_Supplies_SCPAliases ON Prevention_Supplies.SQLID = Prevention_Supplies_SCPAliases.SQLID
			LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_SCPRaceAll r_raw ON r_raw.SQLID = Prevention_Supplies.SQLID
        WHERE COALESCE(Prevention_Supplies.DeleteFlag, 'N') = 'N'

SELECT 
	COALESCE(all_proc.SCPClientID, 'Anonymous Sessions') AS SCPClientID,
	all_proc.Event_ID,
	COALESCE(all_proc.SCPClientAge, -999) AS SCPClientAge,
	CASE WHEN all_proc.SCPClientAge <= 11 THEN '0 - 11'
		WHEN all_proc.SCPClientAge
			BETWEEN 12 AND 14 THEN '12 - 14'
		WHEN all_proc.SCPClientAge
			BETWEEN 15 AND 17 THEN '15 - 17'
		WHEN all_proc.SCPClientAge
			BETWEEN 18 AND 20 THEN '18 - 20'
		WHEN all_proc.SCPClientAge
			BETWEEN 21 AND 24 THEN '21 - 24'
		WHEN all_proc.SCPClientAge
			BETWEEN 25 AND 44 THEN '25 - 44'
		WHEN all_proc.SCPClientAge
			BETWEEN 45 AND 64 THEN '45 - 64'
		WHEN all_proc.SCPClientAge >= 65 THEN '65+ and older'
		ELSE 'Unknown'
	END AS AgeGroup,
	COALESCE(all_proc.Race, 'Unknown') AS Race,
	COALESCE(all_proc.Ethnicity, 'Unknown') AS Ethnicity,
	CASE
		WHEN all_proc.SCPGender IS NULL THEN 'Unknown' 
		WHEN all_proc.SCPGender = '' THEN 'Unknown'
		WHEN all_proc.SCPGender = 'Female, Transgender' THEN 'Transgender Female / Male-to-Female'
		WHEN all_proc.SCPGender = 'Male, Transgender' THEN 'Transgender Male / Female-to-Male'
		WHEN all_proc.SCPGender = 'Choose not to disclose' THEN 'Data not collected'
		WHEN all_proc.SCPGender = 'Client doesn''t know' THEN 'Unknown'
		ELSE all_proc.SCPGender
	END AS SCPGender,
	all_proc.PSEncounterElection,
	all_proc.PSPrevDistType,
	all_proc.PSServiceDate,
	CASE
		WHEN all_proc.Service_State = 'CO' THEN 'Denver'
		ELSE all_proc.Program_Office_Location
	END AS Program_Office_Location,
	CASE 
		WHEN LEN(all_proc.ZipCode) = 5 AND all_proc.ZipCode NOT LIKE '%[^0-9]%' THEN all_proc.ZipCode
		WHEN all_proc.ZipCode = '00000' THEN '-'
        ELSE '-'
	END AS ZipCode,
	CASE
		WHEN all_proc.Service_State IS NOT NULL THEN all_proc.Service_State
		WHEN all_proc.Program_Office_Location IN ('Appleton','Beloit','Eau Claire','Green Bay','Kenosha','La Crosse','Madison','Milwaukee','Superior','Wausau') THEN 'WI'
		WHEN all_proc.Program_Office_Location = 'Denver' THEN 'CO'
		WHEN all_proc.Program_Office_Location = 'Austin' THEN 'TX'
		WHEN all_proc.Program_Office_Location IN ('St. Louis', 'Kansas City') THEN 'MO'
	END AS Service_State,
	all_proc.Provider_Name,
	CASE
		WHEN all_proc.Condom_Distribution_Count IN (0, -999) THEN NULL	
		ELSE all_proc.CondomDist_Funding_Source
	END AS CondomDist_Funding_Source,
	IIF(all_proc.Condom_Distribution_Count= 0, -999, all_proc.Condom_Distribution_Count) AS Condom_Distribution_Count,
	CASE
		WHEN all_proc.HygieneKit_Distribution_Count  IN (0, -999) THEN NULL	
		ELSE all_proc.HygieneKitDist_Funding_Source
	END AS HygieneKitDist_Funding_Source,
	IIF(all_proc.HygieneKit_Distribution_Count = 0, -999, all_proc.HygieneKit_Distribution_Count) AS HygieneKit_Distribution_Count,
	CASE
		WHEN all_proc.InjectNaloxone_Distribution_Count = 0 THEN NULL
		WHEN all_proc.InjectNaloxone_Distribution_Count = -999 THEN NULL
		ELSE all_proc.InjectNaloxoneDist_Funding_Source
	END AS InjectNaloxoneDist_Funding_Source,
	IIF(all_proc.InjectNaloxone_Distribution_Count = 0, -999, all_proc.InjectNaloxone_Distribution_Count) AS InjectNaloxone_Distribution_Count,
	CASE
		WHEN all_proc.Lube_Distribution_Count IN (0, -999) THEN NULL	
		ELSE all_proc.LubeDist_Funding_Source
	END AS LubeDist_Funding_Source,
	IIF(all_proc.Lube_Distribution_Count = 0, -999, all_proc.Lube_Distribution_Count) AS Lube_Distribution_Count,
	CASE
		WHEN all_proc.NasalNaloxone_Distribution_Count  IN (0, -999) THEN NULL
		ELSE all_proc.NasalNaloxoneDist_Funding_Source
	END AS NasalNaloxoneDist_Funding_Source,
	IIF(all_proc.NasalNaloxone_Distribution_Count = 0, -999, all_proc.NasalNaloxone_Distribution_Count) AS NasalNaloxone_Distribution_Count,
	CASE
		WHEN all_proc.SaferSexKit_Distribution_Count IN (0, -999) THEN NULL
		ELSE all_proc.SaferSexKitDist_Funding_Source
	END AS SaferSexKitDist_Funding_Source,
	IIF(all_proc.SaferSexKit_Distribution_Count = 0, -999, all_proc.SaferSexKit_Distribution_Count) AS SaferSexKit_Distribution_Count,
	CASE
		WHEN all_proc.Smoke_Distribution_Count IN (0, -999) THEN NULL
		ELSE all_proc.Smoke_Funding_Source
	END AS Smoke_Funding_Source,
	IIF(all_proc.Smoke_Distribution_Count = 0, -999, all_proc.Smoke_Distribution_Count) AS Smoke_Distribution_Count,
	CASE
		WHEN all_proc.Syringe_Distribution_Count IN (0, -999) THEN NULL
		ELSE all_proc.SyringeDist_Funding_Source
	END AS SyringeDist_Funding_Source,
	IIF(all_proc.Syringe_Distribution_Count = 0, -999, all_proc.Syringe_Distribution_Count) AS Syringe_Distribution_Count,
	CASE
		WHEN all_proc.WoundCare_Distribution_Count IN (0, -999) THEN NULL
		ELSE all_proc.WoundCare_Funding_Source
	END AS WoundCare_Funding_Source,
	IIF(all_proc.WoundCare_Distribution_Count = 0, -999, all_proc.WoundCare_Distribution_Count) AS WoundCare_Distribution_Count,
	CASE
		WHEN all_proc.Xylazine_Distribution_Count IN (0, -999) THEN NULL
		ELSE all_proc.XylazineDist_Funding_Source
	END AS XylazineDist_Funding_Source,
	IIF(all_proc.Xylazine_Distribution_Count = 0, -999, all_proc.Xylazine_Distribution_Count) AS Xylazine_Distribution_Count,
	IIF(all_proc.PSFentanylTraining = 0, -999, all_proc.PSFentanylTraining) AS PSFentanylTraining,
	IIF(all_proc.PSFentanylTrainingAttendees = 0, -999, all_proc.PSFentanylTrainingAttendees) AS PSFentanylTrainingAttendees,
	IIF(all_proc.PSFentanylTrainingsHeld = 0, -999, all_proc.PSFentanylTrainingsHeld) AS PSFentanylTrainingsHeld,
	IIF(all_proc.PSInjectNaloxone911 = 0, -999, all_proc.PSInjectNaloxone911) AS PSInjectNaloxone911,
	IIF(all_proc.PSInjectNaloxoneDosesUsed = 0, -999, all_proc.PSInjectNaloxoneDosesUsed) AS PSInjectNaloxoneDosesUsed,
	IIF(all_proc.PSInjectNaloxoneHospital = 0, -999, all_proc.PSInjectNaloxoneHospital) AS PSInjectNaloxoneHospital,
	IIF(all_proc.PSInjectNaloxoneTraining = 0, -999, all_proc.PSInjectNaloxoneTraining) AS PSInjectNaloxoneTraining,
	IIF(all_proc.PSInjectNaloxoneTrainingAttendees = 0, -999, all_proc.PSInjectNaloxoneTrainingAttendees) AS PSInjectNaloxoneTrainingAttendees,
	IIF(all_proc.PSInjectNaloxoneTrainingsHeld = 0, -999, all_proc.PSInjectNaloxoneTrainingsHeld) AS PSInjectNaloxoneTrainingsHeld,
	IIF(all_proc.PSInjectNaloxoneUsed = 0, -999, all_proc.PSInjectNaloxoneUsed) AS PSInjectNaloxoneUsed,
	IIF(all_proc.PSNasalNaloxone911 = 0, -999, all_proc.PSNasalNaloxone911) AS PSNasalNaloxone911,
	IIF(all_proc.PSFentanylNegative = 0, -999, all_proc.PSFentanylNegative) AS PSFentanylNegative,
	IIF(all_proc.PSFentanylPositive = 0, -999, all_proc.PSFentanylPositive) AS PSFentanylPositive,
	IIF(all_proc.PSFentanylTestStripsDistributed = 0, -999, all_proc.PSFentanylTestStripsDistributed) AS PSFentanylTestStripsDistributed,
	IIF(all_proc.PSNasalNaloxoneDistribution = 0, -999, all_proc.PSNasalNaloxoneDistribution) AS PSNasalNaloxoneDistribution,
	IIF(all_proc.PSNasalNaloxoneUsed = 0, -999, all_proc.PSNasalNaloxoneUsed) AS PSNasalNaloxoneUsed,
	IIF(all_proc.PSNasalNaloxoneDosesUsed = 0, -999, all_proc.PSNasalNaloxoneDosesUsed) AS PSNasalNaloxoneDosesUsed,
	IIF(all_proc.PSNasalNaloxoneHospital = 0, -999, all_proc.PSNasalNaloxoneHospital) AS PSNasalNaloxoneHospital,
	IIF(all_proc.PSNasalNaloxoneTraining = 0, -999, all_proc.PSNasalNaloxoneTraining) AS PSNasalNaloxoneTraining,
	IIF(all_proc.PSNasalNaloxoneTrainingAttendees = 0, -999, all_proc.PSNasalNaloxoneTrainingAttendees) AS PSNasalNaloxoneTrainingAttendees,
	IIF(all_proc.PSNasalNaloxoneTrainingsHeld = 0, -999, all_proc.PSNasalNaloxoneTrainingsHeld) AS PSNasalNaloxoneTrainingsHeld,
	IIF(all_proc.PSQtyPickupFor = 0, -999, all_proc.PSQtyPickupFor) AS PSQtyPickupFor,
	IIF(all_proc.PSReferralsGiven = 0, -999, all_proc.PSReferralsGiven) AS PSReferralsGiven,
	IIF(all_proc.PSSmokeDistroCount = 0, -999, all_proc.PSSmokeDistroCount) AS PSSmokeDistroCount,
	IIF(all_proc.PSSmokePeopleCount = 0, -999, all_proc.PSSmokePeopleCount) AS PSSmokePeopleCount,
	IIF(all_proc.PSSmokeTrainingAttendees = 0, -999, all_proc.PSSmokeTrainingAttendees) AS PSSmokeTrainingAttendees,
	IIF(all_proc.PSSmokeTrainingsHeld = 0, -999, all_proc.PSSmokeTrainingsHeld) AS PSSmokeTrainingsHeld,
	IIF(all_proc.PSSyringesCollected = 0, -999, all_proc.PSSyringesCollected) AS PSSyringesCollected,
	IIF(all_proc.PSXylazineNegative = 0, -999, all_proc.PSXylazineNegative) AS PSXylazineNegative,
	IIF(all_proc.PSXylazinePositive = 0, -999, all_proc.PSXylazinePositive) AS PSXylazinePositive,
	IIF(all_proc.PSXylazineTraining = 0, -999, all_proc.PSXylazineTraining) AS PSXylazineTraining,
	IIF(all_proc.PSXylazineTrainingAttendees = 0, -999, all_proc.PSXylazineTrainingAttendees) AS PSXylazineTrainingAttendees,
	IIF(all_proc.PSXylazineTrainingsHeld = 0, -999, all_proc.PSXylazineTrainingsHeld) AS PSXylazineTrainingsHeld,
	IIF(all_proc.PSSharpsContainers = 0, -999, all_proc.PSSharpsContainers) AS PSSharpsContainers,
	IIF(all_proc.PSSmokeCrackKitCount = 0, -999, all_proc.PSSmokeCrackKitCount) AS PSSmokeCrackKitCount,
	IIF(all_proc.PSSmokeMethKitCount = 0, -999, all_proc.PSSmokeMethKitCount) AS PSSmokeMethKitCount,
	IIF(all_proc.PSSmokeSafeCount = 0, -999, all_proc.PSSmokeSafeCount) AS PSSmokeSafeCount,
	IIF(all_proc.PSSmokeSafeSnortCount = 0, -999, all_proc.PSSmokeSafeSnortCount) AS PSSmokeSafeSnortCount,
	IIF(all_proc.PSSterileWater = 0, -999, all_proc.PSSterileWater) AS PSSterileWater,
	IIF(all_proc.PSSyringe29Gauge = 0, -999, all_proc.PSSyringe29Gauge) AS PSSyringe29Gauge,
	IIF(all_proc.PSSyringe31Gauge = 0, -999, all_proc.PSSyringe31Gauge) AS PSSyringe31Gauge,
	all_proc.PSSyringeExchange,
	all_proc.PSNasalNaloxoneDistTo,
	all_proc.PSInjectNaloxoneDistTo,
	all_proc.PSSiteName AS Site,
	all_proc.Raw_Race_Value 'Race (raw)',
	IIF(all_proc.NumPeopleHelped = 0, -999, all_proc.NumPeopleHelped) AS NumPeopleHelped,
	all_proc.Income,
	all_proc.UPDATE_DTTM,
	all_proc.[First Visit YN],
	all_proc.SiteCounty 'Supplies County'

FROM 
	#all_proc all_proc
	
;
DROP TABLE #all_proc