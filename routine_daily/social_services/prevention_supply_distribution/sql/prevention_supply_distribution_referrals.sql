SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#referrals') IS NOT NULL DROP TABLE #referrals;
SELECT Prevention_Supplies.SQLID,
       psr.PSReferrals AS 'Referral-Prevention Navigation',
       psr2.PSReferrals AS 'Referral-STI Test',
       psr3.PSReferrals AS 'Referral-HIV testing',
       psr4.PSReferrals AS 'Referral-HIV treatment',
       psr5.PSReferrals AS 'Referral-HCV testing',
       psr6.PSReferrals AS 'Referral-HCV treatment',
       psr7.PSReferrals AS 'Referral-Pre-exposure prophylaxis',
       psr8.PSReferrals AS 'Referral-Mental health services',
       psr9.PSReferrals AS 'Referral-Other: sex worker assistance',
       psr10.PSReferrals AS 'Referral-Case management',
       psr11.PSReferrals AS 'Referral-Medically assisted treatment',
       psr12.PSReferrals AS 'Referral-Other: clothing',
       psr13.PSReferrals AS 'Referral-Food assistance (snack distribution)',
       psr14.PSReferrals AS 'Referral-Food assistance',
       psr15.PSReferrals AS 'Referral-Substance use treatment',
       psr16.PSReferrals AS 'Referral-Domestic Violence assistance',
       psr17.PSReferrals AS 'Referral-Medicaid/insurance enrollment',
       psr18.PSReferrals AS 'Referral-DMV/ID services',
       psr19.PSReferrals AS 'Referral-Transportation: bus ticket',
       psr20.PSReferrals AS 'Referral-Skin/soft tissue or wound infection treatment',
       psr21.PSReferrals AS 'Referral-Other SUS services',
       psr22.PSReferrals AS 'Referral-Health education (class)',
       psr23.PSReferrals AS 'Referral-Soft Tissue/Wound Care',
       psr24.PSReferrals AS 'Referral-Motivational Counseling'
INTO #referrals
FROM ODS.PROVIDE.vwPrevention_Supplies Prevention_Supplies
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr ON psr.SQLID = Prevention_Supplies.SQLID
                                                                  AND psr.PSReferrals = 'Prevention Navigation services'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr2 ON psr2.SQLID = Prevention_Supplies.SQLID
                                                                   AND psr2.PSReferrals = 'STI testing'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr3 ON psr3.SQLID = Prevention_Supplies.SQLID
                                                                   AND psr3.PSReferrals = 'HIV testing'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr4 ON psr4.SQLID = Prevention_Supplies.SQLID
                                                                   AND psr4.PSReferrals = 'HIV treatment'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr5 ON psr5.SQLID = Prevention_Supplies.SQLID
                                                                   AND psr5.PSReferrals = 'HCV testing'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr6 ON psr6.SQLID = Prevention_Supplies.SQLID
                                                                   AND psr6.PSReferrals = 'HCV treatment'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr7 ON psr7.SQLID = Prevention_Supplies.SQLID
                                                                   AND psr7.PSReferrals = 'Pre-exposure prophylaxis'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr8 ON psr8.SQLID = Prevention_Supplies.SQLID
                                                                   AND psr8.PSReferrals = 'Mental health services'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr9 ON psr9.SQLID = Prevention_Supplies.SQLID
                                                                   AND psr9.PSReferrals = 'Other: sex worker assistance'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr10 ON psr10.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr10.PSReferrals = 'Case management'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr11 ON psr11.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr11.PSReferrals = 'Medically assisted treatment'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr12 ON psr12.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr12.PSReferrals = 'Other: clothing'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr13 ON psr13.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr13.PSReferrals = 'Food assistance (snack distribution)'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr14 ON psr14.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr14.PSReferrals = 'Food assistance'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr15 ON psr15.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr15.PSReferrals = 'Substance use treatment'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr16 ON psr16.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr16.PSReferrals = 'Domestic Violence assistance'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr17 ON psr17.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr17.PSReferrals = 'Medicaid/insurance enrollment'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr18 ON psr18.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr18.PSReferrals = 'DMV/ID services'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr19 ON psr19.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr19.PSReferrals = 'Transportation: bus ticket'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr20 ON psr20.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr20.PSReferrals = 'Skin/soft tissue or wound infection treatment'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr21 ON psr21.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr21.PSReferrals = 'Other SUS services'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr22 ON psr22.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr22.PSReferrals = 'Health education (class)'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr23 ON psr23.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr23.PSReferrals = 'Soft Tissue/Wound Care'
    LEFT JOIN ODS.PROVIDE.vwPrevention_Supplies_PSReferrals psr24 ON psr24.SQLID = Prevention_Supplies.SQLID
                                                                    AND psr24.PSReferrals = 'Motivational Counseling'
;

IF OBJECT_ID('tempdb..#af') IS NOT NULL									
DROP TABLE #af;
    SELECT SQLID,
           'Prevention Navigation' AS REFERRAL_TYPE

	INTO #af
    FROM #referrals
    WHERE [Referral-Prevention Navigation] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'STI Test' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-STI Test] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'HIV Testing' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-HIV testing] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'HIV Treatment' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-HIV treatment] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'HCV Testing' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-HCV testing] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'HCV Treatment' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-HCV treatment] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Pre-exposure prophylaxis' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Pre-exposure prophylaxis] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Mental health services' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Mental health services] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Other: sex worker assistance' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Other: sex worker assistance] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'Case management' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-Case management] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Medically assisted treatment' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Medically assisted treatment] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'Other: clothing' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-Other: clothing] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Food assistance (snack distribution)' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Food assistance (snack distribution)] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'Food assistance' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-Food assistance] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Substance use treatment' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Substance use treatment] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Domestic Violence assistance' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Domestic Violence assistance] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Medicaid/insurance enrollment' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Medicaid/insurance enrollment] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'DMV/ID services' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-DMV/ID services] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Transportation: bus ticket' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Transportation: bus ticket] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Skin/soft tissue or wound infection treatment' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Skin/soft tissue or wound infection treatment] IS NOT NULL
    UNION ALL
    SELECT SQLID, 'Other SUS services' AS REFERRAL_TYPE FROM #referrals WHERE [Referral-Other SUS services] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Health education (class)' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Health education (class)] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Soft Tissue/Wound Care' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Soft Tissue/Wound Care] IS NOT NULL
    UNION ALL
    SELECT SQLID,
           'Motivational Counseling' AS REFERRAL_TYPE
    FROM #referrals
    WHERE [Referral-Motivational Counseling] IS NOT NULL
;
SELECT Prevention_Supplies.DistributionID AS Event_ID,
       all_referrals.REFERRAL_TYPE,
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
       COALESCE(REPLACE(COALESCE(Prevention_Supplies.SCPZipSCPZipWhileUsing, Prevention_Supplies.SCPZipSCPZipWhileNotUsing), '-', ''), '-') AS ZipCode,
       UPPER(REVERSE(substring(reverse(Prevention_Supplies.AOrg),1, charindex(' ', reverse(Prevention_Supplies.AOrg)) -1))) AS Service_State,
       CASE
		WHEN Prevention_Supplies.ACreateBy LIKE '%/%' THEN SUBSTRING(Prevention_Supplies.ACreateBy, 1, CHARINDEX('/', Prevention_Supplies.ACreateBy) - 1) 
		WHEN Prevention_Supplies.ACreateBy LIKE 'Import: %' THEN STUFF(Prevention_Supplies.ACreateBy, 1, 8, '')
		ELSE Prevention_Supplies.ACreateBy
       END AS Provider_Name,
	Prevention_Supplies.PSSiteName AS 'Site'

FROM 
	#af all_referrals
    INNER JOIN ODS.PROVIDE.vwPrevention_Supplies AS Prevention_Supplies ON all_referrals.SQLID = Prevention_Supplies.SQLID;

;
DROP TABLE #af
DROP TABLE #referrals