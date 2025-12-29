/*  Preferred Language – PE side (to be joined to EPIC by MRN in Python)
    Returns MRN + PE_PREFERRED_LANGUAGE + Site/State already calculated in PE
*/
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#act') IS NOT NULL DROP TABLE #act;

SELECT
    sa.ClientProfileID,
    sa.AOrg,
    CASE
        WHEN sa.AOrg = 'Vivent Health Colorado' THEN 'Denver'
        WHEN sa.AOrg = 'Vivent Health Texas' THEN 'Austin'
        WHEN sa.ProgramOfficeLocation IS NOT NULL AND sa.ProgramOfficeLocation <> ''
            THEN CASE WHEN sa.ProgramOfficeLocation = 'St. Louis' THEN 'St Louis' ELSE sa.ProgramOfficeLocation END
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Calumet','Fond du Lac','Fond Du La','Green Lake','Marquette','Outagamie','Sheboygan','Waupaca','Waushara','Winnebago') THEN 'Appleton'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Barron','Buffalo','Burnett','Chippewa','Clark','Dunn','Eau Claire','Pepin','Pierce','Polk','Rusk','St. Croix') THEN 'Eau Claire'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Brown','Door','Kewaunee','Manitowoc','Marinette','Menominee','Oconto','Shawano') THEN 'Green Bay'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Jefferson','Kenosha','Racine','Walworth') THEN 'Kenosha'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Jackson','La Crosse','Monroe','Trempealeau','Vernon') THEN 'La Crosse'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Adams','Columbia','Crawford','Dane','Dodge','Grant','Iowa','Juneau','Lafayette','Richland','Sauk') THEN 'Madison'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Milwaukee','Ozaukee','Washington','Waukesha') THEN 'Milwaukee'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Ashland','Bayfield','Douglas','Iron','Sawyer','Washburn') THEN 'Superior'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN
            ('Florence ','Forest','Langlade','Lincoln','Marathon','Oneida','Portage','Price','Taylor','Vilas','Wood') THEN 'Wausau'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' AND sa.County IN ('Rock','Green') THEN 'Beloit'
        WHEN sa.AOrg = 'Vivent Health Missouri' AND cp.SCPState = 'IL' AND sa.County = 'Clinton' THEN 'St Louis'
        WHEN sa.AOrg = 'Vivent Health Missouri' AND sa.County IN
            ('St. Louis (city)','St Louis Cty','St. Louis','Saint Loui','Warren','Macoupin','Franklin','Calhoun','Jefferson','Jeff',
             'St. Clair','SAINT CLAIR','Saint Clai','SAINT CHARLES','Saint Char','St. Charles','Lincoln','Madison','Jersey','Bond',
             'Monroe','SAINT LOUIS CITY','SAINT LOUIS','Santa Barb','Ste. Genevieve','Dunklin','Fulton','Cape Girar','Gasconade','Marion')
            THEN 'St Louis'
        WHEN sa.AOrg = 'Vivent Health Missouri' AND sa.County IN
            ('Lafayette','Cass','Clay','Linn','Miami','Platte','Wyandotte','Bates','Johnson','Leavenworth','Jackson','Caldwell','Clinton','Ray')
            THEN 'Kansas City'
        WHEN sa.AOrg = 'Vivent Health Missouri' AND cp.SCPCounty IN
            ('St. Louis (city)','St. Louis','Saint Loui','Warren','Macoupin','Franklin','Calhoun','Jefferson','St. Clair','SAINT CLAIR',
             'Saint Clai','SAINT CHARLES','Saint Char','St. Charles','Lincoln','Madison','Jersey','Bond','Monroe','Marion')
            THEN 'St Louis'
        WHEN sa.AOrg = 'Vivent Health Missouri' AND cp.SCPCounty IN
            ('Lafayette','Cass','Clay','Linn','Miami','Platte','Wyandotte','Bates','Johnson','Leavenworth','Jackson','Caldwell','Clinton','Ray')
            THEN 'Kansas City'
        WHEN sa.County = 'Milwaukee' THEN 'Milwaukee'
        WHEN sa.County = 'SAINT LOUIS CITY' THEN 'St Louis'
        WHEN sa.AOrg = 'Vivent Health Wisconsin' THEN 'Milwaukee'
        ELSE 'Unknown'
    END AS Site,
    REVERSE(SUBSTRING(REVERSE(sa.AOrg), 1, CHARINDEX(' ', REVERSE(sa.AOrg)) - 1)) AS STATE,
    ROW_NUMBER() OVER (PARTITION BY sa.ClientProfileID ORDER BY sa.AActivityDate DESC) AS ROW_NUM_DESC
INTO #act
FROM vivent.dbo.vwService_Activity_All sa
JOIN vivent.dbo.vwClient_Profile_all cp ON cp.ClientProfileID = sa.ClientProfileID
WHERE sa.AActivityDate > DATEADD(MONTH, -12, GETDATE())
  AND sa.AActivityDate <= GETDATE();

SELECT
    cp.SCPMRN                                        AS MRN,
    cp.SCPLanguagePreferredSpoken                    AS PE_PREFERRED_LANGUAGE,
    act.STATE,
    UPPER(act.Site)                                  AS Site
FROM vivent.dbo.vwClient_Profile_all cp
JOIN #act AS act
  ON act.ClientProfileID = cp.ClientProfileID
WHERE act.ROW_NUM_DESC = 1;
