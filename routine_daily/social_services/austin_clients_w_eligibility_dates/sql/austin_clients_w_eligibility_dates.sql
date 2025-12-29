SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

IF OBJECT_ID('tempdb..#a') IS NOT NULL DROP TABLE #a;
SELECT cp.SCPClientID 'PE Client ID',
       cp.ASAClientID 'ASA Client ID',
       cp.SCPMRN 'Epic MRN',
       cp.SCPClientLast 'Last Name',
       cp.SCPClientFirst 'First Name',
       COALESCE(pr.SCPProvider, 'No Provider') 'Provider',
       CASE WHEN pr.SCPProviderRelationship IS NULL THEN enr.Program
           ELSE pr.SCPProviderRelationship
       END AS 'Provider Relationship',
       cp.SCPDateOfBirth,
       CONVERT(NVARCHAR(30), cp.SCPDateOfBirth, 101) AS 'DOB',
       enr.Program,
       DATEADD(YEAR, DATEPART(YEAR, GETDATE()) - DATEPART(YEAR, cp.SCPDateOfBirth), cp.SCPDateOfBirth) AS 'Birthday This Year',
       COALESCE((CASE WHEN cp.SCPClientPhone1Type = 'Cell Phone' THEN cp.SCPClientPhone1Number
                 END), (CASE WHEN cp.SCPClientPhone2Type = 'Cell Phone' THEN cp.SCPClientPhone2Number
                        END)) 'Mobile Phone',
       cp.SCPEmailAddr 'Email',
       CONVERT(NVARCHAR(30), elig.DateCompleted, 101) AS 'Last Eligibility Assessment'
INTO #a
FROM vivent.dbo.vwClient_Profile_all cp
    LEFT JOIN vivent.dbo.vwProvider_Relationship pr ON pr.ClientProfileID = cp.ClientProfileID
                                                       AND pr.SCPProviderDateEnd IS NULL
                                                       AND pr.SCPProviderType = 'Individual'
                                                       AND COALESCE(pr.DeleteFlag, 'N') = 'N'
                                                       AND pr.SCPProviderRelationship NOT IN ( 'Primary Care Provider', 'Primary Care Physician',
                                                                                               'Nurse Practitioner', 'Nursing Services Coordinator',
                                                                                               'Other Non-Medical Provider', 'Infectious Disease Specialist',
                                                                                               'Infectious Disease Physician', 'Dentist', 'Nutritionist',
                                                                                               'Other Medical Provider', 'Resource Team Contact',
                                                                                               'Medical Clinic' )
    INNER JOIN (SELECT pe.ClientProfileID,
                       pe.CLAProgramName 'Program'
                FROM vivent.dbo.vwProgram_Enrollment pe
                WHERE pe.CLAProgramDateEnd IS NULL
                      AND COALESCE(pe.DeleteFlag, 'N') = 'N'
                      AND pe.AOrg = 'Vivent Health Texas') enr ON enr.ClientProfileID = cp.ClientProfileID
    LEFT JOIN (SELECT ea.ClientProfileID,
                      ea.DateCompleted,
                      ROW_NUMBER() OVER (PARTITION BY ea.ClientProfileID ORDER BY ea.DateCompleted DESC) AS ROW_NUM_DESC
               FROM vivent.dbo.vwEligibility_Assessment ea
               WHERE ea.DateCompleted > DATEADD(MONTH, -36, GETDATE())
                     AND COALESCE(ea.DeleteFlag, 'N') = 'N') elig ON elig.ClientProfileID = cp.ClientProfileID
                                                                     AND elig.ROW_NUM_DESC = 1;


IF OBJECT_ID('tempdb..#b') IS NOT NULL DROP TABLE #b;
SELECT a.[PE Client ID],
       a.[ASA Client ID],
       a.[Epic MRN],
       a.[Last Name],
       a.[First Name],
       UPPER(a.Provider) 'Provider',
       a.[Provider Relationship],
       a.SCPDateOfBirth,
       a.DOB,
       a.Program,
       a.[Birthday This Year],
       CASE WHEN DATEDIFF(MONTH, GETDATE(), a.[Birthday This Year]) >= 6 THEN DATEADD(MONTH, -6, a.[Birthday This Year]) -- For bdays more than 6 months in the future
           WHEN DATEDIFF(MONTH, GETDATE(), a.[Birthday This Year]) < -6 THEN DATEADD(MONTH, 12, a.[Birthday This Year])  -- When b-day more than 6 mo ago
           WHEN DATEDIFF(MONTH, GETDATE(), a.[Birthday This Year]) < 0 THEN DATEADD(MONTH, 6, a.[Birthday This Year])    -- When b-day -1 to -6 mo ago
           WHEN DATEDIFF(MONTH, GETDATE(), a.[Birthday This Year]) < 6 THEN a.[Birthday This Year]                       -- For bday 0-6 mos from today
       END AS 'Eligibility Due',
       a.[Mobile Phone],
       a.Email,
       a.[Last Eligibility Assessment]
INTO #b
FROM #a a;


SELECT b.[PE Client ID],
       b.[ASA Client ID],
       b.[Epic MRN],
       b.[Last Name],
       b.[First Name],
       b.Provider,
       b.[Provider Relationship],
       b.SCPDateOfBirth,
       b.DOB,
       b.Program,
       b.[Birthday This Year],
       b.[Eligibility Due],
       DATEDIFF(DAY, GETDATE(), b.[Eligibility Due]) 'Days Until Eligibility Due',
       b.[Mobile Phone],
       b.Email,
       b.[Last Eligibility Assessment]
FROM #b b
WHERE b.Program NOT LIKE 'PrEP%';
