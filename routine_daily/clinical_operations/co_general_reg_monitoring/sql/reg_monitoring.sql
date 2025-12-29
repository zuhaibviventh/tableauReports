--SET NOCOUNT ON;
--SET ANSI_WARNINGS OFF;


--IF OBJECT_ID('tempdb..#patient_addresses') IS NOT NULL DROP TABLE #patient_addresses;
--SELECT PAT_ADDRESS.PAT_ID,
--       MAX(IIF(PAT_ADDRESS.LINE = 1, PAT_ADDRESS.ADDRESS, NULL)) AS ADDRESS_LINE_1,
--       MAX(IIF(PAT_ADDRESS.LINE = 2, PAT_ADDRESS.ADDRESS, NULL)) AS ADDRESS_LINE_2
--INTO #patient_addresses
--FROM Clarity.dbo.PAT_ADDRESS AS PAT_ADDRESS
--GROUP BY PAT_ADDRESS.PAT_ID;


--IF OBJECT_ID('tempdb..#care_team') IS NOT NULL DROP TABLE #care_team;
--SELECT PAT_PCP.PAT_ID,
--       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C = '3', CLARITY_SER.PROV_NAME, NULL)) AS RN,
--       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C = '102', CLARITY_SER.PROV_NAME, NULL)) AS PHARMD,
--       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C = '108', CLARITY_SER.PROV_NAME, NULL)) AS DENTIST,
--       MAX(IIF(PAT_PCP.RELATIONSHIP_C = '1010', CLARITY_SER.PROV_NAME, NULL)) AS MCM,
--       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C IN ( '171', '136', '117', '134', '10', '164', '110' ), CLARITY_SER.PROV_NAME, NULL)) AS MH_PROVIDER
--INTO #care_team
--FROM Clarity.dbo.PAT_PCP_VIEW AS PAT_PCP
--    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_PCP.PCP_PROV_ID = CLARITY_SER.PROV_ID
--WHERE PAT_PCP.TERM_DATE IS NULL
--GROUP BY PAT_PCP.PAT_ID;


--IF OBJECT_ID('tempdb..#comm__phone') IS NOT NULL DROP TABLE #comm__phone;
--SELECT OTHER_COMMUNCTN.PAT_ID,
--       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 1 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM, NULL)) AS MOBILE_NUM,
--       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 1 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.CONTACT_PRIORITY, NULL)) AS MOBILE_PRIORITY,
--       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 7 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM, NULL)) AS HOME_NUM,
--       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 7 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.CONTACT_PRIORITY, NULL)) AS HOME_PRIORITY,
--       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 8 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM, NULL)) AS WORK_NUM,
--       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 8 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.CONTACT_PRIORITY, NULL)) AS WORK_PRIORITY
--INTO #comm__phone
--FROM Clarity.dbo.OTHER_COMMUNCTN AS OTHER_COMMUNCTN
--    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON OTHER_COMMUNCTN.PAT_ID = IDENTITY_ID.PAT_ID
--    INNER JOIN Clarity.dbo.ZC_OTHER_COMMUNIC AS ZC_OTHER_COMMUNIC ON OTHER_COMMUNCTN.OTHER_COMMUNIC_C = ZC_OTHER_COMMUNIC.OTHER_COMMUNIC_C
--GROUP BY OTHER_COMMUNCTN.PAT_ID;


--/* The IIF() statement is not gonna work but Mitch had it in his code, so I guess it stays for now. */
--IF OBJECT_ID('tempdb..#patient_type__hiv_pos') IS NOT NULL DROP TABLE #patient_type__hiv_pos;
--SELECT IDENTITY_ID.PAT_ID,
--       MAX(IIF(EDG_CURRENT_ICD10.CODE IS NOT NULL, 'HIV+', 'Unknown')) AS PATIENT_TYPE
--INTO #patient_type__hiv_pos
--FROM Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
--    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW AS PROBLEM_LIST ON IDENTITY_ID.PAT_ID = PROBLEM_LIST.PAT_ID
--    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON PROBLEM_LIST.DX_ID = EDG_CURRENT_ICD10.DX_ID
--WHERE PROBLEM_LIST.PROBLEM_STATUS_C = 1 -- Dx is active on problem list
--      AND PROBLEM_LIST.RESOLVED_DATE IS NULL -- Dx is unresolved
--      AND EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21', 'Z78.9', 'B97.35' ) --Z78.9 = false positive HIV test
--GROUP BY IDENTITY_ID.PAT_ID;


--IF OBJECT_ID('tempdb..#patient_type__hiv_neg') IS NOT NULL DROP TABLE #patient_type__hiv_neg;
--SELECT IDENTITY_ID.PAT_ID,
--       MAX(IIF(PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IS NOT NULL, 'HIV-', 'Unknown')) AS PATIENT_TYPE_2
--INTO #patient_type__hiv_neg
--FROM Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
--    INNER JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS ON IDENTITY_ID.PAT_ID = PATIENT_FYI_FLAGS.PATIENT_ID
--WHERE PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IN ( '640005', '640008', '9800035', '640007', '640017', '9800065' )
--GROUP BY IDENTITY_ID.PAT_ID;


--IF OBJECT_ID('tempdb..#doc_info') IS NOT NULL DROP TABLE #doc_info;
--SELECT DOC_INFORMATION.DOC_PT_ID AS PAT_ID,
--       DOC_INFORMATION.DOC_RECV_TIME,
--       DOC_INFORMATION.DOC_INFO_TYPE_C,
--       ZC_DOC_INFO_TYPE.NAME AS DOC_TYPE,
--       ROW_NUMBER() OVER (PARTITION BY DOC_INFORMATION.DOC_PT_ID,
--                                       ZC_DOC_INFO_TYPE.NAME
--                          ORDER BY DOC_INFORMATION.DOC_RECV_TIME DESC) AS ROW_NUM_DESC
--INTO #doc_info
--FROM Clarity.dbo.DOC_INFORMATION_VIEW AS DOC_INFORMATION
--    INNER JOIN Clarity.dbo.ZC_DOC_INFO_TYPE AS ZC_DOC_INFO_TYPE ON DOC_INFORMATION.DOC_INFO_TYPE_C = ZC_DOC_INFO_TYPE.DOC_INFO_TYPE_C
--WHERE DOC_INFORMATION.DOC_INFO_TYPE_C IN ( '140263', '4100150', '4100026', '4100028', '4100025', '4100027', '103239', '103238', '103241' )
--      AND (DOC_INFORMATION.IS_ESIGNED_YN = 'Y' OR DOC_INFORMATION.IS_SCANNED_YN = 'Y');


--IF OBJECT_ID('tempdb..#patient_documents') IS NOT NULL DROP TABLE #patient_documents;
--SELECT #doc_info.PAT_ID,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '140263', 'Yes', 'No')) AS FDS_CONSENT_TO_TREAT,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '140263', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_CONSENT_TO_TREAT_DT,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '4100150', 'Yes', 'No')) AS SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '4100150', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE_DT,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100026', '4100028' ), 'Yes', 'No')) AS SA64_RIGHTS_AND_RESPONSIBILITIES,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100026', '4100028' ), CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_RIGHTS_AND_RESPONSIBILITIES_DT,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100025', '4100027' ), 'Yes', 'No')) AS SA64_ACKNOWLEDGEMENT_OF_RECEIPT,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100025', '4100027' ), CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_ACKNOWLEDGEMENT_OF_RECEIPT_DT,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103239', 'Yes', 'No')) AS FDS_PHOTO_ID,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103239', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_PHOTO_ID_DT,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103238', 'Yes', 'No')) AS FDS_PRIVATE_INSURANCE,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103238', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_PRIVATE_INSURANCE_DT,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103241', 'Yes', 'No')) AS FDS_INCOME_VERIFICATION,
--       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103241', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_INCOME_VERIFICATION_DT
--INTO #patient_documents
--FROM #doc_info
--WHERE #doc_info.ROW_NUM_DESC = 1
--GROUP BY #doc_info.PAT_ID;


--IF OBJECT_ID('tempdb..#registration') IS NOT NULL DROP TABLE #registration;
--SELECT MAX(CLARITY_DEP.DEPARTMENT_NAME) AS REGISTRAR_LOGIN_DEPT,
--       MAX(CLARITY_EMP.NAME) AS REGISTRAR,
--       MAX(CAST(REG_HX.REG_HX_INST_UTC_DTTM AS DATE)) AS REG_DATE,
--       MAX(REG_HX.REG_HX_INST_UTC_DTTM) AS DATE_OF_REGISTRATION,
--       MAX(DATEDIFF(DAY, REG_HX.REG_HX_INST_UTC_DTTM, GETDATE())) AS DAYS_SINCE_REGISTRATION,
--       REG_HX.REG_HX_OPEN_PAT_ID,
--       REG_HX.REG_HX_OPEN_PAT_CSN AS PAT_ENC_CSN_ID
--INTO #registration
--FROM CLARITY.dbo.REG_HX AS REG_HX
--    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON REG_HX.REG_HX_LOGIN_DEP_ID = CLARITY_DEP.DEPARTMENT_ID
--    INNER JOIN CLARITY.dbo.ZC_REG_HX_EVENT AS ZC_REG_HX_EVENT ON REG_HX.REG_HX_EVENT_C = ZC_REG_HX_EVENT.REG_HX_EVENT_C
--    INNER JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON REG_HX.REG_HX_USER_ID = CLARITY_EMP.USER_ID
--    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON CLARITY_EMP.PROV_ID = CLARITY_SER.PROV_ID
--    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON REG_HX.REG_HX_OPEN_PAT_ID = IDENTITY_ID.PAT_ID
--WHERE REG_HX.REG_HX_INST_UTC_DTTM > DATEADD(DAY, -90, GETDATE())
--      AND (CLARITY_SER.PROVIDER_TYPE_C IS NULL
--           OR CLARITY_SER.PROVIDER_TYPE_C IN ( '222', '156', '125', '132' ))
--GROUP BY REG_HX.REG_HX_OPEN_PAT_ID,
--         REG_HX.REG_HX_OPEN_PAT_CSN;


--IF OBJECT_ID('tempdb..#patient_relationship') IS NOT NULL DROP TABLE #patient_relationship;
--SELECT PAT_RELATIONSHIPS.PAT_ID,
--       MAX('Yes') AS EMERGENCY_CONTACT_RECORDED
--INTO #patient_relationship
--FROM CLARITY.dbo.PAT_RELATIONSHIPS AS PAT_RELATIONSHIPS
--GROUP BY PAT_RELATIONSHIPS.PAT_ID;


--IF OBJECT_ID('tempdb..#home_status') IS NOT NULL DROP TABLE #home_status;
--SELECT X_LAST_MIGRANT_HOMELESS_STATUS.PAT_ID,
--       MAX(ZC_MIGRANT.NAME) AS MIGRANT_OR_SEASONAL,
--       MAX(ZC_PAT_HOMELESS.NAME) AS HOMELESSNESS
--INTO #home_status
--FROM CLARITY.dbo.X_LAST_MIGRANT_HOMELESS_STATUS_VIEW AS X_LAST_MIGRANT_HOMELESS_STATUS
--    LEFT JOIN CLARITY.dbo.ZC_MIGRANT AS ZC_MIGRANT ON X_LAST_MIGRANT_HOMELESS_STATUS.MIGRANT_SEASONAL_C = ZC_MIGRANT.MIGRANT_SEASONAL_C
--    LEFT JOIN CLARITY.dbo.ZC_PAT_HOMELESS AS ZC_PAT_HOMELESS ON X_LAST_MIGRANT_HOMELESS_STATUS.PAT_HOMELESS_C = ZC_PAT_HOMELESS.PAT_HOMELESS_C
--GROUP BY X_LAST_MIGRANT_HOMELESS_STATUS.PAT_ID;


--IF OBJECT_ID('tempdb..#fpl_information') IS NOT NULL DROP TABLE #fpl_information;
--SELECT X_FPL_MAX.PAT_ID,
--       MIN(X_FPL_MAX.fpl_percentage) AS FPL_PCT
--INTO #fpl_information
--FROM CLARITY.dbo.X_FPL_MAX_VIEW AS X_FPL_MAX
--GROUP BY X_FPL_MAX.PAT_ID;


--SELECT IDENTITY_ID.IDENTITY_ID AS [MRN],
--       #registration.REGISTRAR_LOGIN_DEPT AS [Registrar Login Dept],
--       IIF(CLARITY_DEP.DEPARTMENT_NAME = 'XXXVH AUS DENTAL', 'VH AUS DENTAL', CLARITY_DEP.DEPARTMENT_NAME) AS [VISIT DEPT],
--       SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2) AS [STATE],
--       #registration.REGISTRAR,
--       #registration.REG_DATE AS [Reg Date],
--       #registration.DATE_OF_REGISTRATION AS [Date of Registration],
--       #registration.DAYS_SINCE_REGISTRATION AS [Days Since Registration],
--       CLARITY_PRC.PRC_NAME AS [VISIT TYPE],
--       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS [VISIT DATE],
--       PATIENT.PAT_NAME,
--       PATIENT.SSN,
--       CAST(PATIENT.BIRTH_DATE AS DATE) AS BIRTH_DATE,
--       #patient_addresses.ADDRESS_LINE_1 AS [ADDRESS LINE 1],
--       IIF(#patient_addresses.ADDRESS_LINE_1 IS NULL, 'No', 'Yes') AS [HAS ADDRESS],
--       #patient_addresses.ADDRESS_LINE_2 AS [ADDRESS LINE 2],
--       #comm__phone.HOME_NUM AS [HOME #],
--       #comm__phone.HOME_PRIORITY AS [HOME PRIORITY],
--       #comm__phone.MOBILE_NUM AS [MOBILE #],
--       #comm__phone.MOBILE_PRIORITY AS [MOBILE PRIORITY],
--       #comm__phone.WORK_NUM AS [WORK #],
--       #comm__phone.WORK_PRIORITY AS [WORK PRIORITY],
--       IIF(#comm__phone.HOME_PRIORITY IS NULL AND #comm__phone.MOBILE_PRIORITY IS NULL AND #comm__phone.WORK_PRIORITY IS NULL, 'No', 'Yes') AS [PHONE PRIORITY SET],
--       PATIENT.EMAIL_ADDRESS AS EMAIL,
--       CASE WHEN PATIENT_4.NO_EMAIL_REASON_C IS NOT NULL THEN 'Declined'
--           WHEN PATIENT.EMAIL_ADDRESS IS NULL THEN 'Missing'
--           WHEN PATIENT.EMAIL_ADDRESS IN ( '#nomail@nomail.com', '#nomail@nomamil.com', 'momail@nomail.com', 'no@email.com', 'no@no.com', 'no@noemail.com',
--                                           'noemai@noemail.com', 'Noemai@nomail.com', 'noemail!@noemail.org', 'noemail@aol.com', 'noemail@email.com',
--                                           'noemail@email.org', 'noemail@emial.com', 'noemail@gmail.com', 'noemail@mail.com', 'noemail@msn.com',
--                                           'noemail@noeamail.com', 'noemail@noeamil.com', 'noemail@noeamil.org', 'noemail@noemai.com', 'noemail@noemail.cm',
--                                           'noemail@noemail.com', 'no-email@noemail.com', 'noemail@noemail.org', 'noemail@noemal.com', 'noemail@noemal.org',
--                                           'noemail@noemial.com', 'noemail@noemil.com', 'noemail@nomail.com', 'noemail@nomemail.com', 'noemail@normail.com',
--                                           'noemail@yahoo.com', 'noemial@noemail.com', 'noemil@noemail.com', 'nomail@email.com', 'nomail@noemail.com',
--                                           'nomail@noemail.org', 'nomail@nomai.com', 'nomail@nomail.com', 'nomail@nomaile.com', 'nomail@nomial.com',
--                                           'nomial@nomail.com', 'nomial@nomial.com', 'non@none.com', 'noname@noname.com', 'none@gmail.com', 'none@noemail.com',
--                                           'none@non.com', 'none@none.com', 'noone@noone.com', 'NOWMAIL@NOEMAIL.COM', 'unknown@aol.com', 'unknown@chs.org',
--                                           'unknown@unknown.com' ) THEN 'Invalid'
--           ELSE 'Valid'
--       END AS [EMAIL STATUS],
--       CASE WHEN PATIENT.VETERAN_STATUS_C IS NULL THEN 'Uncollected'
--           WHEN PATIENT.VETERAN_STATUS_C = '100' THEN 'No'
--           ELSE ZC_VETERAN_STAT.NAME
--       END AS [VETERAN STATUS],
--       CLARITY_SER.PROV_NAME AS [Care Team PCP],
--       #care_team.RN AS [Care Team RN],
--       #care_team.PHARMD AS [Care Team Pharmacist],
--       #care_team.DENTIST AS [Care Team Dentist],
--       #care_team.MCM AS [Care Team Case Manager],
--       #care_team.MH_PROVIDER AS [Care Team MH Provider],
--       CASE WHEN #care_team.RN IS NULL THEN 'No'
--           WHEN #care_team.PHARMD IS NULL THEN 'No'
--           WHEN #care_team.DENTIST IS NULL THEN 'No'
--           WHEN #care_team.MCM IS NULL THEN 'No'
--           WHEN #care_team.MH_PROVIDER IS NULL THEN 'No'
--           ELSE 'Yes'
--       END AS [COMPLETE_CARE_TEAM],
--       COALESCE(#patient_relationship.EMERGENCY_CONTACT_RECORDED, 'No') AS [EMERGENCY CONTACT RECORDED],
--       COALESCE(ZC_EMPY_STATUS.NAME, 'Missing') AS [EMPLOYMENT STATUS],
--       COALESCE(PATIENT.INTRPTR_NEEDED_YN, 'Missing') AS [Interpreter Needed],
--       COALESCE(ZC_LANGUAGE.NAME, 'Missing') AS [LANGUAGE],
--       COALESCE(ZC_ENGLISH_FLUENCY.NAME, 'Missing') AS [ENGLISH_FLUENCY],
--       CASE WHEN #patient_type__hiv_pos.PATIENT_TYPE = 'HIV+' THEN 'HIV+'
--           WHEN #patient_type__hiv_neg.PATIENT_TYPE_2 = 'HIV-' THEN 'HIV-'
--           ELSE 'Unknown'
--       END AS [PATIENT TYPE],
--       COALESCE(#patient_documents.FDS_CONSENT_TO_TREAT, 'No') AS [FDS - Consent to Treat],
--       #patient_documents.FDS_CONSENT_TO_TREAT_DT AS [FDS - Consent to Treat Date],
--       DATEDIFF(MONTH, #patient_documents.FDS_CONSENT_TO_TREAT_DT, #registration.REG_DATE) AS [FDS - Consent to Treat Months Old],
--       COALESCE(#patient_documents.SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE, 'No') AS [SA64 E-Sig Grievance Policy and Procedure],
--       #patient_documents.SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE_DT AS [SA64 E-Sig Grievance Policy and Procedure Date],
--       DATEDIFF(MONTH, #patient_documents.SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE_DT, #registration.REG_DATE) AS [SA64 E-Sig Grievance Policy and Procedure Months Old],
--       COALESCE(#patient_documents.SA64_RIGHTS_AND_RESPONSIBILITIES, 'No') AS [SA64 E-SIG Rights and Responsibilities],
--       #patient_documents.SA64_RIGHTS_AND_RESPONSIBILITIES_DT AS [SA64 E-SIG Rights and Responsibilities Date],
--       DATEDIFF(MONTH, #patient_documents.SA64_RIGHTS_AND_RESPONSIBILITIES_DT, #registration.REG_DATE) AS [SA64 E-SIG Rights and Responsibilities Months Old],
--       COALESCE(#patient_documents.SA64_ACKNOWLEDGEMENT_OF_RECEIPT, 'No') AS [SA64 E-SIG Acknowledgment of Receipt of Privacy Notice],
--       #patient_documents.SA64_ACKNOWLEDGEMENT_OF_RECEIPT_DT AS [SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Date],
--       DATEDIFF(MONTH, #patient_documents.SA64_ACKNOWLEDGEMENT_OF_RECEIPT_DT, #registration.REG_DATE) AS [SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Months Old],
--       COALESCE(#patient_documents.FDS_PHOTO_ID, 'No') AS [FDS - Photo ID],
--       #patient_documents.FDS_PHOTO_ID_DT AS [FDS - Photo ID Date],
--       DATEDIFF(MONTH, #patient_documents.FDS_PHOTO_ID_DT, #registration.REG_DATE) AS [FDS - Photo ID Months Old],
--       COALESCE(#patient_documents.FDS_PRIVATE_INSURANCE, 'No') AS [FDS - Private Insurance],
--       #patient_documents.FDS_PRIVATE_INSURANCE_DT AS [FDS - Private Insurance Date],
--       DATEDIFF(MONTH, #patient_documents.FDS_PRIVATE_INSURANCE_DT, #registration.REG_DATE) AS [FDS - Private Insurance Months Old],
--       COALESCE(#patient_documents.FDS_INCOME_VERIFICATION, 'No') AS [FDS - Income Verification],
--       #patient_documents.FDS_INCOME_VERIFICATION_DT AS [FDS - Income Verification Date],
--       DATEDIFF(MONTH, #patient_documents.FDS_INCOME_VERIFICATION_DT, #registration.REG_DATE) AS [FDS - Income Verification Months Old],
--       COALESCE(#home_status.MIGRANT_OR_SEASONAL, 'Missing') AS [Migrant/Seasonal],
--       COALESCE(#home_status.HOMELESSNESS, 'Missing') AS [Homelessness],
--       CAST(#fpl_information.FPL_PCT AS INT) AS [FPL%],
--       IIF(#fpl_information.FPL_PCT IS NULL, 'Missing', 'Has FPL') AS [FPL STATUS]
--FROM CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
--    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
--    INNER JOIN CLARITY.dbo.PATIENT_3 AS PATIENT_3 ON IDENTITY_ID.PAT_ID = PATIENT_3.PAT_ID
--    INNER JOIN CLARITY.dbo.PATIENT_4 AS PATIENT_4 ON IDENTITY_ID.PAT_ID = PATIENT_4.PAT_ID
--    LEFT JOIN CLARITY.dbo.ZC_ENGLISH_FLUENCY AS ZC_ENGLISH_FLUENCY ON PATIENT_3.ENGLISH_FLUENCY_C = ZC_ENGLISH_FLUENCY.ENGLISH_FLUENCY_C
--    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PATIENT.CUR_PCP_PROV_ID = CLARITY_SER.PROV_ID
--    LEFT JOIN CLARITY.dbo.ZC_VETERAN_STAT AS ZC_VETERAN_STAT ON PATIENT.VETERAN_STATUS_C = ZC_VETERAN_STAT.VETERAN_STATUS_C
--    LEFT JOIN CLARITY.dbo.ZC_EMPY_STATUS AS ZC_EMPY_STATUS ON PATIENT.EMPY_STATUS_C = ZC_EMPY_STATUS.EMPY_STATUS_C
--    LEFT JOIN CLARITY.dbo.ZC_LANGUAGE AS ZC_LANGUAGE ON PATIENT.LANGUAGE_C = ZC_LANGUAGE.LANGUAGE_C
--    LEFT JOIN #patient_addresses ON IDENTITY_ID.PAT_ID = #patient_addresses.PAT_ID
--    LEFT JOIN #care_team ON IDENTITY_ID.PAT_ID = #care_team.PAT_ID
--    LEFT JOIN #comm__phone ON IDENTITY_ID.PAT_ID = #comm__phone.PAT_ID
--    LEFT JOIN #patient_type__hiv_pos ON IDENTITY_ID.PAT_ID = #patient_type__hiv_pos.PAT_ID
--    LEFT JOIN #patient_type__hiv_neg ON IDENTITY_ID.PAT_ID = #patient_type__hiv_neg.PAT_ID
--    LEFT JOIN #patient_documents ON IDENTITY_ID.PAT_ID = #patient_documents.PAT_ID
--    INNER JOIN #registration ON IDENTITY_ID.PAT_ID = #registration.REG_HX_OPEN_PAT_ID
--    LEFT JOIN #patient_relationship ON IDENTITY_ID.PAT_ID = #patient_relationship.PAT_ID
--    LEFT JOIN #home_status ON IDENTITY_ID.PAT_ID = #home_status.PAT_ID
--    LEFT JOIN #fpl_information ON IDENTITY_ID.PAT_ID = #fpl_information.PAT_ID
--    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON #registration.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID
--    LEFT JOIN CLARITY.dbo.CLARITY_PRC AS CLARITY_PRC ON PAT_ENC.APPT_PRC_ID = CLARITY_PRC.PRC_ID
--    LEFT JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
--WHERE PATIENT_4.PAT_LIVING_STAT_C = 1;


SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;


IF OBJECT_ID('tempdb..#patient_addresses') IS NOT NULL DROP TABLE #patient_addresses;
SELECT PAT_ADDRESS.PAT_ID,
       MAX(IIF(PAT_ADDRESS.LINE = 1, PAT_ADDRESS.ADDRESS, NULL)) AS ADDRESS_LINE_1,
       MAX(IIF(PAT_ADDRESS.LINE = 2, PAT_ADDRESS.ADDRESS, NULL)) AS ADDRESS_LINE_2
INTO #patient_addresses
FROM Clarity.dbo.PAT_ADDRESS AS PAT_ADDRESS
GROUP BY PAT_ADDRESS.PAT_ID;


IF OBJECT_ID('tempdb..#care_team') IS NOT NULL DROP TABLE #care_team;
SELECT PAT_PCP.PAT_ID,
       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C = '3', CLARITY_SER.PROV_NAME, NULL)) AS RN,
       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C = '102', CLARITY_SER.PROV_NAME, NULL)) AS PHARMD,
       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C = '108', CLARITY_SER.PROV_NAME, NULL)) AS DENTIST,
       MAX(IIF(PAT_PCP.RELATIONSHIP_C = '1010', CLARITY_SER.PROV_NAME, NULL)) AS MCM,
       MAX(IIF(CLARITY_SER.PROVIDER_TYPE_C IN ( '171', '136', '117', '134', '10', '164', '110' ), CLARITY_SER.PROV_NAME, NULL)) AS MH_PROVIDER
INTO #care_team
FROM Clarity.dbo.PAT_PCP_VIEW AS PAT_PCP
    INNER JOIN Clarity.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PAT_PCP.PCP_PROV_ID = CLARITY_SER.PROV_ID
WHERE PAT_PCP.TERM_DATE IS NULL
GROUP BY PAT_PCP.PAT_ID;


IF OBJECT_ID('tempdb..#comm__phone') IS NOT NULL DROP TABLE #comm__phone;
SELECT OTHER_COMMUNCTN.PAT_ID,
       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 1 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM, NULL)) AS MOBILE_NUM,
       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 1 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.CONTACT_PRIORITY, NULL)) AS MOBILE_PRIORITY,
       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 7 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM, NULL)) AS HOME_NUM,
       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 7 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.CONTACT_PRIORITY, NULL)) AS HOME_PRIORITY,
       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 8 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM, NULL)) AS WORK_NUM,
       MAX(IIF(OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 8 AND OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM <> 'none', OTHER_COMMUNCTN.CONTACT_PRIORITY, NULL)) AS WORK_PRIORITY
INTO #comm__phone
FROM Clarity.dbo.OTHER_COMMUNCTN AS OTHER_COMMUNCTN
    INNER JOIN Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON OTHER_COMMUNCTN.PAT_ID = IDENTITY_ID.PAT_ID
    INNER JOIN Clarity.dbo.ZC_OTHER_COMMUNIC AS ZC_OTHER_COMMUNIC ON OTHER_COMMUNCTN.OTHER_COMMUNIC_C = ZC_OTHER_COMMUNIC.OTHER_COMMUNIC_C
GROUP BY OTHER_COMMUNCTN.PAT_ID;


/* The IIF() statement is not gonna work but Mitch had it in his code, so I guess it stays for now. */
IF OBJECT_ID('tempdb..#patient_type__hiv_pos') IS NOT NULL DROP TABLE #patient_type__hiv_pos;
SELECT IDENTITY_ID.PAT_ID,
       MAX(IIF(EDG_CURRENT_ICD10.CODE IS NOT NULL, 'HIV+', 'Unknown')) AS PATIENT_TYPE
INTO #patient_type__hiv_pos
FROM Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
    INNER JOIN Clarity.dbo.PROBLEM_LIST_VIEW AS PROBLEM_LIST ON IDENTITY_ID.PAT_ID = PROBLEM_LIST.PAT_ID
    INNER JOIN Clarity.dbo.EDG_CURRENT_ICD10 AS EDG_CURRENT_ICD10 ON PROBLEM_LIST.DX_ID = EDG_CURRENT_ICD10.DX_ID
WHERE PROBLEM_LIST.PROBLEM_STATUS_C = 1 -- Dx is active on problem list
      AND PROBLEM_LIST.RESOLVED_DATE IS NULL -- Dx is unresolved
      AND EDG_CURRENT_ICD10.CODE IN ( 'B20', 'Z21', 'Z78.9', 'B97.35' ) --Z78.9 = false positive HIV test
GROUP BY IDENTITY_ID.PAT_ID;


IF OBJECT_ID('tempdb..#patient_type__hiv_neg') IS NOT NULL DROP TABLE #patient_type__hiv_neg;
SELECT IDENTITY_ID.PAT_ID,
       MAX(IIF(PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IS NOT NULL, 'HIV-', 'Unknown')) AS PATIENT_TYPE_2
INTO #patient_type__hiv_neg
FROM Clarity.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
    INNER JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW AS PATIENT_FYI_FLAGS ON IDENTITY_ID.PAT_ID = PATIENT_FYI_FLAGS.PATIENT_ID
WHERE PATIENT_FYI_FLAGS.PAT_FLAG_TYPE_C IN ( '640005', '640008', '9800035', '640007', '640017', '9800065' )
GROUP BY IDENTITY_ID.PAT_ID;


IF OBJECT_ID('tempdb..#doc_info') IS NOT NULL DROP TABLE #doc_info;
SELECT DOC_INFORMATION.DOC_PT_ID AS PAT_ID,
       DOC_INFORMATION.DOC_RECV_TIME,
       DOC_INFORMATION.DOC_INFO_TYPE_C,
       ZC_DOC_INFO_TYPE.NAME AS DOC_TYPE,
       ROW_NUMBER() OVER (PARTITION BY DOC_INFORMATION.DOC_PT_ID,
                                       ZC_DOC_INFO_TYPE.NAME
                          ORDER BY DOC_INFORMATION.DOC_RECV_TIME DESC) AS ROW_NUM_DESC
INTO #doc_info
FROM Clarity.dbo.DOC_INFORMATION_VIEW AS DOC_INFORMATION
    INNER JOIN Clarity.dbo.ZC_DOC_INFO_TYPE AS ZC_DOC_INFO_TYPE ON DOC_INFORMATION.DOC_INFO_TYPE_C = ZC_DOC_INFO_TYPE.DOC_INFO_TYPE_C
WHERE DOC_INFORMATION.DOC_INFO_TYPE_C IN ( '140263', '4100150', '4100026', '4100028', '4100025', '4100027', '103239', '103238', '103241' --adding new reg documnets hari.chandan 4/10/2025
, '106992',  -- SA64 Consent for Clinical Treatment
    '106993',  -- SA64 Financial Consent
    '146272','146273',  -- SA64 BHWC Consent for Services
    '146281', '146316', '146315',  -- SA64 CO Consent for Behavioral Health Services
    '146279','146280',  -- SA64 Notice of Privacy Practices
    '848400895',  -- SA64 Mandatory Disclosure KClaunch
    '146408'   -- SA64 General Dental Consent Form
)
      AND (DOC_INFORMATION.IS_ESIGNED_YN = 'Y' OR DOC_INFORMATION.IS_SCANNED_YN = 'Y');


IF OBJECT_ID('tempdb..#patient_documents') IS NOT NULL DROP TABLE #patient_documents;
SELECT #doc_info.PAT_ID,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '140263', 'Yes', 'No')) AS FDS_CONSENT_TO_TREAT,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '140263', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_CONSENT_TO_TREAT_DT,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '4100150', 'Yes', 'No')) AS SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '4100150', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE_DT,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100026', '4100028' ), 'Yes', 'No')) AS SA64_RIGHTS_AND_RESPONSIBILITIES,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100026', '4100028' ), CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_RIGHTS_AND_RESPONSIBILITIES_DT,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100025', '4100027' ), 'Yes', 'No')) AS SA64_ACKNOWLEDGEMENT_OF_RECEIPT,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ( '4100025', '4100027' ), CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_ACKNOWLEDGEMENT_OF_RECEIPT_DT,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103239', 'Yes', 'No')) AS FDS_PHOTO_ID,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103239', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_PHOTO_ID_DT,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103238', 'Yes', 'No')) AS FDS_PRIVATE_INSURANCE,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103238', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_PRIVATE_INSURANCE_DT,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103241', 'Yes', 'No')) AS FDS_INCOME_VERIFICATION,
       MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '103241', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS FDS_INCOME_VERIFICATION_DT,
           -- General Dental Consent
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '146408', 'Yes', 'No')) AS SA64_GENERAL_DENTAL_CONSENT,
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '146408', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_GENERAL_DENTAL_CONSENT_DT,

        -- BHWC Consent for Services (Grouped)
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ('146273', '146272'), 'Yes', 'No')) AS SA64_BHWC_CONSENT,
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ('146273', '146272'), CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_BHWC_CONSENT_DT,

        -- Mandatory Disclosure KClaunch
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '848400895', 'Yes', 'No')) AS SA64_MANDATORY_DISCLOSURE_KCLAUNCH,
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '848400895', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_MANDATORY_DISCLOSURE_KCLAUNCH_DT,

        -- CO Consent for Behavioral Health Services (Grouped)
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ('146281', '146316', '146315'), 'Yes', 'No')) AS SA64_BEHAVIORAL_HEALTH_CONSENT,
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ('146281', '146316', '146315'), CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_BEHAVIORAL_HEALTH_CONSENT_DT,

        -- Consent for Clinical Treatment
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '106992', 'Yes', 'No')) AS SA64_CONSENT_FOR_CLINICAL_TREATMENT,
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '106992', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_CONSENT_FOR_CLINICAL_TREATMENT_DT,

        -- Financial Consent
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '106993', 'Yes', 'No')) AS SA64_FINANCIAL_CONSENT,
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C = '106993', CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_FINANCIAL_CONSENT_DT,

        -- Privacy Notice (Grouped)
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ('146279', '146280'), 'Yes', 'No')) AS SA64_PRIVACY_NOTICE,
        MAX(IIF(#doc_info.DOC_INFO_TYPE_C IN ('146279', '146280'), CAST(#doc_info.DOC_RECV_TIME AS DATE), NULL)) AS SA64_PRIVACY_NOTICE_DT
INTO #patient_documents
FROM #doc_info
WHERE #doc_info.ROW_NUM_DESC = 1
GROUP BY #doc_info.PAT_ID;


IF OBJECT_ID('tempdb..#registration') IS NOT NULL DROP TABLE #registration;
SELECT MAX(CLARITY_DEP.DEPARTMENT_NAME) AS REGISTRAR_LOGIN_DEPT,
       MAX(CLARITY_EMP.NAME) AS REGISTRAR,
       MAX(CAST(REG_HX.REG_HX_INST_UTC_DTTM AS DATE)) AS REG_DATE,
       MAX(REG_HX.REG_HX_INST_UTC_DTTM) AS DATE_OF_REGISTRATION,
       MAX(DATEDIFF(DAY, REG_HX.REG_HX_INST_UTC_DTTM, GETDATE())) AS DAYS_SINCE_REGISTRATION,
       REG_HX.REG_HX_OPEN_PAT_ID,
       REG_HX.REG_HX_OPEN_PAT_CSN AS PAT_ENC_CSN_ID
INTO #registration
FROM CLARITY.dbo.REG_HX AS REG_HX
    INNER JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON REG_HX.REG_HX_LOGIN_DEP_ID = CLARITY_DEP.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.ZC_REG_HX_EVENT AS ZC_REG_HX_EVENT ON REG_HX.REG_HX_EVENT_C = ZC_REG_HX_EVENT.REG_HX_EVENT_C
    INNER JOIN CLARITY.dbo.CLARITY_EMP_VIEW AS CLARITY_EMP ON REG_HX.REG_HX_USER_ID = CLARITY_EMP.USER_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON CLARITY_EMP.PROV_ID = CLARITY_SER.PROV_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID ON REG_HX.REG_HX_OPEN_PAT_ID = IDENTITY_ID.PAT_ID
WHERE REG_HX.REG_HX_INST_UTC_DTTM > DATEADD(DAY, -90, GETDATE())
      AND (CLARITY_SER.PROVIDER_TYPE_C IS NULL
           OR CLARITY_SER.PROVIDER_TYPE_C IN ( '222', '156', '125', '132', '201' ))
GROUP BY REG_HX.REG_HX_OPEN_PAT_ID,
         REG_HX.REG_HX_OPEN_PAT_CSN;


IF OBJECT_ID('tempdb..#patient_relationship') IS NOT NULL DROP TABLE #patient_relationship;
SELECT PAT_RELATIONSHIPS.PAT_ID,
       MAX('Yes') AS EMERGENCY_CONTACT_RECORDED
INTO #patient_relationship
FROM CLARITY.dbo.PAT_RELATIONSHIPS AS PAT_RELATIONSHIPS
GROUP BY PAT_RELATIONSHIPS.PAT_ID;


IF OBJECT_ID('tempdb..#home_status') IS NOT NULL DROP TABLE #home_status;
SELECT X_LAST_MIGRANT_HOMELESS_STATUS.PAT_ID,
       MAX(ZC_MIGRANT.NAME) AS MIGRANT_OR_SEASONAL,
       MAX(ZC_PAT_HOMELESS.NAME) AS HOMELESSNESS
INTO #home_status
FROM CLARITY.dbo.X_LAST_MIGRANT_HOMELESS_STATUS_VIEW AS X_LAST_MIGRANT_HOMELESS_STATUS
    LEFT JOIN CLARITY.dbo.ZC_MIGRANT AS ZC_MIGRANT ON X_LAST_MIGRANT_HOMELESS_STATUS.MIGRANT_SEASONAL_C = ZC_MIGRANT.MIGRANT_SEASONAL_C
    LEFT JOIN CLARITY.dbo.ZC_PAT_HOMELESS AS ZC_PAT_HOMELESS ON X_LAST_MIGRANT_HOMELESS_STATUS.PAT_HOMELESS_C = ZC_PAT_HOMELESS.PAT_HOMELESS_C
GROUP BY X_LAST_MIGRANT_HOMELESS_STATUS.PAT_ID;


IF OBJECT_ID('tempdb..#fpl_information') IS NOT NULL DROP TABLE #fpl_information;
SELECT X_FPL_MAX.PAT_ID,
       MIN(X_FPL_MAX.fpl_percentage) AS FPL_PCT
INTO #fpl_information
FROM CLARITY.dbo.X_FPL_MAX_VIEW AS X_FPL_MAX
GROUP BY X_FPL_MAX.PAT_ID;


SELECT IDENTITY_ID.IDENTITY_ID AS [MRN],
       #registration.REGISTRAR_LOGIN_DEPT AS [Registrar Login Dept],
       IIF(CLARITY_DEP.DEPARTMENT_NAME = 'XXXVH AUS DENTAL', 'VH AUS DENTAL', CLARITY_DEP.DEPARTMENT_NAME) AS [VISIT DEPT],
       SUBSTRING(CLARITY_DEP.DEPT_ABBREVIATION, 3, 2) AS [STATE],
       #registration.REGISTRAR,
       #registration.REG_DATE AS [Reg Date],
       #registration.DATE_OF_REGISTRATION AS [Date of Registration],
       #registration.DAYS_SINCE_REGISTRATION AS [Days Since Registration],
       CLARITY_PRC.PRC_NAME AS [VISIT TYPE],
       CAST(PAT_ENC.CONTACT_DATE AS DATE) AS [VISIT DATE],
       PATIENT.PAT_NAME,
       PATIENT.SSN,
       CAST(PATIENT.BIRTH_DATE AS DATE) AS BIRTH_DATE,
       #patient_addresses.ADDRESS_LINE_1 AS [ADDRESS LINE 1],
       IIF(#patient_addresses.ADDRESS_LINE_1 IS NULL, 'No', 'Yes') AS [HAS ADDRESS],
       #patient_addresses.ADDRESS_LINE_2 AS [ADDRESS LINE 2],
       #comm__phone.HOME_NUM AS [HOME #],
       #comm__phone.HOME_PRIORITY AS [HOME PRIORITY],
       #comm__phone.MOBILE_NUM AS [MOBILE #],
       #comm__phone.MOBILE_PRIORITY AS [MOBILE PRIORITY],
       #comm__phone.WORK_NUM AS [WORK #],
       #comm__phone.WORK_PRIORITY AS [WORK PRIORITY],
       IIF(#comm__phone.HOME_PRIORITY IS NULL AND #comm__phone.MOBILE_PRIORITY IS NULL AND #comm__phone.WORK_PRIORITY IS NULL, 'No', 'Yes') AS [PHONE PRIORITY SET],
       PATIENT.EMAIL_ADDRESS AS EMAIL,
       CASE WHEN PATIENT_4.NO_EMAIL_REASON_C IS NOT NULL THEN 'Declined'
           WHEN PATIENT.EMAIL_ADDRESS IS NULL THEN 'Missing'
           WHEN PATIENT.EMAIL_ADDRESS IN ( '#nomail@nomail.com', '#nomail@nomamil.com', 'momail@nomail.com', 'no@email.com', 'no@no.com', 'no@noemail.com',
                                           'noemai@noemail.com', 'Noemai@nomail.com', 'noemail!@noemail.org', 'noemail@aol.com', 'noemail@email.com',
                                           'noemail@email.org', 'noemail@emial.com', 'noemail@gmail.com', 'noemail@mail.com', 'noemail@msn.com',
                                           'noemail@noeamail.com', 'noemail@noeamil.com', 'noemail@noeamil.org', 'noemail@noemai.com', 'noemail@noemail.cm',
                                           'noemail@noemail.com', 'no-email@noemail.com', 'noemail@noemail.org', 'noemail@noemal.com', 'noemail@noemal.org',
                                           'noemail@noemial.com', 'noemail@noemil.com', 'noemail@nomail.com', 'noemail@nomemail.com', 'noemail@normail.com',
                                           'noemail@yahoo.com', 'noemial@noemail.com', 'noemil@noemail.com', 'nomail@email.com', 'nomail@noemail.com',
                                           'nomail@noemail.org', 'nomail@nomai.com', 'nomail@nomail.com', 'nomail@nomaile.com', 'nomail@nomial.com',
                                           'nomial@nomail.com', 'nomial@nomial.com', 'non@none.com', 'noname@noname.com', 'none@gmail.com', 'none@noemail.com',
                                           'none@non.com', 'none@none.com', 'noone@noone.com', 'NOWMAIL@NOEMAIL.COM', 'unknown@aol.com', 'unknown@chs.org',
                                           'unknown@unknown.com' ) THEN 'Invalid'
           ELSE 'Valid'
       END AS [EMAIL STATUS],
       CASE WHEN PATIENT.VETERAN_STATUS_C IS NULL THEN 'Uncollected'
           WHEN PATIENT.VETERAN_STATUS_C = '100' THEN 'No'
           ELSE ZC_VETERAN_STAT.NAME
       END AS [VETERAN STATUS],
       CLARITY_SER.PROV_NAME AS [Care Team PCP],
       #care_team.RN AS [Care Team RN],
       #care_team.PHARMD AS [Care Team Pharmacist],
       #care_team.DENTIST AS [Care Team Dentist],
       #care_team.MCM AS [Care Team Case Manager],
       #care_team.MH_PROVIDER AS [Care Team MH Provider],
       CASE WHEN #care_team.RN IS NULL THEN 'No'
           WHEN #care_team.PHARMD IS NULL THEN 'No'
           WHEN #care_team.DENTIST IS NULL THEN 'No'
           WHEN #care_team.MCM IS NULL THEN 'No'
           WHEN #care_team.MH_PROVIDER IS NULL THEN 'No'
           ELSE 'Yes'
       END AS [COMPLETE_CARE_TEAM],
       COALESCE(#patient_relationship.EMERGENCY_CONTACT_RECORDED, 'No') AS [EMERGENCY CONTACT RECORDED],
       COALESCE(ZC_EMPY_STATUS.NAME, 'Missing') AS [EMPLOYMENT STATUS],
       COALESCE(PATIENT.INTRPTR_NEEDED_YN, 'Missing') AS [Interpreter Needed],
       COALESCE(ZC_LANGUAGE.NAME, 'Missing') AS [LANGUAGE],
       COALESCE(ZC_ENGLISH_FLUENCY.NAME, 'Missing') AS [ENGLISH_FLUENCY],
       CASE WHEN #patient_type__hiv_pos.PATIENT_TYPE = 'HIV+' THEN 'HIV+'
           WHEN #patient_type__hiv_neg.PATIENT_TYPE_2 = 'HIV-' THEN 'HIV-'
           ELSE 'Unknown'
       END AS [PATIENT TYPE],
       COALESCE(#patient_documents.FDS_CONSENT_TO_TREAT, 'No') AS [FDS - Consent to Treat],
       #patient_documents.FDS_CONSENT_TO_TREAT_DT AS [FDS - Consent to Treat Date],
       DATEDIFF(MONTH, #patient_documents.FDS_CONSENT_TO_TREAT_DT, #registration.REG_DATE) AS [FDS - Consent to Treat Months Old],
       COALESCE(#patient_documents.SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE, 'No') AS [SA64 E-Sig Grievance Policy and Procedure],
       #patient_documents.SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE_DT AS [SA64 E-Sig Grievance Policy and Procedure Date],
       DATEDIFF(MONTH, #patient_documents.SA64_E_SIG_GRIEVANCE_POLICY_AND_PROCEDURE_DT, #registration.REG_DATE) AS [SA64 E-Sig Grievance Policy and Procedure Months Old],
       COALESCE(#patient_documents.SA64_RIGHTS_AND_RESPONSIBILITIES, 'No') AS [SA64 E-SIG Rights and Responsibilities],
       #patient_documents.SA64_RIGHTS_AND_RESPONSIBILITIES_DT AS [SA64 E-SIG Rights and Responsibilities Date],
       DATEDIFF(MONTH, #patient_documents.SA64_RIGHTS_AND_RESPONSIBILITIES_DT, #registration.REG_DATE) AS [SA64 E-SIG Rights and Responsibilities Months Old],
       COALESCE(#patient_documents.SA64_ACKNOWLEDGEMENT_OF_RECEIPT, 'No') AS [SA64 E-SIG Acknowledgment of Receipt of Privacy Notice],
       #patient_documents.SA64_ACKNOWLEDGEMENT_OF_RECEIPT_DT AS [SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Date],
       DATEDIFF(MONTH, #patient_documents.SA64_ACKNOWLEDGEMENT_OF_RECEIPT_DT, #registration.REG_DATE) AS [SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Months Old],
       COALESCE(#patient_documents.FDS_PHOTO_ID, 'No') AS [FDS - Photo ID],
       #patient_documents.FDS_PHOTO_ID_DT AS [FDS - Photo ID Date],
       DATEDIFF(MONTH, #patient_documents.FDS_PHOTO_ID_DT, #registration.REG_DATE) AS [FDS - Photo ID Months Old],
       COALESCE(#patient_documents.FDS_PRIVATE_INSURANCE, 'No') AS [FDS - Private Insurance],
       #patient_documents.FDS_PRIVATE_INSURANCE_DT AS [FDS - Private Insurance Date],
       DATEDIFF(MONTH, #patient_documents.FDS_PRIVATE_INSURANCE_DT, #registration.REG_DATE) AS [FDS - Private Insurance Months Old],
       COALESCE(#patient_documents.FDS_INCOME_VERIFICATION, 'No') AS [FDS - Income Verification],
       #patient_documents.FDS_INCOME_VERIFICATION_DT AS [FDS - Income Verification Date],
       DATEDIFF(MONTH, #patient_documents.FDS_INCOME_VERIFICATION_DT, #registration.REG_DATE) AS [FDS - Income Verification Months Old],
       -- General Dental Consent
        COALESCE(#patient_documents.SA64_GENERAL_DENTAL_CONSENT, 'No') AS [SA64 General Dental Consent Form],
        #patient_documents.SA64_GENERAL_DENTAL_CONSENT_DT AS [SA64 General Dental Consent Form Date],
        DATEDIFF(MONTH, #patient_documents.SA64_GENERAL_DENTAL_CONSENT_DT, #registration.REG_DATE) AS [SA64 General Dental Consent Form Months Old],

        -- BHWC Consent
        COALESCE(#patient_documents.SA64_BHWC_CONSENT, 'No') AS [SA64 BHWC Consent for Services],
        #patient_documents.SA64_BHWC_CONSENT_DT AS [SA64 BHWC Consent for Services Date],
        DATEDIFF(MONTH, #patient_documents.SA64_BHWC_CONSENT_DT, #registration.REG_DATE) AS [SA64 BHWC Consent for Services Months Old],

        -- Mandatory Disclosure
        COALESCE(#patient_documents.SA64_MANDATORY_DISCLOSURE_KCLAUNCH, 'No') AS [SA64 Mandatory Disclosure KClaunch],
        #patient_documents.SA64_MANDATORY_DISCLOSURE_KCLAUNCH_DT AS [SA64 Mandatory Disclosure KClaunch Date],
        DATEDIFF(MONTH, #patient_documents.SA64_MANDATORY_DISCLOSURE_KCLAUNCH_DT, #registration.REG_DATE) AS [SA64 Mandatory Disclosure KClaunch Months Old],

        -- CO Consent for Behavioral Health
        COALESCE(#patient_documents.SA64_BEHAVIORAL_HEALTH_CONSENT, 'No') AS [SA64 CO Consent for Behavioral Health Services],
        #patient_documents.SA64_BEHAVIORAL_HEALTH_CONSENT_DT AS [SA64 CO Consent for Behavioral Health Services Date],
        DATEDIFF(MONTH, #patient_documents.SA64_BEHAVIORAL_HEALTH_CONSENT_DT, #registration.REG_DATE) AS [SA64 CO Consent for Behavioral Health Services Months Old],

        -- Clinical Treatment Consent
        COALESCE(#patient_documents.SA64_CONSENT_FOR_CLINICAL_TREATMENT, 'No') AS [SA64 Consent for Clinical Treatment],
        #patient_documents.SA64_CONSENT_FOR_CLINICAL_TREATMENT_DT AS [SA64 Consent for Clinical Treatment Date],
        DATEDIFF(MONTH, #patient_documents.SA64_CONSENT_FOR_CLINICAL_TREATMENT_DT, #registration.REG_DATE) AS [SA64 Consent for Clinical Treatment Months Old],

        -- Financial Consent
        COALESCE(#patient_documents.SA64_FINANCIAL_CONSENT, 'No') AS [SA64 Financial Consent],
        #patient_documents.SA64_FINANCIAL_CONSENT_DT AS [SA64 Financial Consent Date],
        DATEDIFF(MONTH, #patient_documents.SA64_FINANCIAL_CONSENT_DT, #registration.REG_DATE) AS [SA64 Financial Consent Months Old],

        -- Privacy Notice
        COALESCE(#patient_documents.SA64_PRIVACY_NOTICE, 'No') AS [SA64 Notice of Privacy Practices],
        #patient_documents.SA64_PRIVACY_NOTICE_DT AS [SA64 Notice of Privacy Practices Date],
        DATEDIFF(MONTH, #patient_documents.SA64_PRIVACY_NOTICE_DT, #registration.REG_DATE) AS [SA64 Notice of Privacy Practices Months Old],

       COALESCE(#home_status.MIGRANT_OR_SEASONAL, 'Missing') AS [Migrant/Seasonal],
       COALESCE(#home_status.HOMELESSNESS, 'Missing') AS [Homelessness],
       CAST(#fpl_information.FPL_PCT AS INT) AS [FPL%],
       IIF(#fpl_information.FPL_PCT IS NULL, 'Missing', 'Has FPL') AS [FPL STATUS],
       CONVERT(NVARCHAR(30), appt.APPT_DTTM, 101) AS [Next Appointment Date],
   CASE 
       WHEN flag.PAT_FLAG_TYPE_C = '9800022'
            AND flag.ACTIVE_C = 1 THEN 'Y'
       ELSE 'N'
   END AS [Patient Registration Flag?],
   CONVERT(NVARCHAR(30), flag.LAST_UPDATE_INST, 101) AS [Last Flag Update Date],
   CASE    
       WHEN flag.PAT_FLAG_TYPE_C = '9800022'
            AND flag.ACTIVE_C = 1
            THEN emp.NAME 
       ELSE NULL
   END AS [Registration Flag Applied By]
FROM CLARITY.dbo.IDENTITY_ID_VIEW AS IDENTITY_ID
INNER JOIN CLARITY.dbo.PATIENT_VIEW AS PATIENT ON IDENTITY_ID.PAT_ID = PATIENT.PAT_ID
INNER JOIN CLARITY.dbo.PATIENT_3 AS PATIENT_3 ON IDENTITY_ID.PAT_ID = PATIENT_3.PAT_ID
INNER JOIN CLARITY.dbo.PATIENT_4 AS PATIENT_4 ON IDENTITY_ID.PAT_ID = PATIENT_4.PAT_ID
LEFT JOIN CLARITY.dbo.ZC_ENGLISH_FLUENCY AS ZC_ENGLISH_FLUENCY ON PATIENT_3.ENGLISH_FLUENCY_C = ZC_ENGLISH_FLUENCY.ENGLISH_FLUENCY_C
LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW AS CLARITY_SER ON PATIENT.CUR_PCP_PROV_ID = CLARITY_SER.PROV_ID
LEFT JOIN CLARITY.dbo.ZC_VETERAN_STAT AS ZC_VETERAN_STAT ON PATIENT.VETERAN_STATUS_C = ZC_VETERAN_STAT.VETERAN_STATUS_C
LEFT JOIN CLARITY.dbo.ZC_EMPY_STATUS AS ZC_EMPY_STATUS ON PATIENT.EMPY_STATUS_C = ZC_EMPY_STATUS.EMPY_STATUS_C
LEFT JOIN CLARITY.dbo.ZC_LANGUAGE AS ZC_LANGUAGE ON PATIENT.LANGUAGE_C = ZC_LANGUAGE.LANGUAGE_C
LEFT JOIN #patient_addresses ON IDENTITY_ID.PAT_ID = #patient_addresses.PAT_ID
LEFT JOIN #care_team ON IDENTITY_ID.PAT_ID = #care_team.PAT_ID
LEFT JOIN #comm__phone ON IDENTITY_ID.PAT_ID = #comm__phone.PAT_ID
LEFT JOIN #patient_type__hiv_pos ON IDENTITY_ID.PAT_ID = #patient_type__hiv_pos.PAT_ID
LEFT JOIN #patient_type__hiv_neg ON IDENTITY_ID.PAT_ID = #patient_type__hiv_neg.PAT_ID
LEFT JOIN #patient_documents ON IDENTITY_ID.PAT_ID = #patient_documents.PAT_ID
INNER JOIN #registration ON IDENTITY_ID.PAT_ID = #registration.REG_HX_OPEN_PAT_ID
LEFT JOIN #patient_relationship ON IDENTITY_ID.PAT_ID = #patient_relationship.PAT_ID
LEFT JOIN #home_status ON IDENTITY_ID.PAT_ID = #home_status.PAT_ID
LEFT JOIN #fpl_information ON IDENTITY_ID.PAT_ID = #fpl_information.PAT_ID
INNER JOIN CLARITY.dbo.PAT_ENC_VIEW AS PAT_ENC ON #registration.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID
LEFT JOIN CLARITY.dbo.CLARITY_PRC AS CLARITY_PRC ON PAT_ENC.APPT_PRC_ID = CLARITY_PRC.PRC_ID
LEFT JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS CLARITY_DEP ON PAT_ENC.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
LEFT JOIN Clarity.dbo.PATIENT_FYI_FLAGS_VIEW flag ON IDENTITY_ID.PAT_ID = flag.PATIENT_ID
AND flag.PAT_FLAG_TYPE_C = '9800022'
AND flag.ACTIVE_C = 1
LEFT JOIN clarity.dbo.V_SCHED_APPT_VIEW appt ON appt.PAT_ID = IDENTITY_ID.PAT_ID
AND appt.APPT_DTTM >= CAST(GETDATE() AS DATE)
LEFT JOIN Clarity.dbo.CLARITY_EMP_VIEW emp ON flag.ENTRY_PERSON_ID = emp.USER_ID
WHERE PATIENT_4.PAT_LIVING_STAT_C = 1;

