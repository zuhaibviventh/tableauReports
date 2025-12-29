SET NOCOUNT ON;
/**
 * Custom timeframe report for Nicole Hamilton
 */
-- [01/12/2023] Timeframe to span from 05/01/2022 - Today
-- [02/07/2023] Only include Ausitn, TX Patients
-- [02/14/2023] Include E-Signed Documents

IF OBJECT_ID( 'tempdb..#documents' ) IS NOT NULL DROP TABLE #documents;
SELECT identity_id.IDENTITY_ID AS MRN,
       patient.PAT_NAME AS PatName,
       CAST(patient.BIRTH_DATE AS DATE) AS BirthDate,
       zdio.NAME AS DocumentType,
       doc_group.NAME AS DocumentSubType,
       CAST(doc_information.DOC_RECV_TIME AS DATE) AS DocumentReceivedDate,
       COALESCE( CAST(doc_information.ESIGN_TIME AS DATE), CAST(doc_information.SCAN_TIME AS DATE)) AS DocumentScannedDate,
       COALESCE( doc_information.IS_ESIGNED_YN, 'N' ) AS IsElectronicallySignedYN,
       CAST(GETDATE() AS DATE) AS Today
INTO #documents
FROM CLARITY.dbo.IDENTITY_ID_VIEW AS identity_id
    INNER JOIN CLARITY.dbo.PATIENT_VIEW AS patient ON identity_id.PAT_ID = patient.PAT_ID
    INNER JOIN CLARITY.dbo.DOC_INFORMATION_VIEW AS doc_information ON patient.PAT_ID = doc_information.DOC_PT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS clarity_dep_esign ON doc_information.ESIGN_DEP_ID = clarity_dep_esign.DEPARTMENT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_DEP_VIEW AS clarity_dep ON doc_information.SCAN_DEP_ID = clarity_dep.DEPARTMENT_ID
    LEFT JOIN CLARITY.dbo.ZC_DOC_INFO_TYPE AS zdio ON zdio.DOC_INFO_TYPE_C = doc_information.DOC_INFO_TYPE_C
    LEFT JOIN CLARITY.dbo.ZC_DOC_GRP AS doc_group ON doc_information.DOC_GRP_C = doc_group.DOC_GRP_C
WHERE (doc_information.ESIGN_TIME >= '2022-05-01' OR doc_information.SCAN_TIME >= '2022-05-01')
      AND (doc_information.IS_SCANNED_YN = 'Y' OR doc_information.IS_ESIGNED_YN = 'Y')
      AND (SUBSTRING( clarity_dep.DEPT_ABBREVIATION, 3, 2 ) = 'TX' OR SUBSTRING( clarity_dep_esign.DEPT_ABBREVIATION, 3, 2 ) = 'TX')
GROUP BY identity_id.IDENTITY_ID,
         patient.PAT_NAME,
         patient.BIRTH_DATE,
         zdio.NAME,
         doc_group.NAME,
         doc_information.DOC_RECV_TIME,
         doc_information.SCAN_TIME,
         doc_information.IS_ESIGNED_YN,
         doc_information.ESIGN_TIME
ORDER BY DocumentScannedDate DESC,
         MRN,
         DocumentType;

/* Only deliver datasets that were scanned within the last 7 days */
SELECT MRN,
       PatName,
       BirthDate,
       DocumentType,
       DocumentSubType,
       DocumentReceivedDate,
       DocumentScannedDate,
       IsElectronicallySignedYN,
       Today
FROM #documents
WHERE DATEDIFF( DAY, DocumentScannedDate, Today ) <= 7
ORDER BY DocumentScannedDate,
         MRN,
         DocumentType;
