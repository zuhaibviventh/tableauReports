/*

**********************************************************************************************

 *****   GENERAL INFO   *****

 Object Name:	PrEP Data Quality
 Create Date:	2/27/2023
 Created By:	ViventHealth\MScoggins
 System:		ANL-MKE-SVR-100
 Requested By:	

 Purpose:		

 Description:
 
  BOE Folder Path: SA64 > 


 *****  Modification History *****

 Change Date:		Changed By:			Change Description:
 ------------		-------------		---------------------------------------------------


**********************************************************************************************

 */

SET NOCOUNT ON;

SET ANSI_WARNINGS OFF;

SELECT TOP 1000000 p.PAT_ID,
                   'Y' PrEP_Flagged,
                   p.CUR_PCP_PROV_ID,
                   p.PAT_NAME
INTO #pop
FROM CLARITY.dbo.PATIENT_VIEW p
WHERE p.PAT_ID IN ( SELECT DISTINCT f.PATIENT_ID
                    FROM CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW f
                    WHERE f.PAT_FLAG_TYPE_C = '640005'
                          AND f.ACTIVE_C = 1 )
UNION
SELECT TOP 1000000 p.PAT_ID,
                   'N' PrEP_Flagged,
                   p.CUR_PCP_PROV_ID,
                   p.PAT_NAME
FROM CLARITY.dbo.PATIENT_VIEW p
WHERE p.PAT_ID NOT IN ( SELECT DISTINCT f.PATIENT_ID FROM CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW f WHERE f.PAT_FLAG_TYPE_C = '640005' )
      AND p.PAT_ID IN ( SELECT pev.PAT_ID
                        FROM CLARITY.dbo.PAT_ENC_VIEW pev
                            INNER JOIN CLARITY.dbo.CLARITY_PRC prc ON pev.APPT_PRC_ID = prc.PRC_ID
                        WHERE pev.APPT_STATUS_C IN ( 2, 6 )
                              AND pev.APPT_PRC_ID IN ( '345', '346', '428', '505', '506', '549', '558', '559' ) --New, f/u, lab, Imm, Walk-in, init, Nav f/u, nav init
                              AND pev.CONTACT_DATE > DATEADD(MONTH, -48, GETDATE()) --Just visits over the past four years
);

SELECT TOP 1000000 pop.PAT_ID,
                   pop.PrEP_Flagged,
                   pev.CONTACT_DATE,
                   zas.NAME APPT_STATUS,
                   prc.PRC_NAME VISIT_TYPE,
                   ser.PROV_NAME VISIT_PROVIDER,
                   dep.DEPARTMENT_NAME,
                   pev.PAT_ENC_CSN_ID,
                   id.IDENTITY_ID MRN,
                   serpcp.PROV_NAME PCP,
                   pop.PAT_NAME
INTO #visits
FROM #pop pop
    LEFT JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = pop.PAT_ID
                                              AND pev.CONTACT_DATE > DATEADD(MONTH, -48, GETDATE())
                                              AND pev.APPT_STATUS_C IS NOT NULL
    LEFT JOIN CLARITY.dbo.ZC_APPT_STATUS zas ON zas.APPT_STATUS_C = pev.APPT_STATUS_C
    LEFT JOIN CLARITY.dbo.CLARITY_PRC prc ON pev.APPT_PRC_ID = prc.PRC_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    LEFT JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON pop.PAT_ID = id.PAT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW serpcp ON pop.CUR_PCP_PROV_ID = serpcp.PROV_ID
WHERE pop.PrEP_Flagged = 'Y'
UNION
SELECT TOP 1000000 pop.PAT_ID,
                   pop.PrEP_Flagged,
                   pev.CONTACT_DATE,
                   zas.NAME APPT_STATUS,
                   prc.PRC_NAME VISIT_TYPE,
                   ser.PROV_NAME VISIT_PROVIDER,
                   dep.DEPARTMENT_NAME,
                   pev.PAT_ENC_CSN_ID,
                   id.IDENTITY_ID MRN,
                   serpcp.PROV_NAME PCP,
                   pop.PAT_NAME
FROM #pop pop
    INNER JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON pev.PAT_ID = pop.PAT_ID
    LEFT JOIN CLARITY.dbo.ZC_APPT_STATUS zas ON zas.APPT_STATUS_C = pev.APPT_STATUS_C
    LEFT JOIN CLARITY.dbo.CLARITY_PRC prc ON pev.APPT_PRC_ID = prc.PRC_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW ser ON pev.VISIT_PROV_ID = ser.PROV_ID
    LEFT JOIN CLARITY.dbo.CLARITY_DEP_VIEW dep ON dep.DEPARTMENT_ID = pev.DEPARTMENT_ID
    INNER JOIN CLARITY.dbo.IDENTITY_ID_VIEW id ON pop.PAT_ID = id.PAT_ID
    LEFT JOIN CLARITY.dbo.CLARITY_SER_VIEW serpcp ON pop.CUR_PCP_PROV_ID = serpcp.PROV_ID
WHERE pop.PrEP_Flagged = 'N'
      AND pev.APPT_PRC_ID IN ( '345', '346', '428', '505', '506', '549', '558', '559' ) --New, f/u, lab, Imm, Walk-in, init, Nav f/u, nav init
      AND pev.CONTACT_DATE > DATEADD(MONTH, -48, GETDATE())
      AND pev.APPT_STATUS_C IS NOT NULL
      AND pev.APPT_STATUS_C IN ( 1, 2, 6 );

SELECT TOP 1000000 v.PAT_ID,
                   v.PAT_NAME,
                   v.PrEP_Flagged,
                   v.CONTACT_DATE,
                   v.APPT_STATUS,
                   v.VISIT_TYPE,
                   v.VISIT_PROVIDER,
                   v.DEPARTMENT_NAME,
                   v.PAT_ENC_CSN_ID,
                   v.MRN,
                   v.PCP,
                   CASE WHEN v.PrEP_Flagged = 'Y'
                             AND pev.APPT_PRC_ID NOT IN ( '151', '733', '56', '219', '346', '506', '428', '345', '505', '120' ) THEN v.VISIT_TYPE
                       ELSE NULL
                   END AS BAD_VISIT_TYPES,
                   pev.APPT_PRC_ID,
                   emp.NAME APPT_CREATOR
INTO #last
FROM #visits v
    LEFT JOIN CLARITY.dbo.PAT_ENC_VIEW pev ON pev.PAT_ENC_CSN_ID = v.PAT_ENC_CSN_ID
    LEFT JOIN CLARITY.dbo.CLARITY_EMP_VIEW emp ON pev.APPT_ENTRY_USER_ID = emp.USER_ID;

SELECT TOP 1000000 l.PAT_ID,
                   l.PAT_NAME,
                   l.PrEP_Flagged,
                   CAST(l.CONTACT_DATE AS DATE) CONTACT_DATE,
                   l.APPT_STATUS,
                   l.VISIT_TYPE,
                   l.VISIT_PROVIDER,
                   l.DEPARTMENT_NAME,
                   l.PAT_ENC_CSN_ID,
                   l.MRN,
                   l.PCP,
                   l.BAD_VISIT_TYPES,
                   l.APPT_PRC_ID,
                   l.APPT_CREATOR,
                   MAX(CASE WHEN f2.PATIENT_ID IS NOT NULL THEN 'Y' ELSE 'N' END) AS CP_PrEP_COHORT
FROM #last l
    LEFT JOIN CLARITY.dbo.PATIENT_FYI_FLAGS_VIEW f2 ON l.PAT_ID = f2.PATIENT_ID
                                                       AND f2.PAT_FLAG_TYPE_C = '640027'
                                                       AND f2.ACTIVE_C = 1
GROUP BY l.PAT_ID,
         l.PAT_NAME,
         l.PrEP_Flagged,
         l.CONTACT_DATE,
         l.APPT_STATUS,
         l.VISIT_TYPE,
         l.VISIT_PROVIDER,
         l.DEPARTMENT_NAME,
         l.PAT_ENC_CSN_ID,
         l.MRN,
         l.PCP,
         l.BAD_VISIT_TYPES,
         l.APPT_PRC_ID,
         l.APPT_CREATOR;

DROP TABLE #pop;
DROP TABLE #visits;
DROP TABLE #last;