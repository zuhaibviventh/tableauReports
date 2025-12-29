import os, os.path, time, datetime, shutil

from clinical_quality.cq_bh_depression_measure_2 import main_depression_measure_2
from clinical_quality.cq_bh_depression_measure_3 import main_depression_measure_3
from clinical_quality.cq_bh_depression_measure_4 import main_depression_measure_4
from clinical_quality.cq_bh_depression_measure_5 import main_depression_measure_5
from clinical_quality.cq_bh_mdd_cssrs import main_mdd_cssrs
from clinical_quality.cq_bh_aoda_measure import main_aoda_measure
from clinical_quality.cq_bh_gad_7 import main_gad_7
from clinical_quality.cq_bh_mdd_b6 import main_mdd_b6
from clinical_quality.cq_bh_new_pats_improve import main_new_pats_improve
from clinical_quality.cq_bh_phq9_q10 import main_phq9_q10
from clinical_quality.cq_bh_active_psych_retention import main_active_psych_retention
from clinical_quality.cq_bh_sud_pats_remission import main_sud_pats_remission
from clinical_quality.cq_mh_aoda_measure import main_mh_aoda_measure

#########from clinical_quality.cq_risk_register import main_risk_register #moved to Alteryx
from clinical_quality.cq_medical_breast_cancer_screening import main_breast_cancer_screening
from clinical_quality.cq_medical_cervical_cancer_screening import main_cervical_cancer_screening
from clinical_quality.cq_medical_crc_screening import main_crc_screening
from clinical_quality.cq_medical_htn_blood_pressure import main_htn_blood_pressure
from clinical_quality.cq_medical_tobacco_use import main_tobacco_use
from clinical_quality.cq_medical_vls import main_vls
from clinical_quality.cq_medical_diabetes_microalb import main_diabetes_microalb
from clinical_quality.cq_medical_diabetes_a1c import main_diabetes_a1c
from clinical_quality.cq_sdoh import main_sdoh
from clinical_quality.cq_medical_retention_in_care import main_retention_in_care
from clinical_quality.cq_medical_lipid_panel import main_lipid_panel

from clinical_quality.cq_dental_prophy_1 import main_dental_prophy_1
from clinical_quality.cq_dental_prophy_2 import main_dental_prophy_2
from clinical_quality.cq_dental_documented_treatment_plan import main_documented_treatment_plan
from clinical_quality.cq_dental_perio_disease_mgmt import main_perio_disease_mgmt
from clinical_quality.cq_dental_oral_eval import main_oral_eval
from clinical_quality.cq_dental_oral_health_ed import main_oral_health_ed
#from clinical_quality.cq_dental_pmv import main_pmv_maintenance

from clinical_quality.cq_mcm_npo import main_npo
from clinical_quality.cq_mcm_engaged_in_care import main_pats_engaged_in_care
#from clinical_quality.cq_mcm_pats_linked_to_care import main_pats_linked_to_care

from clinical_quality.medical_home_touches import main_medical_home_touches

from clinical_quality.cq_general_quality_measures import main_general_quality
from clinical_quality.cq_medical_vls_qiwp2026 import main_vls_qiwp2026

from utils import (
    logger,
    context,
    emails,
    teams_msg
)

directory = context.get_context(os.path.abspath(__file__))

logger = logger.setup_logger(
    "routine_daily_logger", 
    f"{directory}/clinical_quality/logs/main.log"
)

def run(shared_drive):
    init_message = "Running Clinical Quality Daily ETL Workflow."
    logger.info(init_message)
    teams_msg.send(
        logger,
        message = init_message,
        title = "Clinical Quality - Daily ETL Workflow"
    )

    start = time.time()

    '''Clinical Quality - Medical'''
    #######main_risk_register.run(shared_drive)  # Risk Register ##moved to Alteryx 9/20/2024 since this process is sending bad data.
    main_breast_cancer_screening.run(shared_drive)  # Breast Cancer Screening
    main_cervical_cancer_screening.run(shared_drive)  # Cervical Cancer Screening
    main_crc_screening.run(shared_drive)  # CRC Screening
    main_htn_blood_pressure.run(shared_drive)  # HTN Blood Pressure
    main_tobacco_use.run(shared_drive)  # Tobacco Use
    main_vls.run(shared_drive)  # Viral Load Suppression
    main_vls_qiwp2026.run(shared_drive) # VLS QIWP 2026
    main_diabetes_microalb.run(shared_drive)  # Diabetes and MicroAlb
    main_diabetes_a1c.run(shared_drive)  # Diabetes A1c
    main_sdoh.run(shared_drive)  # SDOH CAP
    main_retention_in_care.run(shared_drive)  # Medical Retention In-Care
    main_lipid_panel.run(shared_drive)  # Lipid Panel Screening
    
    '''Clinical Quality - Behavioral Health'''
    main_depression_measure_2.run(shared_drive)  # Depression Measure 2 - Depressed patients with specific dx
    main_depression_measure_3.run(shared_drive)  # Depression Measure 3 - Patients on antidepressant and/or MH therapy
    main_depression_measure_4.run(shared_drive)  # Depression Measure 4 - VLS for depressed patients
    main_depression_measure_5.run(shared_drive)  # Depression Measure 5 - Depressed Patients with PHQ9 scores
    main_mdd_cssrs.run(shared_drive)  # Patient with MDD Who Are Screened for Suicide Risk
    main_aoda_measure.run(shared_drive)  # AODA Measures - Completing First 4 Visits(Star QI)
    main_gad_7.run(shared_drive)  # GAD-7 Screening for BH Patients
    main_mdd_b6.run(shared_drive)  # Patients with MDD Who Stay on Antidepressants for 90 days
    main_new_pats_improve.run(shared_drive)  # New Patients and Improvement in Symptoms at Six Months
    main_phq9_q10.run(shared_drive)  # PHQ9 Question 10 Outcome Measure
    main_active_psych_retention.run(shared_drive)  # Active Psychiatry Patients Retention
    main_sud_pats_remission.run(shared_drive)  # Discharged Patients With or Without Remission
    main_mh_aoda_measure.run(shared_drive)  # MHT Patients Completing First 4 Visits(Star QI)
    
    '''Clinical Quality - Dental'''
    main_dental_prophy_1.run(shared_drive)  # Dental - 1+ Prophy
    main_dental_prophy_2.run(shared_drive)  # Dental - 2+ Prophy
    main_documented_treatment_plan.run(shared_drive)  # Documented Treatment Plan
    main_perio_disease_mgmt.run(shared_drive)  # Dental Perio Disease Management
    main_oral_eval.run(shared_drive)  # Oral Eval
    main_oral_health_ed.run(shared_drive)  # Oral Health Education
    # main_pmv_maintenance.run(shared_drive) # Dental - Periodontal Management Visit
    
    '''Clinical Quality - Medical Case Management'''
    main_npo.run(shared_drive)  # New Patient Orientation
    main_pats_engaged_in_care.run(shared_drive)  # Patients Engaged in Care
    #main_pats_linked_to_care.run(shared_drive)  # Patients Linked to Care

    '''Social Services'''
    main_medical_home_touches.run(shared_drive)  # Wisconsin Medical Home Touches

    '''Clinical Quality - General'''
    main_general_quality.run(shared_drive)

    runtime = f"{time.time() - start:.4f}"
    message = f"Total run time is {runtime}s."
    logger.info(message)
    teams_msg.send(
        logger, 
        message = message, 
        title = "Clinical Quality - Daily ETL Workflow"
    )
