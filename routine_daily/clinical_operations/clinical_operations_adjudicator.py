import os, os.path, time, datetime, shutil

from clinical_operations.co_dental_no_open_ep import main_no_open_ep
from clinical_operations.co_dental_dentures_proc import main_lower_dentures_proc
from clinical_operations.co_dental_active_pats import main_dental_active_pats
from clinical_operations.co_dental_perio_missed_opps import main_perio_missed_opps

from clinical_operations.co_medical_mpx_labs import main_mpx_labs
from clinical_operations.co_medical_mpx_dx import main_mpx_dx
from clinical_operations.co_medical_mpx_treatment import main_mpx_treatment
from clinical_operations.co_medical_mpx_vax import main_mpx_vax
from clinical_operations.co_medical_prep_data_quality import prep_data_quality_main
from clinical_operations.co_medical_pcp_empanelment import main_pcp_empanelment
from clinical_operations.co_medical_hiv_pos_neg import hiv_pos_neg_main
from clinical_operations.co_medical_ooc_w_hiv import main_ooc_w_hiv
from clinical_operations.co_medical_prep_pats import main_prep_pats
from clinical_operations.co_medical_cad import main_clin_admin_drugs
from clinical_operations.co_medical_arv_cabenuva import main_arv_cabenuva
from clinical_operations.co_medical_rncm_prog_eval import main_rncm_prog_eval
from clinical_operations.co_medical_n_consecutive_no_shows import main_consec_no_shows

from clinical_operations.co_general_slot_utils import main_slot_utilization
from clinical_operations.co_general_no_shows_excl_sud import main_no_shows_excl_sud
###from clinical_operations.co_general_no_shows_pats import main_no_shows_by_pats --Deleted 7/10/2024
from clinical_operations.co_general_no_shows_aoda import main_no_shows_aoda
from clinical_operations.co_general_new_pats_dx import main_new_pat_dx
from clinical_operations.co_general_visits_pats import main_visits_pats
from clinical_operations.co_general_open_encs import main_open_encs
from clinical_operations.co_general_reg_monitoring import main_reg_monitoring
from clinical_operations.co_general_fpl_audits import main_fpl_audits

from clinical_operations.co_bh_mh_active_patients import main_mh_active_patients
from clinical_operations.co_bh_no_hiv_pl import main_bh_dent_hiv_pl

from clinical_operations.co_cp_act_demos import main_activity_demos
from clinical_operations.co_cp_dm_drug_regimens import main_drug_regimens
from clinical_operations.co_cp_dm_cohort import main_dm_cohort
from clinical_operations.co_cp_htn_cohort import main_htn_cohort
from clinical_operations.co_cp_older_pats_no_a1c import main_pats_no_a1c
from clinical_operations.co_cp_med_review import main_med_review
from clinical_operations.co_cp_touches import main_cp_touches
from clinical_operations.co_cp_hm_imm_alerts import main_hm_immunization_alerts
from clinical_operations.co_cp_hm_imm_alerts import main_hist_hm_immunization_alerts

from clinical_operations.co_pharm_all_meds_capture import main_all_meds_capture
from clinical_operations.co_pharm_arv_capture import main_arv_capture
from clinical_operations.co_pharm_new_psych_med_starts import main_new_psych_med_starts

from clinical_operations.co_mcm_d2c_list import main_d2c_list
from clinical_operations.co_mcm_d2c_cohort import main_d2c_cohort
from clinical_operations.co_mcm_sbirts import main_mcm_sbirts
from clinical_operations.co_mcm_care_team_monitoring import main_care_team_monitoring
#from clinical_operations.co_medicare_annual_wellness_visit import medicare_annual_wellness_visit

from utils import (
    logger,
    context,
    emails,
    teams_msg
)

directory = context.get_context(os.path.abspath(__file__))

logger = logger.setup_logger(
    "routine_daily_logger",
    f"{directory}/clinical_operations/logs/main.log"
)

def run(shared_drive):
    """Run the daily ETL workflow for Clinical Operations.

    This script runs a series of functions to perform daily ETL (Extract, Transform, Load) operations for Clinical Operations.
    The functions are organized into sections for Dental, Medical, General, Behavioral Health, Clinical Pharmacy, Pharmacy, 
    and Medical Case Management.

    Functions:
    - Each function corresponds to a specific ETL operation, such as extracting data for dental patients with no open 
    episodes or updating medication regimens for diabetes patients.
    - The functions are imported from separate modules located in the 'clinical_operations' and 'utils' directories.

    Parameters:
    - shared_drive (str): The path to the shared drive where data will be processed and stored.

    Returns:
    - None

    Note:
    - The script uses the 'logger', 'context', 'emails', and 'teams_msg' utilities from the 'utils' module to log messages, 
    manage context, send emails, and send Teams messages, respectively.
    - The 'logger' utility is configured to log messages to a file located in the 'clinical_operations/logs' directory.
    """
    init_message = "Running Clinical Operations Daily ETL Workflow."
    logger.info(init_message)
    teams_msg.send(
        logger,
        message = init_message,
        title = "Clinical Operations - Daily ETL Workflow"
    )

    start = time.time()
    '''Clinical Operations - Medical --Run first due to rerun requirement'''
    main_ooc_w_hiv.run(shared_drive)  # Out of Care Patients with HIV

    '''Clinical Operations - Dental'''
    main_no_open_ep.run(shared_drive)  # Dental Patients with no Open Episodes
    main_lower_dentures_proc.run(shared_drive)  # Lower Dentures Procedures
    main_dental_active_pats.run(shared_drive)  # Active Patients with Last Visit
    main_perio_missed_opps.run(shared_drive)  # Perio Coding Missed Opportunities
    
    '''Clinical Operations - Medical'''
    main_mpx_labs.run(shared_drive)  # MPX Labs
    main_mpx_dx.run(shared_drive)  # MPX Diagnoses
    main_mpx_treatment.run(shared_drive)  # MPX Treatment/Scripts
    main_mpx_vax.run(shared_drive)  # MPX Vaccination
    prep_data_quality_main.run(shared_drive)  # PrEP Data Quality
    main_pcp_empanelment.run(shared_drive)  # PCP Empanelment
    hiv_pos_neg_main.run(shared_drive)  # Medical Patients not Indicated as HIV- or HIV+
    main_ooc_w_hiv.run(shared_drive)  # Out of Care Patients with HIV
    main_prep_pats.run(shared_drive)  # PrEP Patients with Last Visit
    main_clin_admin_drugs.run(shared_drive)  # Clinic Administered Drugs
    main_arv_cabenuva.run(shared_drive)  # Injectible ARV Cabenuva
    main_rncm_prog_eval.run(shared_drive)  # RN Case Manager Program Evaluation
    main_consec_no_shows.run(shared_drive)  # N Consecutive No Shows 7/10/2024 Renamed dashbaord to "Consecutive No Shows in Health Services"
    #medicare_annual_wellness_visit.run(shared_drive) #Medicare Annual Wellness Visits TRacker (In Alteryx)
    
    '''Clinical Operations - General'''
    main_slot_utilization.run(shared_drive)  # Appt Availability and Slot Utilization
    main_no_shows_excl_sud.run(shared_drive)  # No Shows Excluding SUD
    #########main_no_shows_by_pats.run(shared_drive)  # No Shows by Patients --Deleted 7/10/2024
    main_no_shows_aoda.run(shared_drive)  # AODA - No Shows
    main_new_pat_dx.run(shared_drive)  # New Patients Visits Scheduled
    main_visits_pats.run(shared_drive)  # Visits and Patients
    main_open_encs.run(shared_drive)  # Open Encounters
    main_reg_monitoring.run(shared_drive)  # Registration Monitoring (PSRs)
    main_fpl_audits.run(shared_drive)  # FPL Data Quality Audit for Patient with HIV
    
    '''Clinical Operations - Behavioral Health'''
    main_mh_active_patients.run(shared_drive)  # MH Active Patients
    main_bh_dent_hiv_pl.run(shared_drive)  # BH and Dental Patients Without HIV on Their Problem List
    
    '''Clinical Operations - Clinical Pharmacy'''
    main_activity_demos.run(shared_drive)  # Activity and Demographics
    main_drug_regimens.run(shared_drive)  # DM Drug Regimens
    main_dm_cohort.run(shared_drive)  # DM Cohort
    main_htn_cohort.run(shared_drive)  # HTN Cohort
    main_pats_no_a1c.run(shared_drive)  # Patients 36+ without an A1c
    main_med_review.run(shared_drive)  # Medications Review
    main_cp_touches.run(shared_drive)  # Touch Report
    main_hm_immunization_alerts.run(shared_drive)  # HM Immunization Alerts
    main_hist_hm_immunization_alerts.run(shared_drive)  # Historical HM Immunization Alerts
    
    '''Pharmacy'''
    main_all_meds_capture.run(shared_drive)  # All Meds Capture
    main_arv_capture.run(shared_drive)  # ARV Capture
    main_new_psych_med_starts.run(shared_drive)  # New Psych Med Starts
    
    '''Medical Case Management'''
    main_d2c_list.run(shared_drive)  # Data-to-Care List
    main_d2c_cohort.run(shared_drive)  # Data-to-Care Cohort
    main_mcm_sbirts.run(shared_drive)  # SBIRTs
    main_care_team_monitoring.run(shared_drive)  # Care Team Monitoring

    runtime = f"{time.time() - start:.4f}"
    message = f"Total run time is {runtime}s."
    logger.info(message)
    teams_msg.send(
        logger,
        message = message,
        title = "Clinical Operations - Daily ETL Workflow"
    )
