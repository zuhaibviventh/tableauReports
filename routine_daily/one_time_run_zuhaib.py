import os
import sys
import time
from utils import logger
from utils import context
from utils import teams_msg
from utils import extract_last_word
import importlib
#from clinical_operations.co_medical_ooc_w_hiv import main_ooc_w_hiv
from clinical_quality.cq_dental_oral_eval import main_oral_eval
# from clinical_quality.cq_dental_prophy_1 import main_dental_prophy_1
# from clinical_quality.cq_dental_documented_treatment_plan import main_documented_treatment_plan
from clinical_quality.cq_bh_new_pats_improve import main_new_pats_improve
# from clinical_quality.cq_dental_pmv import main_pmv_maintenance

#from clinical_quality.cq_medical_vls import main_vls
#from general.epic_sti_reporting import main_epic_sti_reporting
#from clinical_operations.co_medicare_annual_wellness_visit import medicare_annual_wellness_visit
#from clinical_operations.co_medical_rncm_prog_eval import main_rncm_prog_eval
# from clinical_operations.co_medical_n_consecutive_no_shows import main_consec_no_shows
from social_services.prevention_supply_distribution import main_supply_distribution
from social_services.preferred_language import main_preferred_language
from social_services.prevention_navigation import main_prev_nav
from clinical_operations.co_general_reg_monitoring import main_reg_monitoring
# from clinical_quality.cq_medical_lipid_panel import main_lipid_panel
# from clinical_quality.cq_medical_crc_screening import main_crc_screening
# from clinical_operations.co_general_no_shows_excl_sud import main_no_shows_excl_sud
# from social_services.preferred_language import main_preferred_language
from clinical_quality.cq_bh_gad_7 import main_gad_7
from clinical_quality.cq_mh_aoda_measure import main_mh_aoda_measure
from clinical_quality.cq_medical_diabetes_microalb import main_diabetes_microalb 
# from clinical_operations.co_dental_dentures_proc import main_lower_dentures_proc
from clinical_quality.cq_bh_phq9_q10 import main_phq9_q10
from clinical_quality.cq_medical_breast_cancer_screening import main_breast_cancer_screening
from clinical_quality.cq_medical_cervical_cancer_screening import main_cervical_cancer_screening
from clinical_quality.cq_medical_crc_screening import main_crc_screening
from clinical_quality.cq_medical_diabetes_a1c import main_diabetes_a1c
from clinical_quality.cq_bh_mdd_cssrs import main_mdd_cssrs
from clinical_quality.cq_bh_depression_measure_4 import main_depression_measure_4
from clinical_quality.cq_bh_active_psych_retention import main_active_psych_retention
from clinical_quality.cq_bh_depression_measure_5 import main_depression_measure_5
from clinical_quality.cq_medical_retention_in_care import main_retention_in_care
from clinical_operations.co_medical_prep_pats import main_prep_pats

directory = context.get_context(os.path.abspath(__file__))
main_logger = logger.setup_logger("routine_daily_main_logger", f"{directory}/routine_daily/logs/main.log")


run_methods_cancer_mapping = {'cq_medical_breast_cancer_screening':'main_breast_cancer_screening', 'cq_medical_diabetes_microalb':'main_diabetes_microalb','cq_bh_gad_7':'main_gad_7',
                          'cq_medical_cervical_cancer_screening':'main_cervical_cancer_screening','cq_bh_phq9_q10':'main_phq9_q10','cq_bh_mdd_cssrs':'main_mdd_cssrs',
                          'cq_medical_crc_screening':'main_crc_screening','cq_medical_retention_in_care':'main_retention_in_care',
                          'cq_medical_diabetes_a1c':'main_diabetes_a1c', 'cq_bh_active_psych_retention':'main_active_psych_retention', 
                                      'cq_bh_depression_measure_5': 'main_depression_measure_5'}

remaning = {'cq_medical_breast_cancer_screening':'main_breast_cancer_screening', 'cq_medical_diabetes_microalb':'main_diabetes_microalb','cq_bh_gad_7':'main_gad_7',
                          'cq_medical_cervical_cancer_screening':'main_cervical_cancer_screening','cq_bh_mdd_cssrs':'main_mdd_cssrs',
                          'cq_medical_crc_screening':'main_crc_screening','cq_medical_retention_in_care':'main_retention_in_care',
                          'cq_medical_diabetes_a1c':'main_diabetes_a1c', 'cq_bh_active_psych_retention':'main_active_psych_retention', 
                                      'cq_bh_depression_measure_5': 'main_depression_measure_5'}

run_methods_depression_mapping = {'cq_bh_active_psych_retention':'main_active_psych_retention', 
                                      'cq_bh_depression_measure_5': 'main_depression_measure_5'}

def run_reports(run_methods_mapping: dict, job_type: str):
    operations_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/clinical_operations/")
    quality_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/clinical_quality/")
    pe_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/provide_enterprise")

    if job_type == 'operations':
        shared_drive = 'operations_shared_drive'
        import_path = 'clinical_operations'
    elif job_type == 'quality': 
        shared_drive = 'quality_shared_drive'
        import_path = 'clinical_quality'
    else:
        raise Exception

    for pkg, report in run_methods_mapping.items():
        start = time.time()
        main_logger.info(f"One-time run for {report} Report.")

        main_logger.info(f"One-time run for {report} Finished.")

        module_name = import_path + '.' + pkg + '.' + report

        mod = importlib.import_module(module_name)

        mod.run(shared_drive)

        runtime = f"{time.time() - start:.4f}"
        teams_msg.send(main_logger,
                    message=f"Total Runtime for {report}: {runtime}s",
                    title="One-Time Run")



def main():
    operations_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/clinical_operations/")
    quality_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/clinical_quality/")
    pe_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/provide_enterprise")
    # if extract_last_word.extract_last_word(sys.prefix) == "venv":
    report = "main_phq9"
    start = time.time()
    main_logger.info(f"One-time run for {report} Report.")
    #medicare_annual_wellness_visit.run(shared_drive)
    #main_ooc_w_hiv.run(operations_shared_drive)  # Out of Care Patients with HIV  
    #main_mh_aoda_measure.run(quality_shared_drive)
    #main_retention_in_care.run(quality_shared_drive)
    #main_new_pats_improve.run(quality_shared_drive)
    #main_active_psych_retention.run(quality_shared_drive)
    #main_diabetes_microalb.run(quality_shared_drive)
    #main_preferred_language.run(operations_shared_drive)
    #main_prep_pats.run(operations_shared_drive)
    #main_new_pats_improve.run(quality_shared_drive)
    #main_lipid_panel.run(quality_shared_drive)
    #main_reg_monitoring.run(operations_shared_drive)
    #main_mdd_cssrs.run(quality_shared_drive)
    #main_depression_measure_4.run(quality_shared_drive)
    #main_gad_7.run(quality_shared_drive)
    #main_supply_distribution.run(pe_shared_drive)
    #main_prev_nav.run(pe_shared_drive)
    run_reports(remaning, 'quality')
    #main_mdd_cssrs.run(quality_shared_drive)
    #main_breast_cancer_screening.run(quality_shared_drive)
    #main_diabetes_microalb.run(quality_shared_drive)
    #main_depression_measure_5.run(quality_shared_drive)
    #main_diabetes_a1c.run(quality_shared_drive)


    main_logger.info(f"One-time run for {report} Finished.")

    runtime = f"{time.time() - start:.4f}"
    teams_msg.send(main_logger,
                    message=f"Total Runtime for {report}: {runtime}s",
                    title="One-Time Run")
    # else:
    #     sys.exit("Virtual environment (venv) not active. Exiting...")
    #     sys.exit(1)
    #     sys.exit(1)

if __name__ == '__main__':
    main()
