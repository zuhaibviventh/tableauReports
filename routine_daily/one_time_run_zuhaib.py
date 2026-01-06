import importlib
import os
import time

from utils import context
from utils import extract_last_word
from utils import logger
from utils import teams_msg

# from clinical_operations.co_medical_ooc_w_hiv import main_ooc_w_hiv
# from clinical_quality.cq_dental_prophy_1 import main_dental_prophy_1
# from clinical_quality.cq_dental_documented_treatment_plan import main_documented_treatment_plan
# from clinical_quality.cq_dental_pmv import main_pmv_maintenance

# from clinical_quality.cq_medical_vls import main_vls
# from general.epic_sti_reporting import main_epic_sti_reporting
# from clinical_operations.co_medicare_annual_wellness_visit import medicare_annual_wellness_visit
# from clinical_operations.co_medical_rncm_prog_eval import main_rncm_prog_eval
# from clinical_operations.co_medical_n_consecutive_no_shows import main_consec_no_shows
# from clinical_quality.cq_medical_lipid_panel import main_lipid_panel
# from clinical_quality.cq_medical_crc_screening import main_crc_screening
# from clinical_operations.co_general_no_shows_excl_sud import main_no_shows_excl_sud
# from social_services.preferred_language import main_preferred_language
# from clinical_operations.co_dental_dentures_proc import main_lower_dentures_proc

directory = context.get_context(os.path.abspath(__file__))
main_logger = logger.setup_logger("routine_daily_main_logger", f"{directory}/routine_daily/logs/main.log")

run_methods_cancer_mapping = {'cq_medical_breast_cancer_screening': 'main_breast_cancer_screening',
                              'cq_medical_diabetes_microalb': 'main_diabetes_microalb', 'cq_bh_gad_7': 'main_gad_7',
                              'cq_medical_cervical_cancer_screening': 'main_cervical_cancer_screening',
                              'cq_bh_phq9_q10': 'main_phq9_q10', 'cq_bh_mdd_cssrs': 'main_mdd_cssrs',
                              'cq_medical_crc_screening': 'main_crc_screening',
                              'cq_medical_retention_in_care': 'main_retention_in_care',
                              'cq_medical_diabetes_a1c': 'main_diabetes_a1c',
                              'cq_bh_active_psych_retention': 'main_active_psych_retention',
                              'cq_bh_depression_measure_5': 'main_depression_measure_5'}

remaning = {'cq_medical_breast_cancer_screening': 'main_breast_cancer_screening',
            'cq_medical_diabetes_microalb': 'main_diabetes_microalb', 'cq_bh_gad_7': 'main_gad_7',
            'cq_medical_cervical_cancer_screening': 'main_cervical_cancer_screening',
            'cq_bh_mdd_cssrs': 'main_mdd_cssrs', 'cq_medical_crc_screening': 'main_crc_screening',
            'cq_medical_retention_in_care': 'main_retention_in_care', 'cq_medical_diabetes_a1c': 'main_diabetes_a1c',
            'cq_bh_active_psych_retention': 'main_active_psych_retention',
            'cq_bh_depression_measure_5': 'main_depression_measure_5'}

run_methods_depression_mapping = {'cq_bh_active_psych_retention': 'main_active_psych_retention',
                                  'cq_bh_depression_measure_5': 'main_depression_measure_5'}

co_lot2 = {'co_dental_active_pats': 'main_dental_active_pats_nm2',
           'co_dental_dentures_proc': 'main_lower_dentures_proc_nm2', 'co_dental_no_open_ep': 'main_no_open_ep_nm2',
           'co_dental_perio_missed_opps': 'main_perio_missed_opps_nm2', 'co_general_fpl_audits': 'main_fpl_audits_nm2',
           'co_general_new_pats_dx': 'main_new_pat_dx_nm2', 'co_general_no_shows_aoda': 'main_no_shows_aoda_nm2',
           'co_general_no_shows_excl_sud': 'main_no_shows_excl_sud_nm2',
           'co_general_no_shows_pats': 'main_no_shows_pats_nm2', 'co_general_open_encs': 'main_new_pat_dx_nm2'}


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
        teams_msg.send(main_logger, message=f"Total Runtime for {report}: {runtime}s", title="One-Time Run")


def main():
    report = "main_phq9"
    start = time.time()
    main_logger.info(f"One-time run for {report} Report.")
    run_reports(co_lot2, 'quality')

    main_logger.info(f"One-time run for {report} Finished.")

    runtime = f"{time.time() - start:.4f}"
    teams_msg.send(main_logger, message=f"Total Runtime for {report}: {runtime}s", title="One-Time Run")


if __name__ == '__main__':
    main()
