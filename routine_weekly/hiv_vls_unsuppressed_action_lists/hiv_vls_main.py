import os, os.path, sys, glob, shutil, time

from hiv_vls_unsuppressed_action_lists import hiv_vls_extraction
from hiv_vls_unsuppressed_action_lists import hiv_vls_transformation
from utils import logger
from utils import connections
from utils import context

directory = context.get_context(os.path.abspath(__file__))

hiv_vls_main_logger = logger.setup_logger(
    "hiv_vls_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

#SHARED_DRIVE = ("\\\\FSS001SVR\\Analysis\\Routines\\weekly\\health\\"
                #"Quality Action Lists\\HIV Virally Unsuppressed")

SHARED_DRIVE = "C:\\Users\\talendservice\\Vivent Health\\Share Drives - Health\\Quality Action Lists\\HIV Virally Unsuppressed"

def run():
    start = time.time()

    hiv_vls_main_logger.info("Running HIV VLS Unsuppressed Action Lists Extracts.")
    hiv_vls_extraction.extract()

    hiv_vls_main_logger.info("Running HIV VLS Unsuppressed Action Lists Transformations.")
    hiv_vls_transformation.transform()

    hiv_vls_main_logger.info(f"Loading CSVs to {SHARED_DRIVE}.")
    hiv_vls_glob = glob.glob(
        f"{directory}/hiv_vls_unsuppressed_action_lists/staging/HIV Virally Unsuppressed -*.csv"
    )

    for counter, source_file in enumerate(hiv_vls_glob):
        shutil.copy(source_file, SHARED_DRIVE)
        hiv_vls_main_logger.debug(f"Loaded {source_file} to {SHARED_DRIVE}")
        os.remove(source_file)
        hiv_vls_main_logger.debug(f"Unstaged {source_file}.")
        counter += 1

    end = time.time()
    hiv_vls_main_logger.info(f"{counter} CSV files loaded in {SHARED_DRIVE}")
    hiv_vls_main_logger.info(
        f"HIV VLS Unsuppressed Action Lists Weekly ETL Finished in {end - start:.4f}s."
    )
