import os, os.path, sys, glob, shutil, time

from cp_dm_cohort import cp_dm_cohort_extraction
from cp_dm_cohort import cp_dm_cohort_transformation
from utils import logger
from utils import connections
from utils import context
from datetime import date

directory = context.get_context(os.path.abspath(__file__))

cp_main_logger = logger.setup_logger(
    "cp_main_logger_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

SHARED_DRIVE = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\CP Cohorts\\Weekly Lists from Mitch")

today = date.today().strftime('%m_%d_%Y')

def run():
    start = time.time()

    cp_main_logger.info("Running CP DM Cohort Extracts.")
    cp_dm_cohort_extraction.extract()

    cp_main_logger.info("Running CP DM Cohort Transformations.")
    cp_dm_cohort_transformation.transform()

    cp_main_logger.info(f"Loading CSVs to {SHARED_DRIVE}.")
    cp_dm_glob = glob.glob(
        f"{directory}/cp_dm_cohort/staging/Clinical Pharmacy DM Cohort - {today}.csv"
    )

    for counter, source_file in enumerate(cp_dm_glob):
        shutil.copy(source_file, SHARED_DRIVE)
        cp_main_logger.debug(f"Loaded {source_file} to {SHARED_DRIVE}")
        os.remove(source_file)
        cp_main_logger.debug(f"Unstaged {source_file}.")
        counter += 1

    end = time.time()
    cp_main_logger.info(f"{counter} CSV files loaded in {SHARED_DRIVE}")
    cp_main_logger.info(
        f"Clinical Pharmacy DM Cohort Weekly ETL Finished in {end - start:.4f}s."
    )
