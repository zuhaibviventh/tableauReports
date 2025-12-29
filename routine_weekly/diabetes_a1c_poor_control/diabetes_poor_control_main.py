import os, os.path, sys, glob, shutil, time

from diabetes_a1c_poor_control import diabetes_poor_control_extraction
from diabetes_a1c_poor_control import diabetes_poor_control_transformation
from utils import logger
from utils import connections
from utils import context

directory = context.get_context(os.path.abspath(__file__))

diabetes_main_logger = logger.setup_logger(
    "diabetes_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

SHARED_DRIVE = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\Health\\Quality Action Lists\\A1c Poor Control")

def run():
    start = time.time()

    diabetes_main_logger.info("Running Diabetes A1c Poor Control Extracts.")
    diabetes_poor_control_extraction.extract()

    diabetes_main_logger.info("Running Diabetes A1c Poor Control Transformations.")
    diabetes_poor_control_transformation.transform()

    diabetes_main_logger.info(f"Loading CSVs to {SHARED_DRIVE}.")
    diabetes_glob = glob.glob(
        f"{directory}/diabetes_a1c_poor_control/staging/A1c Poor Control -*.csv"
    )

    for counter, source_file in enumerate(diabetes_glob):
        shutil.copy(source_file, SHARED_DRIVE)
        diabetes_main_logger.debug(f"Loaded {source_file} to {SHARED_DRIVE}")
        os.remove(source_file)
        diabetes_main_logger.debug(f"Unstaged {source_file}.")
        counter += 1

    end = time.time()
    diabetes_main_logger.info(f"{counter} CSV files loaded in {SHARED_DRIVE}")
    diabetes_main_logger.info(
        f"Diabetes A1c Poor Control Weekly ETL Finished in {end - start:.4f}s."
    )
