import os, os.path, sys, glob, shutil, time

from htn_bp_poor_control import htn_bp_extraction
from htn_bp_poor_control import htn_bp_transformation
from utils import logger
from utils import connections
from utils import context

directory = context.get_context(os.path.abspath(__file__))

htn_bp_main_logger = logger.setup_logger(
    "htn_bp_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

#SHARED_DRIVE = ("\\\\FSS001SVR\\Analysis\\Routines\\weekly\\health\\"
                #"Quality Action Lists\\HTN BP Poor Control")
SHARED_DRIVE = "C:\\Users\\talendservice\\Vivent Health\\Share Drives - Health\\Quality Action Lists\\HTN BP Poor Control"

def run():
    start = time.time()

    htn_bp_main_logger.info("Running HTN BP Poor Control Extracts.")
    htn_bp_extraction.extract()

    htn_bp_main_logger.info("Running HTN BP Poor Control Transformations.")
    htn_bp_transformation.transform()

    htn_bp_main_logger.info(f"Loading CSVs to {SHARED_DRIVE}")
    crc_glob = glob.glob(
        f"{directory}/htn_bp_poor_control/staging/HTN Pts with High BP - *.csv"
    )

    for counter, source_file in enumerate(crc_glob):
        shutil.copy(source_file, SHARED_DRIVE)
        htn_bp_main_logger.debug(f"Loaded {source_file} to {SHARED_DRIVE}")
        os.remove(source_file)
        htn_bp_main_logger.debug(f"Unstaged {source_file}.")
        counter += 1

    end = time.time()
    htn_bp_main_logger.info(f"{counter} CSV files loaded in {SHARED_DRIVE}")
    htn_bp_main_logger.info(
        f"HTN BP Poor Control Weekly ETL Finished in {end - start:.4f}s."
    )
