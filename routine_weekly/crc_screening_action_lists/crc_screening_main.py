import os, os.path, sys, glob, shutil, time

from crc_screening_action_lists import crc_screening_extraction
from crc_screening_action_lists import crc_screening_transformation
from utils import logger
from utils import connections
from utils import context

directory = context.get_context(os.path.abspath(__file__))

crc_main_logger = logger.setup_logger(
    "crc_screening_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

# SHARED_DRIVE = ("\\\\FSS001SVR\\Analysis\\Routines\\weekly\\health\\"
                # "Quality Action Lists\\CRC Screening")
SHARED_DRIVE = "C:\\Users\\talendservice\\Vivent Health\\Share Drives - Health\\Quality Action Lists\\CRC Screening"

def run():
    start = time.time()

    crc_main_logger.info("Running CRC Screening Action Lists Extracts.")
    crc_screening_extraction.extract()

    crc_main_logger.info("Running CRC Screening Action Lists Transformations.")
    crc_screening_transformation.transform()

    crc_main_logger.info(f"Loading CSVs to {SHARED_DRIVE}.")
    crc_glob = glob.glob(
        f"{directory}/crc_screening_action_lists/staging/CRC Screening - *.csv"
    )

    for counter, source_file in enumerate(crc_glob):
        shutil.copy(source_file, SHARED_DRIVE)
        crc_main_logger.debug(f"Loaded {source_file} to {SHARED_DRIVE}")
        os.remove(source_file)
        crc_main_logger.debug(f"Unstaged {source_file}.")
        counter += 1

    end = time.time()
    crc_main_logger.info(f"{counter} CSV files loaded in {SHARED_DRIVE}")
    crc_main_logger.info(
        f"CRC Screening Action Lists Weekly ETL Finished in {end - start:.4f}s."
    )
