import os, os.path, sys, glob, shutil, time

from dental_pts_no_open_episodes import extraction
from dental_pts_no_open_episodes import transformation
from utils import logger
from utils import connections
from utils import context
from datetime import date

directory = context.get_context(os.path.abspath(__file__))

pts_main_logger = logger.setup_logger(
    "cp_tr_main_logger_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

# SHARED_DRIVE = ("C:\\Users\\mscoggins\\Vivent Health\\"
                # "Share Drives - Dental\\Dental Quality Action Lists\\"
                # "Patients with no open Episode")
SHARED_DRIVE = "C:\\Users\\talendservice\\OneDrive - Vivent Health\\Dental\\Dental Quality Action Lists\\Patients with no open Episode"

def run():
    start = time.time()
    pts_main_logger.info("Running Dental Patients with no Open Episodes Extracts.")
    extraction.extract()

    pts_main_logger.info("Running Dental Patients with no Open Episodes Transformations.")
    transformation.transform()

    end = time.time()
    pts_main_logger.info(
        f"Dental Patients with no Open Episodes Weekly ETL Finished in {end-start:.4f}s."
    )
