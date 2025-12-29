import os, os.path, sys, glob, shutil, time

from dental_bh_pats_no_hiv_dx import extract
from dental_bh_pats_no_hiv_dx import transform
from utils import logger
from utils import connections
from utils import context
from datetime import date

directory = context.get_context(os.path.abspath(__file__))

pts_main_logger = logger.setup_logger(
    "dental_bh_pats_no_hiv_dx_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def run():
    start = time.time()
    pts_main_logger.info("Running Dental and BH Patients without HIV Dx Extracts.")
    extract.extract()

    pts_main_logger.info("Running Dental and BH Patients without HIV Dx Transformations.")
    transform.transform()

    end = time.time()
    pts_main_logger.info(
        f"Dental and BH Patients without HIV Dx Weekly ETL Finished in {end-start:.4f}s."
    )
