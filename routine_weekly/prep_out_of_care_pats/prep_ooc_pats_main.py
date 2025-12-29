import os, os.path, sys, glob, shutil, time

from prep_out_of_care_pats import prep_extraction
from prep_out_of_care_pats import prep_transformation
from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

prep_ooc_main_logger = logger.setup_logger(
    "prep_ooc_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def run():
    start = time.time()

    prep_ooc_main_logger.info("Running PrEP Out of Care Patients Extracts.")
    prep_extraction.extract()

    prep_ooc_main_logger.info("Running PrEP Out of Care Patients Transformations.")
    prep_transformation.transform()

    end = time.time()
    prep_ooc_main_logger.info(
        f"PrEP Out of Care Patients Weekly ETL Finished in {end - start:.4f}s."
    )
