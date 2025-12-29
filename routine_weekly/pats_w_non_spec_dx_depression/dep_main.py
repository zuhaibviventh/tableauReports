import os, os.path, sys, glob, shutil, time

from pats_w_non_spec_dx_depression import dep_extract
from pats_w_non_spec_dx_depression import dep_transformation
from utils import logger
from utils import connections
from utils import context

directory = context.get_context(os.path.abspath(__file__))

logger = logger.setup_logger(
    "pats_w_non_spec_dx_depression_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def run():
    start = time.time()

    logger.info("Running Patients with Non-specific Dx of Depression Extracts.")
    dep_extract.extract()

    logger.info("Running Patients with Non-specific Dx of Depression Transformations.")
    dep_transformation.transform()

    end = time.time()
    logger.info(
        f"Patients w/ non-specifc Dx of Depression Weekly ETL Finished in {end - start:.4f}s."
    )
