import os, os.path, sys, glob, shutil, time

from epic_fds_docs import epic_fds_extraction
from epic_fds_docs import epic_fds_transformation
from utils import logger
from utils import connections
from utils import context

directory = context.get_context(os.path.abspath(__file__))

epic_fds_main_logger = logger.setup_logger(
    "epic_fds_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def run():
    start = time.time()
    epic_fds_main_logger.info("Running EPIC FDS Scanned Signed Docs Extracts.")
    epic_fds_extraction.extract()

    epic_fds_main_logger.info("Running EPIC FDS Scanned Signed Docs Transformations.")
    epic_fds_transformation.transform()

    end = time.time()
    epic_fds_main_logger.info(
        f"EPIC FDS Scanned Signed Docs Weekly ETL Finished in {end - start:.4f}s."
    )
