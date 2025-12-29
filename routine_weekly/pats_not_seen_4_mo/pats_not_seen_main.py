import os, os.path, sys, glob, shutil, time

from pats_not_seen_4_mo import extraction
from pats_not_seen_4_mo import transformation
from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

pats_not_seen_main_logger = logger.setup_logger(
    "pats_not_seen_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

TABLEAU_DATA_SOURCE_FILE = "C:\\Users\\talendservice\\Vivent Health\\Share Drives - Health\\Health Informatics and Technology\\Project Management\\Routines\\weekly\\Pats not seen in 4+ months"

def run():
    start = time.time()

    pats_not_seen_main_logger.info("Running Patients Not Seen in 4+ Months Extracts.")
    extraction.extract()

    pats_not_seen_main_logger.info("Running Patients Not Seen in 4+ Months Transformations.")
    transformation.transform()

    # Move Excel file to Tableau Data Source folder
    excel_file = f"{directory}/pats_not_seen_4_mo/staging/Virally unsuppressed patients for psr outreach.xlsx"
    shutil.copy(excel_file, TABLEAU_DATA_SOURCE_FILE)
    os.remove(excel_file)

    end = time.time()
    pats_not_seen_main_logger.info(
        f"Patients Not Seen in 4+ Months Weekly ETL Finished in {end - start:.4f}s."
    )
