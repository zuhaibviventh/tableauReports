import os, os.path
import pandas as pd

from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}/cp_htn/staging"

cp_htn_transform_logger = logger.setup_logger(
    "cp_htn_transformation_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(
            f"{STAGING_LOCATION}/STAGING_cp_htn.csv"
        ) as cp_htn:
            staged_data = pd.read_csv(cp_htn)
    except FileNotFoundError as file_not_found_error:
        cp_htn_transform_logger.error(f"Error finding file: {file_not_found_error}. Exiting.")
        sys.exit(1)

    todays_date = staged_data["TODAY"] \
        .drop_duplicates() \
        .item() \
        .replace("/", "_")

    file_name = f"Clinical Pharmacy HTN Cohort - {todays_date}.csv"

    final_df = staged_data.loc[:, staged_data.columns != "TODAY"]

    with open(f"{STAGING_LOCATION}/{file_name}", "wb") as final_out:
        final_df.to_csv(final_out, index = False)

    cp_htn_transform_logger.debug("Transformation complete.")
