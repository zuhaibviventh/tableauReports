import os, os.path
import pandas as pd

from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}/htn_bp_poor_control/staging"

htn_bp_transform_logger = logger.setup_logger(
    "htn_bp_transformation_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(f"{STAGING_LOCATION}/STAGING_htn_bp_poor_control.csv") as htn_bp:
            staged_data = pd.read_csv(htn_bp)
    except FileNotFoundError as file_not_found_error:
        htn_bp_transform_logger.error(f"Error finding file: {file_not_found_error}")

    renamed_df = staged_data.rename(columns = {
        "IDENTITY_ID": "MRN",
        "PAT_NAME": "Patient",
        "PROV_NAME": "PCP"
    })

    state_removed_df = renamed_df \
        .loc[:, ~renamed_df.columns.isin(["STATE"])]
    sorted_df = state_removed_df.sort_values(by = "CITY")
    final_df = sorted_df[sorted_df["MET_YN"] == 0]

    all_pcp_list = final_df["PCP"] \
        .drop_duplicates() \
        .tolist()

    for pcp in all_pcp_list:
        pcp_filter = final_df[final_df["PCP"] == pcp]

        with open(f'{STAGING_LOCATION}/HTN Pts with High BP - {pcp}.csv', 'wb') as final_out:
            pcp_filter.to_csv(final_out, index = False)

    htn_bp_transform_logger.debug("Transformations complete.")