import os, os.path
import pandas as pd

from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}/hiv_vls_unsuppressed_action_lists/staging"

hiv_vls_transform_logger = logger.setup_logger(
    "hiv_vls_transformation_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(
            f"{STAGING_LOCATION}/STAGING_hiv_vls_unsuppressed_action_lists.csv"
        ) as hiv_vls:
            staged_data = pd.read_csv(hiv_vls)
    except FileNotFoundError as file_not_found_error:
        hiv_vls_transform_logger.error(f"Error finding file: {file_not_found_error}")

    unsuppressed = staged_data[staged_data["VLS_CATEGORY"] == "UNSUPPRESSED"]
    cohort_df = unsuppressed[["MRN", "PAT_ID", "CITY", "GENDER", "PCP", 
                              "PATIENT", "LAST_VL", "LAST_LAB", "VLS_CATEGORY",
                              "ETHNICITY", "SEX", "RACE", "Next Any Appt", 
                              "Next Appt Prov", "Next PCP Appt", 
                              "Next PCP Appt Prov"]]
    final_df = cohort_df.sort_values(by = "CITY")

    all_pcp_list = final_df["PCP"] \
        .drop_duplicates() \
        .tolist()

    for pcp in all_pcp_list:
        pcp_filter = final_df[final_df["PCP"] == pcp]

        with open(f"{STAGING_LOCATION}/HIV Virally Unsuppressed -{pcp}.csv", "wb") as final_out:
            pcp_filter.to_csv(final_out, index = False)

    hiv_vls_transform_logger.debug("Transformations complete.")