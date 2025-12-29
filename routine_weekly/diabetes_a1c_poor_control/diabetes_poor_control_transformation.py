import os, os.path
import pandas as pd

from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}/diabetes_a1c_poor_control/staging"

diabetes_transform_logger = logger.setup_logger(
    "diabetes_transform_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(
            f"{STAGING_LOCATION}/STAGING_diabetes_a1c_poor_control.csv"
        ) as diabetes_a1c:
            staged_data = pd.read_csv(diabetes_a1c)
    except FileNotFoundError as file_not_found_error:
        diabetes_transform_logger.error(f"Error finding file: {file_not_found_error}")

    poor_a1c = staged_data[staged_data["Poor_A1C_9+"] != "Under 9"]
    city_ascend = poor_a1c.sort_values(by = "CITY")

    all_pcp_list = city_ascend["PCP"] \
        .drop_duplicates() \
        .tolist()

    for pcp in all_pcp_list:
        pcp_filter = city_ascend[city_ascend["PCP"] == pcp]

        with open(f'{STAGING_LOCATION}/A1c Poor Control -{pcp}.csv', 'wb') as final_out:
            pcp_filter.to_csv(final_out, index = False)

    diabetes_transform_logger.debug("Transformations complete.")