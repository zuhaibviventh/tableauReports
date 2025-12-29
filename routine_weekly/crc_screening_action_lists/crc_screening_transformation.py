import os, os.path
import pandas as pd

from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}/crc_screening_action_lists/staging"

crc_transform_logger = logger.setup_logger(
    "crc_screening_transformation_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(
            f"{STAGING_LOCATION}/STAGING_crc_screening_action_lists.csv"
        ) as crc_screenings:
            staged_data = pd.read_csv(crc_screenings)
    except FileNotFoundError as file_not_found_error:
        crc_transform_logger.error(f"Error finding file: {file_not_found_error}.")

    renamed_df = staged_data.rename(columns={
        "PAT_NAME": "Patient",
        "BIRTH_DATE": "DOB",
        "last_medical_visit": "Last Medical Visit",
        "MeetsCriteria": "Met Reasons",
        "MeetsCriteriaDates": "Met Date",
        "Exclusion_Reasons": "Exclusions",
        "Exclusion_Dates": "Eclusion Dates",
        "Outcome": "Met Y/N",
        "Report_Period_End": "Report Date"
    })

    not_met_filter = renamed_df[renamed_df["Met Y/N"] == 'Not Met']
    remove_groupers = not_met_filter \
        .loc[:, ~not_met_filter.columns.isin(['Grouper'])]

    all_pcp_list = remove_groupers["PCP"] \
        .drop_duplicates() \
        .tolist()

    for pcp in all_pcp_list:
        pcp_filter = remove_groupers[remove_groupers["PCP"] == pcp]

        with open(f'{STAGING_LOCATION}/CRC Screening - {pcp}.csv', 'wb') as final_out:
            pcp_filter.to_csv(final_out, index = False)

    crc_transform_logger.debug("Transformations complete.")