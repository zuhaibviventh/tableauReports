import os, os.path, csv
import pandas as pd

from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}\\prep_out_of_care_pats\\staging"
LOC_DRIVE = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\Health\\Quality Action Lists\\PrEP\\Lists By Site")
PCP_DRIVE = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\Health\\Quality Action Lists\\PrEP")

prep_transform_logger = logger.setup_logger("prep_transform_logger",
    f"{directory}/logs/routine_weekly_main.log")

def transform():
    try:
        with open(f"{STAGING_LOCATION}/STAGING_prep_pats.csv") as prep_ooc:
            staged_data = pd.read_csv(prep_ooc)
    except FileNotFoundError as file_not_found_error:
        prep_transform_logger.error(f"Error finding file: {file_not_found_error}")

    staged_data["MRN"] = staged_data["MRN"].astype(str)

    df = staged_data[["Patient_Name",
        "MRN",
        "Ethnicity",
        "Last HIV Lab Date",
        "MR_Contact_DEPT_NAME",
        "Last_Visit",
        "SITE",
        "Last_PrEP_Retention_Selection",
        "CURRENT_PCP_VAME"]]

    df["Last HIV Lab Date"] = pd.to_datetime(df["Last HIV Lab Date"])
    df["Last_Visit"] = pd.to_datetime(df["Last_Visit"])

    df = df.rename(columns = {
        "MR_Contact_DEPT_NAME": "Location",
        "CURRENT_PCP_VAME": "Current PCP",
        "Last_Visit": "Last PCP Visit"
    })

    to_loc_drive(df)
    to_pcp_drive(df)

    prep_transform_logger.debug("Transformations complete.")


def to_loc_drive(df):
    locations = df["Location"] \
        .drop_duplicates() \
        .tolist()

    for location in locations:
        loc_filter = df[df["Location"] == location]

        file_name = f"{LOC_DRIVE}/PrEP Out of Care -{location}.csv"
        with open(file_name, "wb") as out:
            loc_filter.to_csv(out, index = False)


def to_pcp_drive(df):
    all_pcp_list = df["Current PCP"] \
        .drop_duplicates() \
        .tolist()

    for pcp in all_pcp_list:
        pcp_filter = df[df["Current PCP"] == pcp]
        file_name = f"{PCP_DRIVE}/PrEP Out of Care -{pcp}.csv"
        with open(file_name, "wb") as out:
            pcp_filter.to_csv(out, index = False)
