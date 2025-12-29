import os, os.path
import pandas as pd

from utils import logger
from utils import context

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}/pats_w_non_spec_dx_depression/staging"
MEDICAL_DRIVE = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\Health\\Quality Action Lists\\Non-specific Diagnosis of Depression - Medical Only")
BH_DRIVE = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\BH & Depression Quality Lists\\Non-specific Diagnosis of Depression")

dep_transform_logger = logger.setup_logger(
    "dep_transform_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(f"{STAGING_LOCATION}/STAGED_DATA.csv") as dep:
            staged_data = pd.read_csv(dep)
    except FileNotFoundError as file_not_found_error:
        dep_transform_logger.error(f"Error finding file: {file_not_found_error}")

    df = staged_data[staged_data["MET_YN"] == 0]
    medical = df[df["indicator"] == "Medical"]
    psych = df[df["indicator"] == "Psych"]
    mh = df[df["indicator"] == "MH"]

    medical = medical[["IDENTITY_ID", 
        "PAT_NAME",
        "PCP",
        "STATE",
        "CITY",
        "PSYCHIATRY",
        "MH_TEAM_MEMBER"]]
    to_medical(medical)

    psych = psych[["IDENTITY_ID", 
        "PAT_NAME",
        "PCP",
        "STATE",
        "CITY",
        "PSYCHIATRY"]]
    to_psych(psych)

    mh = mh[["IDENTITY_ID", 
        "PAT_NAME",
        "PCP",
        "STATE",
        "CITY",
        "MH_TEAM_MEMBER"]]
    to_mh(mh)


def to_medical(df):
    medical_pcps = df["PCP"] \
        .drop_duplicates() \
        .tolist()

    if df.shape[0] != 0:
        for counter, pcp in enumerate(medical_pcps):
            pcp_filter = df[df["PCP"] == pcp]

            with open(f"{MEDICAL_DRIVE}\\Medical Patients with a Non-specific Dx of Depression -{pcp}.csv", "wb") as final:
                pcp_filter.to_csv(final, index=False)
            counter += 1
            dep_transform_logger.debug(f"Unstaged {final.name}")
        dep_transform_logger.info(f"{counter} MEDICAL CSV files loaded in {MEDICAL_DRIVE}.")
    else:
        dep_transform_logger.info(f"No CSV files were loaded in {MEDICAL_DRIVE}.")


def to_psych(df):
    psych_pcps = df["PCP"] \
        .drop_duplicates() \
        .tolist()

    if df.shape[0] != 0:    
        for counter, pcp in enumerate(psych_pcps):
            pcp_filter = df[df["PCP"] == pcp]

            with open(f"{BH_DRIVE}\\BH Patients with a Non-specific Dx of Depression -{pcp}.csv", "wb") as final:
                pcp_filter.to_csv(final, index=False)
            counter += 1
            dep_transform_logger.debug(f"Unstaged {final.name}")
        dep_transform_logger.info(f"{counter} PSYCHIATRY CSV files loaded in {BH_DRIVE}.")
    else:
        dep_transform_logger.info(f"No PSYCHIATRY CSV files were loaded in {BH_DRIVE}.")


def to_mh(df):
    mh_pcps = df["PCP"] \
        .drop_duplicates() \
        .tolist()

    if df.shape[0] != 0:
        for counter, pcp in enumerate(mh_pcps):
            pcp_filter = df[df["PCP"] == pcp]

            with open(f"{BH_DRIVE}\\BH Patients with a Non-specific Dx of Depression -{pcp}.csv", "wb") as final:
                pcp_filter.to_csv(final, index=False)
            counter += 1
            dep_transform_logger.debug(f"Unstaged {final.name}")
        dep_transform_logger.info(f"{counter} MH CSV files loaded in {BH_DRIVE}.")
    else:
        dep_transform_logger.info(f"No MH CSV files were loaded in {BH_DRIVE}.")