import os, os.path
import pandas as pd

from utils import logger
from utils import context
from utils import emails

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}\\dental_bh_pats_no_hiv_dx\\staging"
DENTAL_LOCATION = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\Dental\\Dental Quality Action Lists\\Patients with no HIV Dx")
BH_LOCATION = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\BH & Depression Quality Lists\\BH Patients with no HIV Dx")

dental_patients_csv = f"{STAGING_LOCATION}\\dental_patients.csv"
bh_patients_csv = f"{STAGING_LOCATION}\\bh_patients.csv"

dental_bh_pats_transform_logger = logger.setup_logger(
    "dental_bh_pats_transform_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(f"{dental_patients_csv}", "rb") as dental_pats:
            dental_df = pd.read_csv(dental_pats)

            if len(dental_df) > 0 or not dental_df.empty:
                dental_transformation(dental_df)
            else:
                dental_bh_pats_transform_logger.info(f"{dental_patients_csv} have no data")
    except FileNotFoundError as file_not_found_error:
        dental_bh_pats_transform_logger.error(f"Error finding file: {file_not_found_error}")
        
    try:
        with open(f"{bh_patients_csv}", "rb") as bh_pats:
            bh_df = pd.read_csv(bh_pats)

            if len(bh_df) > 0 or not bh_df.empty:
                bh_transformation(bh_df)
            else:
                dental_bh_pats_transform_logger.info(f"{bh_patients_csv} have no data")
    except FileNotFoundError as file_not_found_error:
        dental_bh_pats_transform_logger.error(f"Error finding file: {file_not_found_error}")


    dental_bh_pats_transform_logger.debug("Transformation complete.")


def dental_transformation(df):
    states = df["STATE"] \
        .drop_duplicates() \
        .tolist()

    for state in states:
        file = f"{DENTAL_LOCATION}\\Patients with no HIV Dx -{state}.csv"
        state_filter = df[df["STATE"] == state]

        with open(file, "wb") as state_out:
            state_filter.to_csv(state_out, index = False)
        send_emails(file, state, "Dental Patients with No HIV Dx", "dental")


def bh_transformation(df):
    states = df["STATE"] \
        .drop_duplicates() \
        .tolist()
    mo_df = df[df["STATE"] == "MO"]
    mo_cities = mo_df["CITY"] \
        .drop_duplicates() \
        .tolist()

    target_header = "BH Patients with No HIV Dx"
    for state in states:
        if state != "MO":
            file = f"{BH_LOCATION}\\Patients with no HIV Dx -{state}.csv"
            state_filter = df[df["STATE"] == state]

            with open(file, "wb") as state_out:
                state_filter.to_csv(state_out, index = False)
            send_emails(file, state, target_header, "bh")
        else:
            for city in mo_cities:
                mo_file = f"{BH_LOCATION}\\Patients with no HIV Dx -{state}.csv"
                city_state_filter = df[(df["STATE"] == state) & 
                    (df["CITY"] == city)
                ]
                with open(mo_file, "wb") as mo_state_out:
                    city_state_filter.to_csv(mo_state_out, index = False)
                send_emails(mo_file, state, target_header, "bh", city)


def send_emails(file_attachment, state, header, target_type, city = None):
    subject = header
    body_text = """
    Hello,

    Here are your patients who've completed a recent visit, but do not have a 
    diagnosis of HIV on their Problem List.

    Best,
    Tanner
    """

    if target_type == "dental":
        if state == "TX":
            to = ["Antonio.Menchaca@viventhealth.org"]
            emails.send_email(
                subject = subject,
                body_text = body_text,
                to_emails = to,
                file_to_attach = file_attachment,
                cc_emails = [],
                bcc_emails = []
            )
        elif state == "MO":
            to = ["Andrea.Morris@viventhealth.org"]
            emails.send_email(
                subject = subject,
                body_text = body_text,
                to_emails = to,
                file_to_attach = file_attachment,
                cc_emails = [],
                bcc_emails = []
            )
        elif state == "CO":
            to = ["Omar.Abuzaineh@viventhealth.org",
                  "caroline.stern@viventhealth.org"]
            emails.send_email(
                subject = subject,
                body_text = body_text,
                to_emails = to,
                file_to_attach = file_attachment,
                cc_emails = [],
                bcc_emails = []
            )
        elif state == "WI":
            to = ["michelle.dacosta@viventhealth.org",
                  "mark.rehorst@viventhealth.org",
                  "steve.groddy@viventhealth.org",
                  "stacey.bisenius@viventhealth.org",
                  "elisabeth.baertlein@viventhealth.org"]
            emails.send_email(
                subject = subject,
                body_text = body_text,
                to_emails = to,
                file_to_attach = file_attachment,
                cc_emails = [],
                bcc_emails = []
            )
    elif target_type == "bh":
        if state == "TX":
            to = ["caren.echols@viventhealth.org"]
            emails.send_email(
                subject = subject,
                body_text = body_text,
                to_emails = to,
                file_to_attach = file_attachment,
                cc_emails = [],
                bcc_emails = []
            )
        elif state == "MO":
            if city == "KANSAS CITY":
                to = ["amanda.rosenbaum-oudealink@viventhealth.org"]
                emails.send_email(
                    subject = subject,
                    body_text = body_text,
                    to_emails = to,
                    file_to_attach = file_attachment,
                    cc_emails = [],
                    bcc_emails = []
                )
            elif city == "ST LOUIS":
                to = ["Matt.Fanning@viventhealth.org"]
                emails.send_email(
                    subject = subject,
                    body_text = body_text,
                    to_emails = to,
                    file_to_attach = file_attachment,
                    cc_emails = [],
                    bcc_emails = []
                )
        elif state == "CO":
            to = ["adam.carlson@viventhealth.org",
                  "caroline.eisenberg@viventhealth.org"]
            emails.send_email(
                subject = subject,
                body_text = body_text,
                to_emails = to,
                file_to_attach = file_attachment,
                cc_emails = [],
                bcc_emails = []
            )
        elif state == "WI":
            to = ["pamela.blaszak@viventhealth.org"]
            emails.send_email(
                subject = subject,
                body_text = body_text,
                to_emails = to,
                file_to_attach = file_attachment,
                cc_emails = [],
                bcc_emails = []
            )

    dental_bh_pats_transform_logger.info("Dental and BH Patients with no HIV Dx emails sent.")
