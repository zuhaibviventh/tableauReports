import os, os.path
import pandas as pd

from utils import logger
from utils import context
from utils import emails

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}/dental_pts_no_open_episodes/staging"

dental_pts_no_open_episodes_transform_logger = logger.setup_logger(
    "dental_pts_no_open_episodes_transform_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(
            f"{STAGING_LOCATION}/STAGING_dental_pts_no_open_episodes.csv"
        ) as dental_pts_no_open_episodes:
            staged_data = pd.read_csv(dental_pts_no_open_episodes)
    except FileNotFoundError as file_not_found_error:
        dental_pts_no_open_episodes_transform_logger.error(f"Error finding file: {file_not_found_error}")

    if len(staged_data) > 0 or not staged_data.empty:
        states = staged_data["STATE"] \
            .drop_duplicates() \
            .tolist()

        for state in states:
            file = f"{STAGING_LOCATION}/Dental Patients with no open Episode - {state}.csv"
            state_filter = staged_data[staged_data["STATE"] == state]

            with open(file, "wb") as state_out:
                state_filter.to_csv(state_out, index = False)

            sendEmails(file, state)

    else:
        dental_pts_no_open_episodes_transform_logger.info("No rows returned.")        

    dental_pts_no_open_episodes_transform_logger.debug("Transformation complete.")


def sendEmails(file_attachment, state):
    subject = "Dental Patients with No open Episode - Automation Testing"
    body_text = """
        Hello,
        
        Here are your patients who've completed a recent visit, but do not have 
        an open episode of care of the type dental. 

        Thanks,
        Tanner
    """

    if state == "WI":
        to = ["michelle.dacosta@viventhealth.org",
              "mark.rehorst@viventhealth.org",
              "steve.groddy@viventhealth.org",
              "stacey.bisenius@viventhealth.org",
              "elisabeth.baertlein@viventhealth.org"]
        emails.send_email(subject = subject, 
            body_text = body_text, 
            to_emails = to, 
            file_to_attach = file_attachment,
            cc_emails = [],
            bcc_emails = [])
    elif state == "TX":
        to = ["Antonio.Menchaca@viventhealth.org"]
        emails.send_email(subject = subject, 
            body_text = body_text, 
            to_emails = to, 
            file_to_attach = file_attachment,
            cc_emails = [],
            bcc_emails = [])
    elif state == "MO":
        to = ["Andrea.Morris@viventhealth.org"]
        emails.send_email(subject = subject, 
            body_text = body_text, 
            to_emails = to, 
            file_to_attach = file_attachment,
            cc_emails = [],
            bcc_emails = [])
    elif state == "CO":
        to = ["Omar.Abuzaineh@viventhealth.org", 
              "caroline.stern@viventhealth.org"]
        emails.send_email(subject = subject, 
            body_text = body_text, 
            to_emails = to, 
            file_to_attach = file_attachment,
            cc_emails = [],
            bcc_emails = [])

    dental_pts_no_open_episodes_transform_logger.info("Dental Patients with no open episodes emails sent.")
