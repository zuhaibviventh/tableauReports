import os, os.path
import pandas as pd

from utils import logger
from utils import context
from utils import emails

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOCATION = f"{directory}/epic_fds_docs/staging"

epic_fds_transform_logger = logger.setup_logger(
    "epic_fds_transform_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(
            f"{STAGING_LOCATION}/STAGING_epic_fds_docs.csv"
        ) as epic_fds:
            staged_data = pd.read_csv(epic_fds)
    except FileNotFoundError as file_not_found_error:
        epic_fds_transform_logger.error(f"Error finding file: {file_not_found_error}")
    final_df = staged_data
    
    excel_file_name = f"{STAGING_LOCATION}/EPIC FDS Scanned Signed Documents.xlsx"
    col_list = final_df.columns
    writer = pd.ExcelWriter(excel_file_name, engine="xlsxwriter")
    final_df.to_excel(
        writer, 
        sheet_name="Sheet1", 
        startrow=1, 
        header=False, 
        index=False
    )

    workbook = writer.book
    worksheet = writer.sheets["Sheet1"]

    for idx, val in enumerate(col_list):
        worksheet.write(0, idx, val)

    writer.close()

    sendEmails()

def sendEmails():
    subject = "EPIC Scanned Signed Documents"
    body_text = """
        Hello Nicole,

        Attached is the EPIC scanned and signed documents Excel report for 
        Austin patients.

        If you're receiving this email early it's because we're testing out some new automation! 
        Please direct all angry messages to Tanner Strom instead of Mitch if more than 1 get sent out.

        Thanks,
        Tanner
    """

    to = ["Nicole.Hamilton@viventhealth.org"]
    #to = ["tanner.strom@viventhealth.org"]
    file_to_attach = f"{STAGING_LOCATION}/EPIC FDS Scanned Signed Documents.xlsx"

    emails.send_email(subject = subject, 
        body_text = body_text, 
        to_emails = to, 
        file_to_attach = file_to_attach,
        cc_emails = [],
        bcc_emails = [])

    epic_fds_transform_logger.info("EPIC FDS Scanned and Signed Docs emails sent.")
