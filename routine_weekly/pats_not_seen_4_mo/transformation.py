import os, os.path
import pandas as pd

from utils import logger
from utils import context
from utils import emails

directory = context.get_context(os.path.abspath(__file__))

STAGING_LOC = f"{directory}/pats_not_seen_4_mo/staging"
HEALTH_DRIVE = ("C:\\Users\\talendservice\\Vivent Health\\Share Drives - Health\\Quality Action Lists\\Patients Not Seen in 4+ Months")
excel_file_name = f"{STAGING_LOC}/Virally unsuppressed patients for psr outreach.xlsx"

transformation_logger = logger.setup_logger(
    "pats_not_seen_logger",
    f"{directory}/logs/routine_weekly_main.log"
)

def transform():
    try:
        with open(
            f"{STAGING_LOC}/STAGING_pats_not_seen.csv"
        ) as pats_not_seen:
            staged_data = pd.read_csv(pats_not_seen)
    except FileNotFoundError as file_not_found_error:
        transformation_logger.error(f"Error finding file: {file_not_found_error}")

    sorted_df = staged_data.sort_values(by = "CITY")
    transformation_logger.debug(f"Writing to {STAGING_LOC}")
    write_to_file(sorted_df)

    unsuppressed_df = sorted_df[sorted_df["SUPRESSION_STATUS"] == "UNSUPPRESSED"]
    pcp_appt_null = unsuppressed_df[unsuppressed_df["NEXT PCP APPT"].isnull()]
    wi_df = pcp_appt_null[pcp_appt_null["STATE"] == "WI"]
    subset_df = wi_df[["MRN", "PCP", "PATIENT", "LAST OFFICE VISIT", "CITY"]]
    selected_mrn = subset_df[~subset_df["MRN"] \
        .isin(["640001761", "64000194", "64000984", "64001212"])]

    selected_mrn["Grouper"] = "Grouper"
    grouped_df = summarize(selected_mrn, "Grouper") # Send emails after summarization

    final_df = selected_mrn \
        .loc[:, ~selected_mrn.columns.isin(["Grouper"])]

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

    #sendEmails() ###uncomment before go live


def write_to_file(data_frame):
    """
    Immediate write to SHARE DRIVE Folder:
    C:\\Users\\talendservice\\OneDrive - Vivent Health\\Health\\Quality Action Lists\\Patients Not Seen in 4+ Months
    """

    all_pcp_list = data_frame["PCP"] \
        .drop_duplicates() \
        .tolist()

    for pcp in all_pcp_list:
        pcp_filter = data_frame[data_frame["PCP"] == pcp]

        with open(f"{HEALTH_DRIVE}\\Not Seen in 4+ Months - {pcp}.csv", "wb") as final_out:
            pcp_filter.to_csv(final_out, index = False)


def summarize(data_frame, column):
    """
    For sending emails, thus let's group by the given column to collapse into 
    one.
    """
    return data_frame.groupby([column])


def sendEmails():
    subject = "Weekly List of Patients for Recall"
    body_text = """
        Hello,

        Here is this week's list of patients who need to be scheduled for a 
        visit with their PCP. PCPs -- Please let the PM/HSA know if anyone on this 
        list should not be contacted. 

        Thanks,
        Tanner
    """
    to = ["Leslie.Cockerham@viventhealth.org",
        "Janaki.Shah@viventhealth.org",
        "Jonathan.Weimer@viventhealth.org",
        "Sol.Aldrete@viventhealth.org",
        "christine.hogan@viventhealth.org",
        "Winsome.Panton@viventhealth.org",
        "Amy.Pease@viventhealth.org",
        "rpw@medicine.wisc.edu",
        "jennifer.garcia@viventhealth.org",
        "Tanya.wolf@viventhealth.org",
        "jen.bartels@viventhealth.org"]

    emails.send_email(subject = subject, 
        body_text = body_text, 
        to_emails = to, 
        file_to_attach = excel_file_name,
        cc_emails = ["mitch.scoggins@viventhealth.org",
                     "tanner.strom@viventhealth.org",
                     "Adam.Carlson@viventhealth.org"],
        bcc_emails = [])

    transformation_logger.info("Patients not seen in 4+ months emails sent.")
