import os, os.path, sys, glob, shutil, json
import pandas as pd

from utils import (
    logger,
    connections, 
    context,
    emails,
    vh_config
)

directory = context.get_context(os.path.abspath(__file__))

excel_file_name = f"{directory}/wi_hcv_tests_email/staging/WI HCV Tests from Last Week.xlsx"

prl = logger.setup_logger(
    "wi_hcv_tests_email_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

config = vh_config.grab(prl)
folder = f"{directory}/wi_hcv_tests_email"

def run():
    prl.info("WI HCV Tests Weekly Workflow.")

    try:
        internal_engine = connections.engine_creation(
            server=config['PEViventHealth']['server'],
            db=config['PEViventHealth']['database'],
            driver=config['PEViventHealth']['driver'],
            uid=config['PEViventHealth']['uid'],
            pwd=config['PEViventHealth']['pwd'],
            internal_use=False
        )

        sql_file = f"{directory}/wi_hcv_tests_email/sql/wi_hcv_tests_email.sql"
        out_file = f"{directory}/wi_hcv_tests_email/staging/WI HCV Tests from Last Week.xlsx"
        with internal_engine.connect() as clarity_connection:
            pr_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if pr_df.empty:
            info = "HCV Tests dataframe is Empty. No data for today."
            teams_msg.send(prl, message=info, title=teams_title)
            prl.info(info)
        else:
            writeExcel(pr_df)
            sendEmails(out_file)
        #with internal_engine.connect() as clarity_connection:
        #    wi_hcv_tests_email_sql = connections.sql_to_df(sql_file, clarity_connection)
        #
        #    if len(wi_hcv_tests_email_sql.index) == 0:
        #        wi_hcv_tests_email_logger.info("There are no data.")
        #        wi_hcv_tests_email_logger.info("WI HCV Tests Weekly Workflow.")
        #    else:
        #        with open(out_file, "wb") as outfile:
        #            wi_hcv_tests_email_sql.to_csv(outfile, index = False)
        #            sendEmails(out_file)

    except ConnectionError as connectionError:
        prl.error(
            f"Unable to connect to Clarity: {connectionError}. Exiting."
        )
        sys.exit(1)
    except KeyError as keyError:
        prl.error(
            f"Incorrect connection keys: {keyError}. Exiting."
        )
        sys.exit(1)

    prl.info("WI HCV Tests Weekly Workflow.")
    
def writeExcel(df):
    prl.info("Writing to Excel.")
    col_list = df.columns
    writer = pd.ExcelWriter(excel_file_name, engine = "xlsxwriter")
    df.to_excel(writer, sheet_name = "Sheet1", 
                startrow = 1, header = False, index = False)
    workbook = writer.book
    worksheet = writer.sheets["Sheet1"]

    for idx, val in enumerate(col_list):
        worksheet.write(0, idx, val)

    writer.close()
    prl.info(f"File saved in {excel_file_name}. WI HCV Tests from Last Week.")


def sendEmails(csv_file):
    prl.info("Sending email.")

    body_text = """
        Hey Kristen,

        Attached is the file of HCV tests in WI last week.

        Thanks,
        Tanner
    """

    recepients = {
        #"Mitch Scoggins": "Mitch.Scoggins@viventhealth.org", 
        "Ben Bruso": "Benjamin.Bruso@viventhealth.org",
        "Kristen Grimes": "Kristen.Grimes@viventhealth.org"
    }

    to = list(recepients.values())

    emails.send_email(
        subject = "WI HCV Tests From Last Week",
        body_text = body_text,
        to_emails = to,
        file_to_attach = csv_file,
        cc_emails = [],
        bcc_emails = []
    )

    to_names = list(recepients.keys())
    prl.info(
        f"WI HCV Tests Weekly Workflow email sent to {', '.join(to_names)}"
    )
