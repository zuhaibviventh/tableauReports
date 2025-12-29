import os
import pandas as pd
import sys

from utils import logger, connections, context, emails, vh_config

directory = context.get_context(os.path.abspath(__file__))
prl = logger.setup_logger(
    "austin_client_wo_tct_id_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)
prl.info("Austin Clients Without a TCT ID.")
excel_file_name = f"{directory}/austin_client_wo_tct_id/staging/Austin Clients Without a TCT ID.xlsx"

def run():
    try:
        config = vh_config.grab(prl)
        internal_engine = connections.engine_creation(
            server=config["PEViventHealth"]["server"],
            db=config["PEViventHealth"]["database"],
            driver=config["PEViventHealth"]["driver"],
            uid=config["PEViventHealth"]["uid"],
            pwd=config["PEViventHealth"]["pwd"],
            internal_use=False,
        )

        sql_file = f"{directory}/austin_client_wo_tct_id/sql/austin_client_wo_tct_id.sql"
        with internal_engine.connect() as pe_connection:
            pr_df = connections.sql_to_df(file=sql_file, connection=pe_connection)

        if pr_df.empty:
            info = "Missing TCT ID dataframe is Empty. No data for today."
            teams_msg.send(prl, message=info, title=teams_title)
            prl.info(info)
        else:
            write_excel(pr_df, directory)
            send_emails(pr_df, directory)

    except ConnectionError as connectionError:
        prl.error(f"Unable to connect to PE: {connectionError}. Exiting.")
        sys.exit(1)
    except KeyError as keyError:
        prl.error(f"Incorrect connection keys: {keyError}. Exiting.")
        sys.exit(1)

    prl.info("Austin Clients Without a TCT ID.")


def write_excel(df, directory):
    prl.info("Writing to Excel.")
    col_list = df.columns
    writer = pd.ExcelWriter(excel_file_name, engine="xlsxwriter")
    df.to_excel(writer, sheet_name="Sheet1", startrow=1, header=False, index=False)
    workbook = writer.book
    worksheet = writer.sheets["Sheet1"]

    for idx, val in enumerate(col_list):
        worksheet.write(0, idx, val)

    writer.close()
    prl.info(f"File saved in {excel_file_name}. Austin Clients Without a TCT ID.")


def send_emails(df, directory):
    prl.info("Sending email.")

    body_text = """
        Hey Lourdes,

        Attached is the file of PE Clients in Austin who do not have a TCT ID.

        --Mitch
    """

    recepients = {
        #"Mitch Scoggins": "Mitch.Scoggins@viventhealth.org",
        "Annette Guebara": "Annette.Guebara@viventhealth.org",
        "Nicole Hamilton": "Nicole.Hamilton@viventhealth.org",
        "Lourdes Pineda": "Lourdes.Pineda@viventhealth.org"
    }

    to = list(recepients.values())

    emails.send_email(
        subject="Austin Clients Without a TCT ID",
        body_text=body_text,
        to_emails=to,
        file_to_attach=excel_file_name,
        cc_emails=[],
        bcc_emails=[],
    )

    to_names = list(recepients.keys())
    prl.info(f"Austin Clients Without a TCT ID email sent to {', '.join(to_names)}")
