import os, os.path, sys, glob, shutil, json

from utils import (
    logger,
    connections, 
    context,
    emails
)

directory = context.get_context(os.path.abspath(__file__))
config_file = f"{directory}/config.json"

mpx_vax_main_logger = logger.setup_logger(
    "mpx_vax_main_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

folder = f"{directory}/MPX_Vaccines_Austin"

def run():
    mpx_vax_main_logger.info("Running MPX Vaccines - Austin.")

    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as fileNotFoundError:
        mpx_vax_main_logger.error(
            f"Config file was not found: {fileNotFoundError}. Exiting."
        )
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=False
        )

        sql_file = f"{directory}/mpx_vax_austin/sql/mpx_vax_austin.sql"
        out_file = f"{directory}/mpx_vax_austin/staging/Austin MPX Vaccines.csv"
        with internal_engine.connect() as clarity_connection:
            mpx_vax_sql = connections.sql_to_df(sql_file, clarity_connection)

            if len(mpx_vax_sql.index) == 0:
                mpx_vax_main_logger.info("There are no data.")
                mpx_vax_main_logger.info("MPX Vaccines - Austin Weekly ETL Complete.")
            else:
                with open(out_file, "wb") as outfile:
                    mpx_vax_sql.to_csv(outfile, index = False)
                    sendEmails(out_file)

    except ConnectionError as connectionError:
        mpx_vax_main_logger.error(
            f"Unable to connect to Clarity: {connectionError}. Exiting."
        )
        sys.exit(1)
    except KeyError as keyError:
        mpx_vax_main_logger.error(
            f"Incorrect connection keys: {keyError}. Exiting."
        )
        sys.exit(1)

    mpx_vax_main_logger.info("MPX Vaccines - Austin Weekly ETL Complete.")


def sendEmails(csv_file):
    mpx_vax_main_logger.info("Sending email.")

    body_text = """
        Hello,

        Attached are patients in Austin for MPX Vaccines. 
        Please upload to Box.com.

        If you're receiving this email early it's because we're testing some new
        automation! If more than 1 get sent out please direct all angry Teams messages
        to Tanner Strom instead of Mitch.

        Thanks,
        Tanner
    """

    recepients = {
        "Erika Colombo": "erika.colombo@viventhealth.org",
        "Emma Sinnott": "emma.sinnott@viventhealth.org"
    }

    to = list(recepients.values())

    emails.send_email(
        subject = "Austin MPX Vaccines",
        body_text = body_text,
        to_emails = to,
        file_to_attach = csv_file,
        cc_emails = [],
        bcc_emails = []
    )

    to_names = list(recepients.keys())
    mpx_vax_main_logger.info(
        f"Austin MPX Vaccines email sent to {', '.join(to_names)}"
    )
