import os, os.path, sys, glob, shutil, json

from utils import (
    logger,
    connections, 
    context,
    emails,
    vh_config
)

directory = context.get_context(os.path.abspath(__file__))

_340b_mou_data_logger = logger.setup_logger(
    "_340b_mou_data_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

config = vh_config.grab(_340b_mou_data_logger)
folder = f"{directory}/_340b_mou_data"

def run():
    _340b_mou_data_logger.info("_340b MOU Data.")

    try:
        internal_engine = connections.engine_creation(
            server=config['PEViventHealth']['server'],
            db=config['PEViventHealth']['database'],
            driver=config['PEViventHealth']['driver'],
            uid=config['PEViventHealth']['uid'],
            pwd=config['PEViventHealth']['pwd'],
            internal_use=False
        )

        sql_file = f"{directory}/_340b_mou_data/sql/340b_mou_data.sql"
        out_file = f"{directory}/_340b_mou_data/staging/340b MOU Data.csv"
        with internal_engine.connect() as clarity_connection:
            _340b_mou_data_sql = connections.sql_to_df(sql_file, clarity_connection)

            if len(_340b_mou_data_sql.index) == 0:
                _340b_mou_data_logger.info("There are no data.")
                _340b_mou_data_logger.info("_340b MOU Data.")
            else:
                with open(out_file, "wb") as outfile:
                    _340b_mou_data_sql.to_csv(outfile, index = False)
                    sendEmails(out_file)

    except ConnectionError as connectionError:
        _340b_mou_data_logger.error(
            f"Unable to connect to PE: {connectionError}. Exiting."
        )
        sys.exit(1)
    except KeyError as keyError:
        _340b_mou_data_main_logger.error(
            f"Incorrect connection keys: {keyError}. Exiting."
        )
        sys.exit(1)

    _340b_mou_data_logger.info("_340b MOU Data.")


def sendEmails(csv_file):
    _340b_mou_data_logger.info("Sending email.")

    body_text = """
        Hey Dan & Eric,

        Here are the 340b/MOU Data.

        --Mitch
    """

    recepients = {
        #"Mitch Scoggins": "Mitch.Scoggins@viventhealth.org"
        "Eric Bauch": "Erik.Bauch@viventhealth.org",
        "Dan Scales": "Dan.Scales@viventhealth.org"
    }

    to = list(recepients.values())

    emails.send_email(
        subject = "340b/MOU Data for WI and CO",
        body_text = body_text,
        to_emails = to,
        file_to_attach = csv_file,
        cc_emails = [],
        bcc_emails = []
    )

    to_names = list(recepients.keys())
    _340b_mou_data_logger.info(
        f"340b MOU Data email sent to {', '.join(to_names)}"
    )
