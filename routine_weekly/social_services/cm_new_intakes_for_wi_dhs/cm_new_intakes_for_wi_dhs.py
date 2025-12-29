import os, os.path, sys, glob, shutil, json

from utils import (
    logger,
    connections, 
    context,
    emails,
    vh_config
)

directory = context.get_context(os.path.abspath(__file__))

cm_new_intakes_for_wi_dhs_logger = logger.setup_logger(
    "cm_new_intakes_for_wi_dhs_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

config = vh_config.grab(cm_new_intakes_for_wi_dhs_logger)
folder = f"{directory}/cm_new_intakes_for_wi_dhs"

def run():
    cm_new_intakes_for_wi_dhs_logger.info("Vivent Health CM New Intakes.")

    try:
        internal_engine = connections.engine_creation(
            server=config['PEViventHealth']['server'],
            db=config['PEViventHealth']['database'],
            driver=config['PEViventHealth']['driver'],
            uid=config['PEViventHealth']['uid'],
            pwd=config['PEViventHealth']['pwd'],
            internal_use=False
        )

        sql_file = f"{directory}/cm_new_intakes_for_wi_dhs/sql/cm_new_intakes_for_wi_dhs.sql"
        out_file = f"{directory}/cm_new_intakes_for_wi_dhs/staging/Vivent Health CM New Intakes.csv"
        with internal_engine.connect() as clarity_connection:
            cm_new_intakes_for_wi_dhs_sql = connections.sql_to_df(sql_file, clarity_connection)

            if len(cm_new_intakes_for_wi_dhs_sql.index) == 0:
                cm_new_intakes_for_wi_dhs_logger.info("There are no data.")
                cm_new_intakes_for_wi_dhs_logger.info("Vivent Health CM New Intakes.")
            else:
                with open(out_file, "wb") as outfile:
                    cm_new_intakes_for_wi_dhs_sql.to_csv(outfile, index = False)
                    sendEmails(out_file)

    except ConnectionError as connectionError:
        cm_new_intakes_for_wi_dhs_logger.error(
            f"Unable to connect to PE: {connectionError}. Exiting."
        )
        sys.exit(1)
    except KeyError as keyError:
        cm_new_intakes_for_wi_dhs_main_logger.error(
            f"Incorrect connection keys: {keyError}. Exiting."
        )
        sys.exit(1)

    cm_new_intakes_for_wi_dhs_logger.info("Vivent Health CM New Intakes.")


def sendEmails(csv_file):
    cm_new_intakes_for_wi_dhs_logger.info("Sending email.")

    body_text = """
        Hey Yi,

        Here are the new Case Management Intakes in Wisconsin last week.

        --Tanner
    """

    recepients = {
        # "Mitch Scoggins": "Mitch.Scoggins@viventhealth.org",
    
        #ADD THESE 3 BACK IN
        "Yi Oh": "yi.ou@dhs.wisconsin.gov",
        "Katie Andaloro": "katie.andaloro@viventhealth.org",
        "Carla Washington": "Carla.Washington@viventhealth.org",
        
        #"Erin Petersen": "Erin.Petersen@viventhealth.org"
        "Tanner Strom": "tanner.strom@viventhealth.org"
    }

    to = list(recepients.values())

    emails.send_email(
        subject = "Last Week's New Intakes in Case Management",
        body_text = body_text,
        to_emails = to,
        file_to_attach = csv_file,
        cc_emails = [],
        bcc_emails = []
    )

    to_names = list(recepients.keys())
    cm_new_intakes_for_wi_dhs_logger.info(
        f"Vivent Health CM New Intakes email sent to {', '.join(to_names)}"
    )
