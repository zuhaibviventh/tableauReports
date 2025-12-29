import os
import os.path
import json
import datetime

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tableauhyperapi import TableName, TableDefinition, SqlType

from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau,
    emails
)

today = datetime.datetime.now()
directory = context.get_context(os.path.abspath(__file__))
staging_folder = f"{directory}/pats_not_marked_with_hiv/staging"
sql_file = f"{directory}/pats_not_marked_with_hiv/sql/pats_not_marked_with_hiv.sql"
hyper_file = f"{staging_folder}/Medical Patients Not Indicated as HIV+ or HIV-.hyper"
hqa_folder = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\Health\\Quality Action Lists\\Patients who are not indicated as either HIV+ or HIV-")

hiv_pos_neg_logger = logger.setup_logger(
    "hiv_pos_neg_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

config = vh_config.grab(hiv_pos_neg_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = hiv_pos_neg_logger
)

def run():
    hiv_pos_neg_logger.info("Running Medical Patients Not Indicated as HIV+ or HIV-.")

    try:
        internal_engine = connections.engine_creation(
            server = config['Clarity - VH']['server'],
            db = config['Clarity - VH']['database'],
            driver = config['Clarity - VH']['driver'],
            internal_use = True
        )

        with internal_engine.connect() as clarity_connection:
            hiv_pos_neg = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

            write_to_hqa(hiv_pos_neg) # Write to Health Quality Action List
            sendEmails(hiv_pos_neg) # Send email UNCOMMENT ON GO LIVE

    except ConnectionError as connection_error:
        hiv_pos_neg_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        hiv_pos_neg_logger.error(f"Incorrect connection keys: {key_error}")

    table_definition = TableDefinition(
        table_name = TableName("Medical Patients Not Indicated as HIV+ or HIV-"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_DATE", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_SITE", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("RESPONSIBLE PERSON", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.text()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.text()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df = hiv_pos_neg, 
        hyper_file = hyper_file, 
        table_definition = table_definition, 
        logger = hiv_pos_neg_logger,
        project_id = project_id
    )


def write_to_hqa(df):
    hiv_pos_neg_logger.info(f"Writing to Health Quality Action Lists ('{hqa_folder}').")

    all_people = df["RESPONSIBLE PERSON"] \
        .drop_duplicates() \
        .tolist()

    for person in all_people:
        person_filter = df[df["RESPONSIBLE PERSON"] == person]

        with open(
            f"{hqa_folder}/Patients needing chart updates with Dx or FYI flag - {person}.csv",
            "wb") as final_out:
            hiv_pos_neg_logger.info(f"Loading {hqa_folder}/Patients needing chart updates with Dx or FYI flag - {person}.csv")
            person_filter.to_csv(final_out, index = False)


def sendEmails(df):
    hiv_pos_neg_logger.info("Sending email.")
    csv_name = "Medical Patients Not Indicated as HIV+ or HIV-.csv"

    cities = df["CITY"] \
        .drop_duplicates() \
        .tolist()

    csv_files = []
    for city in cities:
        csv_file = f"{staging_folder}/{today.strftime('%B %d %Y')} - {city} - {csv_name}"
        city_filter = df[df["CITY"] == city]

        with open(csv_file, "wb") as email_out:
            city_filter.to_csv(email_out, index = False)
        csv_files.append(csv_file)

    subject = "Medical Patients Who Are Not Marked as HIV+ or HIV- in Epic"
    body_text = """
        Hello,

        Attached are the patients who are not marked as either HIV+ or HIV- in Epic.

        Thanks,
        Tanner
    """

    recepients = {
        "Adam Carlson": "Adam.Carlson@viventhealth.org",
        "Rachel Becker": "rachel.becker@miunified.org",
        "Carla Washington": "Carla.Washington@viventhealth.org",
        "Jennifer Garcia": "Jennifer.Garcia@viventhealth.org",
        "Lynne Braverman": "lynne.braverman@viventhealth.org",
        "Valerie Newbern": "Valerie.Newbern@viventhealth.org",
        "Erica Boyle": "Erica.Boyle@viventhealth.org",
        "Erika Colombo": "erika.colombo@viventhealth.org",
        "Caroline Eisenberg": "caroline.eisenberg@viventhealth.org",
        "Jen Bartels": "jen.bartels@viventhealth.org",
        "Ebony Pugh": "ebony.pugh@viventhealth.org",
        "Tabitha Russ": "tabitha.russ@viventhealth.org",
        "Tanya Wolf": "Tanya.wolf@viventhealth.org",
        "Randy Robinson": "Randy.Robinson@viventhealth.org",
        "Benjamin Crouse": "benjamin.crouse@viventhealth.org",
        "Ashley Ries": "Ashley.Ries@miunified.org"
    }

    to = list(recepients.values())
    #to = ["tanner.strom@viventhealth.org"]

    emails.send_email(
        subject = subject,
        body_text = body_text,
        to_emails = to,
        file_to_attach = csv_files,
        cc_emails = [],
        bcc_emails = []
    )

    to_names = list(recepients.keys())
    hiv_pos_neg_logger.info(
        f"Patients who are not marked as HIV+/- email sent to {', '.join(to_names)}"
    )
