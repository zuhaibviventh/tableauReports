import os, os.path, json

import pandas as pd

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

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/co_general_no_shows_excl_sud/sql/no_shows_nm2.sql"
sql_file_eligibility = f"{directory}/co_general_no_shows_excl_sud/sql/no_shows-eligibility_nm2.sql"

no_shows_logger = logger.setup_logger(
    "no_shows_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(no_shows_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = no_shows_logger
)

def run(shared_drive):
    no_shows_logger.info("Clinical Operations - No Shows.")
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
            #server=config['Clarity - OCHIN']['server'],
            #db=config['Clarity - OCHIN']['database'],
            #driver=config['Clarity - OCHIN']['driver'],
            #internal_use=False
        )

        with internal_engine.connect() as clarity_connection:
            no_shows_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

            eligibility_no_shows_df = connections.sql_to_df(
                file = sql_file_eligibility,
                connection = clarity_connection
            )

        if len(no_shows_df.index) == 0:
            no_shows_logger.info("There are no data.")
            no_shows_logger.info("Clinical Operations - No Shows Daily ETL finished.")
        else:
            hyper_file = f"{shared_drive}/No Shows.hyper"
            tableau_push(no_shows_df, hyper_file)

        if len(eligibility_no_shows_df.index) == 0:
            no_shows_logger.info("There are no data.")
            no_shows_logger.info("Clinical Operations - No Shows Daily ETL finished.")
        else:
            hyper_file = f"{shared_drive}/No Shows - Eligibility Appt.hyper"
            tableau_push(eligibility_no_shows_df, hyper_file)

    except ConnectionError as connection_error:
        no_shows_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        no_shows_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    no_shows_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("No Shows"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("BIRTH_DATE", SqlType.date()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("CONTACT_DATE", SqlType.date()),
            TableDefinition.Column("DAY", SqlType.text()),
            TableDefinition.Column("APPT_STATUS_C", SqlType.int()),
            TableDefinition.Column("APPT_DATETIME", SqlType.date()),
            TableDefinition.Column("APPOINTMENT_TIME", SqlType.text()),
            TableDefinition.Column("APPT_TIME", SqlType.text()),
            TableDefinition.Column("NO SHOW", SqlType.text()),
            TableDefinition.Column("COMPLETE", SqlType.text()),
            TableDefinition.Column("APPOINTMENT STATUS", SqlType.text()),
            TableDefinition.Column("APPT_PRC_ID", SqlType.text()),
            TableDefinition.Column("PROV_NAME", SqlType.text()),
            TableDefinition.Column("PROV_TYPE", SqlType.text()),
            TableDefinition.Column("PROVIDER_TYPE_C", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("SEX", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("APPT TYPE", SqlType.text()),
            TableDefinition.Column("2iS Patient", SqlType.text()),
            TableDefinition.Column("PATIENT_TYPE", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=no_shows_logger,
        project_id=project_id
    )

    no_shows_logger.info(
        "Clinical Operations - No Shows pushed to Tableau."
    )

