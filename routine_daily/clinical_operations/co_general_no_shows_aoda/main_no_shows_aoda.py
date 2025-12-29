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
sql_file = f"{directory}/co_general_no_shows_aoda/sql/no_shows_aoda.sql"
no_shows_pats = logger.setup_logger(
    "no_shows_pats",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(no_shows_pats)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = no_shows_pats
)

def run(shared_drive):
    no_shows_pats.info("Clinical Operations - AODA - No Shows.")
    hyper_file = f"{shared_drive}/E_AODA - No Shows.hyper"
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

        if len(no_shows_df.index) == 0:
            no_shows_pats.info("There are no data.")
            no_shows_pats.info("Clinical Operations - AODA - No Shows Daily ETL finished.")
        else:
            tableau_push(no_shows_df, hyper_file)

    except ConnectionError as connection_error:
        no_shows_pats.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        no_shows_pats.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    no_shows_pats.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("AODA - No Shows"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("BIRTH_DATE", SqlType.date()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("CONTACT_DATE", SqlType.date()),
            TableDefinition.Column("DAY", SqlType.text()),
            TableDefinition.Column("APPT_STATUS_C", SqlType.int()),
            TableDefinition.Column("APPT_DATETIME", SqlType.timestamp()),
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
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("SEX", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("ZIP", SqlType.text()),
            TableDefinition.Column("APPT TYPE", SqlType.text())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=no_shows_pats,
        project_id=project_id
    )

    no_shows_pats.info(
        "Clinical Operations - AODA - No Shows pushed to Tableau."
    )

