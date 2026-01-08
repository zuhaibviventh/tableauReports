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
sql_file = f"{directory}/co_general_no_shows_pats/sql/no_shows_by_pats_nm2.sql"
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
    no_shows_pats.info("Clinical Operations - No Shows by Patient.")
    hyper_file = f"{shared_drive}/No Shows by Patient DEV2.hyper"
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

        no_shows_df = no_shows_df[['MRN', 'PATIENT', 'STATE', 'CITY','SERVICE_TYPE','LOS', 'SUB_SERVICE_LINE','PCP','VISIT PROVIDER', 'NO SHOWS',"Next Any Appt","Next Appt Prov","Next PCP Appt",
                                   "No Show Flag - Medical","No Show Flag - BH","No Show Flag - Dental"]]

        if len(no_shows_df.index) == 0:
            no_shows_pats.info("There are no data.")
            no_shows_pats.info("Clinical Operations - No Shows by Patient Daily ETL finished.")
        else:
            tableau_push(no_shows_df, hyper_file)

    except ConnectionError as connection_error:
        no_shows_pats.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        no_shows_pats.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    no_shows_pats.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("No Shows by Patient"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("VISIT PROVIDER", SqlType.text()),
            TableDefinition.Column("NO SHOWS", SqlType.int()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("No Show Flag - Medical", SqlType.text()),
            TableDefinition.Column("No Show Flag - BH", SqlType.text()),
            TableDefinition.Column("No Show Flag - Dental", SqlType.text())
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
        "Clinical Operations - No Shows by Patient pushed to Tableau."
    )

