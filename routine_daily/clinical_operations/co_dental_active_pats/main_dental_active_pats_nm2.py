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
sql_file = f"{directory}/co_dental_active_pats/sql/dental_active_pats_nm2.sql"
active_pats_logger = logger.setup_logger(
    "active_pats_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(active_pats_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = active_pats_logger
)

def run(shared_drive):
    active_pats_logger.info("Clinical Operations - Dental - Active Patients with Last Visit.")
    hyper_file = f"{shared_drive}/Dental - Active Patients with Last Visit.hyper"
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
            active_pats_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(active_pats_df.index) == 0:
            active_pats_logger.info("There are no data.")
            active_pats_logger.info("Clinical Operations - Dental - Active Patients with Last Visit Daily ETL finished.")
        else:
            tableau_push(active_pats_df, hyper_file)

    except ConnectionError as connection_error:
        active_pats_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        active_pats_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    active_pats_logger.info("Creating Hyper Table.")

    df["MONTHS SINCE SEEN"] = df["MONTHS SINCE SEEN"].fillna(0.0).astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Dental - Active Patients with Last Visit"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("COUNTY_OF_RESIDENCE", SqlType.text()),
            TableDefinition.Column("LAST VISIT", SqlType.date()),
            TableDefinition.Column("LAST_PROVIDER", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_ENCOUNTER_NAME", SqlType.text()),
            TableDefinition.Column("MONTHS SINCE SEEN", SqlType.int()),
            TableDefinition.Column("NEXT_PROVIDER", SqlType.text()),
            TableDefinition.Column("NEXT_VISIT_ENCOUNTER_NAME", SqlType.text()),
            TableDefinition.Column("NEXT VISIT", SqlType.date())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=active_pats_logger,
        project_id=project_id
    )

    active_pats_logger.info(
        "Clinical Operations - Dental - Active Patients with Last Visit pushed to Tableau."
    )
