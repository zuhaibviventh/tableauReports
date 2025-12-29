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
sql_file = f"{directory}/cq_bh_active_psych_retention/sql/active_psych_retention.sql"
active_psych_retention_logger = logger.setup_logger(
    "active_psych_retention_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(active_psych_retention_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = active_psych_retention_logger
)

def run(shared_drive):
    active_psych_retention_logger.info(
        "Clinical Quality - Active Psychiatry Patients Retention."
    )
    hyper_file = f"{shared_drive}/Active Psychiatry Patients Retention.hyper"
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
            active_psych_retention_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(active_psych_retention_df.index) == 0:
            active_psych_retention_logger.info("There are no data.")
            active_psych_retention_logger.info("Clinical Quality - Active Psychiatry Patients Retention Daily ETL finished.")
        else:
            tableau_push(active_psych_retention_df, hyper_file)

    except ConnectionError as connection_error:
        active_psych_retention_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        active_psych_retention_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    active_psych_retention_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Active Psychiatry Patients Retention"),
        columns = [
            TableDefinition.Column("pat_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PROV", SqlType.text()),
            TableDefinition.Column("Most_Recent_APPT", SqlType.date()),
            TableDefinition.Column("Months_since_Most_Recent_APPT", SqlType.int()),
            TableDefinition.Column("Psych_Retention", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("PROV_TYPE", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=active_psych_retention_logger,
        project_id=project_id
    )

    active_psych_retention_logger.info(
        "Clinical Quality - Active Psychiatry Patients Retention pushed to Tableau."
    )
