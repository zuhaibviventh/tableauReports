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
sql_file = f"{directory}/co_dental_no_open_ep/sql/no_open_ep_nm2.sql"
dental_logger = logger.setup_logger(
    "no_open_ep_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(dental_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = dental_logger
)

def run(shared_drive):
    dental_logger.info("Clinical Operations - Dental Recent Visits with no Open Episodes.")
    hyper_file = f"{shared_drive}/Dental - Recent Visits with No Open Episode.hyper"
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
            dental_episodes_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(dental_episodes_df.index) == 0:
            dental_logger.info("There are no data.")
            dental_logger.info("Clinical Operations - Dental Recent Visits with no Open Episodes Daily ETL finished.")
        else:
            tableau_push(dental_episodes_df, hyper_file)

    except ConnectionError as connection_error:
        dental_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        dental_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    dental_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Dental - Recent Visits with No Open Episode"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("DENTAL_EPISODE", SqlType.text()),
            TableDefinition.Column("LAST_VISIT", SqlType.date()),
            TableDefinition.Column("VISIT_PROVIDER", SqlType.text())
        ]
    )

    dental_logger.info(
        "Clinical Operations - Dental Recent Visits with no Open Episodes."
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=dental_logger,
        project_id=project_id
    )

    dental_logger.info(
        "Clinical Operations - Dental Recent Visits with no Open Episodes pushed to Tableau."
    )

