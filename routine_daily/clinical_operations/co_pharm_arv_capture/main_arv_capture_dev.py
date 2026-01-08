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
sql_file = f"{directory}/co_pharm_arv_capture/sql/arv_capture_dev.sql"
arv_capture_logger = logger.setup_logger("arv_capture_logger", f"{directory}/logs/main.log")
config = vh_config.grab(arv_capture_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = arv_capture_logger
)

def run(shared_drive):
    arv_capture_logger.info("Clinical Operations - ARV Capture.")
    hyper_file = f"{shared_drive}/ARV Capture Dev.hyper"
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
            dm_cohort_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(dm_cohort_df.index) == 0:
            arv_capture_logger.info("There are no data.")
            arv_capture_logger.info("Clinical Operations - ARV Capture Daily ETL finished.")
        else:
            tableau_push(dm_cohort_df, hyper_file)

    except ConnectionError as connection_error:
        arv_capture_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        arv_capture_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    arv_capture_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("ARV Capture"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("Rx Name", SqlType.text()),
            TableDefinition.Column("Order Date", SqlType.date()),
            TableDefinition.Column("Pharmacy Name", SqlType.text()),
            TableDefinition.Column("Department", SqlType.text()),
            TableDefinition.Column("Ordering Provider", SqlType.text()),
            TableDefinition.Column("Pharmacy", SqlType.text()),
            TableDefinition.Column("Site", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("PATIENT TYPE", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=arv_capture_logger,
        project_id=project_id
    )

    arv_capture_logger.info(
        "Clinical Operations - ARV Capture pushed to Tableau."
    )

