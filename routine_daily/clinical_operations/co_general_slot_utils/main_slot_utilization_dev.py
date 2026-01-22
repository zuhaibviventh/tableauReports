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
sql_file = f"{directory}/co_general_slot_utils/sql/slot_utilization_report_dev.sql"
slot_utils_logger = logger.setup_logger(
    "slot_utils_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(slot_utils_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = slot_utils_logger
)

def run(shared_drive):
    slot_utils_logger.info("Clinical Operations - Appointment Availability and Use (Ops Report).")
    hyper_file = f"{shared_drive}/Appointment Availability and Use (Ops Report) Dev.hyper"
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
            slot_utils_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(slot_utils_df.index) == 0:
            slot_utils_logger.info("There are no data.")
            slot_utils_logger.info("Clinical Operations - Appointment Availability and Use (Ops Report) Daily ETL finished.")
        else:
            tableau_push(slot_utils_df, hyper_file)

    except ConnectionError as connection_error:
        slot_utils_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        slot_utils_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    slot_utils_logger.info("Creating Hyper Table.")

    # Enforce an int
    df["DEPT_ID"] = df["DEPT_ID"].astype(int)
    df["PAT_ENC_CSN_ID"] = pd \
        .to_numeric(df["PAT_ENC_CSN_ID"], errors = "coerce") \
        .fillna(0) \
        .astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Appointment Availability and Use (Ops Report)"),
        columns = [
            TableDefinition.Column("PROVIDER", SqlType.text()),
            TableDefinition.Column("PROV_ID", SqlType.text()),
            TableDefinition.Column("DEPARTMENT", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("DEPT_ID", SqlType.int()),
            TableDefinition.Column("PROVIDER TYPE", SqlType.text()),
            TableDefinition.Column("APPOINTMENT SLOTS", SqlType.int()),
            TableDefinition.Column("SLOT MINUTES", SqlType.int()),
            TableDefinition.Column("APPOINTMENT TIME", SqlType.date()),
            TableDefinition.Column("NUM APPTS SCHEDULED", SqlType.int()),
            TableDefinition.Column("APPOINTMENT MINUTES", SqlType.int()),
            TableDefinition.Column("PAT_ENC_CSN_ID", SqlType.int())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=slot_utils_logger,
        project_id=project_id
    )

    slot_utils_logger.info(
        "Clinical Operations - Appointment Availability and Use (Ops Report) pushed to Tableau."
    )

