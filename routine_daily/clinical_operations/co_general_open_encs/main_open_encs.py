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
sql_file = f"{directory}/co_general_open_encs/sql/open_encs.sql"
open_encs_logger = logger.setup_logger(
    "open_encs_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(open_encs_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = open_encs_logger
)

def run(shared_drive):
    open_encs_logger.info("Clinical Operations - Open Encounters.")
    hyper_file = f"{shared_drive}/Open Encounters.hyper"
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
            open_encs_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(open_encs_df.index) == 0:
            open_encs_logger.info("There are no data.")
            open_encs_logger.info("Clinical Operations - Open Encounters Daily ETL finished.")
        else:
            tableau_push(open_encs_df, hyper_file)

    except ConnectionError as connection_error:
        open_encs_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        open_encs_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    open_encs_logger.info("Creating Hyper Table.")

    df["PAT_ENC_CSN_ID"] = df["PAT_ENC_CSN_ID"].astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Open Encounters"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("CONTACT_DATE", SqlType.date()),
            TableDefinition.Column("PAT_ENC_CSN_ID", SqlType.int()),
            TableDefinition.Column("ENC_TYPE_C", SqlType.text()),
            TableDefinition.Column("ENC_TYPE", SqlType.text()),
            TableDefinition.Column("VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("BUS_DAYS", SqlType.int()),
            TableDefinition.Column("PROC_NAME", SqlType.text()),
            TableDefinition.Column("SCHEDULED_YN", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=open_encs_logger,
        project_id=project_id
    )

    open_encs_logger.info(
        "Clinical Operations - Open Encounters pushed to Tableau."
    )

