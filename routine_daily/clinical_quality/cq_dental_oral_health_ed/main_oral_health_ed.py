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
sql_file = f"{directory}/cq_dental_oral_health_ed/sql/oral_health_ed.sql"
oral_health_ed_logger = logger.setup_logger(
    "oral_health_ed_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(oral_health_ed_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = oral_health_ed_logger
)

def run(shared_drive):
    oral_health_ed_logger.info(
        "Clinical Quality - Dental - Oral Health Ed."
    )
    hyper_file = f"{shared_drive}/Dental - Oral Health Ed.hyper"
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
            oral_health_ed_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(oral_health_ed_df.index) == 0:
            oral_health_ed_logger.info("There are no data.")
            oral_health_ed_logger.info("Clinical Quality - Dental - Oral Health Ed Daily ETL finished.")
        else:
            tableau_push(oral_health_ed_df, hyper_file)

    except ConnectionError as connection_error:
        oral_health_ed_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        oral_health_ed_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    oral_health_ed_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Dental - Oral Health Ed"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("ORAL_HEALTH_ED", SqlType.int()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=oral_health_ed_logger,
        project_id=project_id
    )

    oral_health_ed_logger.info(
        "Clinical Quality - Dental - Oral Health Ed pushed to Tableau."
    )
