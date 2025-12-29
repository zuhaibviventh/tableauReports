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
sql_file = f"{directory}/cq_medical_tobacco_use/sql/tobacco_use.sql"
tobacco_use_logger = logger.setup_logger(
    "tobacco_use_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(tobacco_use_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = tobacco_use_logger
)

def run(shared_drive):
    tobacco_use_logger.info(
        "Clinical Quality - Tobacco Use."
    )
    hyper_file = f"{shared_drive}/Tobacco Use.hyper"
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            tobacco_use_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(tobacco_use_df.index) == 0:
            tobacco_use_logger.info("There are no data.")
            tobacco_use_logger.info("Clinical Quality - Tobacco Use Daily ETL finished.")
        else:
            tableau_push(tobacco_use_df, hyper_file)

    except ConnectionError as connection_error:
        tobacco_use_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        tobacco_use_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    tobacco_use_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Tobacco Use"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("SMOKER", SqlType.int()),
            TableDefinition.Column("MET_YN", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("IN CLINICAL PHARM COHORT", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=tobacco_use_logger,
        project_id=project_id
    )

    tobacco_use_logger.info(
        "Clinical Quality - Tobacco Use pushed to Tableau."
    )
