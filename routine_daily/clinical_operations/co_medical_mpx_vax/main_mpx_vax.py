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
sql_file = f"{directory}/co_medical_mpx_vax/sql/mpx_vax.sql"
mpx_vax_logger = logger.setup_logger("mpx_vax_logger", f"{directory}/logs/main.log")

config = vh_config.grab(mpx_vax_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = mpx_vax_logger
)

def run(shared_drive):
    mpx_vax_logger.info("Clinical Operations - Monkeypox Vaccines.")
    hyper_file = f"{shared_drive}/Monkeypox Vaccines.hyper"
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
            mpx_vax_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(mpx_vax_df.index) == 0:
            mpx_vax_logger.info("There are no data.")
            mpx_vax_logger.info("Clinical Operations - Monkeypox Vaccines Daily ETL finished.")
        else:
            tableau_push(mpx_vax_df, hyper_file)

    except ConnectionError as connection_error:
        mpx_vax_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        mpx_vax_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    mpx_vax_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Monkeypox Vaccines"),
        columns = [
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("Vaccine Date", SqlType.date()),
            TableDefinition.Column("Historical", SqlType.text()),
            TableDefinition.Column("Given By", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=mpx_vax_logger,
        project_id=project_id
    )

    mpx_vax_logger.info(
        "Clinical Operations - Monkeypox Vaccines pushed to Tableau."
    )

