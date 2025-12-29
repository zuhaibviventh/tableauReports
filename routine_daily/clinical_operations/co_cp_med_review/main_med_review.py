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
sql_file = f"{directory}/co_cp_med_review/sql/med_review.sql"
med_review_logger = logger.setup_logger(
    "med_review_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(med_review_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = med_review_logger
)

def run(shared_drive):
    med_review_logger.info("Clinical Operations - Clinical Pharmacy Med Review.")
    hyper_file = f"{shared_drive}/Clinical Pharmacy Med Review.hyper"
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
            med_review_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(med_review_df.index) == 0:
            med_review_logger.info("There are no data.")
            med_review_logger.info("Clinical Operations - Clinical Pharmacy Med Review Daily ETL finished.")
        else:
            tableau_push(med_review_df, hyper_file)

    except ConnectionError as connection_error:
        med_review_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        med_review_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    med_review_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Clinical Pharmacy Med Review"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("MED REVIEW BY PHARMACIST#", SqlType.int()),
            TableDefinition.Column("MED REVIEW BY PHARMACIST", SqlType.text()),
            TableDefinition.Column("PHARMACIST", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("FINANCIAL_CLASS_NAME", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=med_review_logger,
        project_id=project_id
    )

    med_review_logger.info(
        "Clinical Operations - Clinical Pharmacy Med Review pushed to Tableau."
    )

