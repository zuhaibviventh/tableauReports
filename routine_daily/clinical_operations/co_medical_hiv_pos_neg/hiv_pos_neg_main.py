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
sql_file = f"{directory}/co_medical_hiv_pos_neg/sql/hiv_pos_neg.sql"
hiv_pos_neg_logger = logger.setup_logger(
    "hiv_pos_neg_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(hiv_pos_neg_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = hiv_pos_neg_logger
)

def run(shared_drive):
    hiv_pos_neg_logger.info(
        "Clinical Operations - Medical Patients not Indicated as HIV- or HIV+."
    )
    hyper_file = f"{shared_drive}/Medical Patients Not Indicated as HIV+ or HIV-.hyper"
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
            hiv_pos_neg_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(hiv_pos_neg_df.index) == 0:
            hiv_pos_neg_logger.info("There are no data.")
            hiv_pos_neg_logger.info("Clinical Operations - Medical Patients not Indicated as HIV- or HIV+ Daily ETL finished.")
        else:
            tableau_push(hiv_pos_neg_df, hyper_file)

    except ConnectionError as connection_error:
        hiv_pos_neg_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        hiv_pos_neg_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    hiv_pos_neg_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Medical Patients not Indicated as HIV- or HIV+"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("LAST_VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_SITE", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("RESPONSIBLE PERSON", SqlType.text()),
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
        logger=hiv_pos_neg_logger,
        project_id=project_id
    )

    hiv_pos_neg_logger.info(
        "Clinical Operations - Medical Patients not Indicated as HIV- or HIV+ pushed to Tableau."
    )

