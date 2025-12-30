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
sql_file = f"{directory}/co_bh_no_hiv_pl/sql/bh_dent_hiv_pl_nm.sql"
hiv_pl_logger = logger.setup_logger(
    "bh_dent_hiv_pl_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(hiv_pl_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = hiv_pl_logger
)

def run(shared_drive):
    hiv_pl_logger.info("Clinical Operations - BH and Dental Patients Without HIV on Their Problem List.")

    hyper_file = f"{shared_drive}/BH and Dental Patients Without HIV on Their Problem List_DEV.hyper"
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
            hiv_pl_logger.info("There are no data.")
            hiv_pl_logger.info("Clinical Operations - BH and Dental Patients Without HIV on Their Problem List Daily ETL finished.")
        else:
            tableau_push(dental_episodes_df, hyper_file)

    except ConnectionError as connection_error:
        hiv_pl_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        hiv_pl_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    hiv_pl_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("BH and Dental Patients Without HIV on Their Problem List"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("PATIENT TYPE", SqlType.text()),
            TableDefinition.Column("Next Dental Appt", SqlType.text()),
            TableDefinition.Column("Next Dental Prov", SqlType.text()),
            TableDefinition.Column("Next BH Appt", SqlType.text()),
            TableDefinition.Column("Next BH Appt Prov", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("Last BH Appt", SqlType.text()),
            TableDefinition.Column("Last BH Appt Prov", SqlType.text()),
            TableDefinition.Column("Last Dental Appt", SqlType.text()),
            TableDefinition.Column("Last Dental Prov", SqlType.text())
        ]
    )

    hiv_pl_logger.info(
        "Clinical Operations - BH and Dental Patients Without HIV on Their Problem List."
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=hiv_pl_logger,
        project_id=project_id
    )

    hiv_pl_logger.info(
        "Clinical Operations - BH and Dental Patients Without HIV on Their Problem List pushed to Tableau."
    )

