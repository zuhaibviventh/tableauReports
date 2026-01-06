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
sql_file = f"{directory}/co_general_new_pats_dx/sql/new_pat_dx_nm2.sql"
new_pats_logger = logger.setup_logger(
    "new_pats_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(new_pats_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = new_pats_logger
)

def run(shared_drive):
    new_pats_logger.info("Clinical Operations - New Patient Visits Scheduled.")
    hyper_file = f"{shared_drive}/New Patient Visits Scheduled.hyper"
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
            new_pat_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(new_pat_df.index) == 0:
            new_pats_logger.info("There are no data.")
            new_pats_logger.info("Clinical Operations - New Patient Visits Scheduled Daily ETL finished.")
        else:
            tableau_push(new_pat_df, hyper_file)

    except ConnectionError as connection_error:
        new_pats_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        new_pats_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    new_pats_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("New Patient Visits Scheduled"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("Visit Date", SqlType.date()),
            TableDefinition.Column("Visit Provider", SqlType.text()),
            TableDefinition.Column("Visit Type", SqlType.text()),
            TableDefinition.Column("Date Appointment Created", SqlType.date()),
            TableDefinition.Column("Days Until Appt", SqlType.int()),
            TableDefinition.Column("Days Since Appt Created", SqlType.int())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=new_pats_logger,
        project_id=project_id
    )

    new_pats_logger.info(
        "Clinical Operations - New Patient Visits Scheduled pushed to Tableau."
    )

