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
sql_file = f"{directory}/co_dental_perio_missed_opps/sql/perio_missed_opps_nm2.sql"
missed_opps_logger = logger.setup_logger(
    "missed_opps_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(missed_opps_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = missed_opps_logger
)

def run(shared_drive):
    missed_opps_logger.info("Clinical Operations - Dental - Perio Disease Coding Missed Opportunities.")

    hyper_file = f"{shared_drive}/Dental - Perio Disease Coding Missed Opportunities DEV.hyper"
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
            lower_dentures_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(lower_dentures_df.index) == 0:
            missed_opps_logger.info("There are no data.")
            missed_opps_logger.info("Clinical Operations - Dental - Perio Disease Coding Missed Opportunities Daily ETL finished.")
        else:
            tableau_push(lower_dentures_df, hyper_file)

    except ConnectionError as connection_error:
        missed_opps_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        missed_opps_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    missed_opps_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Dental - Perio Disease Coding Missed Opportunities"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("PROVIDER", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("VISIT_DATE", SqlType.date()),
            TableDefinition.Column("PERIO_CODE_USED", SqlType.text())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=missed_opps_logger,
        project_id=project_id
    )

    missed_opps_logger.info(
        "Clinical Operations - Dental - Perio Disease Coding Missed Opportunities pushed to Tableau."
    )
