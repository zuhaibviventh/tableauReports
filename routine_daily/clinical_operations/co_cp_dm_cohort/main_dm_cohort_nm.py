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
sql_file = f"{directory}/co_cp_dm_cohort/sql/dm_cohort_nm.sql"
dm_cohort_logger = logger.setup_logger(
    "dm_cohort_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(dm_cohort_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = dm_cohort_logger
)

def run(shared_drive):
    dm_cohort_logger.info("Clinical Operations - Clinical Pharmacy DM Cohort.")
    hyper_file = f"{shared_drive}/Clinical Pharmacy DM Cohort DEV.hyper"
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
            dm_cohort_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(dm_cohort_df.index) == 0:
            dm_cohort_logger.info("There are no data.")
            dm_cohort_logger.info("Clinical Operations - Clinical Pharmacy DM Cohort Daily ETL finished.")
        else:
            tableau_push(dm_cohort_df, hyper_file)

    except ConnectionError as connection_error:
        dm_cohort_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        dm_cohort_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    dm_cohort_logger.info("Creating Hyper Table.")

    df["LAST_A1c"] = df["LAST_A1c"].fillna(0).astype(float)
    df["FIRST_A1c"] = df["FIRST_A1c"].fillna(0).astype(float)

    table_definition = TableDefinition(
        table_name = TableName("Clinical Pharmacy DM Cohort"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("A1c", SqlType.double()),
            TableDefinition.Column("A1c_Date", SqlType.date()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()), #SERVICE_TYPE
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("COHORT_STATUS", SqlType.text()),
            TableDefinition.Column("COHORT_ENROLL_DATE", SqlType.date()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("COUNTY", SqlType.text()),
            TableDefinition.Column("ADDRESS", SqlType.text()),
            TableDefinition.Column("INACTIVE_DATE", SqlType.date()),
            TableDefinition.Column("LAST_UPDATE_DATE", SqlType.date()),
            TableDefinition.Column("LAST_A1c", SqlType.double()),
            TableDefinition.Column("LAST_A1c_Date", SqlType.date()),
            TableDefinition.Column("FIRST_A1c", SqlType.double()),
            TableDefinition.Column("FIRST_A1c_Date", SqlType.date())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=dm_cohort_logger,
        project_id=project_id
    )

    dm_cohort_logger.info(
        "Clinical Operations - Clinical Pharmacy DM Cohort pushed to Tableau."
    )

