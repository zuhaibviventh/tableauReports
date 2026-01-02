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
sql_file = f"{directory}/co_cp_older_pats_no_a1c/sql/main_pats_no_a1c_nm.sql"
older_pats_no_a1c_logger = logger.setup_logger(
    "older_pats_no_a1c_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(older_pats_no_a1c_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = older_pats_no_a1c_logger
)

def run(shared_drive):
    older_pats_no_a1c_logger.info("Clinical Operations - Clinical Pharmacy - Patients Over 35 Without A1c.")

    hyper_file = f"{shared_drive}/Clinical Pharmacy - Patients Over 35 Without A1c_DEV.hyper"
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
            no_a1c_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(no_a1c_df.index) == 0:
            older_pats_no_a1c_logger.info("There are no data.")
            older_pats_no_a1c_logger.info("Clinical Operations - Clinical Pharmacy - Patients Over 35 Without A1c Daily ETL finished.")
        else:
            tableau_push(no_a1c_df, hyper_file)

    except ConnectionError as connection_error:
        older_pats_no_a1c_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        older_pats_no_a1c_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    older_pats_no_a1c_logger.info("Creating Hyper Table.")

    df["LAST BMI"] = df["LAST BMI"].fillna(0.0)
    df["ASCVD 10YR RISK"] = df["ASCVD 10YR RISK"].fillna(0.0)

    table_definition = TableDefinition(
        table_name = TableName("Clinical Pharmacy - Patients Over 35 Without A1c"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("LEGAL SEX", SqlType.text()),
            TableDefinition.Column("FPL%", SqlType.big_int()),
            TableDefinition.Column("LAST BMI", SqlType.double()),
            TableDefinition.Column("TOBACCO USER", SqlType.text()),
            TableDefinition.Column("ASCVD 10YR RISK", SqlType.double()),
            TableDefinition.Column("HTN", SqlType.text()),
            TableDefinition.Column("MONTHS SINCE FIRST MEDICAL VISIT", SqlType.int()),
            TableDefinition.Column("NEW PT IN LAST 30 DAYS", SqlType.text())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=older_pats_no_a1c_logger,
        project_id=project_id
    )

    older_pats_no_a1c_logger.info(
        "Clinical Operations - Clinical Pharmacy - Patients Over 35 Without A1c pushed to Tableau."
    )

