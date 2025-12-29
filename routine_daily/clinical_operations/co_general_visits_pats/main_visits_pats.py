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
sql_file = f"{directory}/co_general_visits_pats/sql/visits_pats.sql"
visits_pats_logger = logger.setup_logger(
    "visits_pats_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(visits_pats_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = visits_pats_logger
)

def run(shared_drive):
    visits_pats_logger.info("Clinical Operations - Visits and Patients.")
    hyper_file = f"{shared_drive}/Visits and Patients.hyper"
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
            visits_pats_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(visits_pats_df.index) == 0:
            visits_pats_logger.info("There are no data.")
            visits_pats_logger.info("Clinical Operations - Visits and Patients Daily ETL finished.")
        else:
            tableau_push(visits_pats_df, hyper_file)

    except ConnectionError as connection_error:
        visits_pats_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        visits_pats_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    visits_pats_logger.info("Creating Hyper Table.")

    df["PAT_ENC_CSN_ID"] = pd \
        .to_numeric(df["PAT_ENC_CSN_ID"], errors = "coerce") \
        .fillna(0) \
        .astype(int)

    df["EMPLOYMENT STATUS CODE"] = pd \
        .to_numeric(df["EMPLOYMENT STATUS CODE"], errors = "coerce") \
        .fillna(0) \
        .astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Visits and Patients"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("APPT_STATUS", SqlType.text()),
            TableDefinition.Column("APPT STATUS RAW", SqlType.text()),
            TableDefinition.Column("VISIT DATE", SqlType.date()),
            TableDefinition.Column("VISIT PROVIDER", SqlType.text()),
            TableDefinition.Column("PROVIDER TYPE", SqlType.text()),
            TableDefinition.Column("Access Provider Groups", SqlType.text()),
            TableDefinition.Column("PROV_ID", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("AGE RANGE", SqlType.text()),
            TableDefinition.Column("SEX", SqlType.text()),
            TableDefinition.Column("PATIENT TYPE", SqlType.text()),
            TableDefinition.Column("EVER STI", SqlType.text()),
            TableDefinition.Column("APPT_PRC_ID", SqlType.text()),
            TableDefinition.Column("PRC_NAME", SqlType.text()),
            TableDefinition.Column("PATIENT COUNTY", SqlType.text()),
            TableDefinition.Column("PATIENT ZIP CODE", SqlType.text()),
            TableDefinition.Column("PROV_TYPE", SqlType.text()),
            TableDefinition.Column("PAT_ENC_CSN_ID", SqlType.int()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("FPL", SqlType.double()),
            TableDefinition.Column("FPL_INCOME", SqlType.double()),
            TableDefinition.Column("GENDER IDENTITY", SqlType.text()),
            TableDefinition.Column("SEX ASSGN AT BIRTH", SqlType.text()),
            TableDefinition.Column("SEXUAL ORIENTATION", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp()),
            TableDefinition.Column("EMPLOYMENT STATUS CODE", SqlType.int()),
            TableDefinition.Column("EMPLOYMENT STATUS NAME", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=visits_pats_logger,
        project_id=project_id
    )

    visits_pats_logger.info(
        "Clinical Operations - Visits and Patients pushed to Tableau."
    )

