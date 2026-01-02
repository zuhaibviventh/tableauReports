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
sql_file = f"{directory}/co_cp_htn_cohort/sql/htn_cohort_nm.sql"
htn_cohort_logger = logger.setup_logger(
    "htn_cohort_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(htn_cohort_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = htn_cohort_logger
)

def run(shared_drive):
    htn_cohort_logger.info("Clinical Operations - Clinical Pharmacy HTN Cohort.")
    hyper_file = f"{shared_drive}/Clinical Pharmacy HTN Cohort_DEV.hyper"
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
            htn_cohort_logger.info("There are no data.")
            htn_cohort_logger.info("Clinical Operations - Clinical Pharmacy HTN Cohort Daily ETL finished.")
        else:
            tableau_push(dm_cohort_df, hyper_file)

    except ConnectionError as connection_error:
        htn_cohort_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        htn_cohort_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    htn_cohort_logger.info("Creating Hyper Table.")

    df["LATEST_SYSTOLIC"] = df["LATEST_SYSTOLIC"].astype(int)
    df["LATEST_DIASTOLIC"] = df["LATEST_DIASTOLIC"].astype(int)
    df["FIRST_SYSTOLIC"] = df["FIRST_SYSTOLIC"].astype(int)
    df["FIRST_DIASTOLIC"] = df["FIRST_DIASTOLIC"].astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Clinical Pharmacy HTN Cohort"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("LAST_READING", SqlType.date()),
            TableDefinition.Column("SECOND_LAST_READING", SqlType.date()),
            TableDefinition.Column("LATEST_SYSTOLIC", SqlType.int()),
            TableDefinition.Column("LATEST_DIASTOLIC", SqlType.int()),
            TableDefinition.Column("CURRENT_BP", SqlType.text()),
            TableDefinition.Column("FIRST_SYSTOLIC", SqlType.int()),
            TableDefinition.Column("FIRST_DIASTOLIC", SqlType.int()),
            TableDefinition.Column("ORIG_BP", SqlType.text()),
            TableDefinition.Column("ORIG_HIGH_BP", SqlType.int()),
            TableDefinition.Column("CURR_HIGH_BP", SqlType.int()),
            TableDefinition.Column("STATUS", SqlType.text()),
            TableDefinition.Column("MET_YN", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("ACTIVE_TO_COHORT", SqlType.text()),
            TableDefinition.Column("COHORT_ENROLL_DATE", SqlType.date())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=htn_cohort_logger,
        project_id=project_id
    )

    htn_cohort_logger.info(
        "Clinical Operations - Clinical Pharmacy HTN Cohort pushed to Tableau."
    )

