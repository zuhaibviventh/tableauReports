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
sql_file = f"{directory}/cq_medical_diabetes_microalb/sql/diabetes_microalb.sql"
diabetes_microalb_logger = logger.setup_logger(
    "diabetes_microalb_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(diabetes_microalb_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = diabetes_microalb_logger
)

def run(shared_drive):
    diabetes_microalb_logger.info("Clinical Quality - Medical - Patients with Diabetes and MicroAlb.")
    hyper_file = f"{shared_drive}/Medical - Patients with Diabetes and MicroAlb.hyper"
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
            diabetes_microalb_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(diabetes_microalb_df.index) == 0:
            diabetes_microalb_logger.info("There are no data.")
            diabetes_microalb_logger.info("Clinical Quality - Medical - Patients with Diabetes and MicroAlb Daily ETL finished.")
        else:
            tableau_push(diabetes_microalb_df, hyper_file)

    except ConnectionError as connection_error:
        diabetes_microalb_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        diabetes_microalb_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    diabetes_microalb_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Medical - Patients with Diabetes and MicroAlb"),
        columns = [
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("Last Urine MicroAlb Date", SqlType.date()),
            TableDefinition.Column("Months Ago", SqlType.int()),
            TableDefinition.Column("MET YN", SqlType.text()),
            TableDefinition.Column("MET NUMBER", SqlType.int()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("NEXT_APPT", SqlType.date()),
            TableDefinition.Column("NEXT_APPT_PROV", SqlType.text()),
            TableDefinition.Column("NEXT_PCP_APPT", SqlType.date()),
            TableDefinition.Column("PCP APPT PROVIDER", SqlType.text()),
            TableDefinition.Column("IN CLINICAL PHARMACY COHORT", SqlType.text()),
            TableDefinition.Column("IN DIETITIAN CARE", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=diabetes_microalb_logger,
        project_id=project_id
    )

    diabetes_microalb_logger.info(
        "Clinical Quality - Medical - Patients with Diabetes and MicroAlb pushed to Tableau."
    )
