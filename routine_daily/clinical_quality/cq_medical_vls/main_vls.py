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
sql_file = f"{directory}/cq_medical_vls/sql/vls.sql"
vls_logger = logger.setup_logger(
    "vls_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(vls_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = vls_logger
)

def run(shared_drive):
    vls_logger.info(
        "Clinical Quality - Viral Load Suppression."
    )
    hyper_file = f"{shared_drive}/Vial Load Suppression.hyper"
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
            vls_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(vls_df.index) == 0:
            vls_logger.info("There are no data.")
            vls_logger.info("Clinical Quality - Viral Load Suppression Daily ETL finished.")
        else:
            tableau_push(vls_df, hyper_file)

    except ConnectionError as connection_error:
        vls_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        vls_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    vls_logger.info("Creating Hyper Table.")

    df["Result_Output"] = df["Result_Output"].astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Viral Load Suppression"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("DOB", SqlType.date()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("ZIP", SqlType.text()),
            TableDefinition.Column("GENDER", SqlType.text()),
            TableDefinition.Column("IN_CARE", SqlType.int()),
            TableDefinition.Column("PREFERRED LANGUAGE", SqlType.text()),
            TableDefinition.Column("COUNTY", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("LAST_VL", SqlType.text()),
            TableDefinition.Column("LAST_LAB", SqlType.date()),
            TableDefinition.Column("Result_Output", SqlType.int()),
            TableDefinition.Column("SUPPRESSED", SqlType.int()),
            TableDefinition.Column("VLS_CATEGORY", SqlType.text()),
            TableDefinition.Column("VLS_DURABILITY", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("SEX", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("DISPARITY_RACE", SqlType.text()),
            TableDefinition.Column("Report_Date", SqlType.date()),
            TableDefinition.Column("CLINICAL PHARMACY COHORT", SqlType.text()),
            TableDefinition.Column("DENTAL/BH Status", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text()),
            TableDefinition.Column("Sexual Orientation", SqlType.text()),
            TableDefinition.Column("FPL Detail", SqlType.double()),
            TableDefinition.Column("FPL Category", SqlType.text()),
            TableDefinition.Column("Homelessness", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=vls_logger,
        project_id=project_id
    )

    vls_logger.info(
        "Clinical Quality - Viral Load Suppression pushed to Tableau."
    )
