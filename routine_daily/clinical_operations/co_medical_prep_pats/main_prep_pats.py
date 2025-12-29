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
sql_file = f"{directory}/co_medical_prep_pats/sql/prep_registry_report_patient_summary.sql"
prep_pats_logger = logger.setup_logger(
    "prep_pats_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(prep_pats_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = prep_pats_logger
)

def run(shared_drive):
    prep_pats_logger.info("Clinical Operations - PrEP Patients with Last Visit.")
    hyper_file = f"{shared_drive}/PrEP Patients with Last Visit.hyper"
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
            prep_pats_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(prep_pats_df.index) == 0:
            prep_pats_logger.info("There are no data.")
            prep_pats_logger.info("Clinical Operations - PrEP Patients with Last Visit Daily ETL finished.")
        else:
            tableau_push(prep_pats_df, hyper_file)

    except ConnectionError as connection_error:
        prep_pats_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        prep_pats_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    prep_pats_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("PrEP Patients with Last Visit"),
        columns = [
            TableDefinition.Column("Patient_Name", SqlType.text()),
            TableDefinition.Column("Current_Truvada_Med", SqlType.int()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("DOB", SqlType.date()),
            TableDefinition.Column("Current_Age", SqlType.int()),
            TableDefinition.Column("Gender", SqlType.text()),
            TableDefinition.Column("Patient_Race", SqlType.text()),
            TableDefinition.Column("Ethnicity", SqlType.text()),
            TableDefinition.Column("Last HIV Lab Date", SqlType.date()),
            TableDefinition.Column("Last HIV Lab Result", SqlType.text()),
            TableDefinition.Column("MR_Contact_DEPT_NAME", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("Last_Visit", SqlType.date()),
            TableDefinition.Column("SA64 PrEP", SqlType.text()),
            TableDefinition.Column("SITE", SqlType.text()),
            TableDefinition.Column("Last_PrEP_Retention_Selection", SqlType.text()),
            TableDefinition.Column("Last_PrEP_Retention_Selection_Visit_Date", SqlType.date()),
            TableDefinition.Column("Last_PrEP_Non_Retention_Reason", SqlType.text()),
            TableDefinition.Column("Last_PrEP_Non_Retention_Reason_Visit_Date", SqlType.date()),
            TableDefinition.Column("Next_Visit", SqlType.date()),
            TableDefinition.Column("CURRENT_PCP_VAME", SqlType.text()),
            TableDefinition.Column("PAT_PREP_STATUS", SqlType.text()),
            TableDefinition.Column("Report_Period_End", SqlType.date()),
            TableDefinition.Column("CURRENT_ZIP_CODE", SqlType.text()),
            TableDefinition.Column("GENDER_IDENTITY", SqlType.text()),
            TableDefinition.Column("MONTHS SINCE SEEN", SqlType.int())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=prep_pats_logger,
        project_id=project_id
    )

    prep_pats_logger.info(
        "Clinical Operations - PrEP Patients with Last Visit pushed to Tableau."
    )

