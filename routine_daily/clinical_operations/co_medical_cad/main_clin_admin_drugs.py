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
sql_file = f"{directory}/co_medical_cad/sql/clinic_administered_drugs.sql"
cad_logger = logger.setup_logger(
    "cad_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(cad_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = cad_logger
)

def run(shared_drive):
    cad_logger.info("Clinical Operations - Clinic Administered Drugs.")
    hyper_file = f"{shared_drive}/Clinic Administered Drugs.hyper"
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
            cad_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(cad_df.index) == 0:
            cad_logger.info("There are no data.")
            cad_logger.info("Clinical Operations - Clinic Administered Drugs Daily ETL finished.")
        else:
            tableau_push(cad_df, hyper_file)

    except ConnectionError as connection_error:
        cad_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        cad_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    cad_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Clinic Administered Drugs"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT_NAME", SqlType.text()),
            TableDefinition.Column("DOB", SqlType.date()),
            TableDefinition.Column("VISIT_DATE", SqlType.date()),
            TableDefinition.Column("MEDICATION_NAME", SqlType.text()),
            TableDefinition.Column("MEDICATION_ORDERED_DATE", SqlType.date()),
            TableDefinition.Column("MEDICATION_TAKEN_TIME", SqlType.date()),
            TableDefinition.Column("MEDICATION_ADMINISTERED_BY", SqlType.text()),
            TableDefinition.Column("UNITS", SqlType.text()),   
            TableDefinition.Column("NDC_CODE", SqlType.text()),
            TableDefinition.Column("Charge Description", SqlType.text()),
            TableDefinition.Column("CPT_CODE", SqlType.text()),
            TableDefinition.Column("VISIT_PROVIDER_NAME", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("FINANCIAL_CLASS", SqlType.text()),
            TableDefinition.Column("PAYOR", SqlType.text()),
            TableDefinition.Column("TOTAL_CHARGE_AMOUNT", SqlType.double()),
            TableDefinition.Column("CPT Charge Amount", SqlType.double())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=cad_logger,
        project_id=project_id
    )

    cad_logger.info(
        "Clinical Operations - Clinic Administered Drugs pushed to Tableau."
    )

