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
sql_file = f"{directory}/cq_medical_breast_cancer_screening/sql/breast_cancer_screening.sql"
breast_cancer_screening_logger = logger.setup_logger(
    "breast_cancer_screening_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(breast_cancer_screening_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = breast_cancer_screening_logger
)

def run(shared_drive):
    breast_cancer_screening_logger.info(
        "Clinical Quality - Breast Cancer Screening."
    )
    hyper_file = f"{shared_drive}/Breast Cancer Screening.hyper"
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
            breast_cancer_screening_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(breast_cancer_screening_df.index) == 0:
            breast_cancer_screening_logger.info("There are no data.")
            breast_cancer_screening_logger.info("Clinical Quality - Breast Cancer Screening Daily ETL finished.")
        else:
            tableau_push(breast_cancer_screening_df, hyper_file)

    except ConnectionError as connection_error:
        breast_cancer_screening_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        breast_cancer_screening_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    breast_cancer_screening_logger.info("Creating Hyper Table.")

    df["Age"] = df["Age"].astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Breast Cancer Screening"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("Age", SqlType.int()),
            TableDefinition.Column("Screened for Breast Cancer", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("PCP Appt Provider", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("ZIP", SqlType.text()),
            TableDefinition.Column("UPDATED_DTTM", SqlType.timestamp())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=breast_cancer_screening_logger,
        project_id=project_id
    )

    breast_cancer_screening_logger.info(
        "Clinical Quality - Breast Cancer Screening pushed to Tableau."
    )
