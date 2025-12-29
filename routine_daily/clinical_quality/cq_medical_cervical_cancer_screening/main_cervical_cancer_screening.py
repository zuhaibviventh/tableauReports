import os 
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tableauhyperapi import TableName, TableDefinition, SqlType
from global_sql import run_globals
from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau,
    emails
)

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}\\cq_medical_cervical_cancer_screening\\sql\\cervical_cancer_screening.sql"
cervical_cancer_screening_logger = logger.setup_logger(
    "cervical_cancer_screening_logger",
    f"{directory}\\logs\\main.log"
)

config = vh_config.grab(cervical_cancer_screening_logger)
project_id = vh_config.grab_tableau_id("Clinical Quality", cervical_cancer_screening_logger)

def run(shared_drive):
    cervical_cancer_screening_logger.info("Clinical Quality - Cervical Cancer Screening.")
    hyper_file = f"{shared_drive}\\Cervical Cancer Screening.hyper"
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
            run_globals.run(clarity_connection)
            cervical_cancer_screening_df = connections.sql_to_df(sql_file, clarity_connection)

        if len(cervical_cancer_screening_df.index) == 0:
            cervical_cancer_screening_logger.info("There are no data.")
            cervical_cancer_screening_logger.info("Clinical Quality - Cervical Cancer Screening Daily ETL finished.")
        else:
            tableau_push(cervical_cancer_screening_df, hyper_file)

    except ConnectionError as connection_error:
        cervical_cancer_screening_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        cervical_cancer_screening_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    cervical_cancer_screening_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Cervical Cancer Screening"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("PAP", SqlType.int()),
            TableDefinition.Column("MET_YN", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("NEXT PCP APPT", SqlType.date()),
            TableDefinition.Column("PCP APPT PROVIDER", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("GENDER IDENTITY", SqlType.text()),
            TableDefinition.Column("Enrolled in Any CP Cohort", SqlType.text()),
            TableDefinition.Column("RACE_CATEGORY", SqlType.text()),
            TableDefinition.Column("ETHNICITY_CATEGORY", SqlType.text()),
            TableDefinition.Column("UPDATED_DTTM", SqlType.timestamp())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=cervical_cancer_screening_logger,
        project_id=project_id
    )

    cervical_cancer_screening_logger.info(
        "Clinical Quality - Cervical Cancer Screening pushed to Tableau."
    )
