import os, os.path, json

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
sql_file = f"{directory}\\cq_medical_crc_screening\\sql\\crc_screening.sql"
crc_screening_logger = logger.setup_logger(
    "crc_screening_logger",
    f"{directory}\\logs\\main.log"
)

config = vh_config.grab(crc_screening_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = crc_screening_logger
)

def run(shared_drive):
    crc_screening_logger.info("Clinical Quality - CRC Screening.")
    hyper_file = f"{shared_drive}\\CRC Screening.hyper"
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
            crc_screening_df = connections.sql_to_df(sql_file, clarity_connection)

        if len(crc_screening_df.index) == 0:
            crc_screening_logger.info("There are no data.")
            crc_screening_logger.info("Clinical Quality - CRC Screening Daily ETL finished.")
        else:
            tableau_push(crc_screening_df, hyper_file)

    except ConnectionError as connection_error:
        crc_screening_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        crc_screening_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    crc_screening_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("CRC Screening"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("BIRTH_DATE", SqlType.date()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("last_medical_visit", SqlType.date()),
            TableDefinition.Column("MeetsCriteria", SqlType.text()),
            TableDefinition.Column("MeetsCriteriaDates", SqlType.text()),
            TableDefinition.Column("met_date", SqlType.date()),
            TableDefinition.Column("Exclusion_Reasons", SqlType.text()),
            TableDefinition.Column("Exclusion_Dates", SqlType.text()),
            TableDefinition.Column("Outcome", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("Report_Period_End", SqlType.date()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("FINANCIAL_CLASS", SqlType.text()),
            TableDefinition.Column("COLONOSCOPY REFERRAL DATE", SqlType.date()),
            TableDefinition.Column("FOBT_ORDER_DATE", SqlType.date()),
            TableDefinition.Column("FOBT_REUSLT_DATE", SqlType.date()),
            TableDefinition.Column("FitDNA_ORDER_DATE", SqlType.date()),
            TableDefinition.Column("FitDNA_RESULT_DATE", SqlType.date()),
            TableDefinition.Column("FIT_ORDER_DATE", SqlType.date()),
            TableDefinition.Column("FIT_RESULT_DATE", SqlType.date()),
            TableDefinition.Column("CLINICAL PHARMACY COHORT", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text()),
            TableDefinition.Column("CITY_SPECIFIC_GOALS", SqlType.double())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=crc_screening_logger,
        project_id=project_id
    )

    crc_screening_logger.info(
        "Clinical Quality - CRC Screening pushed to Tableau."
    )
