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

sql_file = f"{directory}/cq_medical_htn_blood_pressure/sql/htn_blood_pressure.sql"
htn_blood_pressure_logger = logger.setup_logger("htn_blood_pressure_logger", 
    f"{directory}/logs/main.log")
config = vh_config.grab(htn_blood_pressure_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = htn_blood_pressure_logger
)

def run(shared_drive):
    htn_blood_pressure_logger.info(
        "Clinical Quality - HTN Blood Pressure."
    )
    hyper_file = f"{shared_drive}/HTN Blood Pressure.hyper"
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
            htn_blood_pressure_df = connections.sql_to_df(sql_file, clarity_connection)

        if len(htn_blood_pressure_df.index) == 0:
            htn_blood_pressure_logger.info("There are no data.")
            htn_blood_pressure_logger.info("Clinical Quality - HTN Blood Pressure Daily ETL finished.")
        else:
            tableau_push(htn_blood_pressure_df, hyper_file)

    except ConnectionError as connection_error:
        htn_blood_pressure_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        htn_blood_pressure_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    htn_blood_pressure_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("HTN Blood Pressure"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("PROV_NAME", SqlType.text()),
            TableDefinition.Column("LAST_BP", SqlType.date()),
            TableDefinition.Column("LATEST_SYSTOLIC", SqlType.double()),
            TableDefinition.Column("LATEST_DIASTOLIC", SqlType.double()),
            TableDefinition.Column("IN CLINICAL PARM COHORT", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("MET_YN", SqlType.int()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("IN DIETITIAN CARE", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text()),
            TableDefinition.Column("Active BH", SqlType.text()),
            TableDefinition.Column("Active Dental", SqlType.text()),
            TableDefinition.Column("LATEST_VISIT_PROV_TYPE", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=htn_blood_pressure_logger,
        project_id=project_id
    )

    htn_blood_pressure_logger.info(
        "Clinical Quality - HTN Blood Pressure pushed to Tableau."
    )
