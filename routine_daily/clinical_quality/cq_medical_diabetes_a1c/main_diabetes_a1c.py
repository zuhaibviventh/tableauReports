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
sql_file = f"{directory}/cq_medical_diabetes_a1c/sql/diabetes_a1c.sql"
diabetes_a1c_logger = logger.setup_logger("diabetes_a1c_logger", f"{directory}/logs/main.log")

config = vh_config.grab(diabetes_a1c_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = diabetes_a1c_logger
)

def run(shared_drive):
    diabetes_a1c_logger.info(
        "Clinical Quality - Diabetes A1c."
    )
    hyper_file = f"{shared_drive}/Diabetes A1c.hyper"
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
            diabetes_a1c_df = connections.sql_to_df(sql_file, clarity_connection)

        if len(diabetes_a1c_df.index) == 0:
            diabetes_a1c_logger.info("There are no data.")
            diabetes_a1c_logger.info("Clinical Quality - Diabetes A1c Daily ETL finished.")
        else:
            tableau_push(diabetes_a1c_df, hyper_file)

    except ConnectionError as connection_error:
        diabetes_a1c_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        diabetes_a1c_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    diabetes_a1c_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Diabetes A1c"),
        columns = [
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("LAST_A1c", SqlType.double()),
            TableDefinition.Column("RESULT_DATE", SqlType.date()),
            TableDefinition.Column("Controlled_A1C_<_7", SqlType.text()),
            TableDefinition.Column("Poor_A1C_8+", SqlType.text()),
            TableDefinition.Column("NEXT_APPT", SqlType.date()),
            TableDefinition.Column("NEXT_APPT_PROV", SqlType.text()),
            TableDefinition.Column("NEXT PCP APPT", SqlType.date()),
            TableDefinition.Column("PCP APPT PROVIDER", SqlType.text()),
            TableDefinition.Column("IN CLINICAL PHARMACY COHORT", SqlType.text()),
            TableDefinition.Column("IN DIETITIAN CARE", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("MINI_COG_COMPLETED", SqlType.text()),
            TableDefinition.Column("MINI_COG_SCORE", SqlType.text()),
            TableDefinition.Column("Had a Dental Visit(s)", SqlType.text()),
            TableDefinition.Column("HEALTH_MAINTENANCE_DUE_DATE", SqlType.date()),
            TableDefinition.Column("HEALTH_MAINTENANCE_TOPIC_NAME", SqlType.text()),
            TableDefinition.Column("HEALTH_MAINTENANCE_STATUS", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=diabetes_a1c_logger,
        project_id=project_id
    )

    diabetes_a1c_logger.info(
        "Clinical Quality - Diabetes A1c pushed to Tableau."
    )
