from msilib import Table
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
sql_file = f"{directory}/cq_dental_documented_treatment_plan/sql/documented_treatment_plan.sql"
documented_treatment_plan_logger = logger.setup_logger(
    "documented_treatment_plan_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(documented_treatment_plan_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = documented_treatment_plan_logger
)

def run(shared_drive):
    documented_treatment_plan_logger.info(
        "Clinical Quality - Dental - Documented Treatment Plan."
    )
    hyper_file = f"{shared_drive}/Dental - Documented Treatment Plan.hyper"
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
            documented_treatment_plan_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(documented_treatment_plan_df.index) == 0:
            documented_treatment_plan_logger.info("There are no data.")
            documented_treatment_plan_logger.info("Clinical Quality - Dental - Documented Treatment Plan Daily ETL finished.")
        else:
            tableau_push(documented_treatment_plan_df, hyper_file)

    except ConnectionError as connection_error:
        documented_treatment_plan_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        documented_treatment_plan_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    documented_treatment_plan_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Dental - Documented Treatment Plan"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("Has Treatment Plan", SqlType.text()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next Dental Appt", SqlType.date()),
            TableDefinition.Column("Next Dental Appt Prov", SqlType.text()),
            TableDefinition.Column("Performing Prov", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=documented_treatment_plan_logger,
        project_id=project_id
    )

    documented_treatment_plan_logger.info(
        "Clinical Quality - Dental - Documented Treatment Plan pushed to Tableau."
    )
