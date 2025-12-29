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
sql_file = f"{directory}/cq_bh_new_pats_improve/sql/new_pats_improve.sql"
new_pats_improve_logger = logger.setup_logger(
    "new_pats_improve_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(new_pats_improve_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = new_pats_improve_logger
)

def run(shared_drive):
    new_pats_improve_logger.info(
        "Clinical Quality - BH - New Patients and Improvement in Symptoms at Six Months."
    )
    hyper_file = f"{shared_drive}/BH - New Patients and Improvement in Symptoms at Six Months.hyper"
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
            run_globals.run(clarity_connection)
            mdd_cssrs_df = connections.sql_to_df(sql_file, clarity_connection)

        if len(mdd_cssrs_df.index) == 0:
            new_pats_improve_logger.info("There are no data.")
            new_pats_improve_logger.info("Clinical Quality - BH - New Patients and Improvement in Symptoms at Six Months Daily ETL finished.")
        else:
            tableau_push(mdd_cssrs_df, hyper_file)

    except ConnectionError as connection_error:
        new_pats_improve_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        new_pats_improve_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    new_pats_improve_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("BH - New Patients and Improvement in Symptoms at Six Months"),
        columns = [
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Type", SqlType.text()),
            TableDefinition.Column("MAJOR DEPRESSIVE DISORDER", SqlType.text()),
            TableDefinition.Column("BHMH EPISODE START", SqlType.date()),
            TableDefinition.Column("EPISODE NEW IN MEASUREMENT PERIOD", SqlType.text()),
            TableDefinition.Column("Episode Status", SqlType.text()),
            TableDefinition.Column("PSYCHIATRIST", SqlType.text()),
            TableDefinition.Column("MH THERAPIST", SqlType.text()),
            TableDefinition.Column("Initial PHQ Date", SqlType.date()),
            TableDefinition.Column("Initial PHQ Score", SqlType.int()),
            TableDefinition.Column("Follow-up PHQ Date", SqlType.date()),
            TableDefinition.Column("Follow-up PHQ9 Score", SqlType.int()),
            TableDefinition.Column("Change", SqlType.double()),
            TableDefinition.Column("ZIP", SqlType.text()),
            TableDefinition.Column("RACE_CATEGORY", SqlType.text()),
            TableDefinition.Column("ETHNICITY_CATEGORY", SqlType.text()),
            TableDefinition.Column("Outcome", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=new_pats_improve_logger,
        project_id=project_id
    )

    new_pats_improve_logger.info(
        "Clinical Quality - BH - New Patients and Improvement in Symptoms at Six Months pushed to Tableau."
    )
