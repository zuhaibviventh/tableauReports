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
sql_file = f"{directory}/cq_bh_sud_pats_remission/sql/sud_pats_remission.sql"
sud_remission_logger = logger.setup_logger(
    "sud_remission_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(sud_remission_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = sud_remission_logger
)

def run(shared_drive):
    sud_remission_logger.info(
        "Clinical Quality - BH SUD - Discharged Patients With or Without Remission."
    )
    hyper_file = f"{shared_drive}/BH SUD - Discharged Patients With or Without Remission.hyper"
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
            sud_remission_df = connections.sql_to_df(sql_file, clarity_connection)

        if len(sud_remission_df.index) == 0:
            sud_remission_logger.info("There are no data.")
            sud_remission_logger.info("Clinical Quality - BH SUD - Discharged Patients With or Without Remission Daily ETL finished.")
        else:
            tableau_push(sud_remission_df, hyper_file)

    except ConnectionError as connection_error:
        sud_remission_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        sud_remission_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    sud_remission_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("BH SUD - Discharged Patients With or Without Remission"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("SUD Discharge Date", SqlType.date()),
            TableDefinition.Column("Episode Start Date", SqlType.date()),
            TableDefinition.Column("Months in Program", SqlType.int()),
            TableDefinition.Column("Year of Discharge", SqlType.int()),
            TableDefinition.Column("Remission Code Used", SqlType.text()),
            TableDefinition.Column("Race", SqlType.text()),
            TableDefinition.Column("Ethnicity", SqlType.text()),
            TableDefinition.Column("Age", SqlType.int()),
            TableDefinition.Column("Gender Identity", SqlType.text()),
            TableDefinition.Column("State", SqlType.text()),
            TableDefinition.Column("City", SqlType.text()),
            TableDefinition.Column("ZIP", SqlType.text()), # PAT_FLAG_TYPE_C
            TableDefinition.Column("PAT_FLAG_TYPE_C", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=sud_remission_logger,
        project_id=project_id
    )

    sud_remission_logger.info(
        "Clinical Quality - BH SUD - Discharged Patients With or Without Remission pushed to Tableau."
    )
