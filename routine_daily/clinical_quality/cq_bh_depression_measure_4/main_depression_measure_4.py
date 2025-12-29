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
sql_file = f"{directory}/cq_bh_depression_measure_4/sql/depression_measure_4.sql"
depression_measure_4 = logger.setup_logger(
    "depression_measure_4",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(depression_measure_4)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = depression_measure_4
)

def run(shared_drive):
    depression_measure_4.info(
        "Clinical Quality - Depression Measure 4."
    )
    hyper_file = f"{shared_drive}/Depression Measure 4.hyper"
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
            depression_measure_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(depression_measure_df.index) == 0:
            depression_measure_4.info("There are no data.")
            depression_measure_4.info("Clinical Quality - Depression Measure 4 Daily ETL finished.")
        else:
            tableau_push(depression_measure_df, hyper_file)

    except ConnectionError as connection_error:
        depression_measure_4.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        depression_measure_4.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    depression_measure_4.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Depression Measure 4"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("MET_YN", SqlType.int()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("CLICK HERE FOR PATIENT DETAIL", SqlType.text()),
            TableDefinition.Column("PSYCHIATRY", SqlType.text()),
            TableDefinition.Column("MH_TEAM_MEMBER", SqlType.text()),
            TableDefinition.Column("ORD_VALUE", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=depression_measure_4,
        project_id=project_id
    )

    depression_measure_4.info(
        "Clinical Quality - Depression Measure 4 pushed to Tableau."
    )
