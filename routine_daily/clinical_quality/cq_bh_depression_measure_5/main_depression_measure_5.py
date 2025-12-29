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
sql_file = f"{directory}/cq_bh_depression_measure_5/sql/depression_measure_5.sql"
depression_measure_5 = logger.setup_logger(
    "depression_measure_5",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(depression_measure_5)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = depression_measure_5
)

def run(shared_drive):
    depression_measure_5.info(
        "Clinical Quality - Depression Measure 5."
    )
    hyper_file = f"{shared_drive}/Depression Measure 5.hyper"
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
            depression_measure_5.info("There are no data.")
            depression_measure_5.info("Clinical Quality - Depression Measure 5 Daily ETL finished.")
        else:
            tableau_push(depression_measure_df, hyper_file)

    except ConnectionError as connection_error:
        depression_measure_5.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        depression_measure_5.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    depression_measure_5.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Depression Measure 5"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("LAST_PHQ", SqlType.date()),
            TableDefinition.Column("Value", SqlType.int()),
            TableDefinition.Column("Greater_Than_9", SqlType.int()),
            TableDefinition.Column("Less_Than_10", SqlType.int()),
            TableDefinition.Column("PHQ_STATUS", SqlType.text()),
            TableDefinition.Column("MEAS_VALUE", SqlType.text()),
            TableDefinition.Column("MH_EPISODE", SqlType.text()),
            TableDefinition.Column("ACTIVE_MEDICAL_PT", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("CLICK HERE FOR PATIENT DETAIL", SqlType.text()),
            TableDefinition.Column("PSYCHIATRY", SqlType.text()),
            TableDefinition.Column("MH_TEAM_MEMBER", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=depression_measure_5,
        project_id=project_id
    )

    depression_measure_5.info(
        "Clinical Quality - Depression Measure 5 pushed to Tableau."
    )
