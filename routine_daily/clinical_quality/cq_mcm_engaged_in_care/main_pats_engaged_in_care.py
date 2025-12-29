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
sql_file = f"{directory}/cq_mcm_engaged_in_care/sql/pats_engaged_in_care.sql"
pats_engaged_in_care_logger = logger.setup_logger(
    "pats_engaged_in_care_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(pats_engaged_in_care_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = pats_engaged_in_care_logger
)

def run(shared_drive):
    pats_engaged_in_care_logger.info(
        "Clinical Quality - Medical - Patients Engaged in Care."
    )
    hyper_file = f"{shared_drive}/Medical - Patients Engaged in Care.hyper"
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
            pats_engaged_in_care_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(pats_engaged_in_care_df.index) == 0:
            pats_engaged_in_care_logger.info("There are no data.")
            pats_engaged_in_care_logger.info("Clinical Quality - Medical - Patients Engaged in Care Daily ETL finished.")
        else:
            tableau_push(pats_engaged_in_care_df, hyper_file)

    except ConnectionError as connection_error:
        pats_engaged_in_care_logger.error(f"Unable to connect to VH - Vivent Health: {connection_error}")
    except KeyError as key_error:
        pats_engaged_in_care_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    pats_engaged_in_care_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Medical - Patients Engaged in Care"),
        columns = [
            TableDefinition.Column("NEW_PT", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("FIRST_VISIT", SqlType.date()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("NUM_VISITS", SqlType.int()),
            TableDefinition.Column("MET_YN", SqlType.text()),
            TableDefinition.Column("MET_NUM", SqlType.int()),
            TableDefinition.Column("DAYS_AGO", SqlType.int())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=pats_engaged_in_care_logger,
        project_id=project_id
    )

    pats_engaged_in_care_logger.info(
        "Clinical Quality - Medical - Patients Engaged in Care pushed to Tableau."
    )
