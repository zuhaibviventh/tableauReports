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
sql_file = f"{directory}/cq_mcm_npo/sql/npo.sql"
npo_logger = logger.setup_logger(
    "npo_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(npo_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = npo_logger
)

def run(shared_drive):
    npo_logger.info(
        "Clinical Quality - Medical - New Patient Orientation."
    )
    hyper_file = f"{shared_drive}/Medical - New Patient Orientation.hyper"
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
            npo_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(npo_df.index) == 0:
            npo_logger.info("There are no data.")
            npo_logger.info("Clinical Quality - Medical - New Patient Orientation Daily ETL finished.")
        else:
            tableau_push(npo_df, hyper_file)

    except ConnectionError as connection_error:
        npo_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        npo_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    npo_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Medical - New Patient Orientation"),
        columns = [
            TableDefinition.Column("NEW PT MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("NEW_PT_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("APPT_MADE_DATE", SqlType.date()),
            TableDefinition.Column("APPT_CREATED_BY", SqlType.text()),
            TableDefinition.Column("APPT_STATUS", SqlType.text()),
            TableDefinition.Column("HAD_NPO", SqlType.text()),
            TableDefinition.Column("NPO_Provider", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("NEW PATIENT TYPE", SqlType.text()),
            TableDefinition.Column("Outreach Attempt Count", SqlType.int()),
            TableDefinition.Column("Outreach Attempt YN", SqlType.text()),
            TableDefinition.Column("Outreach Successful", SqlType.text()),
            TableDefinition.Column("LAST_OUTREACH_ATTEMPT", SqlType.date()),
            TableDefinition.Column("NPO Type", SqlType.text()),
            TableDefinition.Column("Any SmartPhrase/List Use", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=npo_logger,
        project_id=project_id
    )

    npo_logger.info(
        "Clinical Quality - Medical - New Patient Orientation pushed to Tableau."
    )
