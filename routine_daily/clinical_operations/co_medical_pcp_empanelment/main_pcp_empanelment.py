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
sql_file = f"{directory}/co_medical_pcp_empanelment/sql/pcp_empanelment.sql"
pcp_empanelment_logger = logger.setup_logger(
    "pcp_empanelment_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(pcp_empanelment_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = pcp_empanelment_logger
)

def run(shared_drive):
    pcp_empanelment_logger.info("Clinical Operations - PCP Empanelment.")
    hyper_file = f"{shared_drive}/PCP Empanelment.hyper"
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
            pcp_empanelment_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(pcp_empanelment_df.index) == 0:
            pcp_empanelment_logger.info("There are no data.")
            pcp_empanelment_logger.info("Clinical Operations - PCP Empanelment Daily ETL finished.")
        else:
            tableau_push(pcp_empanelment_df, hyper_file)

    except ConnectionError as connection_error:
        pcp_empanelment_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        pcp_empanelment_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    pcp_empanelment_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("PCP Empanelment"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("PATIENT TYPE", SqlType.text()),
            TableDefinition.Column("Next Visit Provider", SqlType.text()),
            TableDefinition.Column("LAST VISIT", SqlType.date()),
            TableDefinition.Column("NEXT VISIT", SqlType.date()),
            TableDefinition.Column("MONTHS SINCE SEEN", SqlType.int()),
            TableDefinition.Column("Next Any Visit Provider", SqlType.text()),
            TableDefinition.Column("Next Any Visit", SqlType.date())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=pcp_empanelment_logger,
        project_id=project_id
    )

    pcp_empanelment_logger.info(
        "Clinical Operations - PCP Empanelment pushed to Tableau."
    )

