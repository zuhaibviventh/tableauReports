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
sql_file = f"{directory}/cq_dental_prophy_1/sql/dental_prophy_1.sql"
dental_prophy_1_logger = logger.setup_logger(
    "dental_prophy_1_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(dental_prophy_1_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = dental_prophy_1_logger
)

def run(shared_drive):
    dental_prophy_1_logger.info(
        "Clinical Quality - Dental - 1+ Prophy."
    )
    hyper_file = f"{shared_drive}/Dental - 1+ Prophy.hyper"
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
            dental_prophy_1_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(dental_prophy_1_df.index) == 0:
            dental_prophy_1_logger.info("There are no data.")
            dental_prophy_1_logger.info("Clinical Quality - Dental - 1+ Prophy Daily ETL finished.")
        else:
            tableau_push(dental_prophy_1_df, hyper_file)

    except ConnectionError as connection_error:
        dental_prophy_1_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        dental_prophy_1_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    dental_prophy_1_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Dental - 1+ Prophy"),
        columns = [
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("One_Plus_Prophy", SqlType.int()),
            TableDefinition.Column("MET_YN", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("VISIT_PROVIDER_TYPE", SqlType.text()),
            TableDefinition.Column("Visit Provider ID", SqlType.text()),
            TableDefinition.Column("Visit Provider Name", SqlType.text()),
            TableDefinition.Column("HOME_PHONE", SqlType.text()),
            TableDefinition.Column("WORK_PHONE", SqlType.text()),
            TableDefinition.Column("CELL_PHONE", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=dental_prophy_1_logger,
        project_id=project_id
    )

    dental_prophy_1_logger.info(
        "Clinical Quality - Dental - 1+ Prophy pushed to Tableau."
    )
