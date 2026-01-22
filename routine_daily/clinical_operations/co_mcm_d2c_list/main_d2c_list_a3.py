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
sql_file = f"{directory}/co_mcm_d2c_list/sql/d2c_list_nm3.sql"
d2c_list_logger = logger.setup_logger(
    "d2c_list_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(d2c_list_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = d2c_list_logger
)

def run(shared_drive):
    d2c_list_logger.info("Clinical Operations - Medical - Data-to-Care List.")
    hyper_file = f"{shared_drive}/Medical - Data-to-Care List.hyper"
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
            d2c_list_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(d2c_list_df.index) == 0:
            d2c_list_logger.info("There are no data.")
            d2c_list_logger.info("Clinical Operations - Medical - Data-to-Care List Daily ETL finished.")
        else:
            tableau_push(d2c_list_df, hyper_file)

    except ConnectionError as connection_error:
        d2c_list_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        d2c_list_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    d2c_list_logger.info("Creating Hyper Table.")

    df["Viral Load"] = df["Viral Load"].astype(int).fillna(0)
    df["MONTHS SINCE OOC"] = df["MONTHS SINCE OOC"].astype(int).fillna(0)

    table_definition = TableDefinition(
        table_name = TableName("Medical - Data-to-Care List"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("BIRTH DATE", SqlType.date()),
            TableDefinition.Column("Reason", SqlType.text()),
            TableDefinition.Column("CURRENT PCP", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("SEX", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("PRIMARY LOCATION", SqlType.text()),
            TableDefinition.Column("Viral Load", SqlType.int()),
            TableDefinition.Column("LTFU_DATE", SqlType.date()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("SITE", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("LAST VISIT BEFORE OOC", SqlType.date()),
            TableDefinition.Column("MONTHS SINCE OOC", SqlType.int()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=d2c_list_logger,
        project_id=project_id
    )

    d2c_list_logger.info(
        "Clinical Operations - Medical - Data-to-Care List pushed to Tableau."
    )
