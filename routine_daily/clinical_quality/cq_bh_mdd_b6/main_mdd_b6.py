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
sql_file = f"{directory}/cq_bh_mdd_b6/sql/mdd_b6.sql"
mdd_b6_logger = logger.setup_logger(
    "mdd_b6_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(mdd_b6_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = mdd_b6_logger
)

def run(shared_drive):
    mdd_b6_logger.info(
        "Clinical Quality - BH - Patients with MDD Who Stay on Antidepressants for 90 days."
    )
    hyper_file = f"{shared_drive}/BH - Patients with MDD Who Stay on Antidepressants for 90 days.hyper"
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
            gad_7_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(gad_7_df.index) == 0:
            mdd_b6_logger.info("There are no data.")
            mdd_b6_logger.info("Clinical Quality - BH - Patients with MDD Who Stay on Antidepressants for 90 days Daily ETL finished.")
        else:
            tableau_push(gad_7_df, hyper_file)

    except ConnectionError as connection_error:
        mdd_b6_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        mdd_b6_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    mdd_b6_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("BH - Patients with MDD Who Stay on Antidepressants for 90 days"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("Psychiatrist", SqlType.text()),
            TableDefinition.Column("First RX Date", SqlType.date()),
            TableDefinition.Column("Measurement Period End (90 days)", SqlType.date()),
            TableDefinition.Column("Total Doses Prescribed", SqlType.double()),
            TableDefinition.Column("Days on Meds", SqlType.int()),
            TableDefinition.Column("Measurement Period Complete", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("ZIP", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=mdd_b6_logger,
        project_id=project_id
    )

    mdd_b6_logger.info(
        "Clinical Quality - BH - Patients with MDD Who Stay on Antidepressants for 90 days pushed to Tableau."
    )
