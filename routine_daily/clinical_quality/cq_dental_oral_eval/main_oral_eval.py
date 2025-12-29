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
sql_file = f"{directory}/cq_dental_oral_eval/sql/oral_eval.sql"
oral_eval_logger = logger.setup_logger(
    "oral_eval_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(oral_eval_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = oral_eval_logger
)

def run(shared_drive):
    oral_eval_logger.info(
        "Clinical Quality - Dental - Oral Eval."
    )
    hyper_file = f"{shared_drive}/Dental - Oral Eval.hyper"
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
            oral_eval_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(oral_eval_df.index) == 0:
            oral_eval_logger.info("There are no data.")
            oral_eval_logger.info("Clinical Quality - Dental - Oral Eval Daily ETL finished.")
        else:
            tableau_push(oral_eval_df, hyper_file)

    except ConnectionError as connection_error:
        oral_eval_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        oral_eval_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    oral_eval_logger.info("Creating Hyper Table.")

    df["Months Since Oral Eval"] = df["Months Since Oral Eval"].astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Dental - Oral Eval"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("ORAL_EVAL", SqlType.int()),
            TableDefinition.Column("MET_YN", SqlType.text()),
            TableDefinition.Column("Oral Eval Date", SqlType.date()),
            TableDefinition.Column("Oral Eval Provider", SqlType.text()),
            TableDefinition.Column("Months Since Oral Eval", SqlType.int()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("LAST_OFFICE_VISIT", SqlType.date()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next Dental Appt", SqlType.date()),
            TableDefinition.Column("Next Dental Appt Prov", SqlType.text()),
            TableDefinition.Column("Has Diabetes", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp()),
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=oral_eval_logger,
        project_id=project_id
    )

    oral_eval_logger.info(
        "Clinical Quality - Dental - Oral Eval pushed to Tableau."
    )
