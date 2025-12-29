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
sql_file = f"{directory}/co_medical_rncm_prog_eval/sql/rncm_prog_eval.sql"
rncm_prog_eval_logger = logger.setup_logger(
    "rncm_prog_eval_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(rncm_prog_eval_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = rncm_prog_eval_logger
)

def run(shared_drive):
    rncm_prog_eval_logger.info("Clinical Operations - RN CM Program Evaluation.")
    hyper_file = f"{shared_drive}/RN Case Manager Program Evaluation.hyper"
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
            rncm_prog_eval_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(rncm_prog_eval_df.index) == 0:
            rncm_prog_eval_logger.info("There are no data.")
            rncm_prog_eval_logger.info("Clinical Operations - RN Case Manager Program Evaluation Daily ETL finished.")
        else:
            tableau_push(rncm_prog_eval_df, hyper_file)

    except ConnectionError as connection_error:
        rncm_prog_eval_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        rncm_prog_eval_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    rncm_prog_eval_logger.info("Creating Hyper Table.")

    df["CITY"] = df["CITY"].str.title()
    df["STATE"] = df["STATE"].str.title()
    
    df["PAT_MRN"] = df["PAT_MRN"].astype(str)

    df["FIRST_VIRAL_LOAD"] = df["FIRST_VIRAL_LOAD"].astype(int)
    df["LAST_VIRAL_LOAD"] = df["LAST_VIRAL_LOAD"].astype(int)

    df["BIRTH_DATE"] = pd.to_datetime(df["BIRTH_DATE"])
    df["ENROLLMENT_DATE"] = pd.to_datetime(df["ENROLLMENT_DATE"])
    df["SMART_PHRASE_USE_DATE"] = pd.to_datetime(df["SMART_PHRASE_USE_DATE"])
    df["FIRST_VIRAL_LOAD_DATE"] = pd.to_datetime(df["FIRST_VIRAL_LOAD_DATE"])
    df["LAST_VIRAL_LOAD_DATE"] = pd.to_datetime(df["LAST_VIRAL_LOAD_DATE"])
    df["LAST_VISIT_DATE"] = pd.to_datetime(df["LAST_VISIT_DATE"])
    df["NEXT_VISIT_DATE"] = pd.to_datetime(df["NEXT_VISIT_DATE"])

    table_definition = TableDefinition(
        table_name = TableName("RN Case Manager Program Evaluation"),
        columns = [
            TableDefinition.Column("PAT_MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("GENDER_IDENTITY", SqlType.text()),
            TableDefinition.Column("PATIENT_RACE", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("BIRTH_DATE", SqlType.date()),
            TableDefinition.Column("MONTHS_ENROLLED", SqlType.int()),
            TableDefinition.Column("SMART_PHRASE_NAME", SqlType.text()),
            TableDefinition.Column("SMART_PHRASE_USE_DATE", SqlType.date()),
            TableDefinition.Column("FYI_FLAG_NAME", SqlType.text()),
            TableDefinition.Column("ENROLLMENT_DATE", SqlType.date()),
            TableDefinition.Column("FIRST_VIRAL_LOAD", SqlType.int()),
            TableDefinition.Column("FIRST_VIRAL_LOAD_DATE", SqlType.date()),
            TableDefinition.Column("LAST_VIRAL_LOAD", SqlType.int()),
            TableDefinition.Column("LAST_VIRAL_LOAD_DATE", SqlType.date()),
            TableDefinition.Column("CURRENT_SUPPRESSION_STATUS", SqlType.text()),
            TableDefinition.Column("MONTHS_BETWEEN_LABS", SqlType.int()),
            TableDefinition.Column("LAST_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("LAST_VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_LOCATION", SqlType.text()),
            TableDefinition.Column("NEXT_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("NEXT_VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("NEXT_VISIT_LOCATION", SqlType.text()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("FLAGGED_BY_NAME", SqlType.text()),
            TableDefinition.Column("FLAGGED_BY_ROLE", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=rncm_prog_eval_logger,
        project_id=project_id
    )

    rncm_prog_eval_logger.info(
        "Clinical Operations - RN Case Manager Program Evaluation pushed to Tableau."
    )
