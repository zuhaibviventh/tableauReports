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
sql_file = f"{directory}/co_medical_prep_data_quality/sql/prep_data_quality.sql"
prep_data_quality_logger = logger.setup_logger(
    "prep_data_quality_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(prep_data_quality_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = prep_data_quality_logger
)

def run(shared_drive):
    prep_data_quality_logger.info("Clinical Operations - PrEP Data Quality.")
    hyper_file = f"{shared_drive}/PrEP Data Quality.hyper"
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
            prep_data_quality_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(prep_data_quality_df.index) == 0:
            prep_data_quality_logger.info("There are no data.")
            prep_data_quality_logger.info("Clinical Operations - PrEP Data Quality Daily ETL finished.")
        else:
            tableau_push(prep_data_quality_df, hyper_file)

    except ConnectionError as connection_error:
        prep_data_quality_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        prep_data_quality_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    prep_data_quality_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("PrEP Data Quality"),
        columns = [
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("PrEP_Flagged", SqlType.text()),
            TableDefinition.Column("CONTACT_DATE", SqlType.date()),
            TableDefinition.Column("APPT_STATUS", SqlType.text()),
            TableDefinition.Column("VISIT_TYPE", SqlType.text()),
            TableDefinition.Column("VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("PAT_ENC_CSN_ID", SqlType.double()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("BAD_VISIT_TYPES", SqlType.text()),
            TableDefinition.Column("APPT_PRC_ID", SqlType.text()),
            TableDefinition.Column("APPT_CREATOR", SqlType.text()),
            TableDefinition.Column("CP_PrEP_COHORT", SqlType.text())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=prep_data_quality_logger,
        project_id=project_id
    )

    prep_data_quality_logger.info(
        "Clinical Operations - PrEP Data Quality pushed to Tableau."
    )

