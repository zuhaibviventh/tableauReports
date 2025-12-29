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
sql_file = f"{directory}/cq_mh_aoda_measure/sql/mh_aoda_measure.sql"
mh_aoda_measure_logger = logger.setup_logger(
    "mh_aoda_measure_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(mh_aoda_measure_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = mh_aoda_measure_logger
)

def run(shared_drive):
    mh_aoda_measure_logger.info(
        "Clinical Quality - BH MHT - Patients Completing Four or More Visits Since Episode Start."
    )
    hyper_file = f"{shared_drive}/BH MHT - Patients Completing Four or More Visits Since Episode Start.hyper"
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
            mh_aoda_measure_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(mh_aoda_measure_df.index) == 0:
            mh_aoda_measure_logger.info("There are no data.")
            mh_aoda_measure_logger.info("Clinical Quality - BH MHT - Patients Completing Four or More Visits Since Episode Start Daily ETL finished.")
        else:
            tableau_push(mh_aoda_measure_df, hyper_file)

    except ConnectionError as connection_error:
        mh_aoda_measure_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        mh_aoda_measure_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    mh_aoda_measure_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("BH MHT - Patients Completing Four or More Visits Since Episode Start"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("City", SqlType.text()),
            TableDefinition.Column("State", SqlType.text()),
            TableDefinition.Column("Site", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("VISIT_COUNT_WITHIN_90_DAYS_SINCE_OPEN_EPISODE", SqlType.int()),
            TableDefinition.Column("EPISODE_START_DT", SqlType.date()),
            TableDefinition.Column("FOUR_OR_MORE_COMPLETED_VISITS", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=mh_aoda_measure_logger,
        project_id=project_id
    )

    mh_aoda_measure_logger.info(
        "Clinical Quality - BH MHT - Patients Completing Four or More Visits Since Episode Start pushed to Tableau."
    )
