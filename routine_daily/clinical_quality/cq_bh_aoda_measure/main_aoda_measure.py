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
sql_file = f"{directory}/cq_bh_aoda_measure/sql/aoda_measure.sql"
aoda_measure_logger = logger.setup_logger(
    "aoda_measure_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(aoda_measure_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = aoda_measure_logger
)

def run(shared_drive):
    aoda_measure_logger.info(
        "Clinical Quality - BH SUD - Patients Completing Four or More Visits in Six Weeks."
    )
    hyper_file = f"{shared_drive}/BH SUD - Patients Completing Four or More Visits in Six Weeks.hyper"
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
            aoda_measure_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(aoda_measure_df.index) == 0:
            aoda_measure_logger.info("There are no data.")
            aoda_measure_logger.info("Clinical Quality - BH SUD - Patients Completing Four or More Visits in Six Weeks Daily ETL finished.")
        else:
            tableau_push(aoda_measure_df, hyper_file)

    except ConnectionError as connection_error:
        aoda_measure_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        aoda_measure_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    aoda_measure_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("BH SUD - Patients Completing Four or More Visits in Six Weeks"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("DOB", SqlType.date()),
            TableDefinition.Column("Intake Date", SqlType.date()),
            TableDefinition.Column("Total Visits in Last Twelve Months", SqlType.int()),
            TableDefinition.Column("FOUR_VISITS_SINCE_INTAKE", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("Four or More Completed Visits", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("Visits Within Six Weeks of Intake", SqlType.int()),
            TableDefinition.Column("PAT_FLAG_TYPE_C", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=aoda_measure_logger,
        project_id=project_id
    )

    aoda_measure_logger.info(
        "Clinical Quality - BH SUD - Patients Completing Four or More Visits in Six Weeks pushed to Tableau."
    )
