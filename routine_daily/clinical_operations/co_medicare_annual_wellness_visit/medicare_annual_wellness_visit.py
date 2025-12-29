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
sql_file = f"{directory}/co_medicare_annual_wellness_visit/sql/medicare_annual_wellness_visit.sql"
medicare_annual_wellness_visit_logger = logger.setup_logger(
    "medicare_annual_wellness_visit_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(medicare_annual_wellness_visit_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = medicare_annual_wellness_visit_logger
)

def run(shared_drive):
    medicare_annual_wellness_visit_logger.info("Clinical Operations - Medicare Annual Wellness Visit.")

    hyper_file = f"{shared_drive}/Medicare Annual Wellness Visit.hyper"
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
            medicare_annual_wellness_visit_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(medicare_annual_wellness_visit_df.index) == 0:
            medicare_annual_wellness_visit_logger.info("There are no data.")
            medicare_annual_wellness_visit_logger.info("Clinical Operations - Medicare Annual Wellness Visit Daily ETL finished.")
        else:
            tableau_push(medicare_annual_wellness_visit_df, hyper_file)

    except ConnectionError as connection_error:
        medicare_annual_wellness_visit_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        medicare_annual_wellness_visit_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    medicare_annual_wellness_visit_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Medicare Annual Wellness Visit"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("DOB", SqlType.date()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("Insurance Financial Class", SqlType.text()),
            TableDefinition.Column("Payor Name", SqlType.text()),
            TableDefinition.Column("Plan Name", SqlType.text()),
            TableDefinition.Column("Date Medicare Effective", SqlType.date()),
            TableDefinition.Column("Medicare End Date", SqlType.date()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Service Date", SqlType.date()),
            TableDefinition.Column("Wellness Code Charged", SqlType.text()),
            TableDefinition.Column("Months Since Last Wellness Charge", SqlType.int()),
            TableDefinition.Column("Wellness Visit Status", SqlType.text()),
            TableDefinition.Column("Site", SqlType.text()),
            TableDefinition.Column("State", SqlType.text())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=medicare_annual_wellness_visit_logger,
        project_id=project_id
    )

    medicare_annual_wellness_visit_logger.info(
        "Clinical Operations - Medicare Annual Wellness Visit."
    )
