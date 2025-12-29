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
sql_file = f"{directory}/cq_dental_pmv/sql/pmv_maintenance.sql"
pmv_logger = logger.setup_logger(
    "pmv_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(pmv_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = pmv_logger
)

def run(shared_drive):
    pmv_logger.info(
        "Clinical Quality - Dental - Periodontal Maintenance Visit."
    )
    hyper_file = f"{shared_drive}/Dental - Periodontal Maintenance Visit.hyper"
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
            perio_disease_mgmt_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(perio_disease_mgmt_df.index) == 0:
            pmv_logger.info("There are no data.")
            pmv_logger.info("Clinical Quality - Dental - Periodontal Maintenance Visit Daily ETL finished.")
        else:
            tableau_push(perio_disease_mgmt_df, hyper_file)

    except ConnectionError as connection_error:
        pmv_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        pmv_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    pmv_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Dental - Periodontal Maintenance Visit"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("CPT_CODE", SqlType.text()),
            TableDefinition.Column("VISIT_COUNT", SqlType.int()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("NEXT_DENTAL_APPT", SqlType.date()),
            TableDefinition.Column("NEXT_DENTAL_APPT_PROV", SqlType.text()),
            TableDefinition.Column("NEXT_DENTAL_APPT_PROV_TYPE", SqlType.text()),
            TableDefinition.Column("Dental Provider", SqlType.text()),
            TableDefinition.Column("CITY_SPECIFIC_GOALS", SqlType.double())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=pmv_logger,
        project_id=project_id
    )

    pmv_logger.info(
        "Clinical Quality - Dental - Periodontal Maintenance Visit pushed to Tableau."
    )
