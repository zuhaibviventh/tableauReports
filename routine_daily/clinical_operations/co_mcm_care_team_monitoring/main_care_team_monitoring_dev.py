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
sql_file = f"{directory}/co_mcm_care_team_monitoring/sql/care_team_monitoring_dev.sql"
care_team_monitoring_logger = logger.setup_logger(
    "care_team_monitoring_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(care_team_monitoring_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = care_team_monitoring_logger
)

def run(shared_drive):
    care_team_monitoring_logger.info("Clinical Operations - Care Team Monitoring.")
    hyper_file = f"{shared_drive}/Care Team Monitoring dev.hyper"
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
            care_team_monitoring_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(care_team_monitoring_df.index) == 0:
            care_team_monitoring_logger.info("There are no data.")
            care_team_monitoring_logger.info("Clinical Operations - Care Team Monitoring Daily ETL finished.")
        else:
            tableau_push(care_team_monitoring_df, hyper_file)

    except ConnectionError as connection_error:
        care_team_monitoring_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        care_team_monitoring_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    care_team_monitoring_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Care Team Monitoring"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("RN", SqlType.text()),
            TableDefinition.Column("MH Therapist", SqlType.text()),
            TableDefinition.Column("Psychiatrist", SqlType.text()),
            TableDefinition.Column("Dentist", SqlType.text()),
            TableDefinition.Column("Care Coordinator", SqlType.text()),
            TableDefinition.Column("PharmD", SqlType.text()),
            TableDefinition.Column("Team Coordinator", SqlType.text()),
            TableDefinition.Column("Care Team Status", SqlType.text()),
            TableDefinition.Column("Duplicate Care Team Member(s)", SqlType.text()),
            TableDefinition.Column("Overall Status", SqlType.text()),
            TableDefinition.Column("HIV_DX_DATE", SqlType.date()),
            TableDefinition.Column("VIRAL_LOAD_RNA_VALUE", SqlType.text()),
            TableDefinition.Column("VIRAL_LOAD_SUPPRESSION_STATUS_CATC", SqlType.text()),
            TableDefinition.Column("VIRAL_LOAD_DETECTION_STATUS_CATC", SqlType.text()),
            TableDefinition.Column("VLS_RESULT_DT", SqlType.date()),
            TableDefinition.Column("LAST_MEDICAL_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("LAST_DENTAL_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("LAST_BH_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("MISSED_APPTS_COUNT", SqlType.int()),
            TableDefinition.Column("LAST_CARE_PLAN_DATE", SqlType.date()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=care_team_monitoring_logger,
        project_id=project_id
    )

    care_team_monitoring_logger.info(
        "Clinical Operations - Care Team Monitoring pushed to Tableau."
    )

