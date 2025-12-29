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
diabetes_sql_file = f"{directory}/cq_general_quality_measures/sql/diabetes.sql"
hiv_labs_sql_file = f"{directory}/cq_general_quality_measures/sql/hiv_labs.sql"
htn_sql_file = f"{directory}/cq_general_quality_measures/sql/hypertension.sql"
logger = logger.setup_logger(
    "cq_general_quality_measures_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = logger
)

def run(shared_drive):
    logger.info(
        "Clinical Quality - General Quality."
    )
    diabetes_hyper_file = f"{shared_drive}/General Quality - Diabetes.hyper"
    hiv_labs_hyper_file = f"{shared_drive}/General Quality - HIV Labs.hyper"
    htn_hyper_file = f"{shared_drive}/General Quality - Hypertension.hyper"
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
            diabetes_df = connections.sql_to_df(
                file = diabetes_sql_file,
                connection = clarity_connection
            )
            hiv_labs_df = connections.sql_to_df(
                file = hiv_labs_sql_file,
                connection = clarity_connection
            )
            htn_df = connections.sql_to_df(
                file = htn_sql_file,
                connection = clarity_connection
            )

            diabetes__tableau_push(diabetes_df, diabetes_hyper_file)
            hiv_labs__tableau_push(hiv_labs_df, hiv_labs_hyper_file)
            htn__tableau_push(htn_df, htn_hyper_file)

    except ConnectionError as connection_error:
        logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        logger.error(f"Incorrect connection keys: {key_error}")


def diabetes__tableau_push(df, hyper_file):
    logger.info("Creating Diabetes Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("General Quality - Diabetes"),
        columns = [
            TableDefinition.Column("PATIENT_NAME", SqlType.text()),
            TableDefinition.Column("PATIENT_AGE", SqlType.int()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("DIABETES_DX", SqlType.text()),
            TableDefinition.Column("HBA1C_CATEGORY", SqlType.text()),
            TableDefinition.Column("LATEST_HEMOGLOBIN_A1C_LAB_VALUE",
                                   SqlType.double()),
            TableDefinition.Column("LATEST_LAB_RESULT_DATE", SqlType.date()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=logger,
        project_id=project_id
    )

    logger.info(
        "Clinical Quality - General Quality - Diabetes pushed to Tableau."
    )


def hiv_labs__tableau_push(df, hyper_file):
    logger.info("Creating HIV Labs Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("General Quality - HIV Labs"),
        columns = [
            TableDefinition.Column("PATIENT_NAME", SqlType.text()),
            TableDefinition.Column("PATIENT_AGE", SqlType.int()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("LAST_MEDICAL_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("HAS_VLS_LAB", SqlType.text()),
            TableDefinition.Column("HAS_CD4_LAB", SqlType.text()),
            TableDefinition.Column("VIRAL_LOAD_RNA_VALUE", SqlType.text()),
            TableDefinition.Column("VIRAL_LOAD_SUPPRESSION_STATUS_CATC", SqlType.text()),
            TableDefinition.Column("VIRAL_LOAD_DETECTION_STATUS_CATC", SqlType.text()),
            TableDefinition.Column("VLS_RESULT_DT", SqlType.date()),
            TableDefinition.Column("CD4_LAB_VALUE", SqlType.text()),
            TableDefinition.Column("CD4_RESULT_DT", SqlType.date()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=logger,
        project_id=project_id
    )

    logger.info(
        "Clinical Quality - General Quality - HIV Labs pushed to Tableau."
    )


def htn__tableau_push(df, hyper_file):
    logger.info("Creating Hypertension Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("General Quality - Hypertension"),
        columns = [
            TableDefinition.Column("PATIENT_NAME",SqlType.text()),
            TableDefinition.Column("PATIENT_AGE",SqlType.int()),
            TableDefinition.Column("MRN",SqlType.text()),
            TableDefinition.Column("STATE",SqlType.text()),
            TableDefinition.Column("CITY",SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME",SqlType.text()),
            TableDefinition.Column("PCP",SqlType.text()),
            TableDefinition.Column("HTN_DX",SqlType.text()),
            TableDefinition.Column("BP_SYS_LAST",SqlType.text()),
            TableDefinition.Column("BP_DIA_LAST",SqlType.text()),
            TableDefinition.Column("BLOOD_PRESSURE_CATEGORY",SqlType.text()),
            TableDefinition.Column("BP_LAST_TAKEN_DATE",SqlType.date()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=logger,
        project_id=project_id
    )

    logger.info(
        "Clinical Quality - General Quality - Hypertension pushed to Tableau."
    )
