import os, os.path, json
import pandas as pd
from sqlalchemy import *
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
shared_drive = ("C:/Users/mscoggins/Vivent Health/"
                "Share Drives - Health/Health Informatics and Technology/"
                "Project Management/Routines/daily/clinical_quality")
sql_file = f"{directory}/co_medical_ooc_w_hiv/sql/ooc_w_hiv.sql"
pe_sql_file = f"{directory}/co_medical_ooc_w_hiv/sql/PE__food_pantry_service.sql"
ooc_w_hiv_logger = logger.setup_logger("ooc_w_hiv_logger", f"{directory}/logs/main.log")
config = vh_config.grab(ooc_w_hiv_logger)
project_id = vh_config.grab_tableau_id("Clinical Quality", ooc_w_hiv_logger)

def run(shared_drive):
    ooc_w_hiv_logger.info("Clinical Quality - Out of Care Patients with HIV.")
    hyper_file = f"{shared_drive}/Patients with HIV with Last Visit Info.hyper"
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    try:
        clarity_config = config['Clarity - VH']
        clarity_connection = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
            #server=config['Clarity - OCHIN']['server'],
            #db=config['Clarity - OCHIN']['database'],
            #driver=config['Clarity - OCHIN']['driver'],
            #internal_use=False
        )

        with clarity_connection.connect() as clarity_conn:
            ooc_w_hiv_logger.info("Pulling data from Clarity.")
            ooc_w_hiv_df = connections.sql_to_df(sql_file, clarity_conn)

        if ooc_w_hiv_df.empty:
            ooc_w_hiv_logger.info("There are no data.")
            ooc_w_hiv_logger.info("Clinical Quality - Out of Care Patients with HIV Daily ETL finished.")

    except ConnectionError as connection_error:
        ooc_w_hiv_logger.error(f"Unable to connect to Clarity: {connection_error}")
    except KeyError as key_error:
        ooc_w_hiv_logger.error(f"Incorrect Clarity connection keys: {key_error}")
    

    try:
        pe_config = config['PEViventHealth']
        pe_connection = connections.engine_creation(
            server=pe_config['server'],
            db=pe_config['database'],
            driver=pe_config['driver'],
            uid=pe_config['uid'],
            pwd=pe_config['pwd'],
            internal_use=False
        )

        with pe_connection.connect() as pe_conn:
            ooc_w_hiv_logger.info("Pulling data from Provide Enterprise.")
            pe_food_service_df = connections.sql_to_df(pe_sql_file, pe_conn)
            pe_conn.close()

        if pe_food_service_df.empty:
            ooc_w_hiv_logger.info("There are no data.")
            ooc_w_hiv_logger.info("Clinical Quality - Out of Care Patients with HIV Daily ETL finished.")

    except ConnectionError as connection_error:
        ooc_w_hiv_logger.error(f"Unable to connect to PEViventHealth: {connection_error}")
    except KeyError as key_error:
        ooc_w_hiv_logger.error(f"Incorrect PEViventHealth connection keys: {key_error}")
    except SQLAlchemy.SQLAlchemyError.ResourceClosedError as close_error:
        ooc_w_hiv_logger.error(f"Resource closed with no data returned: {close_error}")

    df = pd.merge(ooc_w_hiv_df, pe_food_service_df, left_on='MRN', right_on='EPIC_MRN', how='left')
    df = df.drop(columns=['EPIC_MRN'])
    df['LATEST_FOOD_PANTRY_SERVICE_DT'].fillna(value=pd.to_datetime("1901-01-01"), inplace=True)
    tableau_push(df, hyper_file)


def create_table_definition():
    return TableDefinition(
        table_name=TableName("Out of Care Patients with HIV"),
        columns=[
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("LAST OFFICE VISIT", SqlType.date()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("MONTHS AGO", SqlType.int()),
            TableDefinition.Column("BIRTH_DATE", SqlType.date()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("SEX", SqlType.text()),
            TableDefinition.Column("LAST LAB", SqlType.date()),
            TableDefinition.Column("LAST VL", SqlType.text()),
            TableDefinition.Column("VL_STATUS", SqlType.text()),
            TableDefinition.Column("NEXT ANY APPT", SqlType.date()),
            TableDefinition.Column("NEXT APPT PROVIDER", SqlType.text()),
            TableDefinition.Column("NEXT PCP APPT", SqlType.date()),
            TableDefinition.Column("PCP APPT PROVIDER", SqlType.text()),
            TableDefinition.Column("MOST_RECENT_RX_FILL", SqlType.date()),
            TableDefinition.Column("LATEST_FOOD_PANTRY_SERVICE_DT", SqlType.date())
        ]
    )


def tableau_push(df, hyper_file):
    ooc_w_hiv_logger.info("Creating Hyper Table.")
    table_definition = create_table_definition()
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=ooc_w_hiv_logger,
        project_id=project_id
    )

    ooc_w_hiv_logger.info(
        "Clinical Quality - Out of Care Patients with HIV pushed to Tableau."
    )
    