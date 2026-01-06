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
sql_file = f"{directory}/co_dental_dentures_proc/sql/lower_dentures_proc_nm2.sql"
sql_file2 = f"{directory}/co_dental_dentures_proc/sql/Aanalytics_department_mapping_nm2.sql"
lower_dentures_logger = logger.setup_logger(
    "lower_dentures_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(lower_dentures_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = lower_dentures_logger
)

def run(shared_drive):
    lower_dentures_logger.info("Clinical Operations - Dental - Lower Dentures Procedures.")
    hyper_file = f"{shared_drive}/Dental - Lower Dentures Procedures.hyper"
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
            lower_dentures_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        analytics_engine = connections.engine_creation(
         server=config['Analytics - VH']['server'],
         db=config['Analytics - VH']['database'],
         driver=config['Analytics - VH']['driver'],
         internal_use=True 
            )

        
        with analytics_engine.connect() as analytics_connection:
            analytics_mapping_df = connections.sql_to_df(
                file = sql_file2,
                connection = analytics_connection
            )
         
        lower_dentures_df['DEPARTMENT_ID']=lower_dentures_df['DEPARTMENT_ID'].astype('Int64')
        analytics_mapping_df['DEPARTMENT_ID']=analytics_mapping_df['DEPARTMENT_ID'].astype('Int64')
        #analytics_mapping_df['SERVICE_LINE']=analytics_mapping_df['SERVICE_LINE'].astype('str').str.strip()


        lower_dentures_df  = lower_dentures_df.reset_index(drop = True) 
        analytics_mapping_df = analytics_mapping_df.reset_index(drop = True) 


        merged_df=pd.merge(
            lower_dentures_df ,
            analytics_mapping_df[['SERVICE_LINE','DEPARTMENT_ID']],
            on='DEPARTMENT_ID',
            how='inner'
        )
        merged_df = merged_df[['MRN','PATIENT','CONTACT_DATE','PROCEDURE DATE','CPT CODE','PROCEDURE','PERFORMING PROVIDER','STATE','CITY','SPECIALTY','SERVICE_LINE']]
        print(merged_df.dtypes)
        if len(merged_df.index) == 0:
            lower_dentures_logger.info("There are no data.")
            lower_dentures_logger.info("Clinical Operations - Dental - Lower Dentures Procedures Daily ETL finished.")
        else:
            tableau_push(merged_df, hyper_file)

    except ConnectionError as connection_error:
        lower_dentures_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        lower_dentures_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    lower_dentures_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Dental - Lower Dentures Procedures"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("CONTACT_DATE", SqlType.date()),
            TableDefinition.Column("PROCEDURE DATE", SqlType.date()),
            TableDefinition.Column("CPT CODE", SqlType.text()),
            TableDefinition.Column("PROCEDURE", SqlType.text()),
            TableDefinition.Column("PERFORMING PROVIDER", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("SPECIALTY", SqlType.text()),
            TableDefinition.Column("SERVICE_LINE", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=lower_dentures_logger,
        project_id=project_id
    )

    lower_dentures_logger.info(
        "Clinical Operations - Dental - Lower Dentures Procedures pushed to Tableau."
    )
