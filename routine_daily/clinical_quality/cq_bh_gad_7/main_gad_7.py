import os, os.path, json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tableauhyperapi import TableName, TableDefinition, SqlType
import numpy as np 
from global_sql import run_globals
from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau,
    emails
)

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/cq_bh_gad_7/sql/gad_7.sql"
gad_7_logger = logger.setup_logger(
    "gad_7_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(gad_7_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = gad_7_logger
)

def run(shared_drive):
    gad_7_logger.info(
        "Clinical Quality - Gad-7 Screening for BH Patients."
    )
    hyper_file = f"{shared_drive}/Gad-7 Screening for BH Patients.hyper"
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            run_globals.run(clarity_connection)
            gad_7_df = connections.sql_to_df(sql_file, clarity_connection)

        # 1. Create the new engine for Analytics - VH
        gad_7_logger.info("Connecting to Analytics - VH database.")
        try:
            analytics_engine = connections.engine_creation(
                server=config['Analytics - VH']['server'],
                db=config['Analytics - VH']['database'],
                driver=config['Analytics - VH']['driver'],
                internal_use=True 
            )
        except Exception as e:
            gad_7_logger.error(f"Failed to create Analytics - VH engine: {e}")
            raise # Stop execution if we can't get mapping tables

        # 2. Fetch the mapping tables
        with analytics_engine.connect() as analytics_connection:
            gad_7_logger.info("Fetching Department mapping table.")
            dept_mapping_df = pd.read_sql("SELECT * FROM Analytics.Transform.DepartmentMapping", analytics_connection)
            
            gad_7_logger.info("Fetching Provider Type mapping table.")
            prov_mapping_df = pd.read_sql("SELECT * FROM ANALYTICS.TRANSFORM.ProviderTypeMapping", analytics_connection)
        
        # 3. Join Provider Type mapping
        prov_mapping_df.rename(columns={'PROVIDER_TYPE_CODE': 'PROVIDER_TYPE_C'}, inplace=True)
        gad_7_df = pd.merge(
            gad_7_df,
            prov_mapping_df[['PROVIDER_TYPE_C', 'PROV_TYPE']],
            on='PROVIDER_TYPE_C',
            how='left'
        )
        # Rename the new column to its final name
        gad_7_df.rename(columns={'PROV_TYPE': 'Provider Type'}, inplace=True)

        # 4. Join Department mapping 
        gad_7_df['Department ID'] = gad_7_df['Department ID'].astype('Int64').astype(str)
        
        # Convert object col to string (just to be safe)
        dept_mapping_df['DEPARTMENT_ID'] = dept_mapping_df['DEPARTMENT_ID'].astype(str)

        dept_mapping_df.rename(columns={'DEPARTMENT_ID': 'Department ID'}, inplace=True)
        gad_7_df = pd.merge(
            gad_7_df,
            dept_mapping_df[['Department ID', 'SERVICE_LINE', 'SUB_SERVICE_LINE']],
            on='Department ID',
            how='left'
        )
        # Rename new columns to their final names
        gad_7_df.rename(columns={
            'SERVICE_LINE': 'Service Line',
            'SUB_SERVICE_LINE': 'Sub-Service Line'
        }, inplace=True)

        # 5. Apply conditional Psychiatry logic (based on GAD-7 provider type)
        gad_7_logger.info("Applying conditional Sub-Service Line logic for Psychiatry.")
        
        # Define the conditions
        is_mental_health = gad_7_df['Sub-Service Line'] == 'Mental Health'
        is_psych_provider = gad_7_df['PROVIDER_TYPE_C'].isin(['136', '164'])
        
        # Apply the update using .loc
        gad_7_df.loc[is_mental_health & is_psych_provider, 'Sub-Service Line'] = 'Psychiatry'

        # 6. Data quality logging
        gad_7_logger.info("Checking for unmapped departments and care team assignments.")
        
        unmapped_depts = gad_7_df[gad_7_df['Service Line'].isna()]['Department ID'].unique()
        if len(unmapped_depts) > 0:
            gad_7_logger.warning(f"Found {len(unmapped_depts)} unmapped departments: {list(unmapped_depts)[:10]}")
        
        # Log care team assignment statistics
        mht_count = gad_7_df['is_mht_assigned'].sum()
        psych_count = gad_7_df['is_psych_assigned'].sum()
        both_count = ((gad_7_df['is_mht_assigned'] == 1) & (gad_7_df['is_psych_assigned'] == 1)).sum()
        
        gad_7_logger.info(f"Care team assignments: {mht_count} have MHT, {psych_count} have Psychiatrist, {both_count} have both")

        # 7. Drop helper columns before Tableau push
        gad_7_logger.info("Dropping helper columns before Tableau push.")
        gad_7_df = gad_7_df.drop(columns=['PROVIDER_TYPE_C', 'Department ID'])

        if len(gad_7_df.index) == 0:
            gad_7_logger.info("There are no data.")
            gad_7_logger.info("Clinical Quality - Gad-7 Screening for BH Patients Daily ETL finished.")
        else:
            gad_7_logger.info("Replacing all NaN values with None before Tableau push.")
            gad_7_df = gad_7_df.replace({np.nan: None})
            
            # Convert text columns to string after NaN replacement
            text_columns = ['Provider Type', 'Service Line', 'Sub-Service Line', 
                           'MH Therapist', 'Psychiatrist']
            for col in text_columns:
                if col in gad_7_df.columns:
                    gad_7_df[col] = gad_7_df[col].apply(lambda x: str(x) if x is not None else None)
            
            # Define the exact order as per your TableDefinition in tableau_push
            final_column_order = [
                "MRN",
                "Patient",
                "BH Provider",
                "Last BH Start Date",
                "Last BH End Date",
                "ZIP",
                "RACE_CATEGORY",
                "ETHNICITY_CATEGORY",
                "Next Any Appt",
                "Next Appt Prov",
                "Next PCP Appt",
                "Next PCP Appt Prov",
                "CITY",
                "STATE",
                "GAD-7 Date",
                "Gad-7 Total Score",
                "GAD-7 Provider",
                "OUTCOME",
                "Provider Type",
                "Service Line",
                "Sub-Service Line",
                "MH Therapist",
                "is_mht_assigned",
                "Psychiatrist",
                "is_psych_assigned"
            ]
            
            # Enforce the order on the dataframe
            # Use intersection to avoid KeyErrors if a column was dropped unexpectedly, 
            # though ideally, you want it to error if a column is missing.
            gad_7_df = gad_7_df[final_column_order]
            # --- FIX ENDS HERE ---

            tableau_push(gad_7_df, hyper_file)

    except ConnectionError as connection_error:
        gad_7_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        gad_7_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    gad_7_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Gad-7 Screening for BH Patients"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("BH Provider", SqlType.text()),
            TableDefinition.Column("Last BH Start Date", SqlType.date()),
            TableDefinition.Column("Last BH End Date", SqlType.date()),
            TableDefinition.Column("ZIP", SqlType.text()),
            TableDefinition.Column("RACE_CATEGORY", SqlType.text()),
            TableDefinition.Column("ETHNICITY_CATEGORY", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("GAD-7 Date", SqlType.date()),
            TableDefinition.Column("Gad-7 Total Score", SqlType.text()),
            TableDefinition.Column("GAD-7 Provider", SqlType.text()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("Provider Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("MH Therapist", SqlType.text()),
            TableDefinition.Column("is_mht_assigned", SqlType.int()),
            TableDefinition.Column("Psychiatrist", SqlType.text()),
            TableDefinition.Column("is_psych_assigned", SqlType.int())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=gad_7_logger,
        project_id=project_id
    )

    gad_7_logger.info(
        "Clinical Quality - Gad-7 Screening for BH Patients pushed to Tableau."
    )