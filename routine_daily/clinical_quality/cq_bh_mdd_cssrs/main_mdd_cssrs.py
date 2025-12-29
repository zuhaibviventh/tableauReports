import os
from tableauhyperapi import TableName, TableDefinition, SqlType
from global_sql import run_globals
from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau
)
import pandas as pd
import numpy as np

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/cq_bh_mdd_cssrs/sql/mdd_cssrs.sql"
mdd_cssrs_logger = logger.setup_logger(
    "mdd_cssrs_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(mdd_cssrs_logger)
project_id = vh_config.grab_tableau_id(
    project_name="Clinical Quality",
    logger=mdd_cssrs_logger
)


def run(shared_drive):
    mdd_cssrs_logger.info(
        "Clinical Quality - BH - Patients with MDD Who Were Screened for Suicide Risk."
    )
    hyper_file = f"{shared_drive}/BH - Patients with MDD Who Were Screened for Suicide Risk.hyper"
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
            mdd_cssrs_df = connections.sql_to_df(sql_file, clarity_connection)

        # 1. Create the new engine for Analytics - VH
        mdd_cssrs_logger.info("Connecting to Analytics - VH database.")
        try:
            analytics_engine = connections.engine_creation(
                server=config['Analytics - VH']['server'],
                db=config['Analytics - VH']['database'],
                driver=config['Analytics - VH']['driver'],
                internal_use=True 
            )
        except Exception as e:
            mdd_cssrs_logger.error(f"Failed to create Analytics - VH engine: {e}")
            raise # Stop execution if we can't get mapping tables

        # 2. Fetch the mapping tables
        with analytics_engine.connect() as analytics_connection:
            mdd_cssrs_logger.info("Fetching Department mapping table.")
            dept_mapping_df = pd.read_sql("SELECT * FROM Analytics.Transform.DepartmentMapping", analytics_connection)
            
            mdd_cssrs_logger.info("Fetching Provider Type mapping table.")
            prov_mapping_df = pd.read_sql("SELECT * FROM ANALYTICS.TRANSFORM.ProviderTypeMapping", analytics_connection)

        if len(mdd_cssrs_df.index) == 0:
            mdd_cssrs_logger.info("There are no data.")
            mdd_cssrs_logger.info("Clinical Quality - BH - Patients with MDD Who Were Screened for Suicide Risk Daily ETL finished.")
        else:
            
            # 3. Join Provider Type mapping
            prov_mapping_df.rename(columns={'PROVIDER_TYPE_CODE': 'PROVIDER_TYPE_C'}, inplace=True)
            mdd_cssrs_df = pd.merge(
                mdd_cssrs_df,
                prov_mapping_df[['PROVIDER_TYPE_C', 'PROV_TYPE']],
                on='PROVIDER_TYPE_C',
                how='left'
            )
            # Rename the new column to its final name
            mdd_cssrs_df.rename(columns={'PROV_TYPE': 'Provider Type'}, inplace=True)

            # 4. Join Department mapping 
            mdd_cssrs_df['Department ID'] = mdd_cssrs_df['Department ID'].astype('Int64').astype(str)
            
            # Convert object col to string (just to be safe)
            dept_mapping_df['DEPARTMENT_ID'] = dept_mapping_df['DEPARTMENT_ID'].astype(str)

            dept_mapping_df.rename(columns={'DEPARTMENT_ID': 'Department ID'}, inplace=True)
            mdd_cssrs_df = pd.merge(
                mdd_cssrs_df,
                dept_mapping_df[['Department ID', 'SERVICE_LINE', 'SUB_SERVICE_LINE']],
                on='Department ID',
                how='left'
            )
            # Rename new columns to their final names
            mdd_cssrs_df.rename(columns={
                'SERVICE_LINE': 'Service Line',
                'SUB_SERVICE_LINE': 'Sub-Service Line'
            }, inplace=True)

            # 5. Apply conditional Psychiatry logic (based on last visit provider type)
            mdd_cssrs_logger.info("Applying conditional Sub-Service Line logic for Psychiatry.")
            
            # Define the conditions
            is_mental_health = mdd_cssrs_df['Sub-Service Line'] == 'Mental Health'
            is_psych_provider = mdd_cssrs_df['PROVIDER_TYPE_C'].isin(['136', '164'])
            
            # Apply the update using .loc
            mdd_cssrs_df.loc[is_mental_health & is_psych_provider, 'Sub-Service Line'] = 'Psychiatry'

            # 6. Data quality logging
            mdd_cssrs_logger.info("Checking for unmapped departments and care team assignments.")
            
            unmapped_depts = mdd_cssrs_df[mdd_cssrs_df['Service Line'].isna()]['Department ID'].unique()
            if len(unmapped_depts) > 0:
                mdd_cssrs_logger.warning(f"Found {len(unmapped_depts)} unmapped departments: {list(unmapped_depts)[:10]}")
            
            # Log care team assignment statistics
            mht_count = mdd_cssrs_df['is_mht_assigned'].sum()
            psych_count = mdd_cssrs_df['is_psych_assigned'].sum()
            both_count = ((mdd_cssrs_df['is_mht_assigned'] == 1) & (mdd_cssrs_df['is_psych_assigned'] == 1)).sum()
            
            mdd_cssrs_logger.info(f"Care team assignments: {mht_count} have MHT, {psych_count} have Psychiatrist, {both_count} have both")

            # 7. Drop helper columns before Tableau push
            mdd_cssrs_logger.info("Dropping helper columns before Tableau push.")
            mdd_cssrs_df = mdd_cssrs_df.drop(columns=['PROVIDER_TYPE_C', 'Department ID'])

            mdd_cssrs_logger.info("Replacing all NaN values with None before Tableau push.")
            mdd_cssrs_df = mdd_cssrs_df.replace({np.nan: None})
            
            # Convert text columns to string after NaN replacement
            text_columns = ['Provider Type', 'Service Line', 'Sub-Service Line', 
                           'MH Therapist', 'Psychiatrist']
            for col in text_columns:
                if col in mdd_cssrs_df.columns:
                    mdd_cssrs_df[col] = mdd_cssrs_df[col].apply(lambda x: str(x) if x is not None else None)
            
            # Define the exact order as per TableDefinition
            final_column_order = [
                "MRN",
                "Patient",
                "ZIP",
                "RACE_CATEGORY",
                "ETHNICITY_CATEGORY",
                "Next Any Appt",
                "Next Appt Prov",
                "Next PCP Appt",
                "Next PCP Appt Prov",
                "CITY",
                "STATE",
                "LOS",
                "Last Visit Provider",
                "Last Office Visit",
                "OUTCOME",
                "DATE_OF_LAST_SCREENER",
                "Total Visits",
                "Provider Type",
                "Service Line",
                "Sub-Service Line",
                "MH Therapist",
                "is_mht_assigned",
                "Psychiatrist",
                "is_psych_assigned"
            ]
            
            # Enforce the order on the dataframe
            mdd_cssrs_df = mdd_cssrs_df[final_column_order]

        if len(mdd_cssrs_df.index) == 0:
            mdd_cssrs_logger.info("There are no data.")
            mdd_cssrs_logger.info("Clinical Quality - BH - Patients with MDD Who Were Screened for Suicide Risk Daily ETL finished.")
        else:
            tableau_push(mdd_cssrs_df, hyper_file)

    except ConnectionError as connection_error:
        mdd_cssrs_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        mdd_cssrs_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    mdd_cssrs_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name=TableName("BH - Patients with MDD Who Were Screened for Suicide Risk"),
        columns=[
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("ZIP", SqlType.text()),
            TableDefinition.Column("RACE_CATEGORY", SqlType.text()),
            TableDefinition.Column("ETHNICITY_CATEGORY", SqlType.text()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("Last Visit Provider", SqlType.text()),
            TableDefinition.Column("Last Office Visit", SqlType.date()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("DATE_OF_LAST_SCREENER", SqlType.date()),
            TableDefinition.Column("Total Visits", SqlType.int()),
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
        logger=mdd_cssrs_logger,
        project_id=project_id
    )

    mdd_cssrs_logger.info("Clinical Quality - BH - Patients with MDD Who Were "
        "Screened for Suicide Risk pushed to Tableau.")