import os, os.path, json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tableauhyperapi import TableName, TableDefinition, SqlType
from global_sql import run_globals
import numpy as np
from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau,
    emails
)

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/cq_bh_phq9_q10/sql/phq9_q10.sql"
overall_sql_file = f"{directory}/cq_bh_phq9_q10/sql/phq9_overall.sql"
phq9_q10_logger = logger.setup_logger(
    "phq9_q10_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(phq9_q10_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = phq9_q10_logger
)

def run(shared_drive):
    phq9_q10_logger.info(
        "Clinical Quality - PHQ9 Question 10 outcome measure."
    )
    hyper_file = f"{shared_drive}/PHQ9 Question 10 outcome measure.hyper"
    overall_hyper_file = f"{shared_drive}/PHQ9 Overall outcome measure.hyper"

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        # Run PHQ9 Q10 SQL
        with internal_engine.connect() as clarity_connection:
            run_globals.run(clarity_connection)
            phq9_q10_df = connections.sql_to_df(sql_file, clarity_connection)

        # Run PHQ9 Overall SQL
        with internal_engine.connect() as clarity_connection:
            run_globals.run(clarity_connection)
            phq9_df = connections.sql_to_df(overall_sql_file, clarity_connection)

        # 1. Create the new engine for Analytics - VH
        phq9_q10_logger.info("Connecting to Analytics - VH database for PHQ9 Overall.")
        try:
            analytics_engine = connections.engine_creation(
                server=config['Analytics - VH']['server'],
                db=config['Analytics - VH']['database'],
                driver=config['Analytics - VH']['driver'],
                internal_use=True 
            )
        except Exception as e:
            phq9_q10_logger.error(f"Failed to create Analytics - VH engine: {e}")
            raise # Stop execution if we can't get mapping tables

        # 2. Fetch the mapping tables
        with analytics_engine.connect() as analytics_connection:
            phq9_q10_logger.info("Fetching Department mapping table.")
            dept_mapping_df = pd.read_sql("SELECT * FROM Analytics.Transform.DepartmentMapping", analytics_connection)
            
            phq9_q10_logger.info("Fetching Provider Type mapping table.")
            prov_mapping_df = pd.read_sql("SELECT * FROM ANALYTICS.TRANSFORM.ProviderTypeMapping", analytics_connection)
        
        # 3. Join Provider Type mapping to phq9_df (Overall dataset)
        prov_mapping_df.rename(columns={'PROVIDER_TYPE_CODE': 'PROVIDER_TYPE_C'}, inplace=True)
        phq9_df = pd.merge(
            phq9_df,
            prov_mapping_df[['PROVIDER_TYPE_C', 'PROV_TYPE']],
            on='PROVIDER_TYPE_C',
            how='left'
        )
        # Rename the new column to its final name
        phq9_df.rename(columns={'PROV_TYPE': 'Provider Type'}, inplace=True)

        # 4. Join Department mapping to phq9_df
        phq9_df['DEPARTMENT_ID'] = phq9_df['DEPARTMENT_ID'].astype('Int64').astype(str)
        
        # Convert object col to string (just to be safe)
        dept_mapping_df['DEPARTMENT_ID'] = dept_mapping_df['DEPARTMENT_ID'].astype(str)

        phq9_df = pd.merge(
            phq9_df,
            dept_mapping_df[['DEPARTMENT_ID', 'SERVICE_LINE', 'SUB_SERVICE_LINE']],
            on='DEPARTMENT_ID',
            how='left'
        )
        # Rename new columns to their final names
        phq9_df.rename(columns={
            'SERVICE_LINE': 'Service Line',
            'SUB_SERVICE_LINE': 'Sub-Service Line'
        }, inplace=True)

        # 5. Apply conditional Psychiatry logic to phq9_df (Overall dataset)
        phq9_q10_logger.info("Applying conditional Sub-Service Line logic for Psychiatry.")
        
        # Define the conditions
        is_mental_health = phq9_df['Sub-Service Line'] == 'Mental Health'
        is_psych_provider = phq9_df['PROVIDER_TYPE_C'].isin(['136', '164'])
        
        # Apply the update using .loc
        phq9_df.loc[is_mental_health & is_psych_provider, 'Sub-Service Line'] = 'Psychiatry'

        # 6. Data quality logging for Overall dataset
        phq9_q10_logger.info("Checking for unmapped departments and care team assignments (Overall).")
        
        unmapped_depts = phq9_df[phq9_df['Service Line'].isna()]['DEPARTMENT_ID'].unique()
        if len(unmapped_depts) > 0:
            phq9_q10_logger.warning(f"Found {len(unmapped_depts)} unmapped departments: {list(unmapped_depts)[:10]}")
        
        # Log care team assignment statistics for Overall dataset
        mht_count_overall = phq9_df['is_mht_assigned'].sum()
        psych_count_overall = phq9_df['is_psych_assigned'].sum()
        both_count_overall = ((phq9_df['is_mht_assigned'] == 1) & (phq9_df['is_psych_assigned'] == 1)).sum()
        
        phq9_q10_logger.info(f"Overall - Care team assignments: {mht_count_overall} have MHT, {psych_count_overall} have Psychiatrist, {both_count_overall} have both")

        # Log care team assignment statistics for Q10 dataset
        mht_count_q10 = phq9_q10_df['is_mht_assigned'].sum()
        psych_count_q10 = phq9_q10_df['is_psych_assigned'].sum()
        both_count_q10 = ((phq9_q10_df['is_mht_assigned'] == 1) & (phq9_q10_df['is_psych_assigned'] == 1)).sum()
        
        phq9_q10_logger.info(f"Q10 - Care team assignments: {mht_count_q10} have MHT, {psych_count_q10} have Psychiatrist, {both_count_q10} have both")

        # 7. Drop helper columns before Tableau push
        phq9_q10_logger.info("Dropping helper columns before Tableau push.")
        phq9_df = phq9_df.drop(columns=['PROVIDER_TYPE_C', 'DEPARTMENT_ID'])

        # Handle PHQ9_SUM data type
        if "PHQ9_SUM" in phq9_df.columns:
            phq9_df["PHQ9_SUM"] = phq9_df["PHQ9_SUM"].astype(str)
            phq9_df["PHQ9_SUM"] = phq9_df["PHQ9_SUM"].replace({"nan": None})

        # Handle NaNs before pushing
        phq9_q10_logger.info("Replacing all NaN values with None before Tableau push.")
        phq9_q10_df = phq9_q10_df.replace({np.nan: None})
        phq9_df = phq9_df.replace({np.nan: None})
        
        # Convert text columns to string after NaN replacement for Overall dataset
        text_columns_overall = ['Provider Type', 'Service Line', 'Sub-Service Line', 
                               'MH Therapist', 'Psychiatrist']
        for col in text_columns_overall:
            if col in phq9_df.columns:
                phq9_df[col] = phq9_df[col].apply(lambda x: str(x) if x is not None else None)
        
        # Convert text columns to string after NaN replacement for Q10 dataset
        text_columns_q10 = ['MH Therapist', 'Psychiatrist']
        for col in text_columns_q10:
            if col in phq9_q10_df.columns:
                phq9_q10_df[col] = phq9_q10_df[col].apply(lambda x: str(x) if x is not None else None)

        # Define column order for phq9_df (Overall)
        overall_column_order = [
            "MRN",
            "PAT_NAME",
            "VISIT_DATE",
            "DEPARTMENT_NAME",
            "CITY",
            "STATE",
            "RACE_CATEGORY",
            "ETHNICITY_CATEGORY",
            "OUTCOME",
            "PHQ9_SUM",
            "PHQ9_USAGE_RECORDED_DATE",
            "PSYCHIATRY_PROVIDER",
            "MH_TEAM_MEMBER",
            "NEXT_ANY_APPT",
            "NEXT_ANY_APPT_PROV",
            "NEXT_PCP_APPT",
            "NEXT_PCP_APPT_PROV",
            "Provider Type",
            "Service Line",
            "Sub-Service Line",
            "MH Therapist",
            "is_mht_assigned",
            "Psychiatrist",
            "is_psych_assigned"
        ]
        
        phq9_df = phq9_df[overall_column_order]

        # Define column order for phq9_q10_df (Q10)
        q10_column_order = [
            "MRN",
            "PATIENT_NAME",
            "VISIT_DATE",
            "DEPARTMENT_NAME",
            "CITY",
            "STATE",
            "service_line",
            "sub_service_line",
            "service_type",
            "RACE_CATEGORY",
            "ETHNICITY_CATEGORY",
            "PHQ9_RECORDED_DATE",
            "PHQ9_Q10",
            "PHQ9_SUM",
            "MET_YN",
            "PSYCHIATRY_PROVIDER",
            "MH_TEAM_MEMBER",
            "NEXT_ANY_APPT",
            "NEXT_ANY_APPT_PROV",
            "NEXT_PCP_APPT",
            "NEXT_PCP_APPT_PROV",
            "MH Therapist",
            "is_mht_assigned",
            "Psychiatrist",
            "is_psych_assigned"
        ]
        
        phq9_q10_df = phq9_q10_df[q10_column_order]

        tableau_push(phq9_q10_df, hyper_file)
        tableau_push_overall(phq9_df, overall_hyper_file)

    except ConnectionError as connection_error:
        phq9_q10_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        phq9_q10_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    phq9_q10_logger.info("Creating Hyper Table for PHQ9 Q10.")

    table_definition = TableDefinition(
        table_name = TableName("PHQ9 Question 10 outcome measure"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT_NAME", SqlType.text()),
            TableDefinition.Column("VISIT_DATE", SqlType.date()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("service_line", SqlType.text()),
            TableDefinition.Column("sub_service_line", SqlType.text()),
            TableDefinition.Column("service_type", SqlType.text()),
            TableDefinition.Column("RACE_CATEGORY", SqlType.text()),
            TableDefinition.Column("ETHNICITY_CATEGORY", SqlType.text()),
            TableDefinition.Column("PHQ9_RECORDED_DATE", SqlType.date()),
            TableDefinition.Column("PHQ9_Q10", SqlType.int()),
            TableDefinition.Column("PHQ9_SUM", SqlType.int()),
            TableDefinition.Column("MET_YN", SqlType.text()),
            TableDefinition.Column("PSYCHIATRY_PROVIDER", SqlType.text()),
            TableDefinition.Column("MH_TEAM_MEMBER", SqlType.text()),
            TableDefinition.Column("NEXT_ANY_APPT", SqlType.date()),
            TableDefinition.Column("NEXT_ANY_APPT_PROV", SqlType.text()),
            TableDefinition.Column("NEXT_PCP_APPT", SqlType.date()),
            TableDefinition.Column("NEXT_PCP_APPT_PROV", SqlType.text()),
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
        logger=phq9_q10_logger,
        project_id=project_id
    )

    phq9_q10_logger.info(
        "Clinical Quality - PHQ9 Question 10 outcome measure pushed to Tableau."
    )


def tableau_push_overall(df, hyper_file):
    phq9_q10_logger.info("Creating Hyper Table for PHQ9 Overall.")

    table_definition = TableDefinition(
        table_name = TableName("PHQ9 Overall outcome measure"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("VISIT_DATE", SqlType.date()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("RACE_CATEGORY", SqlType.text()),
            TableDefinition.Column("ETHNICITY_CATEGORY", SqlType.text()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("PHQ9_SUM", SqlType.text()),
            TableDefinition.Column("PHQ9_USAGE_RECORDED_DATE", SqlType.date()),
            TableDefinition.Column("PSYCHIATRY_PROVIDER", SqlType.text()),
            TableDefinition.Column("MH_TEAM_MEMBER", SqlType.text()),
            TableDefinition.Column("NEXT_ANY_APPT", SqlType.date()),
            TableDefinition.Column("NEXT_ANY_APPT_PROV", SqlType.text()),
            TableDefinition.Column("NEXT_PCP_APPT", SqlType.date()),
            TableDefinition.Column("NEXT_PCP_APPT_PROV", SqlType.text()),
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
        logger=phq9_q10_logger,
        project_id=project_id
    )

    phq9_q10_logger.info(
        "Clinical Quality - PHQ9 Overall outcome measure pushed to Tableau."
    )