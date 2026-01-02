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
sql_file = f"{directory}/co_cp_act_demos/sql/activity_demos_nm.sql"
activity_demos_logger = logger.setup_logger(
    "activity_demos_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(activity_demos_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = activity_demos_logger
)

def run(shared_drive):
    activity_demos_logger.info("Clinical Operations - Clinical Pharmacy Activity and Demographics.")

    hyper_file = f"{shared_drive}/Clinical Pharmacy Activity and Demographics DEV.hyper"
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
            activity_demos_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(activity_demos_df.index) == 0:
            activity_demos_logger.info("There are no data.")
            activity_demos_logger.info("Clinical Operations - Clinical Pharmacy Activity and Demographics Daily ETL finished.")
        else:
            tableau_push(activity_demos_df, hyper_file)

    except ConnectionError as connection_error:
        activity_demos_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        activity_demos_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    activity_demos_logger.info("Creating Hyper Table.")

    # cleanup
    df["AGE"] = df["AGE"].fillna(0).astype(int)
    df["PAT_ENC_CSN_ID"] = pd \
        .to_numeric(df["PAT_ENC_CSN_ID"], errors = "coerce") \
        .fillna(0) \
        .astype(int)
    df["CP Touches"] = pd \
        .to_numeric(df["CP Touches"], errors = "coerce") \
        .fillna(0) \
        .astype(int)
    df["99201/11"] = pd \
        .to_numeric(df["99201/11"], errors = "coerce") \
        .fillna(0) \
        .astype(int)
    df["99202/12"] = pd \
        .to_numeric(df["99202/12"], errors = "coerce") \
        .fillna(0) \
        .astype(int)
    df["99203/13"] = pd \
        .to_numeric(df["99203/13"], errors = "coerce") \
        .fillna(0) \
        .astype(int)
    df["99204/14"] = pd \
        .to_numeric(df["99204/14"], errors = "coerce") \
        .fillna(0) \
        .astype(int)
    df["CP Office Visits"] = pd \
        .to_numeric(df["CP Office Visits"], errors = "coerce") \
        .fillna(0) \
        .astype(int)
    df["Non-billable Visits"] = pd \
        .to_numeric(df["Non-billable Visits"], errors = "coerce") \
        .fillna(0) \
        .astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Clinical Pharmacy Activity and Demographics"),
        columns = [
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("HTN Cohort", SqlType.text()),
            TableDefinition.Column("HTN Cohort Date", SqlType.date()),
            TableDefinition.Column("HTN Cohort Status", SqlType.text()),
            TableDefinition.Column("PrEP Cohort", SqlType.text()),
            TableDefinition.Column("PrEP Cohort Date", SqlType.date()),
            TableDefinition.Column("PrEP Cohort Status", SqlType.text()),
            TableDefinition.Column("DM Cohort", SqlType.text()),
            TableDefinition.Column("DM Cohort Date", SqlType.date()),
            TableDefinition.Column("DM Cohort Status", SqlType.text()),
            TableDefinition.Column("Anticoagulation Cohort", SqlType.text()),
            TableDefinition.Column("Anticoagulation Cohort Date", SqlType.date()),
            TableDefinition.Column("Anticoagulation Cohort Status", SqlType.text()),
            TableDefinition.Column("Pre-DM Cohort", SqlType.text()),
            TableDefinition.Column("Pre-DM Cohort Date", SqlType.date()),
            TableDefinition.Column("Pre-DM Cohort Status", SqlType.text()),
            TableDefinition.Column("Tobacco Cohort", SqlType.text()),
            TableDefinition.Column("Tobacco Cohort Date", SqlType.date()),
            TableDefinition.Column("Tobacco Cohort Status", SqlType.text()),
            TableDefinition.Column("Miscellaneous Cohort", SqlType.text()),
            TableDefinition.Column("Miscellaneous Cohort Date", SqlType.date()),
            TableDefinition.Column("Miscellaneous Cohort Status", SqlType.text()),
            TableDefinition.Column("Active in Any Cohort", SqlType.text()),
            TableDefinition.Column("EARLIEST_ENROLLMENT", SqlType.date()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("BIRTH_SEX", SqlType.text()),
            TableDefinition.Column("GENDER_IDENTITY", SqlType.text()),
            TableDefinition.Column("ZIP", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("ASCVD_10_YR_SCORE", SqlType.double()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("SMOKER", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("IDU", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("SERVICE_TYPE", SqlType.text()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("SUB_SERVICE_LINE", SqlType.text()),
            TableDefinition.Column("PREFERRED_PHARMACY", SqlType.text()),
            TableDefinition.Column("PROC_CODE", SqlType.text()),
            TableDefinition.Column("PROCEDURE", SqlType.text()),
            TableDefinition.Column("CP Touches", SqlType.int()),
            TableDefinition.Column("99201/11", SqlType.int()),
            TableDefinition.Column("99202/12", SqlType.int()),
            TableDefinition.Column("99203/13", SqlType.int()),
            TableDefinition.Column("99204/14", SqlType.int()),
            TableDefinition.Column("CP Office Visits", SqlType.int()),
            TableDefinition.Column("Non-billable Visits", SqlType.int()),
            TableDefinition.Column("PROC_NAME", SqlType.text()),
            TableDefinition.Column("CONTACT_DATE", SqlType.date()),
            TableDefinition.Column("PROV_NAME", SqlType.text()),
            TableDefinition.Column("PAT_ENC_CSN_ID", SqlType.int()),
            TableDefinition.Column("APPT_STATUS", SqlType.text()),
            TableDefinition.Column("VISIT_TYPE", SqlType.text()),
            TableDefinition.Column("TOTAL_CHG_AMOUNT", SqlType.double()),
            TableDefinition.Column("TOTAL_PAY_AMOUNT", SqlType.double()),
            TableDefinition.Column("VISIT_INSURANCE", SqlType.text()),
            TableDefinition.Column("fpl_percentage", SqlType.text()),
            TableDefinition.Column("fpl_percentage_levels", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=activity_demos_logger,
        project_id=project_id
    )

    activity_demos_logger.info(
        "Clinical Operations - Clinical Pharmacy Activity and Demographics pushed to Tableau."
    )

