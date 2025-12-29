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
sql_file = f"{directory}/co_cp_dm_drug_regimens/sql/dm_drug_regimens.sql"
drug_regimens_logger = logger.setup_logger(
    "drug_regimens_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(drug_regimens_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = drug_regimens_logger
)

def run(shared_drive):
    drug_regimens_logger.info("Clinical Operations - Clinical Pharmacy DM Drug Regimens.")
    hyper_file = f"{shared_drive}/Clinical Pharmacy DM Drug Regimens.hyper"
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
            dm_drug_regimens_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(dm_drug_regimens_df.index) == 0:
            drug_regimens_logger.info("There are no data.")
            drug_regimens_logger.info("Clinical Operations - Clinical Pharmacy DM Drug Regimens Daily ETL finished.")
        else:
            tableau_push(dm_drug_regimens_df, hyper_file)

    except ConnectionError as connection_error:
        drug_regimens_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        drug_regimens_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    drug_regimens_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Clinical Pharmacy DM Drug Regimens"),
        columns = [
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("IDENTITY_ID", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("Sulfonylureas", SqlType.int()),
            TableDefinition.Column("Meglitinides", SqlType.int()),
            TableDefinition.Column("Biguanide", SqlType.int()),
            TableDefinition.Column("Thiazolidinediones", SqlType.int()),
            TableDefinition.Column("Alpha Glucosidase Inhibitors", SqlType.int()),
            TableDefinition.Column("Dipeptidyl Peptidase IV (DPP-4) Inhibitors", SqlType.int()),
            TableDefinition.Column("Dopamine Agonists ", SqlType.int()),
            TableDefinition.Column("Sodium Glucose Transport Protein 2 (SGLT2) Inhibitors", SqlType.int()),
            TableDefinition.Column("Glucagon Like Peptide (GLP1) Agonists", SqlType.int()),
            TableDefinition.Column("Insulin Therapy", SqlType.int()),
            TableDefinition.Column("TOTAL", SqlType.int())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=drug_regimens_logger,
        project_id=project_id
    )

    drug_regimens_logger.info(
        "Clinical Operations - Clinical Pharmacy DM Drug Regimens pushed to Tableau."
    )

