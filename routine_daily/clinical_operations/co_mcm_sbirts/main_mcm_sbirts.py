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
sql_file = f"{directory}/co_mcm_sbirts/sql/mcm_sbirts.sql"
mcm_sbirts_logger = logger.setup_logger(
    "mcm_sbirts_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(mcm_sbirts_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = mcm_sbirts_logger
)

def run(shared_drive):
    mcm_sbirts_logger.info("Clinical Operations - SBIRTs.")
    hyper_file = f"{shared_drive}/SBIRTs.hyper"
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
            mcm_sbirts_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(mcm_sbirts_df.index) == 0:
            mcm_sbirts_logger.info("There are no data.")
            mcm_sbirts_logger.info("Clinical Operations - SBIRTs Daily ETL finished.")
        else:
            tableau_push(mcm_sbirts_df, hyper_file)

    except ConnectionError as connection_error:
        mcm_sbirts_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        mcm_sbirts_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    mcm_sbirts_logger.info("Creating Hyper Table.")

    #idk. I'm just redoing some of what Mitch did in Alteryx.
    df["CLATestResult"] = df["CLATestResult"] \
        .fillna(" ") \
        .replace(r"^ +| +$", r"", regex=True)
    df["CLATestCompletedBy"] = df["CLATestCompletedBy"] \
        .fillna(" ") \
        .replace(r"^ +| +$", r"", regex=True)

    table_definition = TableDefinition(
        table_name = TableName("SBIRTs"),
        columns = [
            TableDefinition.Column("SCPMRN", SqlType.text()),
            TableDefinition.Column("CLATestName", SqlType.text()),
            TableDefinition.Column("CLATestResult", SqlType.text()),
            TableDefinition.Column("TR_NUMBER", SqlType.int()),
            TableDefinition.Column("CLAEnhancedMotivation", SqlType.text()),
            TableDefinition.Column("CLANegotiatedPlan", SqlType.text()),
            TableDefinition.Column("CLASeekTreatment", SqlType.text()),
            TableDefinition.Column("CLAFollowupArranged", SqlType.text()),
            TableDefinition.Column("CLABriefIntervention", SqlType.text()),
            TableDefinition.Column("CLATestCompletedBy", SqlType.text()),
            TableDefinition.Column("CLATestCompletedDate", SqlType.date()),
            TableDefinition.Column("AOrg", SqlType.text()),
            TableDefinition.Column("APgm", SqlType.text()),
            TableDefinition.Column("CLATestStatus", SqlType.text()),
            TableDefinition.Column("CLATestResultType", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("TODAY", SqlType.date()),
            TableDefinition.Column("Screen Date", SqlType.date()),
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=mcm_sbirts_logger,
        project_id=project_id
    )

    mcm_sbirts_logger.info(
        "Clinical Operations - SBIRTs pushed to Tableau."
    )
