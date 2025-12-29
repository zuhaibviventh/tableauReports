import os
import os.path
import pandas as pd

from tableauhyperapi import TableName, TableDefinition, SqlType

from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau
)

from global_sql import run_globals

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}\\cq_medical_lipid_panel\\sql\\lipid_panel.sql"
lipid_panel_logger = logger.setup_logger("lipid_panel_logger", 
                                         f"{directory}\\logs\\main.log")

config = vh_config.grab(lipid_panel_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = lipid_panel_logger
)

def run(shared_drive):
    lipid_panel_logger.info("Clinical Quality - Lipid Panel.")
    hyper_file = f"{shared_drive}\\Lipid Panel.hyper"
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
            lipid_panel_df = connections.sql_to_df(sql_file, clarity_connection)

        if len(lipid_panel_df.index) == 0:
            lipid_panel_logger.info("There are no data.")
            lipid_panel_logger.info("Clinical Quality - Lipid Panel Daily ETL finished.")
        else:
            tableau_push(lipid_panel_df, hyper_file)

    except ConnectionError as connection_error:
        lipid_panel_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        lipid_panel_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    lipid_panel_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Lipid Panel"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("DOB", SqlType.date()),
            TableDefinition.Column("RACE_CATC", SqlType.text()),
            TableDefinition.Column("ETHNICITY_CATC", SqlType.text()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("LAST_LIPID_PANEL_ORDER_DATE", SqlType.date()),
            TableDefinition.Column("LAST_TOTAL_CHOL_LAB_VALUE", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("NEXT_ANY_VISIT_PROVIDER_NAME", SqlType.text()),
            TableDefinition.Column("NEXT_ANY_VISIT_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("NEXT_PCP_VISIT_PROVIDER_NAME", SqlType.text()),
            TableDefinition.Column("NEXT_PCP_VISIT_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp()),
            TableDefinition.Column("In Any Clinical Pharmacy Cohort", SqlType.text()),
            TableDefinition.Column("Lab Panel Ordered", SqlType.text()),
            TableDefinition.Column("Lab Component Name", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(df=df,
                               hyper_file=hyper_file,
                               table_definition=table_definition,
                               logger=lipid_panel_logger,
                               project_id=project_id)

    lipid_panel_logger.info("Clinical Quality - Lipid Panel pushed to Tableau.")
