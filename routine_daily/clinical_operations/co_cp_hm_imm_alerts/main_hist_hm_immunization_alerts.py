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

directory = context.get_context(os.path.abspath(__file__))
baseline = f"{directory}/co_cp_hm_imm_alerts/sql/01_baseline.sql"
sql_file = f"{directory}/co_cp_hm_imm_alerts/sql/historical_hm_immunization_alerts.sql"
hm_imm_alerts_logger = logger.setup_logger(
    "hm_imm_alerts_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(hm_imm_alerts_logger)
project_id = vh_config.grab_tableau_id(
    project_name="Clinical Operations",
    logger=hm_imm_alerts_logger
)


def run(shared_drive):
    hm_imm_alerts_logger.info("Clinical Operations - Historical HM Immunization Alerts.")

    hyper_file = f"{shared_drive}/HM Immunization Alerts - Historical.hyper"
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
            with open(baseline, "r") as baseline_file:
                pd.read_sql_query(baseline_file.read(), clarity_connection)


            hm_imm_alerts_df = connections.sql_to_df(
                file=sql_file,
                connection=clarity_connection
            )

        if len(hm_imm_alerts_df.index) == 0:
            hm_imm_alerts_logger.info("There are no data.")
            hm_imm_alerts_logger.info("Clinical Operations - HM Immunization Alerts Daily ETL finished.")
        else:
            tableau_push(hm_imm_alerts_df, hyper_file)

    except ConnectionError as connection_error:
        hm_imm_alerts_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        hm_imm_alerts_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    hm_imm_alerts_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name=TableName("HM Immunization Alerts"),
        columns=[
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("DOB", SqlType.date()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("LATEST_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("VISIT_DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("GENDER", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("PATIENT_TYPE", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("HM_IMMUNIZATION_LAST_COMPLETED_DATE", SqlType.date()),
            TableDefinition.Column("HM_IMMUNIZATION_NEXT_DUE_DATE", SqlType.date()),
            TableDefinition.Column("HM_IMMUNIZATION_NAME", SqlType.text()),
            TableDefinition.Column("HM_IMMUNIZATION_STATUS", SqlType.text()),
            TableDefinition.Column("NEXT_MEDICAL_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("NEXT_APPT_PROVIDER", SqlType.text()),
            TableDefinition.Column("CP_COHORT", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_WITHIN_13_MO", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=hm_imm_alerts_logger,
        project_id=project_id
    )

    hm_imm_alerts_logger.info(
        "Clinical Operations - Historical HM Immunization Alerts pushed to Tableau."
    )
