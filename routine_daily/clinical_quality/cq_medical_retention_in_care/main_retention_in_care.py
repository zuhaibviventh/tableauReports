import os
import os.path

from tableauhyperapi import TableName, TableDefinition, SqlType

from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau
)

directory = context.get_context(os.path.abspath(__file__))



sql_file = f"{directory}/cq_medical_retention_in_care/sql/retention_in_care.sql"
retention_in_care_logger = logger.setup_logger(
    "retention_in_care_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(retention_in_care_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = retention_in_care_logger
)

def run(shared_drive):
    retention_in_care_logger.info(
        "Clinical Quality - Retention In-Care."
    )
    hyper_file = f"{shared_drive}/Retention In-Care.hyper"
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
            retention_in_care_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(retention_in_care_df.index) == 0:
            retention_in_care_logger.info("There are no data.")
            retention_in_care_logger.info("Clinical Quality - Retention In-Care Daily ETL finished.")
        else:
            tableau_push(retention_in_care_df, hyper_file)

    except ConnectionError as connection_error:
        retention_in_care_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        retention_in_care_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    retention_in_care_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("Retention In-Care"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PATIENT_NAME", SqlType.text()),
            TableDefinition.Column("LAST_OFFICE_VISIT", SqlType.date()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub-Service Line", SqlType.text()),
            TableDefinition.Column("VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("GENDER_CATC", SqlType.text()),
            TableDefinition.Column("RACE_CATC", SqlType.text()),
            TableDefinition.Column("ETHNICITY_CATC", SqlType.text()),
            TableDefinition.Column("PAT_AGE_N", SqlType.int()),
            TableDefinition.Column("SEXUAL_ORIENTATION_CATC", SqlType.text()),
            TableDefinition.Column("OUTCOME", SqlType.text()),
            TableDefinition.Column("DATA_UPDATE", SqlType.timestamp())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=retention_in_care_logger,
        project_id=project_id
    )

    retention_in_care_logger.info(
        "Clinical Quality - Retention In-Care pushed to Tableau."
    )
