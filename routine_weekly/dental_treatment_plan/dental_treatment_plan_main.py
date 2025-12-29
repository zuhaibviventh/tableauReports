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
staging_folder = f"{directory}/dental_treatment_plan/staging"
sql_file = f"{directory}/dental_treatment_plan/sql/dental_trtmnt_plan_compltn.sql"
hyper_file = f"{staging_folder}/Dental Treatment Plan Completion.hyper"

dental_treatment_plan_logger = logger.setup_logger(
    "dental_treatment_plan_logger",
    f"{directory}/logs/routine_weekly_main.log"
)

config = vh_config.grab(dental_treatment_plan_logger)
project_id = vh_config.grab_tableau_id(
    project_name="Clinical Operations",
    logger=dental_treatment_plan_logger
)


def run():
    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            dental_treatment_df = connections.sql_to_df(
                file=sql_file,
                connection=clarity_connection
            )
    except ConnectionError as connection_error:
        dental_treatment_plan_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        dental_treatment_plan_logger.error(f"Incorrect connection keys: {key_error}")

    table_definition = TableDefinition(
        table_name=TableName("Dental Treatment Plan Completion"),
        columns=[
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("Service County", SqlType.text()),
            TableDefinition.Column("Service State", SqlType.text()),
            TableDefinition.Column("Current County", SqlType.text()),
            TableDefinition.Column("Current State", SqlType.text()),
            TableDefinition.Column("Treatment Plan Initiated", SqlType.date()),
            TableDefinition.Column("Treatment Plan Init", SqlType.text()),
            TableDefinition.Column("Treatment Plan Completed", SqlType.text()),
            TableDefinition.Column("Met Y/N", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=dental_treatment_df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=dental_treatment_plan_logger,
        project_id=project_id
    )
