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

directory = context.get_context(os.path.abspath(__file__))

sql_file = f"{directory}/cq_sdoh/sql/sdoh.sql"
sdoh_logger = logger.setup_logger("sdoh_logger", f"{directory}/logs/main.log")
config = vh_config.grab(sdoh_logger)
project_id = vh_config.grab_tableau_id("Clinical Quality", sdoh_logger)

def run(shared_drive):
    sdoh_logger.info("Clinical Quality - Medical - SDOH.")
    hyper_file = f"{shared_drive}/Medical - SDOH (Specific).hyper"
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
            run_globals.run(clarity_connection)
            sdoh_df = connections.sql_to_df(sql_file, clarity_connection)

        if len(sdoh_df.index) == 0:
            sdoh_logger.info("There are no data.")
            sdoh_logger.info("Medical - SDOH Daily ETL finished.")
        else:
            tableau_push(sdoh_df, hyper_file)

    except ConnectionError as connection_error:
        sdoh_logger.error(f"Unable to connect to OCHIN: {connection_error}")
    except KeyError as key_error:
        sdoh_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    sdoh_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name=TableName("Medical - SDOH"),
        columns=[
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("SEX", SqlType.text()),
            TableDefinition.Column("RACE", SqlType.text()),
            TableDefinition.Column("ETHNICITY", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("PATIENT_TYPE", SqlType.text()),
            TableDefinition.Column("QUESTIONS_COMPLETED", SqlType.int()),
            TableDefinition.Column("DOMAIN", SqlType.text()),
            TableDefinition.Column("SDOH_QUESTION", SqlType.text()),
            TableDefinition.Column("SDOH_ANSWER", SqlType.text()),
            TableDefinition.Column("SDOH_ENTRY_TIME", SqlType.date()),
            TableDefinition.Column("ENTERED_BY", SqlType.text()),
            TableDefinition.Column("SDOH_COMPLETED_YN", SqlType.text()),
            TableDefinition.Column("SDOH_ASKED_WITHIN_12_MONTHS_YN",
                                   SqlType.text()),
            TableDefinition.Column("LAST_MEDICAL_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("LATEST_MEDICAL_VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("NEXT_ANY_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("NEXT_APPT_PROVIDER", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=sdoh_logger,
        project_id=project_id
    )

    sdoh_logger.info(
        "Clinical Quality - Medical - SDOH pushed to Tableau."
    )
