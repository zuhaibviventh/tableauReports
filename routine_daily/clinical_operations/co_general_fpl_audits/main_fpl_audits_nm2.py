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
sql_file = f"{directory}/co_general_fpl_audits/sql/fpl_audits_nm2.sql"

fpl_audits_logger = logger.setup_logger(
    "fpl_audits_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(fpl_audits_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = fpl_audits_logger
)

def run(shared_drive):
    fpl_audits_logger.info("Clinical Operations - FPL Data Quality Audit.")
    hyper_file = f"{shared_drive}/FPL Data Quality Audit.hyper"
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
            fpl_audits_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(fpl_audits_df.index) == 0:
            fpl_audits_logger.info("There are no data.")
            fpl_audits_logger.info("Clinical Operations - FPL Data Quality Audit Daily ETL finished.")
        else:
            tableau_push(fpl_audits_df, hyper_file)

    except ConnectionError as connection_error:
        fpl_audits_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        fpl_audits_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    fpl_audits_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("FPL Data Quality Audit"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("Income", SqlType.double()),
            TableDefinition.Column("FPL Date", SqlType.date()),
            TableDefinition.Column("Months Until Due", SqlType.int()),
            TableDefinition.Column("FPL Status", SqlType.text()),
            TableDefinition.Column("FPL%", SqlType.double()),
            TableDefinition.Column("Household Size", SqlType.int()),
            TableDefinition.Column("Proof Document", SqlType.text()),
            TableDefinition.Column("Reason", SqlType.text()),
            TableDefinition.Column("DOB", SqlType.date()),
            TableDefinition.Column("Patient Type", SqlType.text()),
            TableDefinition.Column("PSR", SqlType.text()),
            TableDefinition.Column("Last Office Visit", SqlType.date()),
            TableDefinition.Column("Site", SqlType.text()),
            TableDefinition.Column("State", SqlType.text()),
            TableDefinition.Column("Service Type", SqlType.text()),
            TableDefinition.Column("Service Line", SqlType.text()),
            TableDefinition.Column("Sub Service Line", SqlType.text()),
            TableDefinition.Column("Department", SqlType.text()),
            TableDefinition.Column("FDS - Photo ID", SqlType.text()),
            TableDefinition.Column("FDS - Photo ID Date", SqlType.date()),
            TableDefinition.Column("UPDATE_DTMM", SqlType.timestamp())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=fpl_audits_logger,
        project_id=project_id
    )

    fpl_audits_logger.info(
        "Clinical Operations - FPL Data Quality Audit pushed to Tableau."
    )

