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
sql_file = f"{directory}/co_cp_touches/sql/cp_touches.sql"
touches_logger = logger.setup_logger(
    "touches_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(touches_logger)
project_id = vh_config.grab_tableau_id(
    project_name="Clinical Operations",
    logger=touches_logger
)


def run(shared_drive):
    touches_logger.info("Clinical Operations - Clinical Pharmacy Touches.")
    hyper_file = f"{shared_drive}/Clinical Pharmacy Touches.hyper"
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
            cp_touches_df = connections.sql_to_df(
                file=sql_file,
                connection=clarity_connection
            )

        if len(cp_touches_df.index) == 0:
            touches_logger.info("There are no data.")
            touches_logger.info("Clinical Operations - Clinical Pharmacy Touches Daily ETL finished.")
        else:
            tableau_push(cp_touches_df, hyper_file)

    except ConnectionError as connection_error:
        touches_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        touches_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    touches_logger.info("Creating Hyper Table.")

    df["VISIT ID"] = df["VISIT ID"].astype(int)

    table_definition = TableDefinition(
        table_name=TableName("Clinical Pharmacy Touches"),
        columns=[
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("VISIT ID", SqlType.int()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("CONTACT_DATE", SqlType.date()),
            TableDefinition.Column("PROV_NAME", SqlType.text()),
            TableDefinition.Column("ENC_TYPE_C", SqlType.text()),
            TableDefinition.Column("ENC_TYPE", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("TODAY", SqlType.date()),
            TableDefinition.Column("CURRENT_PCP", SqlType.text()),
            TableDefinition.Column("PRC_NAME", SqlType.text())
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=touches_logger,
        project_id=project_id
    )

    touches_logger.info(
        "Clinical Operations - Clinical Pharmacy Touches pushed to Tableau."
    )
