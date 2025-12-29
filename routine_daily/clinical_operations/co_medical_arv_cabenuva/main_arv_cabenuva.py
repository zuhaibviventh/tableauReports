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
sql_file = f"{directory}/co_medical_arv_cabenuva/sql/arv_cabenuva.sql"
arv_cabenuva_logger = logger.setup_logger(
    "arv_cabenuva_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(arv_cabenuva_logger)
project_id = vh_config.grab_tableau_id(
    project_name="Clinical Operations",
    logger=arv_cabenuva_logger
)


def run(shared_drive):
    arv_cabenuva_logger.info("Clinical Operations - ARV Cabenuva.")
    hyper_file = f"{shared_drive}/ARV Cabenuva.hyper"
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
            arv_cabenuva_df = connections.sql_to_df(
                file=sql_file,
                connection=clarity_connection
            )

        if len(arv_cabenuva_df.index) == 0:
            arv_cabenuva_logger.info("There are no data.")
            arv_cabenuva_logger.info("Clinical Operations - "
                                     "ARV Cabenuva Daily ETL finished.")
        else:
            tableau_push(arv_cabenuva_df, hyper_file)

    except ConnectionError as connection_error:
        arv_cabenuva_logger.error("Unable to connect to OCHIN - "
                                  f"Vivent Health: {connection_error}")
    except KeyError as key_error:
        arv_cabenuva_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    arv_cabenuva_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name=TableName("ARV Cabenuva"),
        columns=[
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("SEX_AT_BIRTH", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("FINANCIAL_CLASS_NAME", SqlType.text()),
            TableDefinition.Column("LAST_INJECTION_DATE", SqlType.date()),
            TableDefinition.Column("LAST_INJECTION_DEPARTMENT_NAME",
                                   SqlType.text()),
            TableDefinition.Column("NEXT_INJECTION_DATE", SqlType.date()),
            TableDefinition.Column("NEXT_INJECTION_VISIT_PROVIDER",
                                   SqlType.text()),
            TableDefinition.Column("NEXT_ANY_APPOINTMENT", SqlType.date()),
            TableDefinition.Column("NEXT_APPT_PROVIDER", SqlType.text()),
            TableDefinition.Column("CABENUVA_TYPE", SqlType.text()),
            TableDefinition.Column("BENEFIT", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=arv_cabenuva_logger,
        project_id=project_id
    )

    arv_cabenuva_logger.info(
        "Clinical Operations - ARV Cabenuva pushed to Tableau."
    )
