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
sql_file = f"{directory}/co_bh_mh_active_patients/sql/mh_active_patients.sql"
mh_act_pats_logger = logger.setup_logger(
    "mh_active_patients_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(mh_act_pats_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = mh_act_pats_logger
)

def run(shared_drive):
    mh_act_pats_logger.info("Clinical Operations - MH Active Patients.")
    
    hyper_file = f"{shared_drive}/MH Active Patients.hyper"
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
            mh_pats_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(mh_pats_df.index) == 0:
            mh_act_pats_logger.info("There are no data.")
            mh_act_pats_logger.info("Clinical Operations - MH Active Patients Daily ETL finished.")
        else:
            tableau_push(mh_pats_df, hyper_file)

    except ConnectionError as connection_error:
        mh_act_pats_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        mh_act_pats_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    mh_act_pats_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("MH Active Patients"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("PROV_NAME", SqlType.text()),
            TableDefinition.Column("LAST_VISIT", SqlType.date()),
            TableDefinition.Column("MONTHS_SINCE_SEEN", SqlType.int()),
            TableDefinition.Column("LAST_VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("NEXT_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("NEXT_VISIT_DEPARTMENT", SqlType.text()),
            TableDefinition.Column("LAST_VISIT_PROVIDER_IN_CARE_TEAM_YN",
                                   SqlType.text())
        ]
    )

    mh_act_pats_logger.info("Clinical Operations - MH Active Patients.")

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=mh_act_pats_logger,
        project_id=project_id
    )

    mh_act_pats_logger.info("Clinical Operations - MH Active Patients pushed to Tableau.")
