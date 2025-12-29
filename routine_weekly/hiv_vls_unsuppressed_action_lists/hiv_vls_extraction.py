import os, os.path, sys
import pandas as pd
import json

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from utils import logger
from utils import connections
from utils import context

directory = context.get_context(os.path.abspath(__file__))
config_file = f"{directory}/config.json"

hiv_vls_logger = logger.setup_logger(
    "hiv_vls_logger_extraction_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        hiv_vls_logger.error(f"Config file was not found: {file_not_found_error}")
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            hiv_vls_sql = connections.sql_to_df(
                file = f"{directory}/hiv_vls_unsuppressed_action_lists/sql/hiv_vls_unsuppressed_action_lists.sql",
                connection = clarity_connection
            )

            with open(
                f"{directory}/hiv_vls_unsuppressed_action_lists/staging/STAGING_hiv_vls_unsuppressed_action_lists.csv",
                "wb"
            ) as staging_action_lists:
                hiv_vls_sql.to_csv(staging_action_lists, index = False)
                hiv_vls_logger.debug("Successfully staged HIV VLS Unsuppressed action lists.")
    except ConnectionError as connection_error:
        hiv_vls_logger.error(f"Unable to connect to Clarity: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        hiv_vls_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)

    hiv_vls_logger.debug("Extraction complete.")
