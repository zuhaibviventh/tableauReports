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

crc_logger = logger.setup_logger(
    "crc_screening_extraction_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        crc_logger.error(f"Config file was not found: {file_not_found_error}. Exiting.")
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            crc_screening_sql = connections.sql_to_df(
                file = f"{directory}/crc_screening_action_lists/sql/crc_screening_action_lists.sql",
                connection = clarity_connection
            )

            with open(
                f"{directory}/crc_screening_action_lists/staging/STAGING_crc_screening_action_lists.csv",
                "wb"
            ) as staging_action_lists:
                crc_screening_sql.to_csv(staging_action_lists, index = False)
                crc_logger.debug("Successfully staged CRC screening action lists.")
    except ConnectionError as connection_error:
        crc_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}. Exiting.")
        sys.exit(1)
    except KeyError as key_error:
        crc_logger.error(f"Incorrect connection keys: {key_error}. Exiting.")
        sys.exit(1)

    crc_logger.debug("Extraction complete.")
