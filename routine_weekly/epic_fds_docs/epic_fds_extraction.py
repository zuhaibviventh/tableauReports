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

epic_fds_logger = logger.setup_logger(
    "epic_fds_logger_extraction_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        epic_fds_logger.error(f"Config file was not found: {file_not_found_error}")
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            epic_fds_sql = connections.sql_to_df(
                file = f"{directory}/epic_fds_docs/sql/epic_fds_docs.sql",
                connection = clarity_connection
            )

            with open(
                f"{directory}/epic_fds_docs/staging/STAGING_epic_fds_docs.csv",
                "wb"
            ) as staging_epic_fds_docs:
                epic_fds_sql.to_csv(staging_epic_fds_docs, index = False)
                epic_fds_logger.debug("Successfully staged EPIC FDS Scanned Signed Docs.")
    except ConnectionError as connection_error:
        epic_fds_logger.error(f"Unable to connect to Clarity: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        epic_fds_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)

    epic_fds_logger.debug("Extraction complete.")
