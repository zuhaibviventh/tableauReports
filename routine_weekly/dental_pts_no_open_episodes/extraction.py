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

no_open_episodes_logger = logger.setup_logger(
    "no_open_episodes_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        no_open_episodes_logger.error(f"Config file was not found: {file_not_found_error}.")
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            dental_pts_sql = connections.sql_to_df(
                file = f"{directory}/dental_pts_no_open_episodes/sql/main.sql",
                connection = clarity_connection
            )

            with open(
                f"{directory}/dental_pts_no_open_episodes/staging/STAGING_dental_pts_no_open_episodes.csv",
                "wb"
            ) as staging_file:
                dental_pts_sql.to_csv(staging_file, index = False)
                no_open_episodes_logger.debug("Successfully staged Dental Patients with no open episodes.")
    except ConnectionError as connection_error:
        no_open_episodes_logger.error(f"Unable to connect to Clarity: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        no_open_episodes_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)

    no_open_episodes_logger.debug("Extraction complete.")