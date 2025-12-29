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

poor_control_extraction_logger = logger.setup_logger(
    "poor_control_extraction_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        poor_control_extraction_logger.error(f"Config file was not found: {file_not_found_error}")
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            diabetes_poor_control_sql = connections.sql_to_df(
                file = f"{directory}/diabetes_a1c_poor_control/sql/diabetes_poor_control.sql",
                connection = clarity_connection
            )

            with open(
                f"{directory}/diabetes_a1c_poor_control/staging/STAGING_diabetes_a1c_poor_control.csv",
                "wb"
            ) as staging_diabetes:
                diabetes_poor_control_sql.to_csv(staging_diabetes, index = False)
                poor_control_extraction_logger.debug("Successfully staged Diabetes A1c Poor Control.")
    except ConnectionError as connection_error:
        poor_control_extraction_logger.error(f"Unable to connect to Clarity: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        poor_control_extraction_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)

    poor_control_extraction_logger.debug("Extraction complete.")
