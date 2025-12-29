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

htn_bp_extraction_logger = logger.setup_logger(
    "htn_bp_extraction_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        htn_bp_extraction_logger.error(
            f"Config file was nout found: {file_not_found_error}"
        )
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            htn_bp_sql = connections.sql_to_df(
                file = f"{directory}/htn_bp_poor_control/sql/htn_bp_poor_control.sql",
                connection = clarity_connection
            )

            with open(
                f"{directory}/htn_bp_poor_control/staging/STAGING_htn_bp_poor_control.csv",
                "wb"
            ) as staging_htn_bp:
                htn_bp_sql.to_csv(staging_htn_bp, index = False)
                htn_bp_extraction_logger.debug("Successfully staged HTN BP Poor Control.")
    except ConnectionError as connection_error:
        htn_bp_extraction_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        htn_bp_extraction_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)

    htn_bp_extraction_logger.debug("Extraction complete.")
