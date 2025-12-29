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

dep_extraction_logger = logger.setup_logger(
    "dep_extraction_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        dep_extraction_logger.error(
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
            dep_sql = connections.sql_to_df(
                file = f"{directory}/pats_w_non_spec_dx_depression/sql/pats_w_non_spec_dx_depression.sql",
                connection = clarity_connection
            )

            with open(
                f"{directory}/pats_w_non_spec_dx_depression/staging/STAGED_DATA.csv",
                "wb"
            ) as staging_data:
                dep_sql.to_csv(staging_data, index = False)
                dep_extraction_logger.debug("Successfully staged patients with non-spec dx of depression.")
    except ConnectionError as connection_error:
        dep_extraction_logger.error(f"Unable to connect to Clarity: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        dep_extraction_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)

    dep_extraction_logger.debug("Extraction complete.")
