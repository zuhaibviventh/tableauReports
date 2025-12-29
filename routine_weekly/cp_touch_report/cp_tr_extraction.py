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

cp_tr = logger.setup_logger(
    "cp_tr_extraction_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        cp_tr.error(f"Config file was not found: {file_not_found_error}. Exiting.")
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            cp_tr_sql = connections.sql_to_df(
                file = f"{directory}/cp_touch_report/sql/cp_touch_report.sql",
                connection = clarity_connection
            )

            with open(
                f"{directory}/cp_touch_report/staging/STAGING_cp_tr.csv",
                "wb"
            ) as staging_cp_tr:
                cp_tr_sql.to_csv(staging_cp_tr, index = False)
                cp_tr.debug("Successfully staged Clinical Pharmacy Touch Report Cohort.")
    except ConnectionError as connection_error:
        cp_tr.error(f"Unable to connect to Clarity: {connection_error}. Exiting.")
        sys.exit(1)
    except KeyError as key_error:
        cp_tr.error(f"Incorrect connection keys: {key_error}. Exiting.")
        sys.exit(1)

    cp_tr.debug("Extraction complete.")