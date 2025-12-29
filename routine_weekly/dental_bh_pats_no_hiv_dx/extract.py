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

no_hiv_dx_logger = logger.setup_logger(
    "no_hiv_dx_logger", 
    f"{directory}/logs/routine_weekly_main.log"
)

dental_patients = f"{directory}/dental_bh_pats_no_hiv_dx/staging/dental_patients.csv"
bh_patients = f"{directory}/dental_bh_pats_no_hiv_dx/staging/bh_patients.csv"

def extract():
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        no_hiv_dx_logger.error(f"Config file was not found: {file_not_found_error}.")
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        sql_file = f"{directory}/dental_bh_pats_no_hiv_dx/sql/dental_bh_pats_no_hiv_dx.sql"
        with internal_engine.connect() as clarity_connection:
            dental_bh_pts_sql = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        with open(dental_patients, "wb") as dental_pats_input:
            dental_df = dental_bh_pts_sql[dental_bh_pts_sql["PATIENT TYPE"] == "DENTAL"]
            dental_df.to_csv(dental_pats_input, index=False)

        with open(bh_patients, "wb") as bh_pats_input:
            bh_df = dental_bh_pts_sql[dental_bh_pts_sql["PATIENT TYPE"] == "BH"]
            bh_df.to_csv(bh_pats_input, index=False)
    except ConnectionError as connection_error:
        no_hiv_dx_logger.error(f"Unable to connect to Clarity: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        no_hiv_dx_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)

    no_hiv_dx_logger.debug("Extraction complete.")