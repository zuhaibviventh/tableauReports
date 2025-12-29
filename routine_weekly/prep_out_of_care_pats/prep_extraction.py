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

out_of_care_sql = f"{directory}/prep_out_of_care_pats/sql/prep_out_of_care_patients.sql"
excluded_mrns_sql = f"{directory}/prep_out_of_care_pats/sql/global__excluded_mrns.sql"

staged_data_csv = f"{directory}/prep_out_of_care_pats/staging/STAGING_prep_pats.csv"
prep_extract_logger = logger.setup_logger("prep_extract_logger",
    f"{directory}/logs/routine_weekly_main.log")

def extract():
    print("beginning extract...")
    try:
        with open(config_file, "r") as conf_file:
            print("file exists")
            config = json.load(conf_file)
    except FileNotFoundError as file_not_found_error:
        prep_extract_logger.error(f"Config file was not found: {file_not_found_error}")
        sys.exit(1)

    print("config set")
    params_list = grab_excluded_mrns()
    print("parameters set")
    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )
        with internal_engine.connect() as clarity_connection:
            run_sql(clarity_connection, excluded_mrns_sql, tuple(params_list), parameterized=True)
            print("check 1.5")
            prep_extract_logger.info("Excluded MRNs global temp table set.")
            print("check 1.75")
            prep_sql = connections.sql_to_df(out_of_care_sql, clarity_connection)
            print("check 2")
            with open(staged_data_csv, "wb") as staging_pats:
                prep_sql.to_csv(staging_pats, index = False)
                prep_extract_logger.debug("Successfully staged PrEP Out Of Care Patients.")
            print("check 3")
    except ConnectionError as connection_error:
        prep_extract_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        prep_extract_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)
    print("made it here")
    prep_extract_logger.debug("Extraction complete.")


def grab_excluded_mrns():
    excluded_mrns_csv = f"{directory}\\prep_out_of_care_pats\\staging\\excluded_mrns.csv"
    with open(excluded_mrns_csv, "r") as excluded_mrns:
        target_mrns = pd.read_csv(excluded_mrns)
        mrns = target_mrns["MRN"].drop_duplicates()
        mrn_list = [str(mrn) for mrn in mrns]
    return mrn_list


def run_sql(db_connection, file, parameter_list=None, parameterized=False):
    if parameterized:
        print("parameterized?")
        parameterized_run_sql(db_connection, file, parameter_list)
        print("parameterized!")
    else:
        with open(file, "r") as sql_file:
            pd.read_sql_query(sql_file.read(), db_connection)


def parameterized_run_sql(db_connection, file, parameter_list):
    placeholders = ",".join("?" * len(parameter_list))
    print("placeholders set")
    with open(file, "r") as sql_file:
        sql_query = sql_file.read()
        print("query set")
        sql_with_placeholders = sql_query.replace("%s", placeholders)
        print("sql set")
        pd.read_sql_query(sql_with_placeholders, db_connection, params=parameter_list)
        print("query ran")