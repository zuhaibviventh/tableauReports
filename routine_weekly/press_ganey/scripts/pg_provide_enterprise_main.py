import os
import json
import pandas as pd
import sys
from utils import context
from utils import logger
from utils import connections
from datetime import datetime

directory = context.get_context(os.path.abspath(__file__))
pg_provide_enterprise_logger = logger.setup_logger(
    "pg_provide_enterprise_logger", 
    f"{directory}\\logs\\routine_weekly_main.log"
)


def execute(config_file, sql_file_pe, pg_pe_csv, final_pg_pe_csv):
    """
    Execute the ETL process for Press Ganey - PE Weekly.

    Parameters:
    - config_file (str): Path to the JSON config file.
    - sql_file_pe (str): Path to the SQL file for full PE data.
    - pg_pe_csv (str): Path to save the CSV file for Press Ganey.
    - final_pg_pe_csv (str): Final path for the CSV file to be delivered.

    Returns:
    - None
    """
    pg_provide_enterprise_logger.info("Running Press Ganey - PE Weekly ETL.")

    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as e:
        pg_provide_enterprise_logger.error(f"Config file was not found: {e}")
        sys.exit(1)

    try:
        internal_engine = connections.engine_creation(
            server=config['PEViventHealth']['server'],
            db=config['PEViventHealth']['database'],
            driver=config['PEViventHealth']['driver'],
            uid=config['PEViventHealth']['uid'],
            pwd=config['PEViventHealth']['pwd'],
            internal_use=False
        )

        with internal_engine.connect() as clarity_connection:
            full_pg_visits_df = connections.sql_to_df(sql_file_pe, clarity_connection)
            save_data("Press Ganey - PE Visits", pg_pe_csv, full_pg_visits_df) # To Shared Drive
            save_data("Press Ganey - PE Visits", final_pg_pe_csv, full_pg_visits_df) # To Press Ganey

    except ConnectionError as connection_error:
        pg_provide_enterprise_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        pg_provide_enterprise_logger.error(f"Incorrect connection keys: {key_error}")


def run_sql(db_connection, file):
    """
    Execute an SQL query from a file.

    Parameters:
    - db_connection (object): Database connection object.
    - file (str): Path to the SQL file containing the query.

    Returns:
    - None
    """
    with open(file, "r") as sql_file:
        pd.read_sql_query(sql_file.read(), db_connection)


def save_data(df_name, csv_file, df):
    """
    Save a DataFrame to a CSV file.

    Parameters:
    - df_name (str): Name of the DataFrame.
    - csv_file (str): Path to the CSV file to save the DataFrame to.
    - df (DataFrame): DataFrame to be saved.

    Returns:
    - None
    """
    with open(csv_file, "wb") as output:
        df.to_csv(output, index=False)
    pg_provide_enterprise_logger.info(f"{df_name} sql file saved to {csv_file}.")


def run():
    parent_dir = os.path.abspath(os.path.join(context.get_context(directory), os.pardir))
    config_file = f"{parent_dir}\\routine_weekly\\config.json"
    today = datetime.now().strftime("%a %m %d %Y %H.%M.%S.%f")

    press_ganey_folder = "C:\\Users\\talendservice\\OneDrive - Vivent Health\\Quality\\Press Ganey\\"
    saving_folder = f"{press_ganey_folder}\\Data Files Sent to Press Ganey 331180 (our number) and MMDDYYYY"
    staging_folder = f"{saving_folder}\\staging"

    sql_file_pe = f"{directory}\\sql\\full_pe_pg.sql"

    pg_pe_csv = f"{saving_folder}\\331180SS.csv {today}.csv" # to save
    final__pg_pe_csv = f"{staging_folder}\\331180SS.csv" # to deliver

    execute(config_file, sql_file_pe, pg_pe_csv, final__pg_pe_csv)
