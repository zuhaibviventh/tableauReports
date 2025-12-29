import os
import json
import pandas as pd
import sys
from utils import context
from utils import logger
from utils import connections
from datetime import datetime

directory = context.get_context(os.path.abspath(__file__))
pg_clinical_logger = logger.setup_logger(
    "pg_clinical_logger", 
    f"{directory}\\logs\\routine_weekly_main.log"
)


def execute(config_file, sql_file_survey_opt_outs, sql_file_clinical_visits,
            sql_file_pharmacy_visits, sql_file_clinical, pg_clinical_csv, final_pg_clinical_csv):
    """
    Execute the ETL process for Press Ganey - Clinical Weekly.

    Parameters:
    - config_file (str): Path to the JSON config file.
    - sql_file_survey_opt_outs (str): Path to the SQL file for survey opt-outs.
    - sql_file_clinical_visits (str): Path to the SQL file for clinical visits.
    - sql_file_pharmacy_visits (str): Path to the SQL file for pharmacy visits.
    - sql_file_clinical (str): Path to the SQL file for full clinical data.
    - pg_clinical_csv (str): Path to save the CSV file for Press Ganey.
    - final_pg_clinical_csv (str): Final path for the CSV file to be delivered.

    Returns:
    - None
    """
    pg_clinical_logger.info("Running Press Ganey - Clinical Weekly ETL.")

    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as e:
        pg_clinical_logger.error(f"Config file was not found: {e}")
        sys.exit(1)

    params_list = grab_survey_opt_out()

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            print(params_list)
            run_sql(clarity_connection, sql_file_survey_opt_outs, tuple(params_list), parameterized=True)
            pg_clinical_logger.info("Survey Opt Out global temp table set.")

            run_sql(clarity_connection, sql_file_clinical_visits)
            pg_clinical_logger.info("Clinical visits global temp table set.")

            run_sql(clarity_connection, sql_file_pharmacy_visits)
            pg_clinical_logger.info("Pharmacy visits global temp table set.")

            full_pg_visits_df = connections.sql_to_df(sql_file_clinical, clarity_connection)
            save_data("Press Ganey - Clinical Visits", pg_clinical_csv, full_pg_visits_df) # To Shared Drive
            save_data("Press Ganey - Clinical Visits", final_pg_clinical_csv, full_pg_visits_df) # To Press Ganey

    except ConnectionError as connection_error:
        pg_clinical_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        pg_clinical_logger.error(f"Incorrect connection keys: {key_error}")


def run_sql(db_connection, file, parameter_list=None, parameterized=False):
    """
    Execute an SQL query from a file.

    Parameters:
    - db_connection (object): Database connection object.
    - file (str): Path to the SQL file containing the query.
    - parameter_list (list, optional): List of parameters to be used in the query.
    - parameterized (bool): If True, the query is parameterized.

    Returns:
    - None
    """
    if parameterized:
        parameterized_run_sql(db_connection, file, parameter_list)
    else:
        with open(file, "r") as sql_file:
            pd.read_sql_query(sql_file.read(), db_connection)


def parameterized_run_sql(db_connection, file, parameter_list):
    """
    Execute a parameterized SQL query from a file.

    Parameters:
    - db_connection (object): Database connection object.
    - file (str): Path to the SQL file containing the query.
    - parameter_list (list): List of parameters to be used in the query.

    Returns:
    - None
    """
    placeholders = ",".join("?" * len(parameter_list))

    with open(file, "r") as sql_file:
        sql_query = sql_file.read()
        sql_with_placeholders = sql_query.replace("%s", placeholders)
        pd.read_sql_query(sql_with_placeholders, db_connection, params=parameter_list)


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
    pg_clinical_logger.info(f"{df_name} sql file saved to {csv_file}.")


def grab_survey_opt_out():
    """
    Retrieve the list of MRNs for patients who have opted out of the survey.

    Parameters:
    - directory (str): Path to the directory containing the survey opt-out CSV file.

    Returns:
    - list: List of MRNs for patients who have opted out of the survey.
    """
    survey_opt_out_csv = f"{directory}\\data\\survey_opt_outs.csv"
    with open(survey_opt_out_csv, "r") as survey_opt_outs:
        opted_out_df = pd.read_csv(survey_opt_outs)
        mrns = opted_out_df["MRN"].drop_duplicates()
        mrn_list = [str(mrn) for mrn in mrns]
    return mrn_list


def run():
    parent_dir = os.path.abspath(os.path.join(context.get_context(directory), os.pardir))
    config_file = f"{parent_dir}\\routine_weekly\\config.json"
    today = datetime.now().strftime("%a %m %d %Y %H.%M.%S.%f")

    press_ganey_folder = "C:\\Users\\talendservice\\OneDrive - Vivent Health\\Quality\\Press Ganey"
    saving_folder = f"{press_ganey_folder}\\Data Files Sent to Press Ganey 331180 (our number) and MMDDYYYY"
    staging_folder = f"{saving_folder}\\staging"

    sql_file_survey_opt_outs = f"{directory}\\sql\\global__survey_opt_outs.sql"
    sql_file_clinical_visits = f"{directory}\\sql\\global__clinical_visits.sql"
    sql_file_pharmacy_visits = f"{directory}\\sql\\global__pharmacy_visits.sql"
    sql_file_clinical = f"{directory}\\sql\\full_clinical_pg.sql"

    pg_clinical_csv = f"{saving_folder}\\331180CL.csv {today}.csv" # to save
    final__pg_clinical_csv = f"{staging_folder}\\331180CL.csv" # to deliver

    execute(config_file, sql_file_survey_opt_outs, sql_file_clinical_visits, 
        sql_file_pharmacy_visits, sql_file_clinical, pg_clinical_csv, 
        final__pg_clinical_csv)
