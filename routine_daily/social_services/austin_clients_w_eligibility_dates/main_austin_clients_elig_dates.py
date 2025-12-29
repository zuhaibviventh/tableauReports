import os
from tableauhyperapi import SqlType
from tableauhyperapi import TableName
from tableauhyperapi import TableDefinition
from utils import logger
from utils import connections
from utils import context
from utils import vh_config
from utils import vh_tableau
import pandas as pd
import numpy as np
import pantab

# Constants
directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/austin_clients_w_eligibility_dates/sql/austin_clients_w_eligibility_dates.sql"
epic_sql_file = f"{directory}/austin_clients_w_eligibility_dates/sql/epic_austin_clients_w_eligibility_dates.sql"
logger = logger.setup_logger("austin_clients_eligibility_dates_logger", f"{directory}/logs/main.log")
config = vh_config.grab(logger)
project_id = vh_config.grab_tableau_id(project_name="Social Services", logger=logger)


def run(shared_drive):
    logger.info("Social Services - PE - Austin Clients with Upcoming Eligibility Dates.")
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    hyper_file = f"{shared_drive}/Austin Clients with Upcoming Eligibility Due Dates.hyper"

    try:
        internal_engine = connections.engine_creation(
            server=config['PEViventHealth']['server'],
            db=config['PEViventHealth']['database'],
            driver=config['PEViventHealth']['driver'],
            uid=config['PEViventHealth']['uid'],
            pwd=config['PEViventHealth']['pwd'],
            internal_use=False
        )

        with internal_engine.connect() as pe_connection:
            logger.info("Pulling data from PE")
            austin_clients_eligibility_dates_df = connections.sql_to_df(sql_file, pe_connection)
    except ConnectionError as connection_error:
        logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        logger.error(f"Incorrect connection keys: {key_error}")

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            logger.info("Pulling data from Epic")
            epic_austin_clients_eligibility_dates_df = connections.sql_to_df(epic_sql_file, clarity_connection)
    except ConnectionError as connection_error:
        logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        logger.error(f"Incorrect connection keys: {key_error}")

    # Full outer join
    df_merged = pd.merge(austin_clients_eligibility_dates_df,
                         epic_austin_clients_eligibility_dates_df,
                         how="outer",
                         on=["Epic MRN",
                             "Last Name",
                             "First Name",
                             "Provider",
                             "Provider Relationship",
                             "SCPDateOfBirth",
                             "DOB",
                             "Program",
                             "Birthday This Year",
                             "Eligibility Due",
                             "Days Until Eligibility Due",
                             "Mobile Phone",
                             "Email"])

    df_merged["SCPDateOfBirth"] = pd.to_datetime(df_merged["SCPDateOfBirth"])
    df_merged["DOB"] = pd.to_datetime(df_merged["DOB"])
    df_merged["Birthday This Year"] = pd.to_datetime(df_merged["Birthday This Year"])
    df_merged["Eligibility Due"] = pd.to_datetime(df_merged["Eligibility Due"])
    df_merged["Last Eligibility Assessment"] = pd.to_datetime(df_merged["Last Eligibility Assessment"])
    df_merged["Next Any Appt"] = pd.to_datetime(df_merged["Next Any Appt"])
    df_merged["Next PCP Appt"] = pd.to_datetime(df_merged["Next PCP Appt"])
    df_merged["Next Dental Appt"] = pd.to_datetime(df_merged["Next Dental Appt"])

    df_merged["Provider"] = df_merged['Provider'].apply(lambda x: "NO PROVIDER" if pd.isnull(x) else x)
    df_merged["Client ID"] = df_merged.apply(lambda row: row["Epic MRN"] 
                                                                if row["PE Client ID"] == "nan" or 
                                                                    pd.isnull(row["PE Client ID"]) or 
                                                                    row["PE Client ID"] == "None" 
                                                                else row["PE Client ID"], axis=1)
    df_merged = df_merged.drop('PE Client ID', axis=1)
    df_merged.drop_duplicates(inplace=True)

    lst = ["Client ID",
            "ASA Client ID",
            "Epic MRN",
            "Last Name",
            "First Name",
            "Provider",
            "Provider Relationship",
            "SCPDateOfBirth",
            "DOB",
            "Program",
            "Birthday This Year",
            "Eligibility Due",
            "Days Until Eligibility Due",
            "Mobile Phone",
            "Email",
            "Last Eligibility Assessment",
            "Next Any Appt",
            "Next Appt Prov",
            "Next PCP Appt",
            "Next PCP Appt Prov",
            "Next Dental Appt",
            "Next Dental Appt Prov"]

    df_merged = df_merged[lst]

    process_data("Austin Clients with Upcoming Eligibility Dates", df_merged, hyper_file)


def process_data(process_name, data_df, hyper_file):
    if len(data_df.index) == 0:
        logger.info(f"There are no data for {process_name}.")
    else:
        logger.info(f"Pushing to Tableau: {process_name}")
        pantab.frame_to_hyper(data_df, hyper_file, table=process_name)
        vh_tableau.publish_data_source(project_id=project_id, logger=logger, hyper_file=hyper_file)
        logger.info(f"{process_name} Daily ETL finished.")

if __name__ == "__main__":
    run()
