import os
import sys
import pandas as pd
import json

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tableauhyperapi import TableName, TableDefinition, SqlType
from datetime import datetime

from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau
)

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/co_medical_n_consecutive_no_shows/sql/consecutive_no_shows.sql"
extr_logger = logger.setup_logger("extr_logger", f"{directory}/logs/main.log")
config = vh_config.grab(extr_logger)
project_id = vh_config.grab_tableau_id("Clinical Operations", extr_logger)

def run(shared_drive):
    extr_logger.info("Clinical Operations - N Consecutive No Shows.")
    hyper_file = f"{shared_drive}/N Consecutive No Shows.hyper"
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
            #server=config['Clarity - OCHIN']['server'],
            #db=config['Clarity - OCHIN']['database'],
            #driver=config['Clarity - OCHIN']['driver'],
            #internal_use=False
        )

        with internal_engine.connect() as clarity_connection:
            df = connections \
                .sql_to_df(file=sql_file, connection=clarity_connection)

        if len(df.index) == 0:
            extr_logger.info("There are no data.")
            extr_logger.info("N Consecutive No Shows Daily ETL Finished")
        else:
            df.loc[df["NO_SHOWS_GRP"] == "YES", "NO_SHOWS_GRP"] = 1
            df.loc[df["NO_SHOWS_GRP"] == "NO", "NO_SHOWS_GRP"] = 0

            df["value_group"] = (df.NO_SHOWS_GRP.diff(1) != 0) \
                .astype("int") \
                .cumsum()

            extr_logger.info("Grouping DataFrame.")
            grouped_df = pd.DataFrame({
                "PATIENT_NAME": df.groupby(["MRN", "value_group", "LOS"]).PAT_NAME.first(),
                "MRN": df.groupby(["MRN", "value_group", "LOS"]).MRN.first(),
                "BEGIN_VISIT_DATE" : df.groupby(["MRN", "value_group", "LOS"]).VISIT_DATE.first(),
                "END_VISIT_DATE" : df.groupby(["MRN", "value_group", "LOS"]).VISIT_DATE.last(),
                "NUM_OF_NO_SHOWS" : df.groupby(["MRN", "value_group", "LOS"]).size(),
                "LOS" : df.groupby(["MRN", "value_group", "LOS"]).LOS.first(),
                "STATE" : df.groupby(["MRN", "value_group", "LOS"]).STATE.first(),
                "CITY" : df.groupby(["MRN", "value_group", "LOS"]).CITY.first(),
                "No_Show_Flag_Medical" : df.groupby(["MRN", "value_group", "LOS"]).No_Show_Flag_Medical.first(),
                "No_Show_Flag_BH" : df.groupby(["MRN", "value_group", "LOS"]).No_Show_Flag_BH.first(),
                "No_Show_Flag_Dental" : df.groupby(["MRN", "value_group", "LOS"]).No_Show_Flag_Dental.first(),
                "APPOINTMENT_STATUS" : df.groupby(["MRN", "value_group", "LOS"]).APPT_STATUS.first()}) \
                                        .reset_index(drop=True)

            extr_logger.info("Grabbing only No Shows.")
            final_df = grouped_df[(grouped_df["APPOINTMENT_STATUS"] \
                    .isin(["No Show",
                           "Left without seen,"
                           "Late Cancel",
                           "Late - Patient too late to be seen"])) &
                           (grouped_df["NUM_OF_NO_SHOWS"] >= 3)]

            final_df \
                .loc[final_df["APPOINTMENT_STATUS"] != "No Show",
                              "APPOINTMENT_STATUS"] = "No Show"
            final_df["UPDATE_DTTM"] = datetime.now()

            tableau_push(final_df, hyper_file)
    except ConnectionError as connection_error:
        extr_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
        sys.exit(1)
    except KeyError as key_error:
        extr_logger.error(f"Incorrect connection keys: {key_error}")
        sys.exit(1)

    extr_logger.debug("Extraction complete.")


def tableau_push(df, hyper_file):
    extr_logger.info("Creating Hyper Table.")

    table_definition = TableDefinition(
        table_name = TableName("N Consecutive No Shows"),
        columns = [
            TableDefinition.Column("PATIENT_NAME", SqlType.text()),
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("BEGIN_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("END_VISIT_DATE", SqlType.date()),
            TableDefinition.Column("NUM_OF_NO_SHOWS", SqlType.int()),
            TableDefinition.Column("LOS", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("No_Show_Flag_Medical", SqlType.text()),
            TableDefinition.Column("No_Show_Flag_BH", SqlType.text()),
            TableDefinition.Column("No_Show_Flag_Dental", SqlType.text()),
            TableDefinition.Column("APPOINTMENT_STATUS", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
            
        ]
    )
    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=extr_logger,
        project_id=project_id
    )

    extr_logger \
        .info("Clinical Operations - N Consecutive No Shows pushed to Tableau.")
