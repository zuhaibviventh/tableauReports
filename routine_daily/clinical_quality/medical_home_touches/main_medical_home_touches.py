import os
import os.path
import datetime

from tableauhyperapi import TableName, TableDefinition, SqlType

from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau
)

directory = context.get_context(os.path.abspath(__file__))
today = datetime.datetime.now()
sql_file = f"{directory}/medical_home_touches/sql/medical_home_touches.sql"

med_home_touch_logger = logger.setup_logger(
    "med_home_touch_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(med_home_touch_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = med_home_touch_logger
)

def run(shared_drive):
    med_home_touch_logger.info(
        "Clinical Quality - Medical Home Touches."
    )
    dashboard_shared_drive = "//FSS001SVR/Analysis/Routines/daily/clinical_quality"
    hyper_file = f"{shared_drive}/Medical Home Touches.hyper"
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
            med_home_touch_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(med_home_touch_df.index) == 0:
            med_home_touch_logger.info("There are no data.")
            med_home_touch_logger.info("Clinical Quality - Medical Home Touches Daily ETL finished.")
        else:
            tableau_push(med_home_touch_df, hyper_file)
            to_health_drive(med_home_touch_df, dashboard_shared_drive)

    except ConnectionError as connection_error:
        med_home_touch_logger.error(f"Unable to connect to VH - Vivent Health: {connection_error}")
    except KeyError as key_error:
        med_home_touch_logger.error(f"Incorrect connection keys: {key_error}")


def to_health_drive(df, dashboard_shared_drive):
    med_home_touch_logger.info(f"Writing to {dashboard_shared_drive}.")
    csv_name = f'Medical Home Billing Dashboard Data {today.strftime("%B %d %Y")}'

    with open(f"{dashboard_shared_drive}/{csv_name}.csv", "wb") as data:
        df.to_csv(data, index = False)

    med_home_touch_logger.info(f"Finished writing to {dashboard_shared_drive}.")


def tableau_push(df, hyper_file):
    med_home_touch_logger.info("Creating Hyper Table.")

    df["PLAN_ID"] = df["PLAN_ID"].astype(int)
    df["PAYOR_ID"] = df["PAYOR_ID"].astype(int)
    df["Days Since Assessment"] = df["Days Since Assessment"].astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Medical Home Touches"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("Patient", SqlType.text()),
            TableDefinition.Column("Care Team PCP", SqlType.text()),
            TableDefinition.Column("Care Team RN", SqlType.text()),
            TableDefinition.Column("Care Team Pharmacist", SqlType.text()),
            TableDefinition.Column("Care Team Dentist", SqlType.text()),
            TableDefinition.Column("Care Team Case Manager", SqlType.text()),
            TableDefinition.Column("Care Team MH Provider", SqlType.text()),
            TableDefinition.Column("Patient Registrar", SqlType.text()),
            TableDefinition.Column("Registration Date", SqlType.date()),
            TableDefinition.Column("Primary Location", SqlType.text()),
            TableDefinition.Column("Complete Care Team", SqlType.text()),
            TableDefinition.Column("Last Assessment if Less than 19 Months Old", SqlType.date()),
            TableDefinition.Column("Days Since Assessment", SqlType.int()),
            TableDefinition.Column("Days Since Pharmacy Assessment", SqlType.int()),
            TableDefinition.Column("Last SBIRT", SqlType.date()),
            TableDefinition.Column("Months Since SBIRT", SqlType.int()),
            TableDefinition.Column("Completed Clinical Visit", SqlType.text()),
            TableDefinition.Column("Completed Patient Phone Contact", SqlType.text()),
            TableDefinition.Column("Refill", SqlType.text()),
            TableDefinition.Column("Referral", SqlType.text()),
            TableDefinition.Column("Interim Note", SqlType.text()),
            TableDefinition.Column("PLAN_ID", SqlType.int()),
            TableDefinition.Column("BENEFIT_PLAN_NAME", SqlType.text()),
            TableDefinition.Column("PAYOR_ID", SqlType.int()),
            TableDefinition.Column("PAYOR_NAME", SqlType.text()),
            TableDefinition.Column("CVG_EFF_DT", SqlType.date()),
            TableDefinition.Column("CVG_TERM_DT", SqlType.date()),
            TableDefinition.Column("Today", SqlType.text()),
            TableDefinition.Column("Care Coord Note", SqlType.text()),
            TableDefinition.Column("MyChart Message", SqlType.text()),
            TableDefinition.Column("Last Care Plan", SqlType.date()),
            TableDefinition.Column("Care Plan Creator", SqlType.text()),
            TableDefinition.Column("Mos Since Care Plan", SqlType.int()),
            TableDefinition.Column("Next Medical Visit Provider", SqlType.text()),
            TableDefinition.Column("Next Medical Visit", SqlType.date()),
            TableDefinition.Column("CLINICAL PHARMACY COHORT", SqlType.text()),
            TableDefinition.Column("WAI_SMARTPHRASE_USED", SqlType.text()),
            TableDefinition.Column("WAI_SMARTPHRASE_USED_DATE", SqlType.date()),
            TableDefinition.Column("MONTHS_SINCE_SA64WAI", SqlType.int()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text()),
            TableDefinition.Column("Last Pharmacy Assessment Date", SqlType.date()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("COMP_MED_REVIEW_DATE", SqlType.date()),
            TableDefinition.Column("COMP_MED_REVIEW_ENCOUNTER_TYPE", SqlType.text()),
            TableDefinition.Column("COMP_MED_REVIEW_SMARTPHRASE_USE", SqlType.text()),
            TableDefinition.Column("COMP_MED_REVIEW_SMARTPHRASE_USE_DATE", SqlType.date())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=med_home_touch_logger,
        project_id=project_id
    )

    med_home_touch_logger.info(
        "Clinical Quality - Medical Home Touches pushed to Tableau."
    )
