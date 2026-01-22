# import os, os.path, json

# import pandas as pd

# from sqlalchemy import create_engine
# from sqlalchemy.engine import URL
# from tableauhyperapi import TableName, TableDefinition, SqlType

# from utils import (
#     logger,
#     connections,
#     context,
#     vh_config,
#     vh_tableau,
#     emails
# )

# directory = context.get_context(os.path.abspath(__file__))
# sql_file = f"{directory}/co_general_reg_monitoring/sql/reg_monitoring_nm3.sql"
# reg_monitoring_logger = logger.setup_logger(
#     "reg_monitoring_logger",
#     f"{directory}/logs/main.log"
# )

# config = vh_config.grab(reg_monitoring_logger)
# project_id = vh_config.grab_tableau_id(
#     project_name = "Clinical Operations",
#     logger = reg_monitoring_logger
# )

# def run(shared_drive):
#     reg_monitoring_logger.info("Clinical Operations - Registration Monitoring (PSRs).")
#     hyper_file = f"{shared_drive}/Registration Monitoring (PSRs).hyper"
#     if not os.path.exists(shared_drive):
#         os.makedirs(shared_drive)

#     try:
#         internal_engine = connections.engine_creation(
#             server=config['Clarity - VH']['server'],
#             db=config['Clarity - VH']['database'],
#             driver=config['Clarity - VH']['driver'],
#             internal_use=True
#             #server=config['Clarity - OCHIN']['server'],
#             #db=config['Clarity - OCHIN']['database'],
#             #driver=config['Clarity - OCHIN']['driver'],
#             #internal_use=False
#         )

#         with internal_engine.connect() as clarity_connection:
#             reg_monitoring_df = connections.sql_to_df(
#                 file = sql_file,
#                 connection = clarity_connection
#             )

#         if len(reg_monitoring_df.index) == 0:
#             reg_monitoring_logger.info("There are no data.")
#             reg_monitoring_logger.info("Clinical Operations - Registration Monitoring (PSRs) Daily ETL finished.")
#         else:
#             tableau_push(reg_monitoring_df, hyper_file)

#     except ConnectionError as connection_error:
#         reg_monitoring_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
#     except KeyError as key_error:
#         reg_monitoring_logger.error(f"Incorrect connection keys: {key_error}")


# def tableau_push(df, hyper_file):
#     reg_monitoring_logger.info("Creating Hyper Table.")

#     df["HOME PRIORITY"] = pd \
#         .to_numeric(df["HOME PRIORITY"]) \
#         .fillna(0) \
#         .astype(int)

#     df["MOBILE PRIORITY"] = pd \
#         .to_numeric(df["MOBILE PRIORITY"]) \
#         .fillna(0) \
#         .astype(int)

#     df["WORK PRIORITY"] = pd \
#         .to_numeric(df["WORK PRIORITY"]) \
#         .fillna(0) \
#         .astype(int)

#     df["FDS - Consent to Treat Months Old"] = pd \
#         .to_numeric(df["FDS - Consent to Treat Months Old"]) \
#         .fillna(0) \
#         .astype(int)
        
#     df["SA64 E-Sig Grievance Policy and Procedure Months Old"] = pd \
#         .to_numeric(df["SA64 E-Sig Grievance Policy and Procedure Months Old"]) \
#         .fillna(0) \
#         .astype(int)
        
#     df["SA64 E-SIG Rights and Responsibilities Months Old"] = pd \
#         .to_numeric(df["SA64 E-SIG Rights and Responsibilities Months Old"]) \
#         .fillna(0) \
#         .astype(int)
        
#     df["SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Months Old"] = pd \
#         .to_numeric(df["SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Months Old"]) \
#         .fillna(0) \
#         .astype(int)
        
#     df["FDS - Photo ID Months Old"] = pd \
#         .to_numeric(df["FDS - Photo ID Months Old"]) \
#         .fillna(0) \
#         .astype(int)
        
#     df["FDS - Private Insurance Months Old"] = pd \
#         .to_numeric(df["FDS - Private Insurance Months Old"]) \
#         .fillna(0) \
#         .astype(int)
        
#     df["FDS - Income Verification Months Old"] = pd \
#         .to_numeric(df["FDS - Income Verification Months Old"]) \
#         .fillna(0) \
#         .astype(int)

#     table_definition = TableDefinition(
#         table_name = TableName("Registration Monitoring (PSRs)"),
#         columns = [
#             TableDefinition.Column("MRN", SqlType.text()),
#             TableDefinition.Column("Registrar Login Dept", SqlType.text()),
#             TableDefinition.Column("VISIT DEPT", SqlType.text()),
#             TableDefinition.Column("STATE", SqlType.text()),
#             TableDefinition.Column("Registrar", SqlType.text()),
#             TableDefinition.Column("Reg Date", SqlType.date()),
#             TableDefinition.Column("Date of Registration", SqlType.timestamp()),
#             TableDefinition.Column("Days Since Registration", SqlType.int()),
#             TableDefinition.Column("VISIT TYPE", SqlType.text()),
#             TableDefinition.Column("VISIT DATE", SqlType.date()),
#             TableDefinition.Column("PAT_NAME", SqlType.text()),
#             TableDefinition.Column("SSN", SqlType.text()),
#             TableDefinition.Column("BIRTH_DATE", SqlType.date()),
#             TableDefinition.Column("ADDRESS LINE 1", SqlType.text()),
#             TableDefinition.Column("HAS ADDRESS", SqlType.text()),
#             TableDefinition.Column("ADDRESS LINE 2", SqlType.text()),
#             TableDefinition.Column("HOME #", SqlType.text()),
#             TableDefinition.Column("HOME PRIORITY", SqlType.int()),
#             TableDefinition.Column("MOBILE #", SqlType.text()),
#             TableDefinition.Column("MOBILE PRIORITY", SqlType.int()),
#             TableDefinition.Column("WORK #", SqlType.text()),
#             TableDefinition.Column("WORK PRIORITY", SqlType.int()),
#             TableDefinition.Column("PHONE PRIORITY SET", SqlType.text()),
#             TableDefinition.Column("EMAIL", SqlType.text()),
#             TableDefinition.Column("EMAIL STATUS", SqlType.text()),
#             TableDefinition.Column("VETERAN STATUS", SqlType.text()),
#             TableDefinition.Column("Care Team PCP", SqlType.text()),
#             TableDefinition.Column("Care Team RN", SqlType.text()),
#             TableDefinition.Column("Care Team Pharmacist", SqlType.text()),
#             TableDefinition.Column("Care Team Dentist", SqlType.text()),
#             TableDefinition.Column("Care Team Case Manager", SqlType.text()),
#             TableDefinition.Column("Care Team MH Provider", SqlType.text()),
#             TableDefinition.Column("COMPLETE CARE TEAM", SqlType.text()),
#             TableDefinition.Column("EMERGECY CONTACT RECORDED", SqlType.text()),
#             TableDefinition.Column("EMPLOYMENT STATUS", SqlType.text()),
#             TableDefinition.Column("Interpreter Needed", SqlType.text()),
#             TableDefinition.Column("LANGUAGE", SqlType.text()),
#             TableDefinition.Column("ENGISH FLUENCY", SqlType.text()),
#             TableDefinition.Column("PATIENT TYPE", SqlType.text()),
#             TableDefinition.Column("FDS - Consent to Treat", SqlType.text()),
#             TableDefinition.Column("FDS - Consent to Treat Date", SqlType.date()),
#             TableDefinition.Column("FDS - Consent to Treat Months Old", SqlType.int()),
#             TableDefinition.Column("SA64 E-Sig Grievance Policy and Procedure", SqlType.text()),
#             TableDefinition.Column("SA64 E-Sig Grievance Policy and Procedure Date", SqlType.date()),
#             TableDefinition.Column("SA64 E-Sig Grievance Policy and Procedure Months Old", SqlType.int()),
#             TableDefinition.Column("SA64 E-SIG Rights and Responsibilities", SqlType.text()),
#             TableDefinition.Column("SA64 E-SIG Rights and Responsibilities Date", SqlType.date()),
#             TableDefinition.Column("SA64 E-SIG Rights and Responsibilities Months Old", SqlType.int()),
#             TableDefinition.Column("SA64 E-SIG Acknowledgment of Receipt of Privacy Notice", SqlType.text()),
#             TableDefinition.Column("SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Date", SqlType.date()),
#             TableDefinition.Column("SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Months Old", SqlType.int()),
#             TableDefinition.Column("FDS - Photo ID", SqlType.text()),
#             TableDefinition.Column("FDS - Photo ID Date", SqlType.date()),
#             TableDefinition.Column("FDS - Photo ID Months Old", SqlType.int()),
#             TableDefinition.Column("FDS - Private Insurance", SqlType.text()),
#             TableDefinition.Column("FDS - Private Insurance Date", SqlType.date()),
#             TableDefinition.Column("FDS - Private Insurance Months Old", SqlType.int()),
#             TableDefinition.Column("FDS - Income Verification", SqlType.text()),
#             TableDefinition.Column("FDS - Income Verification Date", SqlType.date()),
#             TableDefinition.Column("FDS - Income Verification Months Old", SqlType.int()),
#             TableDefinition.Column("Migrant/Seasonal", SqlType.text()),
#             TableDefinition.Column("Homelessness", SqlType.text()),
#             TableDefinition.Column("FPL%", SqlType.double()),
#             TableDefinition.Column("FPL STATUS", SqlType.text())
#         ]
#     )

#     vh_tableau.push_to_tableau(
#         df=df,
#         hyper_file=hyper_file,
#         table_definition=table_definition,
#         logger=reg_monitoring_logger,
#         project_id=project_id
#     )

#     reg_monitoring_logger.info(
#         "Clinical Operations - Registration Monitoring (PSRs) pushed to Tableau."
#     )

import os, os.path, json

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tableauhyperapi import TableName, TableDefinition, SqlType

from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau,
    emails
)

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/co_general_reg_monitoring/sql/reg_monitoring_nm3.sql"
reg_monitoring_logger = logger.setup_logger(
    "reg_monitoring_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(reg_monitoring_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Operations",
    logger = reg_monitoring_logger
)

def run(shared_drive):
    reg_monitoring_logger.info("Clinical Operations - Registration Monitoring (PSRs).")
    hyper_file = f"{shared_drive}/Registration Monitoring (PSRs).hyper"
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
            reg_monitoring_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

        if len(reg_monitoring_df.index) == 0:
            reg_monitoring_logger.info("There are no data.")
            reg_monitoring_logger.info("Clinical Operations - Registration Monitoring (PSRs) Daily ETL finished.")
        else:
            tableau_push(reg_monitoring_df, hyper_file)

    except ConnectionError as connection_error:
        reg_monitoring_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        reg_monitoring_logger.error(f"Incorrect connection keys: {key_error}")


def tableau_push(df, hyper_file):
    reg_monitoring_logger.info("Creating Hyper Table.")

    df["HOME PRIORITY"] = pd \
        .to_numeric(df["HOME PRIORITY"]) \
        .fillna(0) \
        .astype(int)

    df["MOBILE PRIORITY"] = pd \
        .to_numeric(df["MOBILE PRIORITY"]) \
        .fillna(0) \
        .astype(int)

    df["WORK PRIORITY"] = pd \
        .to_numeric(df["WORK PRIORITY"]) \
        .fillna(0) \
        .astype(int)

    df["FDS - Consent to Treat Months Old"] = pd \
        .to_numeric(df["FDS - Consent to Treat Months Old"]) \
        .fillna(0) \
        .astype(int)
        
    df["SA64 E-Sig Grievance Policy and Procedure Months Old"] = pd \
        .to_numeric(df["SA64 E-Sig Grievance Policy and Procedure Months Old"]) \
        .fillna(0) \
        .astype(int)
        
    df["SA64 E-SIG Rights and Responsibilities Months Old"] = pd \
        .to_numeric(df["SA64 E-SIG Rights and Responsibilities Months Old"]) \
        .fillna(0) \
        .astype(int)
        
    df["SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Months Old"] = pd \
        .to_numeric(df["SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Months Old"]) \
        .fillna(0) \
        .astype(int)
        
    df["FDS - Photo ID Months Old"] = pd \
        .to_numeric(df["FDS - Photo ID Months Old"]) \
        .fillna(0) \
        .astype(int)
        
    df["FDS - Private Insurance Months Old"] = pd \
        .to_numeric(df["FDS - Private Insurance Months Old"]) \
        .fillna(0) \
        .astype(int)
        
    df["FDS - Income Verification Months Old"] = pd \
        .to_numeric(df["FDS - Income Verification Months Old"]) \
        .fillna(0) \
        .astype(int)

    df["SA64 General Dental Consent Form Months Old"] = pd \
    .to_numeric(df["SA64 General Dental Consent Form Months Old"]) \
    .fillna(0) \
    .astype(int)

    df["SA64 BHWC Consent for Services Months Old"] = pd \
        .to_numeric(df["SA64 BHWC Consent for Services Months Old"]) \
        .fillna(0) \
        .astype(int)

    df["SA64 Mandatory Disclosure KClaunch Months Old"] = pd \
        .to_numeric(df["SA64 Mandatory Disclosure KClaunch Months Old"]) \
        .fillna(0) \
        .astype(int)

    df["SA64 CO Consent for Behavioral Health Services Months Old"] = pd \
        .to_numeric(df["SA64 CO Consent for Behavioral Health Services Months Old"]) \
        .fillna(0) \
        .astype(int)

    df["SA64 Consent for Clinical Treatment Months Old"] = pd \
        .to_numeric(df["SA64 Consent for Clinical Treatment Months Old"]) \
        .fillna(0) \
        .astype(int)

    df["SA64 Financial Consent Months Old"] = pd \
        .to_numeric(df["SA64 Financial Consent Months Old"]) \
        .fillna(0) \
        .astype(int)

    df["SA64 Notice of Privacy Practices Months Old"] = pd \
        .to_numeric(df["SA64 Notice of Privacy Practices Months Old"]) \
        .fillna(0) \
        .astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Registration Monitoring (PSRs)"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("Registrar Login Dept", SqlType.text()),
            TableDefinition.Column("VISIT DEPT", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("Registrar", SqlType.text()),
            TableDefinition.Column("Reg Date", SqlType.date()),
            TableDefinition.Column("Date of Registration", SqlType.timestamp()),
            TableDefinition.Column("Days Since Registration", SqlType.int()),
            TableDefinition.Column("VISIT TYPE", SqlType.text()),
            TableDefinition.Column("VISIT DATE", SqlType.date()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("SSN", SqlType.text()),
            TableDefinition.Column("BIRTH_DATE", SqlType.date()),
            TableDefinition.Column("ADDRESS LINE 1", SqlType.text()),
            TableDefinition.Column("HAS ADDRESS", SqlType.text()),
            TableDefinition.Column("ADDRESS LINE 2", SqlType.text()),
            TableDefinition.Column("HOME #", SqlType.text()),
            TableDefinition.Column("HOME PRIORITY", SqlType.int()),
            TableDefinition.Column("MOBILE #", SqlType.text()),
            TableDefinition.Column("MOBILE PRIORITY", SqlType.int()),
            TableDefinition.Column("WORK #", SqlType.text()),
            TableDefinition.Column("WORK PRIORITY", SqlType.int()),
            TableDefinition.Column("PHONE PRIORITY SET", SqlType.text()),
            TableDefinition.Column("EMAIL", SqlType.text()),
            TableDefinition.Column("EMAIL STATUS", SqlType.text()),
            TableDefinition.Column("VETERAN STATUS", SqlType.text()),
            TableDefinition.Column("Care Team PCP", SqlType.text()),
            TableDefinition.Column("Care Team RN", SqlType.text()),
            TableDefinition.Column("Care Team Pharmacist", SqlType.text()),
            TableDefinition.Column("Care Team Dentist", SqlType.text()),
            TableDefinition.Column("Care Team Case Manager", SqlType.text()),
            TableDefinition.Column("Care Team MH Provider", SqlType.text()),
            TableDefinition.Column("COMPLETE CARE TEAM", SqlType.text()),
            TableDefinition.Column("EMERGECY CONTACT RECORDED", SqlType.text()),
            TableDefinition.Column("EMPLOYMENT STATUS", SqlType.text()),
            TableDefinition.Column("Interpreter Needed", SqlType.text()),
            TableDefinition.Column("LANGUAGE", SqlType.text()),
            TableDefinition.Column("ENGISH FLUENCY", SqlType.text()),
            TableDefinition.Column("PATIENT TYPE", SqlType.text()),
            TableDefinition.Column("FDS - Consent to Treat", SqlType.text()),
            TableDefinition.Column("FDS - Consent to Treat Date", SqlType.date()),
            TableDefinition.Column("FDS - Consent to Treat Months Old", SqlType.int()),
            TableDefinition.Column("SA64 E-Sig Grievance Policy and Procedure", SqlType.text()),
            TableDefinition.Column("SA64 E-Sig Grievance Policy and Procedure Date", SqlType.date()),
            TableDefinition.Column("SA64 E-Sig Grievance Policy and Procedure Months Old", SqlType.int()),
            TableDefinition.Column("SA64 E-SIG Rights and Responsibilities", SqlType.text()),
            TableDefinition.Column("SA64 E-SIG Rights and Responsibilities Date", SqlType.date()),
            TableDefinition.Column("SA64 E-SIG Rights and Responsibilities Months Old", SqlType.int()),
            TableDefinition.Column("SA64 E-SIG Acknowledgment of Receipt of Privacy Notice", SqlType.text()),
            TableDefinition.Column("SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Date", SqlType.date()),
            TableDefinition.Column("SA64 E-SIG Acknowledgment of Receipt of Privacy Notice Months Old", SqlType.int()),
            TableDefinition.Column("FDS - Photo ID", SqlType.text()),
            TableDefinition.Column("FDS - Photo ID Date", SqlType.date()),
            TableDefinition.Column("FDS - Photo ID Months Old", SqlType.int()),
            TableDefinition.Column("FDS - Private Insurance", SqlType.text()),
            TableDefinition.Column("FDS - Private Insurance Date", SqlType.date()),
            TableDefinition.Column("FDS - Private Insurance Months Old", SqlType.int()),
            TableDefinition.Column("FDS - Income Verification", SqlType.text()),
            TableDefinition.Column("FDS - Income Verification Date", SqlType.date()),
            TableDefinition.Column("FDS - Income Verification Months Old", SqlType.int()),
            # New SA64 Document Columns
        TableDefinition.Column("SA64 General Dental Consent Form", SqlType.text()),
        TableDefinition.Column("SA64 General Dental Consent Form Date", SqlType.date()),
        TableDefinition.Column("SA64 General Dental Consent Form Months Old", SqlType.int()),

        TableDefinition.Column("SA64 BHWC Consent for Services", SqlType.text()),
        TableDefinition.Column("SA64 BHWC Consent for Services Date", SqlType.date()),
        TableDefinition.Column("SA64 BHWC Consent for Services Months Old", SqlType.int()),

        TableDefinition.Column("SA64 Mandatory Disclosure KClaunch", SqlType.text()),
        TableDefinition.Column("SA64 Mandatory Disclosure KClaunch Date", SqlType.date()),
        TableDefinition.Column("SA64 Mandatory Disclosure KClaunch Months Old", SqlType.int()),

        TableDefinition.Column("SA64 CO Consent for Behavioral Health Services", SqlType.text()),
        TableDefinition.Column("SA64 CO Consent for Behavioral Health Services Date", SqlType.date()),
        TableDefinition.Column("SA64 CO Consent for Behavioral Health Services Months Old", SqlType.int()),

        TableDefinition.Column("SA64 Consent for Clinical Treatment", SqlType.text()),
        TableDefinition.Column("SA64 Consent for Clinical Treatment Date", SqlType.date()),
        TableDefinition.Column("SA64 Consent for Clinical Treatment Months Old", SqlType.int()),

        TableDefinition.Column("SA64 Financial Consent", SqlType.text()),
        TableDefinition.Column("SA64 Financial Consent Date", SqlType.date()),
        TableDefinition.Column("SA64 Financial Consent Months Old", SqlType.int()),

        TableDefinition.Column("SA64 Notice of Privacy Practices", SqlType.text()),
        TableDefinition.Column("SA64 Notice of Privacy Practices Date", SqlType.date()),
        TableDefinition.Column("SA64 Notice of Privacy Practices Months Old", SqlType.int()),

            TableDefinition.Column("Migrant/Seasonal", SqlType.text()),
            TableDefinition.Column("Homelessness", SqlType.text()),
            TableDefinition.Column("FPL%", SqlType.double()),
            TableDefinition.Column("FPL STATUS", SqlType.text()),
            TableDefinition.Column("Next Appointment Date", SqlType.text()),       
TableDefinition.Column("Patient Registration Flag?", SqlType.text()),
TableDefinition.Column("Last Flag Update Date", SqlType.text()),
TableDefinition.Column("Registration Flag Applied By", SqlType.text())
        ]
    )

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=reg_monitoring_logger,
        project_id=project_id
    )

    reg_monitoring_logger.info(
        "Clinical Operations - Registration Monitoring (PSRs) pushed to Tableau."
    )

