import os
import os.path

from tableauhyperapi import TableName, TableDefinition, SqlType

from utils import (
    logger,
    connections,
    context,
    vh_config,
    vh_tableau
)

directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/cq_risk_register/sql/risk_register.sql"
risk_register_logger = logger.setup_logger(
    "risk_register_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(risk_register_logger)
project_id = vh_config.grab_tableau_id(
    project_name = "Clinical Quality",
    logger = risk_register_logger
)

def run(shared_drive):
    risk_register_logger.info("Clinical Quality - Risk Register.")
    hyper_file = f"{shared_drive}/Risk Register.hyper"
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
            risk_register_df = connections.sql_to_df(
                file = sql_file,
                connection = clarity_connection
            )

    except ConnectionError as connection_error:
        risk_register_logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        risk_register_logger.error(f"Incorrect connection keys: {key_error}")

    risk_register_logger.debug("Table cleanup.")
    risk_register_df["Med Count"] = risk_register_df["Med Count"].fillna(0)
    risk_register_df["Months Since Care Plan"] = risk_register_df["Months Since Care Plan"].fillna(0)
    risk_register_df["Last VL"] = risk_register_df["Last VL"].fillna(0.0)
    risk_register_df["Last A1c (DM only)"] = risk_register_df["Last A1c (DM only)"].fillna(0.0)
    risk_register_df["BMI_LAST"] = risk_register_df["BMI_LAST"].fillna(0.0)

    risk_register_df["Psychiatric Diagnosis Type"] = risk_register_df["Psychiatric Diagnosis Type"].fillna("None")
    risk_register_df["Substance"] = risk_register_df["Substance"].fillna("None")
    risk_register_df["Last BP (HTN Only)"] = risk_register_df["Last BP (HTN Only)"].fillna("None")
    risk_register_df["HAS COPD"] = risk_register_df["HAS COPD"].fillna("None")
    risk_register_df["HVD_TYPE"] = risk_register_df["HVD_TYPE"].fillna("None")
    risk_register_df["CKD_SEVERITY"] = risk_register_df["CKD_SEVERITY"].fillna("None")
    risk_register_df["CI_SEVERITY"] = risk_register_df["CI_SEVERITY"].fillna("None")

    risk_register_df["HOUSING_PROBLEMS"] = risk_register_df["HOUSING_PROBLEMS"].fillna("Stable")
    risk_register_df["TRANSPORT_PROBLEMS"] = risk_register_df["TRANSPORT_PROBLEMS"].fillna("No Transp Problems")
    risk_register_df["Financial Problem"] = risk_register_df["Financial Problem"].fillna("Not Hard")

    risk_register_df["BMI_LAST"] = risk_register_df["BMI_LAST"].astype(float)
    risk_register_df["Last A1c (DM only)"] = risk_register_df["Last A1c (DM only)"].astype(float)
    risk_register_df["Last VL"] = risk_register_df["Last VL"].astype(float)

    risk_register_df["CKD Risk"] = risk_register_df["CKD Risk"].astype(int)
    risk_register_df["BMI Risk"] = risk_register_df["BMI Risk"].astype(int)
    risk_register_df["Transport Risk"] = risk_register_df["Transport Risk"].astype(int)
    risk_register_df["HVD Risk"] = risk_register_df["HVD Risk"].astype(int)
    risk_register_df["COPD Risk"] = risk_register_df["Financial Risk"].astype(int)
    risk_register_df["BP Risk"] = risk_register_df["BP Risk"].astype(int)
    risk_register_df["A1c Risk"] = risk_register_df["A1c Risk"].astype(int)
    risk_register_df["CI Risk"] = risk_register_df["CI Risk"].astype(int)
    risk_register_df["VL Risk"] = risk_register_df["VL Risk"].astype(int)
    risk_register_df["Med Risk"] = risk_register_df["Med Risk"].astype(int)
    risk_register_df["Housing Risk"] = risk_register_df["Housing Risk"].astype(int)
    risk_register_df["Substance Use Risk"] = risk_register_df["Substance Use Risk"].astype(int)
    risk_register_df["Psychiatric Diagnosis Risk"] = risk_register_df["Psychiatric Diagnosis Risk"].astype(int)
    risk_register_df["Risk Score"] = risk_register_df["Risk Score"].astype(int)
    risk_register_df["Med Count"] = risk_register_df["Med Count"].astype(int)
    risk_register_df["Months Since Care Plan"] = risk_register_df["Months Since Care Plan"].astype(int)
    risk_register_df["AGE"] = risk_register_df["AGE"].astype(int)

    table_definition = TableDefinition(
        table_name = TableName("Risk Register"),
        columns = [
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_NAME", SqlType.text()),
            TableDefinition.Column("AGE", SqlType.int()),
            TableDefinition.Column("SEX", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("PCP", SqlType.text()),
            TableDefinition.Column("Psychiatric Diagnosis Type", SqlType.text()),
            TableDefinition.Column("Psychiatric Diagnosis Risk", SqlType.int()),
            TableDefinition.Column("Substance", SqlType.text()),
            TableDefinition.Column("Substance Use Risk", SqlType.int()),
            TableDefinition.Column("Med Count", SqlType.int()),
            TableDefinition.Column("Med Risk", SqlType.int()),
            TableDefinition.Column("Last VL", SqlType.double()),
            TableDefinition.Column("VL Risk", SqlType.int()),
            TableDefinition.Column("Last A1c (DM only)", SqlType.double()),
            TableDefinition.Column("A1c Risk", SqlType.int()),
            TableDefinition.Column("Last BP (HTN Only)", SqlType.text()),
            TableDefinition.Column("BP Risk", SqlType.int()),
            TableDefinition.Column("HAS COPD", SqlType.text()),
            TableDefinition.Column("COPD Risk", SqlType.int()),
            TableDefinition.Column("HVD_TYPE", SqlType.text()),
            TableDefinition.Column("HVD Risk", SqlType.int()),
            TableDefinition.Column("CKD_SEVERITY", SqlType.text()),
            TableDefinition.Column("CKD Risk", SqlType.int()),
            TableDefinition.Column("BMI_LAST", SqlType.double()),
            TableDefinition.Column("BMI Risk", SqlType.int()),
            TableDefinition.Column("CI_SEVERITY", SqlType.text()),
            TableDefinition.Column("CI Risk", SqlType.int()),
            TableDefinition.Column("HOUSING_PROBLEMS", SqlType.text()),
            TableDefinition.Column("Housing Risk", SqlType.int()),
            TableDefinition.Column("TRANSPORT_PROBLEMS", SqlType.text()),
            TableDefinition.Column("Transport Risk", SqlType.int()),
            TableDefinition.Column("Financial Problem", SqlType.text()),
            TableDefinition.Column("Financial Risk", SqlType.int()),
            TableDefinition.Column("Risk Score", SqlType.int()),
            TableDefinition.Column("RISK CATEGORY", SqlType.text()),
            TableDefinition.Column("Last Care Plan", SqlType.date()),
            TableDefinition.Column("Months Since Care Plan", SqlType.int()),
            TableDefinition.Column("Care Plan Creator", SqlType.text()),
            TableDefinition.Column("Last BH Visit", SqlType.date()),
            TableDefinition.Column("Last BH Visit Provider", SqlType.text()),
            TableDefinition.Column("Next BH Visit", SqlType.date()),
            TableDefinition.Column("Next BH Visit Provider", SqlType.text()),
            TableDefinition.Column("Last CP Provider", SqlType.text()),
            TableDefinition.Column("Last CP Visit", SqlType.date()),
            TableDefinition.Column("Next CP Visit Provider", SqlType.text()),
            TableDefinition.Column("Next CP Visit", SqlType.date()),
            TableDefinition.Column("Last Nutritionist Visit Provider", SqlType.text()),
            TableDefinition.Column("Last Nutritionist Visit", SqlType.date()),
            TableDefinition.Column("Next Nutritionist Visit Provider", SqlType.text()),
            TableDefinition.Column("Next Nutritionist Visit", SqlType.date()),
            TableDefinition.Column("Last Medical Visit Provider", SqlType.text()),
            TableDefinition.Column("Last Medical Visit", SqlType.date()),
            TableDefinition.Column("Next Medical Visit Provider", SqlType.text()),
            TableDefinition.Column("Next Medical Visit", SqlType.date()),
            TableDefinition.Column("Wisconsin Medicaid Medical Home Patient", SqlType.text()),
            TableDefinition.Column("TODAY", SqlType.date())
        ]
    )
    risk_register_logger.info("Clinical Quality - Risk Register ETL finished")

    vh_tableau.push_to_tableau(
        df = risk_register_df,
        hyper_file = hyper_file,
        table_definition = table_definition,
        logger = risk_register_logger,
        project_id = project_id
    )

    risk_register_logger.info(
        "Clinical Quality - Risk Register Datasource pushed to Tableau."
    )