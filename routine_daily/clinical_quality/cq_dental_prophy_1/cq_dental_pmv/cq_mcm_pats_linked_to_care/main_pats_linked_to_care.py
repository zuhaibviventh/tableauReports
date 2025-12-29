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
sql_file = (f"{directory}/cq_mcm_pats_linked_to_care/"
            "sql/pats_linked_to_care.sql")
pats_linked_to_care_logger = logger.setup_logger(
    "pats_linked_to_care_logger",
    f"{directory}/logs/main.log"
)

config = vh_config.grab(pats_linked_to_care_logger)
project_id = vh_config.grab_tableau_id(
    project_name="Clinical Quality",
    logger=pats_linked_to_care_logger
)


def run(shared_drive):
    pats_linked_to_care_logger.info(
        "Clinical Quality - Medical - Patients Linked to Care."
    )
    hyper_file = f"{shared_drive}/Medical - Patients Linked to Care.hyper"
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
            pats_linked_to_care_df = connections.sql_to_df(
                file=sql_file,
                connection=clarity_connection
            )

        if len(pats_linked_to_care_df.index) == 0:
            pats_linked_to_care_logger.info("There are no data.")
            pats_linked_to_care_logger.info("Clinical Quality - Medical - "
                                            "Patients Linked to Care Daily "
                                            "ETL finished.")
        else:
            tableau_push(pats_linked_to_care_df, hyper_file)

    except ConnectionError as connection_error:
        pats_linked_to_care_logger.error("Unable to connect to OCHIN - "
                                         f"Vivent Health: {connection_error}")
    except KeyError as key_error:
        pats_linked_to_care_logger.error("Incorrect connection keys: "
                                         f"{key_error}")


def appt_status(first_complete, linkage):
    if first_complete is None and linkage == "UNMET":
        return "No Show"
    elif first_complete is not None and linkage == "UNMET":
        return "Attended After 30 days"
    else:
        return "Attended within 30 days"


def tableau_push(df, hyper_file):
    pats_linked_to_care_logger.info("Creating Hyper Table.")

    df["Appointment Status"] = df[["FIRST_COMPLETE", "Linked in 30 Days"]] \
        .apply(lambda x: appt_status(*x), axis=1)

    table_definition = TableDefinition(
        table_name=TableName("Medical - Patients Linked to Care"),
        columns=[
            TableDefinition.Column("MRN", SqlType.text()),
            TableDefinition.Column("PAT_ID", SqlType.text()),
            TableDefinition.Column("PATIENT", SqlType.text()),
            TableDefinition.Column("FIRST_ATTEMPTED_APPT", SqlType.date()),
            TableDefinition.Column("FIRST_APPT_STATUS", SqlType.text()),
            TableDefinition.Column("DEPARTMENT_NAME", SqlType.text()),
            TableDefinition.Column("VISIT_TYPE", SqlType.text()),
            TableDefinition.Column("VISIT_PROVIDER", SqlType.text()),
            TableDefinition.Column("CITY", SqlType.text()),
            TableDefinition.Column("STATE", SqlType.text()),
            TableDefinition.Column("FIRST_COMPLETE", SqlType.date()),
            TableDefinition.Column("DAYS_TO_COMPLETE", SqlType.int()),
            TableDefinition.Column("MET_YN", SqlType.text()),
            TableDefinition.Column("MET_NUM", SqlType.int()),
            TableDefinition.Column("Linked in 30 Days", SqlType.text()),
            TableDefinition.Column("Num Linked in 30 Days", SqlType.int()),
            TableDefinition.Column("Next Any Appt", SqlType.date()),
            TableDefinition.Column("Next Appt Prov", SqlType.text()),
            TableDefinition.Column("Next PCP Appt", SqlType.date()),
            TableDefinition.Column("Next PCP Appt Prov", SqlType.text()),
            TableDefinition.Column("Second Any Appt", SqlType.date()),
            TableDefinition.Column("Second Appt Prov", SqlType.text()),
            TableDefinition.Column("Third Any Appt", SqlType.date()),
            TableDefinition.Column("Third Appt Prov", SqlType.text()),
            TableDefinition.Column("Appointment Status", SqlType.text())
        ]
    )



    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=pats_linked_to_care_logger,
        project_id=project_id
    )

    pats_linked_to_care_logger.info(
        "Clinical Quality - Medical - Patients Linked to Care pushed to Tableau."
    )
