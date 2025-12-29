import os
from tableauhyperapi import SqlType, TableName, TableDefinition
from utils import logger, connections, context, vh_config, vh_tableau

# Constants
directory = context.get_context(os.path.abspath(__file__))
sql_file = f"{directory}/prevention_navigation/sql/prevention_navigation.sql"
pna_sql_file = f"{directory}/prevention_navigation/sql/primary_needs_assessment.sql"
mla_sql_file = f"{directory}/prevention_navigation/sql/motivational_leader_assessment.sql"
ira_sql_file = f"{directory}/prevention_navigation/sql/individual_risk_assessment.sql"

logger = logger.setup_logger("prevention_navigation_logger", f"{directory}/logs/main.log")
config = vh_config.grab(logger)
project_id = vh_config.grab_tableau_id(project_name="Prevention", logger=logger)


def run(shared_drive):
    """
    Execute the Prevention Navigation process.

    This function connects to the database, retrieves data, and pushes it to Tableau.

    Raises:
        ConnectionError: If unable to connect to OCHIN - Vivent Health.
        KeyError: If incorrect connection keys are provided.
    """
    logger.info("Prevention - PE - Prevention Navigation.")
    
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    hyper_file = f"{shared_drive}/PE - Prevention Navigation Extract.hyper"
    pna_hyper_file = f"{shared_drive}/PE - Prevention Navigation - Primary Needs Assessment Extract.hyper"
    mla_hyper_file = f"{shared_drive}/PE - Prevention Navigation - Motivational Leader Assessment Extract.hyper"
    ira_hyper_file = f"{shared_drive}/PE - Prevention Navigation - Individual Risk Assessment Extract.hyper"

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
            prevention_navigation_df = connections.sql_to_df(sql_file, clarity_connection)
            pna_df = connections.sql_to_df(pna_sql_file, clarity_connection)
            mla_df = connections.sql_to_df(mla_sql_file, clarity_connection)
            ira_df = connections.sql_to_df(ira_sql_file, clarity_connection)

        process_data("Prevention Navigation", prevention_navigation_df, hyper_file)
        process_data("Primary Needs Assessment", pna_df, pna_hyper_file)
        process_data("Motivational Ladder Assessment", mla_df, mla_hyper_file)
        process_data("Individual Risk Assessment", ira_df, ira_hyper_file)

    except ConnectionError as connection_error:
        logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        logger.error(f"Incorrect connection keys: {key_error}")


def process_data(process_name, data_df, hyper_file):
    """
    Process the data based on the given process name.

    Args:
        process_name (str): Name of the data processing.
        data_df (pd.DataFrame): DataFrame containing the data.

    Returns:
        None
    """
    if len(data_df.index) == 0:
        logger.info(f"There are no data for {process_name}.")
    else:
        tableau_push(data_df, process_name, hyper_file)
        logger.info(f"{process_name} Daily ETL finished.")


def tableau_push(df, process_name, hyper_file):
    """
    Push the DataFrame to Tableau.

    Args:
        df (pd.DataFrame): DataFrame to be pushed.
        process_name (str): Name of the process.

    Returns:
        None
    """
    logger.info("Creating Hyper Table.")

    table_definition = get_table_definition(process_name)

    vh_tableau.push_to_tableau(
        df=df,
        hyper_file=hyper_file,
        table_definition=table_definition,
        logger=logger,
        project_id=project_id
    )

    logger.info(f"Prevention - PE - {process_name} pushed to Tableau.")


def get_table_definition(process_name):
    """
    Get the Table Definition based on the process name.

    Args:
        process_name (str): Name of the process.

    Returns:
        TableDefinition: Table definition for Hyper table.
    """
    if process_name == "Prevention Navigation":
        columns = [
            TableDefinition.Column("SCPClientID", SqlType.text()),
            TableDefinition.Column("SCPClientFirst", SqlType.text()),
            TableDefinition.Column("SCPClientLast", SqlType.text()),
            TableDefinition.Column("SCPRaceCat", SqlType.text()),
            TableDefinition.Column("SCPEthnicity", SqlType.text()),
            TableDefinition.Column("GenderIdentity", SqlType.text()),
            TableDefinition.Column("ZipCode", SqlType.text()),
            TableDefinition.Column("CLAProgramStatus", SqlType.text()),
            TableDefinition.Column("Service_State", SqlType.text()),
            TableDefinition.Column("CLAProgramOfficeLocation", SqlType.text()),
            TableDefinition.Column("PreventionNavigator", SqlType.text()),
            TableDefinition.Column("ProviderName", SqlType.text()),
            TableDefinition.Column("CLAProgramDateStart", SqlType.date()),
            TableDefinition.Column("CLAProgramDateEnd", SqlType.date()),
            TableDefinition.Column("SessionCreateDate", SqlType.date()),
            TableDefinition.Column("DateCompleted", SqlType.date()),
            TableDefinition.Column("DayToDataEntry", SqlType.int()),
            TableDefinition.Column("DataEntryLag", SqlType.int()),
            TableDefinition.Column("SessionNumber", SqlType.text()),
            TableDefinition.Column("Status", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
        table_name = TableName("PE - Prevention Navigation")
    elif process_name == "Primary Needs Assessment":
        columns = [
            TableDefinition.Column("SCPClientID", SqlType.text()),
            TableDefinition.Column("SCPClientFirst", SqlType.text()),
            TableDefinition.Column("SCPClientLast", SqlType.text()),
            TableDefinition.Column("Service_State", SqlType.text()),
            TableDefinition.Column("CLAProgramOfficeLocation", SqlType.text()),
            TableDefinition.Column("PreventionNavigator", SqlType.text()),
            TableDefinition.Column("SessionNumber", SqlType.text()),
            TableDefinition.Column("Food Security", SqlType.text()),
            TableDefinition.Column("Food Security Rank", SqlType.text()),
            TableDefinition.Column("Transportation", SqlType.text()),
            TableDefinition.Column("Transportation Rank", SqlType.text()),
            TableDefinition.Column("Sleep/Housing", SqlType.text()),
            TableDefinition.Column("Sleep/Housing Rank", SqlType.text()),
            TableDefinition.Column("Insurance", SqlType.text()),
            TableDefinition.Column("Insurance Rank", SqlType.text()),
            TableDefinition.Column("Healthcare", SqlType.text()),
            TableDefinition.Column("Healthcare Rank", SqlType.text()),
            TableDefinition.Column("Mental Health", SqlType.text()),
            TableDefinition.Column("Mental Health Rank", SqlType.text()),
            TableDefinition.Column("Income", SqlType.text()),
            TableDefinition.Column("Income Rank", SqlType.text()),
            TableDefinition.Column("Relationships", SqlType.text()),
            TableDefinition.Column("Relationships Rank", SqlType.text()),
            TableDefinition.Column("Legal", SqlType.text()),
            TableDefinition.Column("Legal Rank", SqlType.text()),
            TableDefinition.Column("PNA Comments", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
        table_name = TableName("PE - Prevention Navigation - Primary Needs Assessment")
    elif process_name == "Motivational Ladder Assessment":
        columns = [
            TableDefinition.Column("SCPClientID", SqlType.text()),
            TableDefinition.Column("SCPClientFirst", SqlType.text()),
            TableDefinition.Column("SCPClientLast", SqlType.text()),
            TableDefinition.Column("Service_State", SqlType.text()),
            TableDefinition.Column("CLAProgramOfficeLocation", SqlType.text()),
            TableDefinition.Column("PreventionNavigator", SqlType.text()),
            TableDefinition.Column("SessionNumber", SqlType.text()),
            TableDefinition.Column("(HIV/HCV) At this time: do you have a goal of lowering your risk of getting HIV or HCV?", SqlType.text()),
            TableDefinition.Column("(HIV/HCV) What number represents where you are at on the motivational ladder?", SqlType.text()),
            TableDefinition.Column("(Overdose) At this time: do you have a goal of lowering your risk of overdosing?", SqlType.text()),
            TableDefinition.Column("(Overdose) What number represents where you are at on the motivational ladder?", SqlType.text()),
            TableDefinition.Column("(HepC) At this time: do you have a goal of accessing treatment for Hepatitis C?", SqlType.text()),
            TableDefinition.Column("(HepC) What number represents where you are at on the motivational ladder?", SqlType.text()),
            TableDefinition.Column("(SubstanceUseTreatment) At this time: do you have a goal of accessing substance use treatment?", SqlType.text()),
            TableDefinition.Column("(SubstanceUseTreatment) What number represents where you are at on the motivational ladder?", SqlType.text()),
            TableDefinition.Column("(SubstanceUseReduction) At this time: do you have a goal of reducing the amount of substances you use?", SqlType.text()),
            TableDefinition.Column("(SubstanceUseReduction) What number represents where you are at on the motivational ladder?", SqlType.text()),
            TableDefinition.Column("(SubstanceUseCessation) At this time: do you have a goal of stopping using substances completely?", SqlType.text()),
            TableDefinition.Column("(SubstanceUseCessation) What number represents where you are at on the motivational ladder?", SqlType.text()),
            TableDefinition.Column("(SubstanceUseAcceptance) At this time: do you have a goal of being more accepting of yourself regarding your substance use?", SqlType.text()),
            TableDefinition.Column("(SubstanceUseAcceptance) What number represents where you are at on the motivational ladder?", SqlType.text()),
            TableDefinition.Column("(StableHousing) At this time: do you have a goal of accessing stable housing?", SqlType.text()),
            TableDefinition.Column("(StableHousing) What number represents where you are at on the motivational ladder?", SqlType.text()),
            TableDefinition.Column("MLA Comments", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
        table_name = TableName("PE - Prevention Navigation - Motivational Ladder Assessment")
    elif process_name == "Individual Risk Assessment":
        columns = [
            TableDefinition.Column("SCPClientID", SqlType.text()),
            TableDefinition.Column("SCPClientFirst", SqlType.text()),
            TableDefinition.Column("SCPClientLast", SqlType.text()),
            TableDefinition.Column("Service_State", SqlType.text()),
            TableDefinition.Column("CLAProgramOfficeLocation", SqlType.text()),
            TableDefinition.Column("PreventionNavigator", SqlType.text()),
            TableDefinition.Column("SessionNumber", SqlType.text()),
            TableDefinition.Column("Did you discuss the HIV assessment with the client?", SqlType.text()),
            TableDefinition.Column("How confident is the client in their knowledge of HIV is transmitted?", SqlType.text()),
            TableDefinition.Column("How often does the client get tested for HIV?", SqlType.text()),
            TableDefinition.Column("How often does the client use condoms when sexually active?", SqlType.text()),
            TableDefinition.Column("How often is the client in their knowledge of PrEP?", SqlType.text()),
            TableDefinition.Column("How confident is the client in their ability to obtain PrEP?", SqlType.text()),
            TableDefinition.Column("Did you discuss the HCV assessment with the client?", SqlType.text()),
            TableDefinition.Column("Does the client know their HCV status?", SqlType.text()),
            TableDefinition.Column("How confident is the client in their knowledge of how HCV is transmitted?", SqlType.text()),
            TableDefinition.Column("How often does the client share razors, toothbrushes, and/or nail clippers?", SqlType.text()),
            TableDefinition.Column("How often does the client get tested for HCV?", SqlType.text()),
            TableDefinition.Column("How confident is the client in accessing HCV treatment, if needed?", SqlType.text()),
            TableDefinition.Column("How often does the client share straws or pipes for snorting and/or smoking?", SqlType.text()),
            TableDefinition.Column("Did you discuss the Safer Injection assessment with the client?", SqlType.text()),
            TableDefinition.Column("How often does the client use an un-used syringe and works when injecting?", SqlType.text()),
            TableDefinition.Column("How often does the client rotate injection sites?", SqlType.text()),
            TableDefinition.Column("How often does the client use a sharps container or return syringes to a syringe service program?", SqlType.text()),
            TableDefinition.Column("How confident is the client in accessing un-used supplies from a syringe service program or other source?", SqlType.text()),
            TableDefinition.Column("How comfortable is the client in accessing services via a syringe service program?", SqlType.text()),
            TableDefinition.Column("How confident is the client in preparing and injecting without the help of others?", SqlType.text()),
            TableDefinition.Column("How often in the past 6 months does the client report having a skin or soft tissue infection when related to injecting?", SqlType.text()),
            TableDefinition.Column("What treatment methods were used with the client's most recent skin or soft tissue infection?", SqlType.text()),
            TableDefinition.Column("Did you discuss the Opioid Overdose assessment with the client?", SqlType.text()),
            TableDefinition.Column("How often does the client report using drugs in the past week?", SqlType.text()),
            TableDefinition.Column("How many times has the client overdosed in the past six months?", SqlType.text()),
            TableDefinition.Column("How confident is the client of their ability to recognize signs of an overdose?", SqlType.text()),
            TableDefinition.Column("How confident is the client in how to use naloxone?", SqlType.text()),
            TableDefinition.Column("How often does the client report carrying naloxone or having it nearby during use?", SqlType.text()),
            TableDefinition.Column("How confident is the client in their ability to perform rescue breathing on someone else?", SqlType.text()),
            TableDefinition.Column("How often does the client report calling 911 when witnessing or responding to an overdose?", SqlType.text()),
            TableDefinition.Column("How confident is the client in the protections afforded to them under the Good Samaritan Law?", SqlType.text()),
            TableDefinition.Column("How often does the client use drug checking methods like test strips?", SqlType.text()),
            TableDefinition.Column("How often does the client taste their shot before injecting?", SqlType.text()),
            TableDefinition.Column("How often does the client use alone?", SqlType.text()),
            TableDefinition.Column("Did you discuss the Substance Use Treatment assessment with the client?", SqlType.text()),
            TableDefinition.Column("How confident is the client in their knowledge of what substance use treatment options are available in their area?", SqlType.text()),
            TableDefinition.Column("How confident is the client in accessing their preferred method of substance use treatment if they wanted to?", SqlType.text()),
            TableDefinition.Column("How likely is the client to seek out their preferred substance use treatment method if they wanted to?", SqlType.text()),
            TableDefinition.Column("In the past year has the client been unable to access their preferred substance use treatment method when they tried to?", SqlType.text()),
            TableDefinition.Column("Did you discuss the Stigma assessment with the client?", SqlType.text()),
            TableDefinition.Column("How likely is the client to avoid healthcare due to their substance use?", SqlType.text()),
            TableDefinition.Column("How likely is the client to tell their primary care provider about their substance use?", SqlType.text()),
            TableDefinition.Column("How likely is the client to avoid social situations due to their substance use?", SqlType.text()),
            TableDefinition.Column("How likely is the client to avoid family and friends due to their substance use?", SqlType.text()),
            TableDefinition.Column("How isolated does the client's substance use make them feel?", SqlType.text()),
            TableDefinition.Column("IRAComments", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp())
        ]
        table_name = TableName("PE - Prevention Navigation - Individual Risk Assessment")

    return TableDefinition(table_name, columns)


if __name__ == "__main__":
    run()
