import os, pgeocode
from tableauhyperapi import SqlType, TableName, TableDefinition
from utils import logger, connections, context, vh_config, vh_tableau

# Constants
directory = context.get_context(os.path.abspath(__file__))

sql_file = f"{directory}/prevention_supply_distribution/sql/prevention_supply_distribution.sql"
summaries_sql_file = f"{directory}/prevention_supply_distribution/sql/prevention_supply_distribution_summaries.sql"
referrals_sql_file = f"{directory}/prevention_supply_distribution/sql/prevention_supply_distribution_referrals.sql"

logger = logger.setup_logger("prevention_supply_distribution_logger", f"{directory}/logs/main.log")
config = vh_config.grab(logger)
project_id = vh_config.grab_tableau_id(project_name="Prevention", logger=logger)


def run(shared_drive):
    """
    Execute the Prevention Supply Distribution process.

    This function connects to the database, retrieves data, and pushes it to Tableau.

    Raises:
        ConnectionError: If unable to connect to OCHIN - Vivent Health.
        KeyError: If incorrect connection keys are provided.
    """
    logger.info("Prevention - PE - Prevention Supply Distribution.")
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    hyper_file = f"{shared_drive}/PE - Prevention Supply Distribution Extract.hyper"
    summaries_hyper_file = f"{shared_drive}/PE - Prevention Supply Distribution Summaries Extract.hyper"
    referrals_hyper_file = f"{shared_drive}/PE - Prevention Supply Distribution Referrals Extract.hyper"

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            #uid=config['Clarity - VH']['uid'],
            #pwd=config['Clarity - VH']['pwd'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            prevention_supply_distribution_df = grab_county(connections.sql_to_df(sql_file, clarity_connection))
            prevention_supply_distribution_summaries_df = grab_county(connections.sql_to_df(summaries_sql_file, clarity_connection))
            prevention_supply_distribution_referrals_df = grab_county(connections.sql_to_df(referrals_sql_file, clarity_connection))

        process_data("Prevention Supply Distribution", prevention_supply_distribution_df, hyper_file)
        process_data("Prevention Supply Distribution Summaries", prevention_supply_distribution_summaries_df, summaries_hyper_file)
        process_data("Prevention Supply Distribution Referrals", prevention_supply_distribution_referrals_df, referrals_hyper_file)

    except ConnectionError as connection_error:
        logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        logger.error(f"Incorrect connection keys: {key_error}")


def grab_county(df):
    """
    Process that gets the county from the zip

    Args:
        df (pd.DataFrame): DataFrame containing the data.

    Returns:
        df: The original DataFrame with a new, County, column.
    """
    connections.pd.set_option('display.max_rows', 10)
    connections.pd.set_option('display.max_columns', None)
    connections.pd.set_option('display.width', 200) 
    print(df) 
    nomi = pgeocode.Nominatim('us')
    df["County"] = df["ZipCode"].apply(lambda x: nomi.query_postal_code(x).county_name)

    df["County"] = df["County"].fillna("UNKNOWN")    
    return df


def process_data(process_name, data_df, hyper_file):
    """
    Process the data based on the given process name.

    Args:
        process_name (str): Name of the data processing.
        data_df (pd.DataFrame): DataFrame containing the data.

    Returns:
        None
    """
    logger.info(f"processing data...")
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
    logger.info(f"getting table definition...")
    if process_name == "Prevention Supply Distribution":
        columns = [
            TableDefinition.Column("SCPClientID", SqlType.text()),
            TableDefinition.Column("Event_ID", SqlType.text()),
            TableDefinition.Column("SCPClientAge", SqlType.int()),
            TableDefinition.Column("AgeGroup", SqlType.text()),
            TableDefinition.Column("Race", SqlType.text()),
            TableDefinition.Column("Ethnicity", SqlType.text()),
            TableDefinition.Column("SCPGender", SqlType.text()),
            TableDefinition.Column("PSEncounterElection", SqlType.text()),
            TableDefinition.Column("PSPrevDistType", SqlType.text()),
            TableDefinition.Column("PSServiceDate", SqlType.date()),
            TableDefinition.Column("Program_Office_Location", SqlType.text()),
            TableDefinition.Column("ZipCode", SqlType.text()),
            TableDefinition.Column("Service_State", SqlType.text()),
            TableDefinition.Column("Provider_Name", SqlType.text()),
            TableDefinition.Column("CondomDist_Funding_Source", SqlType.text()),
            TableDefinition.Column("Condom_Distribution_Count", SqlType.int()),
            TableDefinition.Column("HygieneKitDist_Funding_Source", SqlType.text()),
            TableDefinition.Column("HygieneKit_Distribution_Count", SqlType.int()),
            TableDefinition.Column("InjectNaloxoneDist_Funding_Source", SqlType.text()),
            TableDefinition.Column("InjectNaloxone_Distribution_Count", SqlType.int()),
            TableDefinition.Column("LubeDist_Funding_Source", SqlType.text()),
            TableDefinition.Column("Lube_Distribution_Count", SqlType.int()),
            TableDefinition.Column("NasalNaloxoneDist_Funding_Source", SqlType.text()),
            TableDefinition.Column("NasalNaloxone_Distribution_Count", SqlType.int()),
            TableDefinition.Column("SaferSexKitDist_Funding_Source", SqlType.text()),
            TableDefinition.Column("SaferSexKit_Distribution_Count", SqlType.int()),
            TableDefinition.Column("Smoke_Funding_Source", SqlType.text()),
            TableDefinition.Column("Smoke_Distribution_Count", SqlType.int()),
            TableDefinition.Column("SyringeDist_Funding_Source", SqlType.text()),
            TableDefinition.Column("Syringe_Distribution_Count", SqlType.int()),
            TableDefinition.Column("WoundCare_Funding_Source", SqlType.text()),
            TableDefinition.Column("WoundCare_Distribution_Count", SqlType.int()),
            TableDefinition.Column("XylazineDist_Funding_Source", SqlType.text()),
            TableDefinition.Column("Xylazine_Distribution_Count", SqlType.int()),
            TableDefinition.Column("PSFentanylTraining", SqlType.int()),
            TableDefinition.Column("PSFentanylTrainingAttendees", SqlType.int()),
            TableDefinition.Column("PSFentanylTrainingsHeld", SqlType.int()),
            TableDefinition.Column("PSInjectNaloxone911", SqlType.int()),
            TableDefinition.Column("PSInjectNaloxoneDosesUsed", SqlType.int()),
            TableDefinition.Column("PSInjectNaloxoneHospital", SqlType.int()),
            TableDefinition.Column("PSInjectNaloxoneTraining", SqlType.int()),
            TableDefinition.Column("PSInjectNaloxoneTrainingAttendees", SqlType.int()),
            TableDefinition.Column("PSInjectNaloxoneTrainingsHeld", SqlType.int()),
            TableDefinition.Column("PSInjectNaloxoneUsed", SqlType.int()),
            TableDefinition.Column("PSNasalNaloxone911", SqlType.int()),
            TableDefinition.Column("PSFentanylNegative", SqlType.int()),
            TableDefinition.Column("PSFentanylPositive", SqlType.int()),
            TableDefinition.Column("PSFentanylTestStripsDistributed", SqlType.int()),
            TableDefinition.Column("PSNasalNaloxoneDistribution", SqlType.int()),
            TableDefinition.Column("PSNasalNaloxoneDosesUsed", SqlType.int()),
            TableDefinition.Column("PSNasalNaloxoneUsed", SqlType.int()),
            TableDefinition.Column("PSNasalNaloxoneHospital", SqlType.int()),
            TableDefinition.Column("PSNasalNaloxoneTraining", SqlType.int()),
            TableDefinition.Column("PSNasalNaloxoneTrainingAttendees", SqlType.int()),
            TableDefinition.Column("PSNasalNaloxoneTrainingsHeld", SqlType.int()),
            TableDefinition.Column("PSQtyPickupFor", SqlType.int()),
            TableDefinition.Column("PSReferralsGiven", SqlType.int()),
            TableDefinition.Column("PSSmokeDistroCount", SqlType.int()),
            TableDefinition.Column("PSSmokePeopleCount", SqlType.int()),
            TableDefinition.Column("PSSmokeTrainingAttendees", SqlType.int()),
            TableDefinition.Column("PSSmokeTrainingsHeld", SqlType.int()),
            TableDefinition.Column("PSSyringesCollected", SqlType.int()),
            TableDefinition.Column("PSXylazineNegative", SqlType.int()),
            TableDefinition.Column("PSXylazinePositive", SqlType.int()),
            TableDefinition.Column("PSXylazineTraining", SqlType.int()),
            TableDefinition.Column("PSXylazineTrainingAttendees", SqlType.int()),
            TableDefinition.Column("PSXylazineTrainingsHeld", SqlType.int()),
            TableDefinition.Column("PSSharpsContainers", SqlType.int()),
            TableDefinition.Column("PSSmokeCrackKitCount", SqlType.int()),
            TableDefinition.Column("PSSmokeMethKitCount", SqlType.int()),
            TableDefinition.Column("PSSmokeSafeCount", SqlType.int()),
            TableDefinition.Column("PSSmokeSafeSnortCount", SqlType.int()),
            TableDefinition.Column("PSSterileWater", SqlType.int()),
            TableDefinition.Column("PSSyringe29Gauge", SqlType.int()),
            TableDefinition.Column("PSSyringe31Gauge", SqlType.int()),
            TableDefinition.Column("PSSyringeExchange", SqlType.text()),
            TableDefinition.Column("PSNasalNaloxoneDistTo", SqlType.text()),
            TableDefinition.Column("PSInjectNaloxoneDistTo", SqlType.text()),
            TableDefinition.Column("Site", SqlType.text()),
            TableDefinition.Column("Race (raw)", SqlType.text()),
            TableDefinition.Column("NumPeopleHelped", SqlType.int()),
            TableDefinition.Column("Income", SqlType.text()),
            TableDefinition.Column("UPDATE_DTTM", SqlType.timestamp()),
            TableDefinition.Column("First Visit YN", SqlType.text()),
            TableDefinition.Column("Supplies County", SqlType.text()),
            TableDefinition.Column("County", SqlType.text()) #County has to be last and is not pulled from SQL
            
        ]
        table_name = TableName("PE - Prevention Supply Distribution")
        logger.info(f"table definition set.")
    elif process_name == "Prevention Supply Distribution Summaries":
        columns = [
            TableDefinition.Column("Event_ID", SqlType.text()),
            TableDefinition.Column("ZipCode", SqlType.text()),
            TableDefinition.Column("Item_Type", SqlType.text()),
            TableDefinition.Column("Dist_Funding_Source", SqlType.text()),
            TableDefinition.Column("Distribution_Count", SqlType.int()),
            TableDefinition.Column("PSEncounterElection", SqlType.text()),
            TableDefinition.Column("PSPrevDistType", SqlType.text()),
            TableDefinition.Column("PSServiceDate", SqlType.date()),
            TableDefinition.Column("Program_Office_Location", SqlType.text()),
            TableDefinition.Column("Service_State", SqlType.text()),
            TableDefinition.Column("Provider_Name", SqlType.text()),
            TableDefinition.Column("PSSiteName", SqlType.text()),
            TableDefinition.Column("First Visit YN", SqlType.text()),
            TableDefinition.Column("County", SqlType.text()) #County has to be last and is not pulled from SQL
            
        ]
        table_name = TableName("PE - Prevention Supply Distribution Summaries")
    elif process_name == "Prevention Supply Distribution Referrals":
        columns = [
            TableDefinition.Column("Event_ID", SqlType.text()),
            TableDefinition.Column("REFERRAL_TYPE", SqlType.text()),
            TableDefinition.Column("PSEncounterElection", SqlType.text()),
            TableDefinition.Column("PSPrevDistType", SqlType.text()),
            TableDefinition.Column("PSServiceDate", SqlType.date()),
            TableDefinition.Column("Program_Office_Location", SqlType.text()),
            TableDefinition.Column("ZipCode", SqlType.text()),
            TableDefinition.Column("Service_State", SqlType.text()),
            TableDefinition.Column("Provider_Name", SqlType.text()),
            TableDefinition.Column("Site", SqlType.text()),
            TableDefinition.Column("County", SqlType.text()) #County has to be last and is not pulled from SQL
        ]
        table_name = TableName("PE - Prevention Supply Distribution Referrals")
    else:
        raise ValueError(f"Invalid process_name: {process_name}")

    return TableDefinition(table_name, columns)


if __name__ == "__main__":
    run()
