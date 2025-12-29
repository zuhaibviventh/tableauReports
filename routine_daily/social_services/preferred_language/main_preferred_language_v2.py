import os
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
pe_sql_file = f"{directory}/preferred_language/sql/pe_preferred_language_v2.sql"
epic_sql_file = f"{directory}/preferred_language/sql/epic_preferred_language_v2.sql"
#dim_sql_file = f"{directory}/preferred_language/sql/dim_patient_client.sql"
logger = logger.setup_logger("preferred_language_logger", f"{directory}/logs/main.log")
config = vh_config.grab(logger)
project_id = vh_config.grab_tableau_id(project_name="Clinical Operations", logger=logger)


def run(shared_drive):
    logger.info("Clinical Operations - PE and Epic - Preferred Language.")
    if not os.path.exists(shared_drive):
        os.makedirs(shared_drive)

    hyper_file = f"{shared_drive}/Preferred Language.hyper"

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
            pe_preferred_language_df = connections.sql_to_df(pe_sql_file, pe_connection)
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
            epic_preferred_language_df = connections.sql_to_df(epic_sql_file, clarity_connection)
           # logger.info("Pulling data from Epic - DIM Paitient Client")
           # dim_patient_client_df = connections.sql_to_df(dim_sql_file, clarity_connection)
    except ConnectionError as connection_error:
        logger.error(f"Unable to connect to OCHIN - Vivent Health: {connection_error}")
    except KeyError as key_error:
        logger.error(f"Incorrect connection keys: {key_error}")


    # Full outer join
    df_left = pd.merge(pe_preferred_language_df, epic_preferred_language_df, how="left", on=["MRN"])
    df_inner = pd.merge(pe_preferred_language_df, epic_preferred_language_df, how="inner", on=["MRN"])
    df_right = pd.merge(pe_preferred_language_df, epic_preferred_language_df, how="right", on=["MRN"])
    df_merged = pd.concat([df_left, df_inner, df_right])

    #df_merged = pd.merge(df_merged, dim_patient_client_df, how="left", on="MRN")

    df_merged["CLIENT_ID"] = df_merged.apply(lambda row: row["CLIENT_ID_y"] if pd.isnull(row["CLIENT_ID_x"]) else row["CLIENT_ID_x"], axis=1)
    df_merged["PATIENT_ID"] = df_merged.apply(lambda row: row["CLIENT_ID"] if pd.isnull(row["MRN"]) else row["MRN"], axis=1)
    df_merged["PREFERRED LANGUAGE"] = df_merged.apply(lambda row: row["PREFERRED LANGUAGE_y"] if pd.isnull(row["PREFERRED LANGUAGE_x"]) else row["PREFERRED LANGUAGE_x"], axis=1)
    df_merged["STATE"] = df_merged.apply(lambda row: row["STATE_y"] if pd.isnull(row["STATE_x"]) else row["STATE_x"], axis=1)
    df_merged["Site"] = df_merged.apply(lambda row: row["Site_y"] if pd.isnull(row["Site_x"]) else row["Site_x"], axis=1)

    # 1. Pull in DIM_PATIENT_CLIENT
        
    

# 2. Merge it with your merged DF
    

    lst = ["PATIENT_ID", "CLIENT_ID", "MRN", "PAT_NAME", "BIRTH_DATE", "SCPClientFirst", "SCPClientLast", "SCPClientMI", "SCPDateOfBirth", "PREFERRED LANGUAGE", "STATE", "Site"]
    df_merged = df_merged[lst]

    df_merged["PE_FULL_NAME"] = df_merged.apply(
    lambda row: (
        f"{row['SCPClientLast']},{row['SCPClientFirst']} {row['SCPClientMI'] if pd.notnull(row['SCPClientMI']) else ''}"
    ).strip().upper()
    if pd.notnull(row['SCPClientFirst']) and pd.notnull(row['SCPClientLast'])
    else None,
    axis=1
)




    print(df_merged.head(10))
    df_merged.to_csv(
    r"C:\routine_daily\social_services\preferred_language\staging\preferred_language_debug.csv",
    index=False)
    
    
    
    # ---- CHECKS ----
    #pc_name_full_null = df_merged[df_merged["PC_NAME_FULL"].isnull()]

#     df_merged["missing_both_names"] = (
#         df_merged["PAT_NAME"].isnull() & df_merged["SCPClientFirst"].isnull()
#     )

#     df_merged["missing_both_dobs"] = (
#         df_merged["BIRTH_DATE"].isnull() & df_merged["SCPDateOfBirth"].isnull()
#     )

#     df_merged["name_mismatch"] = df_merged.apply(
#     lambda row: (
#         pd.notnull(row["PAT_NAME"])
#         and pd.notnull(row["PE_FULL_NAME"])
#         and row["PAT_NAME"].strip().upper() != row["PE_FULL_NAME"]
#     ),
#     axis=1
# )
#     def is_middle_only_mismatch(row):
#         try:
#             if pd.isnull(row["PAT_NAME"]) or pd.isnull(row["SCPClientFirst"]) or pd.isnull(row["SCPClientLast"]):
#                 return False

#             epic_parts = row["PAT_NAME"].split(",")
#             if len(epic_parts) < 2:
#                 return False

#             epic_last = epic_parts[0].strip().upper()
#             epic_first_full = epic_parts[1].strip().upper().split(" ")
#             epic_first = epic_first_full[0]
#             epic_middle = " ".join(epic_first_full[1:]) if len(epic_first_full) > 1 else ""

#             pe_first = row["SCPClientFirst"].strip().upper()
#             pe_last = row["SCPClientLast"].strip().upper()
#             pe_middle = row["SCPClientMI"].strip().upper() if pd.notnull(row["SCPClientMI"]) else ""

#             return (
#                 epic_first == pe_first and
#                 epic_last == pe_last and
#                 epic_middle != pe_middle
#             )
#         except:
#             return False

#     df_merged["middle_only_mismatch"] = df_merged.apply(is_middle_only_mismatch, axis=1)


#     df_merged["dob_mismatch"] = df_merged.apply(
#         lambda row: (
#             pd.notnull(row["BIRTH_DATE"])
#             and pd.notnull(row["SCPDateOfBirth"])
#             and row["BIRTH_DATE"].date() != row["SCPDateOfBirth"]
#         ),
#         axis=1
#     )

#     df_merged["language_missing"] = df_merged["PREFERRED LANGUAGE"].isnull()

#     df_merged["exists_only_in_epic"] = (
#         df_merged["PAT_NAME"].notnull() & df_merged["SCPClientFirst"].isnull()
#     )

#     df_merged["exists_only_in_pe"] = (
#         df_merged["PAT_NAME"].isnull() & df_merged["SCPClientFirst"].notnull()
#     )
#     df_merged["true_name_mismatch"] = df_merged["name_mismatch"] & (~df_merged["middle_only_mismatch"])
#     df_merged.to_csv(
#     r"C:\routine_daily\social_services\preferred_language\staging\preferred_language_debug.csv",
#     index=False, encoding = "utf-8")

#     # ---- DATA QUALITY METRICS ----
#     total_rows = len(df_merged)
#     print(f"\n🔍 Total Rows: {total_rows:,}\n")

#     checks = {
#         "Missing both names": "missing_both_names",
#         "Missing both DOBs": "missing_both_dobs",
#         "Name mismatch": "name_mismatch",
#         "DOB mismatch": "dob_mismatch",
#         "Missing preferred language": "language_missing",
#         "Only in EPIC": "exists_only_in_epic",
#         "Only in PE": "exists_only_in_pe",
#         "Middle name only mismatch": "middle_only_mismatch",
#         "True name mismatch": "true_name_mismatch"
#     }

#     for label, col in checks.items():
#         count = df_merged[col].sum()
#         percent = (count / total_rows) * 100
#         print(f"{label:<30}: {count:>5} rows ({percent:.2f}%)")

#     mismatched_rows = df_merged[df_merged["name_mismatch"] | df_merged["dob_mismatch"]][
#     ["MRN", "PAT_NAME", "PE_FULL_NAME", "BIRTH_DATE", "SCPDateOfBirth"]]

#     mismatched_rows.to_csv(
#     r"C:\routine_daily\social_services\preferred_language\staging\name_dob_mismatches.csv",
#     index=False,
#     encoding="utf-8")

#     df_merged[df_merged["middle_only_mismatch"]][
#     ["MRN", "PAT_NAME", "PE_FULL_NAME", "SCPClientMI"]].to_csv(r"C:\routine_daily\social_services\preferred_language\staging\middle_name_mismatches.csv",
#     index=False,
#     encoding="utf-8")

    

#     # Export just the true mismatches
#     df_merged[df_merged["true_name_mismatch"]][
#         ["MRN", "PAT_NAME", "PE_FULL_NAME", "BIRTH_DATE", "SCPDateOfBirth"]
#     ].to_csv(
#         r"C:\routine_daily\social_services\preferred_language\staging\true_name_mismatches.csv",
#         index=False,
#         encoding="utf-8"
#     )

#    pc_name_full_null.to_csv(
#   r"C:\routine_daily\social_services\preferred_language\staging\missing_pc_name_full.csv",
#    index=False,
#    encoding="utf-8"
#)
#    count_null_pc_name = len(pc_name_full_null)
#    total_rows = len(df_merged)
#    percent_null_pc_name = (count_null_pc_name / total_rows) * 100

#    print(f"\n🧾 Missing PC_NAME_FULL: {count_null_pc_name:,} rows ({percent_null_pc_name:.2f}%) out of {total_rows:,} total\n")

    # file_path = r"C:\routine_daily\social_services\preferred_language\staging\preferred_language_debug.csv"
    # df = pd.read_csv(file_path)

    # Filter rows that only exist in PE
    #only_in_pe_df = df[df["exists_only_in_pe"] == True]

    # Save to new CSV
    #output_path = r"C:\routine_daily\social_services\preferred_language\staging\only_in_pe.csv"
    #only_in_pe_df.to_csv(output_path, index=False)

    # Show some of the data for confirmation
    #only_in_pe_df.head(10)
    #process_data("Preferred Language", df_merged, hyper_file)

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
