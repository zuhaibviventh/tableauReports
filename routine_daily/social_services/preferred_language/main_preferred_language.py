import os
import pandas as pd
import numpy as np

from utils import logger as logger_mod
from utils import connections, context, vh_config, vh_tableau
import pantab

# --------------------------------------------------------------------------------------------------
# Constants & setup
# --------------------------------------------------------------------------------------------------
directory = context.get_context(os.path.abspath(__file__))
pe_sql_file = f"{directory}/preferred_language/sql/pe_preferred_language.sql"
epic_sql_file = f"{directory}/preferred_language/sql/epic_preferred_language.sql"

logger = logger_mod.setup_logger("preferred_language_logger", f"{directory}/logs/main.log")
config = vh_config.grab(logger)
project_id = vh_config.grab_tableau_id(project_name="Clinical Operations", logger=logger)

OUTPUT_COLUMNS = [
    "MRN",
    "PATIENT_NAME",
    "DOB",
    "EPIC_PREFERRED_LANGUAGE",
    "PE_PREFERRED_LANGUAGE",
    "PREFERRED_LANGUAGE",
    "STATE",
    "Site",
    "PCP_ID",
    "PCP_NAME"
]

def run(shared_drive=r"C:\routine_daily\social_services\preferred_language"):
    logger.info("Clinical Operations - Preferred Language (EPIC base with PE fallback)")

    os.makedirs(shared_drive, exist_ok=True)
    hyper_file = os.path.join(shared_drive, "Preferred Language.hyper")
    debug_csv = os.path.join(shared_drive, "staging", "preferred_language_debug.csv")
    os.makedirs(os.path.dirname(debug_csv), exist_ok=True)

    # ---------------------------
    # Pull EPIC (base population)
    # ---------------------------
    try:
        epic_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )
        with epic_engine.connect() as cxn:
            logger.info("Pulling EPIC preferred language + attribution + demographics")
            epic_df = connections.sql_to_df(epic_sql_file, cxn)
    except ConnectionError as e:
        logger.error(f"Unable to connect to EPIC (Clarity - VH): {e}")
        return
    except KeyError as e:
        logger.error(f"Incorrect EPIC connection keys: {e}")
        return

    # Normalize MRN for safe joins
    epic_df["MRN"] = epic_df["MRN"].astype(str).str.strip()

    # ---------------------------------------
    # Pull PE preferred language (for EPIC MRNs only)
    # ---------------------------------------
    try:
        pe_engine = connections.engine_creation(
            server='pe.viventhealth.org',
            db=config['PEViventHealth']['database'],
            driver=config['PEViventHealth']['driver'],
            uid=config['PEViventHealth']['uid'],
            pwd=config['PEViventHealth']['pwd'],
            internal_use=False
        )
        with pe_engine.connect() as cxn:
            logger.info("Pulling PE preferred language")
            pe_df = connections.sql_to_df(pe_sql_file, cxn)
    except ConnectionError as e:
        logger.error(f"Unable to connect to PE (PEViventHealth): {e}")
        return
    except KeyError as e:
        logger.error(f"Incorrect PE connection keys: {e}")
        return

    pe_df["MRN"] = pe_df["MRN"].astype(str).str.strip()

    # # Restrict PE to only MRNs that exist in EPIC (requirement #1)
    # epic_mrns = set(epic_df["MRN"].unique())
    # pe_df = pe_df[pe_df["MRN"].isin(epic_mrns)].copy()

    # ---------------------------
    # Join: EPIC base LEFT JOIN PE
    # ---------------------------
    merged = pd.merge(
        epic_df,
        pe_df[["MRN", "PE_PREFERRED_LANGUAGE"]],
        how="left",
        on="MRN"
    )

    # Compute final Preferred Language (EPIC first, then PE)
    merged["PREFERRED_LANGUAGE"] = np.where(
        merged["EPIC_PREFERRED_LANGUAGE"].notna() & (merged["EPIC_PREFERRED_LANGUAGE"].astype(str).str.strip() != ""),
        merged["EPIC_PREFERRED_LANGUAGE"],
        merged["PE_PREFERRED_LANGUAGE"]
    )

    # Final column order
    final_df = merged[OUTPUT_COLUMNS].copy()
    print(final_df)
    logger.info(f"Final row count (EPIC patients only): {len(final_df):,}")

    # Debug dump
    final_df.to_csv(debug_csv, index=False, encoding="utf-8")
    logger.info(f"Wrote debug CSV: {debug_csv}")

    # Publish to Tableau
    process_data("Preferred Language", final_df, hyper_file)

def process_data(process_name, data_df, hyper_file):
    if data_df.empty:
        logger.info(f"There are no data for {process_name}.")
        return

    logger.info(f"Pushing to Tableau: {process_name}")
    pantab.frame_to_hyper(data_df, hyper_file, table=process_name)
    vh_tableau.publish_data_source(project_id=project_id, logger=logger, hyper_file=hyper_file)
    logger.info(f"{process_name} Daily ETL finished.")

if __name__ == "__main__":
    run()
