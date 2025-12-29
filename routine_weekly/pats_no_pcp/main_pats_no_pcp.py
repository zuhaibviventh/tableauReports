import os
import os.path
import datetime as dt

from utils import (
    logger,
    connections,
    context,
    vh_config,
    emails
)

directory = context.get_context(os.path.abspath(__file__))

sql_file = f"{directory}\\pats_no_pcp\\sql\\pats_no_pcp.sql"
staging_folder = ("C:\\Users\\talendservice\\OneDrive - Vivent Health\\Health\\Health Informatics and Technology\\Project Management\\Epic Routines (Mitch)\\monthly\\pats_no_pcp")

pats_no_pcp_logger = logger.setup_logger(
    "pats_no_pcp_logger",
    f"{directory}\\logs\\main.log"
)

config = vh_config.grab(pats_no_pcp_logger)


def run():
    pats_no_pcp_logger.info("Patients with no PCP")

    if not os.path.exists(staging_folder):
        os.makedirs(staging_folder)

    try:
        internal_engine = connections.engine_creation(
            server=config['Clarity - VH']['server'],
            db=config['Clarity - VH']['database'],
            driver=config['Clarity - VH']['driver'],
            internal_use=True
        )

        with internal_engine.connect() as clarity_connection:
            df = connections.sql_to_df(
                file=sql_file,
                connection=clarity_connection
            )

        tx_df = df[(df["STATE"] == "TX")]
        wi_df = df[df["STATE"] == "WI"]
        co_df = df[df["STATE"] == "CO"]
        mo_sl_df = df[(df["STATE"] == "MO") & df["CITY"] == "SL"]
        mo_kc_df = df[(df["STATE"] == "MO") & df["CITY"] == "KC"]

        texas = (tx_df,
                 {"Erika Colombo": "Erika.Colombo@viventhealth.org",
                  "Konya Smart": "Konya.Smart@viventhealth.org"},
                 "TX")
        wisconsin = (wi_df,
                     {"Jen Bartels": "Jen.Bartels@viventhealth.org",
                     "Tanya Wolf": "Tanya.wolf@viventhealth.org",
                     "Carla Washington": "Carla.Washington@viventhealth.org"},
                     "WI")
        colorado = (co_df,
                    {"Caroline Eisenberg":
                     "Caroline.Eisenberg@viventhealth.org"},
                    "CO")
        mo_sl = (mo_sl_df,
                 {"Jen Weiler": "Jen.Weiler@viventhealth.org"},
                 "MO - St_Louis")
        mo_kc = (mo_kc_df,
                 {"Paul Rotert": "paul.rotert@viventhealth.org",
                  "Latisha Heard": "Latisha.Heard@viventhealth.org"},
                 "MO - Kansas City")

        delivery = (texas, wisconsin, colorado, mo_sl, mo_kc)

        for item in delivery:
            delivery_handler(item)

    except ConnectionError as conn_err:
        pats_no_pcp_logger.error(
            f"Unable to connect to OCHIN - Vivent Health: {conn_err}"
        )
    except KeyError as key_err:
        pats_no_pcp_logger.error(f"Incorrect connection keys: {key_err}")


def delivery_handler(to_deliver):
    today = dt.datetime.today().strftime("%B-%Y")
    df = to_deliver[0]
    recipients = to_deliver[1]
    state = to_deliver[2]

    if df.empty:
        pats_no_pcp_logger.info(f"No data to send for {state}.")
    else:
        csv_file = (f"{staging_folder}\\{today} - Patients with no PCP and "
                    f"a recent medical visit - {state}.csv")
        with open(csv_file, "wb") as delivery_file:
            df.to_csv(delivery_file, index=False)

        sendEmails(recipients, csv_file, state)


def sendEmails(recipients, csv_file, state):
    to = list(recipients.values())

    emails.send_email(
        subject=f"Patients with No PCP and a Recent Medical Visit - {state}",
        body_text=("Hello,"
                   "\n\n"
                   "Attached are a list of patients with No PCP and a Recent Medical Visit."
                   "\n\n"
                   "Thank You"),
        to_emails=to,
        file_to_attach=csv_file,
        cc_emails=["Adam.Carlson@viventhealth.org",
                   "mitch.scoggins@viventhealth.org"],
        bcc_emails=[]
    )

    to_names = list(recipients.keys())
    pats_no_pcp_logger.info(
        f"Patients with no PCP Monthly Data email sent to {', '.join(to_names)}"
    )
