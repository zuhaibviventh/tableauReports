import os
import sys
from clinical_operations import clinical_operations_adjudicator
from clinical_quality import clinical_quality_adjudicator
from social_services import social_services_adjudicator
from general import general_adjudicator
from utils import emails
from utils import logger
from utils import context
from utils import extract_last_word
from datetime import date

directory = context.get_context(os.path.abspath(__file__))
main_logger = logger.setup_logger("routine_daily_main_logger", f"{directory}/routine_daily/logs/main.log")

def flag_run(flag_value):
    """
    Update the flag value in the flag.txt file.

    Args:
        flag_value (date): The new flag value (today's date).
    """
    flag_file = f"{directory}/routine_daily/utils/flag.txt"

    with open(flag_file, "wt") as f:
        f.write(str(flag_value))

def sendEmail():
    """
    Send an email notification about the completion of the daily ETL process.

    This function constructs an email message with the subject "Daily ETL" and the body
    "Routine daily ETL has run successfully." The email is sent to the recipient(s)
    specified in the `recepients` dictionary.
    """
    recepients = { "Mitch Scoggins": "mitch.scoggins@viventhealth.org" }
    to = list(recepients.values())

    emails.send_email(
        subject = "Daily ETL",
        body_text = "Routine daily ETL has ran succesfully.",
        to_emails = to,
        file_to_attach = [],
        cc_emails = [],
        bcc_emails = []
    )

def run_proc():
    """
    Run the daily ETL process.

    This function calls the run method for each adjudicator module
    (clinical_operations_adjudicator, clinical_quality_adjudicator,
    social_services_adjudicator, and general_adjudicator) to perform
    their respective tasks. After all adjudicators have run, an email
    notification is sent to indicate the completion of the process.
    """
    operations_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/clinical_operations")
    quality_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/clinical_quality")
    pe_shared_drive = ("//FSS001SVR/Analysis/Routines/daily/provide_enterprise")
    
    social_services_adjudicator.run(pe_shared_drive)
    clinical_operations_adjudicator.run(operations_shared_drive)
    clinical_quality_adjudicator.run(quality_shared_drive) # CHANGE THE PROJECT ID BACK IN  tableau_project_ids.json in utils
    
    
    #general_adjudicator.run() #per Shannon Breein, this isn't working so isn't needed.

    sendEmail() 

def main():
    """
    Run the main process.

    This function sets the flag to 1 at the start, indicating that the process has started.
    It then attempts to run the main process, handling any exceptions that may occur.
    If the process runs successfully, the flag is reset to 0 at the end.
    Finally, an email notification is sent to indicate the completion of the process.
    """
    try:
        if extract_last_word.extract_last_word(sys.prefix) == "venv":
            main_logger.info("Running Daily ETL Process")
            run_proc()
        else:
            main_logger.error("Virtual environment (venv) not active. Exiting...")
            sys.exit(1)
    except Exception as e:
        main_logger.error(f"Exception: {e}")
    else:
        flag_run(date.today())

if __name__ == "__main__":
    main()
