import os
import os.path
import time


#from prevention.testing_sessions_hiv import testing_sessions_hiv
from prevention.wi_hcv_tests_email import wi_hcv_tests_email

from utils import (
    logger,
    context,
    teams_msg
)

directory = context.get_context(os.path.abspath(__file__))

logger = logger.setup_logger(
    "routine_daily_logger",
    f"{directory}/prevention/logs/main.log"
)

def run():
    init_message = "Running Prevention Weekly ETL Workflow."
    logger.info(init_message)
    teams_msg.send(
        logger,
        message = init_message,
        title = "Prevention - Weekly ETL Workflow"
    )

    start = time.time()
    
    '''Prevention Data'''
    wi_hcv_tests_email.run()

    runtime = f"{time.time() - start:.4f}"
    message = f"Total run time is {runtime}s."
    logger.info(message)
    teams_msg.send(
        logger,
        message=message,
        title="Prevention - Weekly ETL Workflow"
    )