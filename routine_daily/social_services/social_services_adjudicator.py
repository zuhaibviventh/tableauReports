import os
import os.path
import time

from utils import (
    logger,
    context,
    teams_msg
)

from social_services.prevention_supply_distribution import main_supply_distribution
from social_services.prevention_navigation import main_prev_nav
from social_services.austin_clients_w_eligibility_dates import main_austin_clients_elig_dates
from social_services.preferred_language import main_preferred_language

directory = context.get_context(os.path.abspath(__file__))

logger = logger.setup_logger(
    "routine_daily_logger",
    f"{directory}/social_services/logs/main.log"
)


def run(shared_drive):
    init_message = "Running Social Services Daily ETL Workflow."
    logger.info(init_message)
    teams_msg.send(
        logger,
        message = init_message,
        title = "Social Services - Daily ETL Workflow"
    )

    start = time.time()

    
    main_prev_nav.run(shared_drive) # Prevention Navigation
    main_austin_clients_elig_dates.run(shared_drive)
    main_preferred_language.run(shared_drive)
    main_supply_distribution.run(shared_drive) # Prevention Supply Distribution

    runtime = f"{time.time() - start:.4f}"
    message = f"Total run time is {runtime}s."
    logger.info(message)
    teams_msg.send(
        logger,
        message=message,
        title="Social Services - Daily ETL Workflow"
    )
