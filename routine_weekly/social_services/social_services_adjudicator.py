import os
import os.path
import time


from social_services.cm_new_intakes_for_wi_dhs import cm_new_intakes_for_wi_dhs
from social_services._340b_mou_data import _340b_mou_data
from social_services.austin_client_wo_tct_id import austin_client_wo_tct_id

from utils import (
    logger,
    context,
    teams_msg
)

directory = context.get_context(os.path.abspath(__file__))

logger = logger.setup_logger(
    "routine_daily_logger",
    f"{directory}/social_services/logs/main.log"
)


def run():
    init_message = "Running Social Services Weekly ETL Workflow."
    logger.info(init_message)
    
    teams_msg.send(
        logger,
        message = init_message,
        title = "Social Services - Weekly ETL Workflow"
    )
    

    start = time.time()


    '''Case Management Data'''
    cm_new_intakes_for_wi_dhs.run() 
    #_340b_mou_data.run() #discontinued on 2/27/2023 per Erik Bauch's Request
    
    '''Housing Data'''
    
    '''Social Services Operations Data'''
    austin_client_wo_tct_id.run() #Austin clients who do not have a TCT ID in PE (emailed to Lourdes, et al) ADD BACK IN
    
    '''Prevention Data'''
    
    '''Food Pantry Data'''
    
    runtime = f"{time.time() - start:.4f}"
    message = f"Total run time is {runtime}s."
    logger.info(message)
    ''' ADD BACK IN
    teams_msg.send(
        logger,
        message=message,
        title="Social Services - Weekly ETL Workflow"
    )
        '''