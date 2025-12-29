from press_ganey.scripts import pg_clinical_main
from press_ganey.scripts import pg_provide_enterprise_main
from press_ganey.scripts import pg_send_data

def run():
    pg_clinical_main.run()
    pg_provide_enterprise_main.run()
    pg_send_data.transfer_sftp()
