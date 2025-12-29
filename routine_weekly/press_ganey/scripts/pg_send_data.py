import os
import json
import paramiko
from utils import context
from utils import logger

directory = context.get_context(os.path.abspath(__file__))
log_file = f"{directory}\\logs\\routine_weekly_main.log"

pg_sftp_logger = logger.setup_logger("pg_sftp_logger", log_file)
paramiko.util.log_to_file(log_file)

def connect_sftp(config_file):
    """
    Connect to SFTP using the provided configuration file.

    Args:
    - config_file (str): Path to the JSON configuration file.

    Raises:
    - FileNotFoundError: If the configuration file is not found.
    - KeyError: If the required keys are missing in the configuration file.

    Returns:
    - paramiko.SFTPClient: An SFTPClient object connected to the server.
    """
    try:
        with open(config_file, "r") as conf_file:
            config = json.load(conf_file)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Config file was not found: {e}")

    try:
        host = config['Press Ganey SFTP - PROD']['host']
        port = config['Press Ganey SFTP - PROD']['port']
        uid = config['Press Ganey SFTP - PROD']['username']
        pwd = config['Press Ganey SFTP - PROD']['password']
    except KeyError as e:
        raise KeyError(f"Required key is missing in the configuration file: {e}")

    transport = paramiko.Transport((host, port))
    transport.connect(None, uid, pwd)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp

def close_sftp(sftp):
    """
    Close the SFTP connection.

    Args:
    - sftp (paramiko.SFTPClient): An SFTPClient object.
    """
    if sftp:
        sftp.close()

def upload_file(sftp, local_path, remote_path):
    """
    Upload a file to the SFTP server.

    Args:
    - sftp (paramiko.SFTPClient): An SFTPClient object connected to the server.
    - local_path (str): Path of the file to upload locally.
    - remote_path (str): Path to save the uploaded file on the server.
    """
    sftp.put(local_path, remote_path)

def transfer_sftp():
    parent_dir = os.path.abspath(os.path.join(context.get_context(directory), os.pardir))
    press_ganey_folder = "C:\\Users\\talendservice\\OneDrive - Vivent Health\\Quality\\Press Ganey\\"
    saving_folder = f"{press_ganey_folder}\\Data Files Sent to Press Ganey 331180 (our number) and MMDDYYYY"
    staging_folder = f"{saving_folder}\\staging"

    final__pg_clinical_csv = f"{staging_folder}\\331180CL.csv"
    final__pg_pe_csv = f"{staging_folder}\\331180SS.csv"
    config_file = f"{parent_dir}\\routine_weekly\\config.json"

    sftp = None
    try:
        sftp = connect_sftp(config_file)

        sftp.put(localpath=final__pg_clinical_csv, remotepath="Inbox/331180CL.csv")
        sftp.put(localpath=final__pg_pe_csv, remotepath="Inbox/331180SS.csv")
    finally:
        close_sftp(sftp)
