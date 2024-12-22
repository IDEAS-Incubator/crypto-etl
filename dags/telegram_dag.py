from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import os
from datetime import datetime as dt, timedelta
from social_tools.telegram.telegram_downloader import TelegramChatDownloader
from social_tools.aws.s3 import AwsS3Manager
from airflow.sensors.external_task import ExternalTaskSensor

import shutil

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



default_args = {
    'owner': 'Astro',
    'start_date': days_ago(1),
    'retries': 3,
}

# Define DAG
with DAG(
    dag_id='telegram_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    @task()
    def get_channels()->list[str]:
                
        logger.info(f"Current Directory {os.getcwd()}")
        # Ensure the group_name.txt file exists in the current directory
        group_name_file = os.path.join(os.getcwd(), 'group_name.txt')

        if not os.path.exists(group_name_file):
            logger.error(f"{group_name_file} not found.")
            raise FileNotFoundError(f"{group_name_file} not found.")
        
        # Read the chat usernames from the text file
        try:
            with open(group_name_file, 'r') as file:
                chat_usernames = [line.strip() for line in file.readlines() if line.strip()]  # Read all non-empty lines

            logger.info(f"chat_usernames : {chat_usernames}")
            return chat_usernames
        except Exception as e:
            logger.error(f"Error reading {group_name_file}: {e}")
            raise Exception(f"Error reading {group_name_file}: {e}")
        
    @task()
    def download_telegram_data(chat_usernames:list[str])->list[str]:
        """Download Telegram chat data for multiple groups using the Telegram API."""
        
        limit = 1000
        from_date = (dt.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        to_date = dt.now().strftime('%Y-%m-%d')
        downloaded_filepaths=[]
        downloader = TelegramChatDownloader()

        # Iterate over each chat username
        for chat_username in chat_usernames:
            try:
                filepath = downloader.start(chat_username=chat_username, limit=limit, from_date=from_date, to_date=to_date)
                logger.info(f"Chat data downloaded successfully for {chat_username}! Saved messages to {filepath}")

                if not os.path.exists(filepath):
                    raise FileNotFoundError(f"The file {filepath} was not found after download.")
                
                # You can add more logic here to upload to S3 or other processing if needed
                downloaded_filepaths.append(filepath)  # Add filepath to the list
                
                

            except Exception as e:
                logger.error(f"Error downloading file for {chat_username}: {e}")
                raise Exception(f"Error during download for {chat_username}: {e}")
            
        logger.info(f"dowload location : {downloaded_filepaths}")
        return downloaded_filepaths
    @task()
    def upload_to_s3(filepaths: list[str]):
        """Upload the downloaded files to S3 and delete the download folder after successful uploads."""
        try:
            s3_manager = AwsS3Manager()
            # Get the directory of the first file to identify the download folder
            if filepaths:
                download_folder = os.path.dirname(filepaths[0])
            else:
                logger.warning("No files to upload. Skipping folder deletion.")
                return
            
            for filepath in filepaths:
                try:
                    chat_username = os.path.basename(filepath).split('.')[0]  # Extract chat username from filename
                    year, month, day = dt.now().year, dt.now().month, dt.now().day
                    file_name = os.path.basename(filepath)
                    s3_folder = f"telegram/{chat_username}/{year}/{month}/{day}/{file_name}"
                    
                    # Upload the file to S3
                    s3_manager.upload_file(file_path=filepath,key=s3_folder)
                    logger.info(f"Uploaded file to S3: {s3_folder}")
                    
                    # Remove the file after successful upload
                    os.remove(filepath)
                    logger.info(f"Removed local file: {filepath}")
                except Exception as e:
                    logger.error(f"Error processing file {filepath}: {e}")
                    continue  # Skip to the next file in case of an error
            
            # Remove the download folder after all files are processed
            if os.path.exists(download_folder):
                shutil.rmtree(download_folder)
                logger.info(f"Removed download folder: {download_folder}")
            else:
                logger.warning(f"Download folder {download_folder} does not exist or was already removed.")

        except Exception as e:
            logger.error(f"Error during S3 upload process: {e}")
            raise Exception(f"Error during S3 upload process: {e}")

    # Define task dependencies
    chat_usernames=get_channels()
    downloaded_filepath = download_telegram_data(chat_usernames=chat_usernames)
    upload_to_s3(downloaded_filepath)
