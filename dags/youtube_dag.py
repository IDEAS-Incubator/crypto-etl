from airflow.decorators import  task
from airflow import DAG
from airflow.utils.dates import days_ago
from pendulum import datetime
from social_tools.youtube.youtube_data_handler import YouTubeDataHandler
from social_tools.aws.s3 import AwsS3Manager
import os
from loguru import logger
import json

default_args = {
    "owner": "Astro",
    "start_date": days_ago(1),
    "retries": 3,
}

# Define DAG
with DAG(
    dag_id="youtube_elt_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    @task()
    def get_channels() -> list[str]:
        file_path = os.path.join(os.getcwd(), "channel_list.txt")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File does not exist: {file_path}")

        try:
            with open(file_path, "r") as file:
                channel_list = [line.strip() for line in file if line.strip()]
            logger.info(f"Channel List: {channel_list}")
            return channel_list
        except Exception as e:
            logger.error(f"Error reading channel list: {e}")
            raise

    @task()
    def get_latest_videos(channel_list: list[str]) -> list[dict]:
        try:
            youtube = YouTubeDataHandler()
            videos = youtube.get_latest_videos(channel_list=channel_list)
            logger.info(f"Latest Videos: {videos}")
            return [
                {
                    "title": title,
                    "publish_time": publish_time,
                    "video_id": video_id,
                    "channel": handle,
                }
                for title, publish_time, video_id, handle in videos
            ]
        except Exception as e:
            logger.error(f"Error fetching YouTube data: {e}")
            raise


    @task()
    def upload_to_s3(videos: list[dict]):
        try:
            youtube = YouTubeDataHandler()
            aws = AwsS3Manager()

            # Define a temporary directory for storing JSON files
            temp_dir = os.path.join(os.getcwd(), "temp")
            os.makedirs(temp_dir, exist_ok=True)

            for video in videos:
                try:
                    video_info_dict = {
                        "title": video["title"],
                        "channel": video["channel"],
                        "transcript": youtube.get_transcript(video["video_id"]),
                        "publish_time": video["publish_time"],
                    }

                    # Create a temporary JSON file
                    file_name = f"{video['video_id']}.json"
                    file_path = os.path.join(temp_dir, file_name)

                    with open(file_path, "w") as file:
                        json.dump(video_info_dict, file, ensure_ascii=False)

                    # Define S3 folder structure
                    today = datetime.today()
                    year, month, day = today.year, today.month, today.day
                    s3_folder = f"youtube/{video_info_dict['channel']}/{year}/{month}/{day}"

                    aws.upload_file(file_path, file_name, key=f"{s3_folder}/{file_name}")
                    logger.info(f"Uploaded {file_path} to S3 at {s3_folder}/{file_name}")

                except Exception as e:
                    logger.error(f"Error processing video {video['video_id']}: {e}")
                    continue  # Skip to the next video in case of an error

            # Clean up temporary files
            for file in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, file))
            os.rmdir(temp_dir)

        except Exception as e:
            logger.error(f"Error uploading files to S3: {e}")
            raise

    # Define task dependencies
    channel_list = get_channels()
    latest_videos = get_latest_videos(channel_list=channel_list)
    # latest_videos=[
    #     {'title': 'Bitcoin Market Cycle Theory', 'publish_time': '2024-12-18T04:23:49Z', 'video_id': '5j2ROXGDEBw', 'channel': '@intothecryptoverse'}
    # ]
    upload_to_s3(videos=latest_videos)

