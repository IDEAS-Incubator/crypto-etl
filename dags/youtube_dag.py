from airflow.decorators import task
from airflow import DAG
from airflow.utils.dates import days_ago
from social_tools.youtube.youtube_data_handler import YouTubeDataHandler
from social_tools.aws.s3 import AwsS3Manager
import os
import json
from datetime import datetime as dt
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
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
    description="Extract, transform, and load YouTube data into S3.",
) as dag:

    @task()
    def get_channels() -> list[str]:
        """Retrieve the list of YouTube channels from a file."""
        file_path = os.path.join(os.getcwd(), "channel_list.txt")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "r") as file:
            channel_list = [line.strip() for line in file if line.strip()]

        logger.info(f"Channel List: {channel_list}")
        return channel_list

    @task()
    def get_latest_videos(channel_list: list[str]) -> list[dict]:
        """Fetch the latest videos from the specified YouTube channels."""
        youtube = YouTubeDataHandler()
        videos = youtube.get_latest_videos(channel_list=channel_list)

        video_data = [
            {
                "title": title,
                "publish_time": publish_time,
                "video_id": video_id,
                "channel": handle,
            }
            for title, publish_time, video_id, handle in videos
        ]

        logger.info(f"Retrieved {len(video_data)} videos.")
        return video_data

    @task()
    def upload_to_s3(videos: list[dict]):
        """Upload video metadata and transcripts to S3."""
        youtube = YouTubeDataHandler()
        aws = AwsS3Manager()

        # Define a temporary directory for storing JSON files
        temp_dir = os.path.join(os.getcwd(), "temp")
        os.makedirs(temp_dir, exist_ok=True)

        for video in videos:
            try:
                # Parse the publish time
                publish_time = dt.strptime(video["publish_time"], "%Y-%m-%dT%H:%M:%SZ")
                year, month, day = publish_time.year, publish_time.month, publish_time.day
                s3_folder = f"youtube/{video['channel']}/{year}/{month}/{day}"
                file_name = f"{video['video_id']}.json"

                # Skip if the video already exists in S3
                s3_key = f"{s3_folder}/{file_name}"
                if aws.file_exists(key=s3_key):
                    logger.info(f"Video {video['video_id']} already exists in S3.")
                    continue

                # Fetch the video transcript
                transcript = youtube.get_transcript(video["video_id"])
                video_info = {
                    "title": video["title"],
                    "channel": video["channel"],
                    "transcript": transcript,
                    "publish_time": video["publish_time"],
                }

                # Write video data to a temporary JSON file
                temp_file_path = os.path.join(temp_dir, file_name)
                with open(temp_file_path, "w") as file:
                    json.dump(video_info, file, ensure_ascii=False)

                # Upload the file to S3
                url=aws.upload_file(temp_file_path, key=s3_key)
                logger.info(f"Uploaded to S3 at {url}")

            except Exception as e:
                logger.error(f"Error processing video {video['video_id']}: {e}")
                continue  # Skip to the next video on error

        # Clean up temporary files
        for file in os.listdir(temp_dir):
            os.remove(os.path.join(temp_dir, file))
        os.rmdir(temp_dir)

    # Task dependencies
    channel_list = get_channels()
    latest_videos = get_latest_videos(channel_list=channel_list)
    upload_to_s3(videos=latest_videos)

