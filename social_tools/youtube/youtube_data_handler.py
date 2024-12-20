import json
import os
import requests
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from googleapiclient.discovery import build
from dotenv import load_dotenv
from loguru import logger


# Loading environment variables from .env
load_dotenv()

class YouTubeDataHandler:
    def __init__(self,  temp_file_path=None):
        # Get the base directory where the DAG script is located
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Use the base directory to construct absolute paths
        self.temp_file_path = temp_file_path or os.path.join(base_dir, "temp.json")

        self.YOUTUBE_DATA_API = self.get_env_variable("YOUTUBE_DATA_API")
        self.VLEARN_BASE_URL = self.get_env_variable("VLEARN_BASE_URL")
        self.VLEARN_TOKEN = self.get_env_variable("VLEARN_TOKEN")
        
        # Create YouTube API client
        self.youtube = build('youtube', 'v3', developerKey=self.YOUTUBE_DATA_API)

   



    def get_env_variable(self, name: str) -> str:
        """Get the environment variable and raise error if not exist."""
        result = os.getenv(name)
        if not result:
            raise EnvironmentError(f"Please set the {name} in .env")
        return result

    def check_response(self, response: requests.Response):
        """Check if there is InternalError"""
        if not response or response.status_code == 500:
            raise NotImplementedError

    def get_channel_videos(self, handle: str) -> list:
        """Get the list of videos from a channel posted after yesterday midnight"""
        videos = []
        next_page_token = None
        today = datetime.utcnow().date()
        yesterday_midnight = datetime.combine(today - timedelta(days=1), datetime.min.time())
        yesterday_midnight_iso = yesterday_midnight.isoformat() + "Z"

        while True:
            # Fetch playlist ID of the channel's uploads
            response = self.youtube.channels().list(
                part='contentDetails',
                forHandle=handle,
            ).execute()

            # Get the playlist ID for the channel's uploaded videos
            playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

            # Fetch videos in the playlist
            playlist_response = self.youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in playlist_response['items']:
                video_title = item['snippet']['title']
                video_publish_date = item['snippet']['publishedAt']
                video_id = item['snippet']['resourceId']['videoId']

                if video_publish_date >= yesterday_midnight_iso and "live show" not in video_title and "Livestream" not in video_title:
                    videos.append((video_title, video_publish_date, video_id, handle))

            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break

        return videos

    def get_latest_videos(self, channel_list: list) -> list:
        """Get latest videos from a list of channels"""
        new_videos = []
        for channel in channel_list:
            try:
                new_videos += self.get_channel_videos(channel)
            except Exception as e:
                logger.error(f"Error fetching videos for {channel}: {e}")
        return new_videos

    def get_transcript(self, video_id: str) -> str:
        """Get the transcript for a given video"""
        vlearn_base_url = self.VLEARN_BASE_URL
        header = {"Authorization": f"Bearer {self.VLEARN_TOKEN}"}
        url = f"{vlearn_base_url}getTranscript?video_id={video_id}"
        response = requests.get(url, headers=header)
        self.check_response(response)

        transcript = response.json()["transcript"]
        return " ".join([item["text"] for item in transcript])


# Run the script
if __name__ == "__main__":
    handler = YouTubeDataHandler()
