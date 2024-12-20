import os
import json
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from social_tools.telegram.utils import MediaDownloader, parse_date
import pytz

class TelegramChatDownloader:
    def __init__(self, download_path="downloads"):
        self.api_id = self._get_env_variable("API_ID")
        self.api_hash = self._get_env_variable("API_HASH")
        self.phone_number = self._get_env_variable("PHONE_NUMBER")
        self.download_path = download_path
        self.client = TelegramClient('session_name', self.api_id, self.api_hash)
    
    def _get_env_variable(self, name: str) -> str:
        result = os.getenv(name)
        if not result:
            raise EnvironmentError(f"Please set the {name} environment variable.")
        return result

    def login(self):
        """Logs in to Telegram client."""
        try:
            print("Logging in...")
            self.client.start(phone=self.phone_number)
            print("Logged in successfully!")
        except SessionPasswordNeededError:
            print("Two-step verification enabled. Please enter your password.")
        except Exception as e:
            print(f"An error occurred during login: {e}")
            raise
    async def download_chat(self, username, limit=100, from_date=None, to_date=None):
        if from_date:
            from_date = parse_date(from_date)
        if to_date:
            to_date = parse_date(to_date)

        downloader = MediaDownloader(f"{self.download_path}/{username}")
        os.makedirs(self.download_path, exist_ok=True)

        messages_data = []

        async for message in self.client.iter_messages(username, limit=limit):
            message_date = message.date.astimezone(pytz.UTC) if message.date else None
            if from_date and message_date < from_date:
                continue
            if to_date and message_date > to_date:
                continue

            message_record = {
                "date": message_date.isoformat() if message_date else None,
                "sender_id": message.sender_id,
                "message": message.text,
                "message_id": message.id,
                "media": []
            }

            messages_data.append(message_record)

            if message.media:
                file_type = 'others'
                file_extension = None
                if hasattr(message.media, 'document'):
                    document = message.media.document
                    file_extension = document.mime_type.split('/')[1]

                    if document.mime_type.startswith('image'):
                        file_type = 'images'
                    elif document.mime_type.startswith('video'):
                        file_type = 'videos'
                    elif document.mime_type.startswith('audio'):
                        file_type = 'audios'
                    elif document.mime_type.startswith('application/pdf') or document.mime_type.startswith('text'):
                        file_type = 'documents'

                file_path = downloader.get_download_path(file_type)
                os.makedirs(file_path, exist_ok=True)

                if file_extension:
                    file_path = await message.download_media(file=f"{file_path}/{message.id}.{file_extension}")
                else:
                    file_path = await message.download_media(file=file_path)

                message_record['media'].append(file_path)
                print(f"Downloaded: {file_path}")

        json_filename = os.path.join(self.download_path, f"{username}.json")
        with open(json_filename, mode="w", encoding="utf-8") as json_file:
            json.dump(messages_data, json_file, ensure_ascii=False, indent=4)

        print(f"Messages saved to {json_filename}")
        return json_filename  # Ensure this returns the full path


    def start(self, chat_username, limit=100, from_date=None, to_date=None)->str:
        """
        Starts the client, logs in, and downloads the chat data.

        Args:
            chat_username (str): Username or ID of the chat/group.
            limit (int): Number of messages to retrieve (default: 100).
            from_date (str): Start date for filtering messages (YYYY-MM-DD).
            to_date (str): End date for filtering messages (YYYY-MM-DD).
        """
        with self.client:
            try:
                print("Logging in...")
                self.client.start(phone=self.phone_number)
                print("Logged in successfully!")

                # Call the download_chat function with the provided parameters
                filepath = self.client.loop.run_until_complete(self.download_chat(
                    chat_username, limit=limit, from_date=from_date, to_date=to_date))
                
                return filepath

            except SessionPasswordNeededError:
                print("Two-step verification enabled. Please enter your password.")
            except Exception as e:
                print(f"An error occurred: {e}")



if __name__ == "__main__":
    chat_username = "GodeChain"  # Replace with the desired chat or group username
    limit = 100
    from_date = "2024-10-17"
    to_date = "2024-12-18"
    download_path = "downloads"
    chat_downloader = TelegramChatDownloader(download_path)
    chat_downloader.start(chat_username, limit, from_date, to_date)
    print("Chat data downloaded successfully!")
    download_path = "downloads"
    
    filepath=f"{download_path}/{chat_username}.json"
    print(f"Saved messages to filepath")
    from social_tools.aws.s3 import AwsS3Manager
    s3_manager=AwsS3Manager()
    from datetime import datetime
                # Upload the file to S3 with new file path logic
    channel = chat_username  # Use the username or provide logic for channel
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    file_name="data.json"
    s3_folder = f"telegram/{channel}/{year}/{month}/{day}/{file_name}"
    s3_manager.upload_file(filepath,file_name,key=f"{s3_folder}")
    print("Exiting...")