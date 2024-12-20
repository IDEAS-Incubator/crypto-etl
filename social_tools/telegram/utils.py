import os
from datetime import datetime,timedelta
import pytz
# Define the download paths for different media types
class MediaDownloader:
    def __init__(self, base_path):
        self.__base_path = base_path
        # Define file type paths
        self.__download_paths = {
            'images': os.path.join(self.__base_path, 'images'),
            'videos': os.path.join(self.__base_path, 'videos'),
            'audios': os.path.join(self.__base_path, 'audios'),
            'documents': os.path.join(self.__base_path, 'documents'),
            'others': os.path.join(self.__base_path, 'others'),
        }

    def get_download_path(self, file_type):
        """Return the appropriate path based on file type."""
        return self.__download_paths.get(file_type, self.__download_paths['others'])


# Function to convert string to datetime with timezone
def parse_date(date_string):
    try:
        # Use UTC timezone to ensure consistent comparison
        return datetime.strptime(date_string, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        return None

def get_default_dates():
    today = datetime.now(pytz.UTC)
    default_from_date = today - timedelta(days=7)  # Default "from" date: 7 days ago
    default_to_date = today  # Default "to" date: today
    return default_from_date, default_to_date
