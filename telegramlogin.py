from dotenv import load_dotenv
load_dotenv()  # This loads the environment variables from the .env file

from social_tools.telegram.telegram_downloader import TelegramChatDownloader

def login_to_telegram():
    """Handles the login process for Telegram."""
    # Create an instance of TelegramChatDownloader
    chat_downloader = TelegramChatDownloader(download_path="downloads")

    # Call the login method to authenticate the user
    chat_downloader.login()

    print("Telegram login completed successfully.")

if __name__ == "__main__":
    login_to_telegram()
