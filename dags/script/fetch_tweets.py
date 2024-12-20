import asyncio
import json
import time
from configparser import ConfigParser
from typing import List, NoReturn
from twikit import Client, Tweet
import os
from social_tools.aws.s3 import AwsS3Manager
from datetime import datetime as dt
import logging
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CHECK_INTERVAL = 60 * 5  # 5 minutes
COOKIES_FILE = 'cookies.json'
TWEETS_DIR = "tweets"  # Directory to save tweets JSON files

os.makedirs(TWEETS_DIR, exist_ok=True)

client = Client(language='en-US')
aws3 = AwsS3Manager()

def get_usernames() -> List[str]:
    """Retrieve the list of usernames to scrape."""
    
    # We Change by API Later on 
    
    usernames = [
    "VitalikButerin", "saylor", "APompliano", "aantonop", "SatoshiLite", "IvanOnTech", "woonomic", "PeterSchiff", 
    "Bitboy_Crypto", "novogratz", "tyler", "cameron", "laurashin", "PrestonPysh", "LynAldenContact"
]

    return usernames

def upload_tweets_to_s3(tweets: List[Tweet], username: str) -> None:
    """
    Save tweets to a JSON file and upload to S3.
    """
    timestamp = int(time.time())
    tweet_data = [
        {
            "id": tweet.id,
            "user_name": tweet.user.screen_name,
            "screen_name": tweet.user.name if tweet.user else None,
            "user_id": tweet.user.id if tweet.user else None,
            "text": tweet.text,
            "created_at": tweet.created_at,
            "lang": tweet.lang,
            "reply_count": tweet.reply_count,
            "retweet_count": tweet.retweet_count,
            "favorite_count": tweet.favorite_count,
            "hashtags": tweet.hashtags,
            "urls": tweet.urls,
        }
        for tweet in tweets
    ]
    
    filename = f"{TWEETS_DIR}/tweets_{username}_{timestamp}.json"
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(tweet_data, f, indent=4)
        
        # Upload to S3
        year, month, day = dt.now().year, dt.now().month, dt.now().day
        s3_key = f"twitter/{username}/{year}/{month}/{day}/{os.path.basename(filename)}"
        aws3.upload_file(file_path=filename,file_name="data.json",key=s3_key)
        logger.info(f"Uploaded file to S3: {s3_key}")
    except Exception as e:
        logger.info(f"Error uploading file to S3: {e}")
    finally:
        if os.path.exists(filename):
            os.remove(filename)  # Clean up local file
            pass

async def login(username: str, email: str, password: str) -> None:
    """Logs in the user and saves cookies for future use."""
    try:
        await client.login(auth_info_1=username, auth_info_2=email, password=password)
        client.save_cookies(COOKIES_FILE)
        logger.info("Successfully logged in and saved cookies.")
    except Exception as e:
        logger.info(f"Login failed: {e}")
        raise

async def fetch_tweets_for_user(usernames: List[str], count: int = 19) -> None:
    """Fetch tweets for each username and save them to a file."""
    for username in usernames:
        try:
            logger.info(f"Fetching tweets for user: {username}")
            user = await client.get_user_by_screen_name(username)
            tweets = await user.get_tweets(tweet_type='Tweets', count=count)
            upload_tweets_to_s3(tweets, username)
            logger.info(f"Fetched and uploaded tweets for {username}.")
        except Exception as e:
            if "Rate limit exceeded" in str(e):
                logger.info(f"Rate limit exceeded for {username}. Sleeping for {CHECK_INTERVAL} seconds...")
                await asyncio.sleep(CHECK_INTERVAL)
                user = await client.get_user_by_screen_name(username)
                tweets = await user.get_tweets(tweet_type='Tweets', count=count)
                upload_tweets_to_s3(tweets, username)
            else:
                logger.info(f"Error fetching tweets for {username}: {e}")

async def main_loop(usernames: List[str]) -> NoReturn:
    """Main loop to fetch tweets periodically with rate limiting."""
    await fetch_tweets_for_user(usernames)


async def main() -> None:
    """Main entry point for the script."""
    # Read config for credentials

    logger.info("Starting Twitter scraper...")
    # Get credentials from config file

    username = os.getenv("Twitter_username")
    email = os.getenv('Twitter_email')
    password = os.getenv('Twitter_password')
    
    if not username or not email or not password:
        logger.info("Missing required credentials. Please set the Twitter_username, faijan@bayes, and faijankhanpr@gmail.com environment variables.")
        raise ValueError

    # Load cookies or log in
    try:
        client.load_cookies(COOKIES_FILE)
        logger.info("Loaded cookies successfully.")
    except Exception:
        logger.info("Cookies not found or invalid. Logging in again...")
        await login(username, email, password)

    # Start main loop
    usernames = get_usernames()
    await main_loop(usernames)

# Entry point
if __name__ == "__main__":
    asyncio.run(main())
