
# Project Name: Social Media Automation with Airflow on Astro

This project automates the process of downloading messages from Telegram channels and processing YouTube video data using **Apache Airflow** on **Astro (Astronomer)**. The automation involves:

- Downloading messages from a specified Telegram channel and uploading them to AWS S3.
- Processing and uploading YouTube video data to S3.

## Table of Contents
- [Overview](#overview)
- [Setup Instructions](#setup-instructions)
- [Astro Configuration](#astro-configuration)
- [Task Descriptions](#task-descriptions)
- [File Structure](#file-structure)
- [Dependencies](#dependencies)
- [Environment Variables](#environment-variables)
- [License](#license)

## Overview

This project is built using **Apache Airflow** and **Astro** (by Astronomer), and automates two tasks:
1. **Telegram Downloader**: Downloads messages from a specific Telegram channel and uploads them to **AWS S3**.
2. **YouTube Data Handler**: Processes YouTube videos from a list, performing necessary operations, and uploads the results to **AWS S3**.

These tasks are implemented as Airflow DAGs, which are scheduled and executed automatically.

## Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/social-media-automation.git
   cd social-media-automation
   ```

2. **Install Astro CLI**:
   Ensure you have the **Astro CLI** installed. You can find installation instructions on the [Astro documentation](https://www.astronomer.io/docs).

3. **Install dependencies**:
   You can install the required dependencies using:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up `.env` file**:
   - Copy the `.env.example` file to `.env`:
     ```bash
     cp .env.example .env
     ```
   - Update the `.env` file with your credentials, including AWS and YouTube API keys.

5. **Login to Telegram**:
   Before running any DAG, make sure to login to Telegram using the following command:
   ```bash
   python telegramlogin.py
   ```
   This will authenticate your Telegram account and establish a session. If prompted for a password, provide the 2-step verification code.

6. **Run Airflow locally with Astro CLI**:
   Start your local Airflow environment with the Astro CLI:
   ```bash
   astro dev start
   ```

7. **Open Airflow UI**:
   After starting Astro locally, you can open the Airflow UI at [http://localhost:8080](http://localhost:8080).

## Astro Configuration

### Astro Setup

1. **Astro Project Initialization**:
   If you don't have an Astro project yet, you can create one by running:
   ```bash
   astro dev init
   ```

2. **Running Airflow Locally**:
   - To run the Airflow scheduler and web server locally, use the following commands:
     ```bash
     astro dev start
     ```
   - This will start Airflow and allow you to interact with the DAGs in the Airflow UI at `http://localhost:8080`.

### Environment Variables

- The required environment variables are stored in the `.env` file, which is referenced during the development and deployment of your Airflow project. Make sure the `.env` file includes the following:
  
  #### AWS S3 Credentials:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_REGION`
  
  #### YouTube API Credentials:
  - `YOUTUBE_API_KEY`

## Task Descriptions

### Telegram Downloader Task
- **Telegram DAG (`telegram_dag.py`)**:
  - Downloads messages from a specified Telegram channel.
  - Saves them to a JSON file.
  - Uploads the file to AWS S3.

### YouTube Data Processing Task
- **YouTube DAG (`youtube_dag.py`)**:
  - Processes YouTube video data from a list (`channel_list.txt`).
  - Saves the processed data to a temporary JSON file.
  - Uploads the results to AWS S3.

Both tasks are scheduled to run **daily**.

## File Structure

```
social-media-automation/
│
├── dags/
│   ├── telegram_dag.py             # DAG for Telegram automation
│   ├── youtube_dag.py              # DAG for YouTube automation
│
├── social_tools/
│   ├── telegram/
│   │   └── telegram_downloader.py  # Telegram downloader logic
│   ├── youtube/
│   │   └── youtube_data_handler.py # YouTube video handler logic
│
├── .env.example                    # Example .env file with required environment variables
├── .env                            # Environment file for local development (copy from .env.example)
├── requirements.txt                # Project dependencies
└── README.md                       # This file
```

## Dependencies

- **Airflow**: Used for task orchestration.
- **Astro (Astronomer)**: Provides a local development environment for Airflow.
- **social_tools**: Custom library for interacting with Telegram and YouTube.
- **AWS SDK (boto3)**: Used for interacting with AWS services like S3.
- **pendulum**: A datetime library used by Airflow for scheduling tasks.

To install the dependencies, run the following command:
```bash
pip install -r requirements.txt
```

## Environment Variables

This project requires the following environment variables for AWS and API credentials:

### AWS S3 Credentials
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`

These credentials can be set in your `.env` file or through the Airflow UI (under "Connections" for Airflow).

### YouTube API Credentials
- `YOUTUBE_API_KEY`:
  - Set this in your `.env` file after creating a project in [Google Cloud](https://console.cloud.google.com/), enabling the YouTube Data API v3, and obtaining the API key.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

