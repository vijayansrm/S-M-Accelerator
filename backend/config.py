"""
Configuration module for the CSV Processing & Merging API.
Contains all application-wide settings and constants.
"""
# pylint: disable=invalid-name
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Configuration
API_TITLE = "CSV Processing & Merging API"
HOST = "0.0.0.0"
PORT = 8000

# CSV Processing Configuration
ROW_COUNT_THRESHOLD = 100000
RESPONSE_DIR = "Response"
RESPONSE_FILENAME = "response.json"
RESPONSE_FILE_PATH = os.path.join(RESPONSE_DIR, RESPONSE_FILENAME)

# Logging Configuration
LOG_DIR = "Logs"
INVALID_RECORDS_FILENAME = "invalid_records.log"
INVALID_RECORDS_LOG_PATH = os.path.join(LOG_DIR, INVALID_RECORDS_FILENAME)
VALID_RECORDS_FILENAME = "valid_records.log"
VALID_RECORDS_LOG_PATH = os.path.join(LOG_DIR, VALID_RECORDS_FILENAME)

# CORS Configuration
CORS_ALLOW_ORIGINS = ["*"]
CORS_ALLOW_CREDENTIALS = True
CORS_ALLOW_METHODS = ["*"]
CORS_ALLOW_HEADERS = ["*"]

# MySQL Database Configuration
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_POOL_NAME = "csv_api_pool"
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "32"))
