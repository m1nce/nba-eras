import os
from dotenv import load_dotenv
import requests
import json
import pyodbc

# Take environment variables from .env.
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/stats/advanced"

# Set up the headers with the API key.
headers = {
    'Authorization': API_KEY
}

# Make the GET reqeuest
response = requests.get(API_ENDPOINT, headers=headers)
response_json = response.json()
