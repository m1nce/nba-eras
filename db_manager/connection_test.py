import requests
from requests.exceptions import ConnectionError
import os
from dotenv import load_dotenv

# Take environment variables from .env.
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/stats/advanced"

# Set up the headers with the API key.
headers = {
    'Authorization': API_KEY
}

params = {
    "seasons[]": 2014,
    "per_page": 100,
}

try:
    response = requests.get(API_ENDPOINT, headers=headers, params=params)
    response.raise_for_status()  # Raises an HTTPError if the response was an error
    data = response.json()
    print(data)
except Exception as e:
    print(e)