import os
import time
from dotenv import load_dotenv
import requests
import psycopg2
from tqdm import trange
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log
import numpy as np
from sqlalchemy import create_engine
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Take environment variables from .env.
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/box_scores"

# Set up the headers with the API key.
headers = {
    'Authorization': API_KEY
}

# Database connection details
conn_str = (f"dbname=nba_stats user={os.getenv('DB_USER')} " +
            f"password={os.getenv('DB_PASS')} host={os.getenv('DB_HOST')} " +
            f"port={os.getenv('DB_PORT')}")

# Connect to PostgreSQL server
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

# Create an engine instance
engine = create_engine(f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/nba_stats")

# Query the dates of database
query = """
SELECT DISTINCT date
FROM game
ORDER BY date ASC;
"""

dates = pd.read_sql(query, engine)
flattened_dates = dates.to_numpy().flatten()

box_score_insert_query = """
INSERT INTO box_score (
    player_id, date, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, 
    oreb, dreb, reb, ast, stl, blk, turnover, pf, pts
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Retry configuration with jitter and handling specific errors
@retry(wait=wait_exponential(multiplier=1, min=4, max=10) + wait_exponential(multiplier=0.5, max=1), stop=stop_after_attempt(5), retry=retry_if_exception_type(requests.exceptions.RequestException), before_sleep=before_sleep_log(logger, logging.INFO))
def make_request(params):
    try:
        response = requests.get(API_ENDPOINT, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred: {conn_err}")
        logger.error(f"Response content: {conn_err}")
        time.sleep(10)  # Add a more significant delay
        raise

# Get the box scores for each date
for i in trange(flattened_dates.size):
    params = {
        "date": flattened_dates[i],
    }
    try:
        response = make_request(params)
        data = response.json()
        for player in data['home_team']['players']:
            cursor.execute(box_score_insert_query, (
                player['player']['id'], data['date'], int(player['min']), player['fgm'], 
                player['fga'], player['fg_pct'], player['fg_pct'], player['fg3m'], 
                player['fg3a'], player['fg3_pct'], player['ftm'], player['fta'], 
                player['ft_pct'], player['oreb'], player['dreb'], player['reb'], 
                player['ast'], player['stl'], player['blk'], player['turnover'], player['pf'], 
                player['pts']
            ))
            conn.commit()
        for player in data['away_team']['players']:
            cursor.execute(box_score_insert_query, (
                player['player']['id'], data['date'], int(player['min']), player['fgm'], 
                player['fga'], player['fg_pct'], player['fg_pct'], player['fg3m'], 
                player['fg3a'], player['fg3_pct'], player['ftm'], player['fta'], 
                player['ft_pct'], player['oreb'], player['dreb'], player['reb'], 
                player['ast'], player['stl'], player['blk'], player['turnover'], player['pf'], 
                player['pts']
            ))
            conn.commit()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")  # HTTP error
        logger.error(f"Response content: {http_err.response.content}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error occurred: {req_err}")  # Network errors
    except Exception as err:
        logger.error(f"Other error occurred: {err}")  # Other errors

# Close the connection
cursor.close()
conn.close()

print("------------------------------------")
print("Done!")
