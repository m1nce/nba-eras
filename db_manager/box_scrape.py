import os
import time
from dotenv import load_dotenv
import requests
import psycopg2
from tqdm import tqdm
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, RetryError
import pandas as pd
from sqlalchemy import create_engine
from queue import Queue, Empty
from threading import Thread

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
engine = create_engine(f'postgresql+psycopg2://{os.getenv("DB_USER")}:{os.getenv("DB_PASS")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/nba_stats')

# Query the dates of the database
query = """
SELECT DISTINCT date
FROM game
ORDER BY date ASC;
"""

dates = pd.read_sql(query, engine)
flattened_dates = dates.to_numpy().flatten()

print(f"Processing {len(flattened_dates)} dates...")

box_score_insert_query = """
INSERT INTO box_score (
    player_id, date, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, 
    oreb, dreb, reb, ast, stl, blk, turnover, pf, pts
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# List to store dates that encounter errors
error_dates = []

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(requests.exceptions.RequestException))
def make_request(params):
    response = requests.get(API_ENDPOINT, headers=headers, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

def process_date(date):
    params = {
        "date": date,
    }
    try:
        data = make_request(params)
        records = []
        for game in data['data']:
            for team in ['home_team', 'visitor_team']:
                for player in game[team]['players']:
                    min_played = player['min']
                    if min_played is None:
                        min_played = 0
                    else:
                        min_parts = min_played.split(":")
                        min_played = int(min_parts[0]) + int(min_parts[1]) / 60 if len(min_parts) == 2 else 0

                    record = (
                        player['player']['id'], game['date'], min_played, 
                        player['fgm'], player['fga'], player['fg_pct'], player['fg3m'], 
                        player['fg3a'], player['fg3_pct'], player['ftm'], player['fta'], 
                        player['ft_pct'], player['oreb'], player['dreb'], player['reb'], 
                        player['ast'], player['stl'], player['blk'], player['turnover'], 
                        player['pf'], player['pts']
                    )
                    records.append(record)
        return records
    except Exception as e:
        print(f"Error processing date {date}: {e}")
        error_dates.append(date)  # Add date to the error list
        return []

def batch_insert(records):
    try:
        cursor.executemany(box_score_insert_query, records)
        conn.commit()
    except Exception as e:
        print(f"Error during batch insert: {e}")
        conn.rollback()

def worker(queue, progress_bar):
    while True:
        try:
            date = queue.get_nowait()
            records = process_date(date)
            if records:
                batch_insert(records)
            queue.task_done()
            progress_bar.update(1)
            # Add a delay to respect the rate limit
            time.sleep(1 / ((300 / 60) / num_workers)) # Makes at most 300 requests per minute.
        except Empty:
            break
        except Exception as e:
            print(f"Error in worker: {e}")

# Function to reprocess error dates
def reprocess_error_dates():
    queue = Queue()
    for date in error_dates:
        queue.put(date)
    
    threads = []
    with tqdm(total=len(error_dates), desc="Reprocessing errors") as pbar:
        for _ in range(num_workers):  # Number of worker threads
            t = Thread(target=worker, args=(queue, pbar))
            t.start()
            threads.append(t)
        
        queue.join()

        for t in threads:
            t.join()

    conn.commit()

# Create a queue and add dates
queue = Queue()
for date in flattened_dates:
    queue.put(date)

# Create and start threads
num_workers = 4
threads = []
with tqdm(total=len(flattened_dates)) as pbar:
    for _ in range(num_workers):  # Number of worker threads
        t = Thread(target=worker, args=(queue, pbar))
        t.start()
        threads.append(t)

    # Wait for all tasks in the queue to be processed
    queue.join()

    # Wait for all threads to finish
    for t in threads:
        t.join()

# Reprocess dates that encountered errors
if error_dates:
    print(f"Reprocessing {len(error_dates)} error dates...")
    reprocess_error_dates()

# Close the connection
cursor.close()
conn.close()

print("------------------------------------")
print("Done!")
