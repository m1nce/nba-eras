import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# Load environment variables from .env
load_dotenv()

# PostgreSQL connection details
admin_conn_str = f"dbname=postgres user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"

# Connect to PostgreSQL as admin
admin_conn = psycopg2.connect(admin_conn_str)
admin_conn.autocommit = True
admin_cursor = admin_conn.cursor()

# Create database if it doesn't exist
database_name = "nba_stats"
admin_cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [database_name])
exists = admin_cursor.fetchone()
if not exists:
    admin_cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
    print(f"Database '{database_name}' created successfully.")
else:
    print(f"Database '{database_name}' already exists.")

admin_cursor.close()
admin_conn.close()

# Connection details for the new database
conn_str = f"dbname={database_name} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"

# Connect to new database
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

# SQL commands to create tables
create_team_table = """
CREATE TABLE IF NOT EXISTS team (
    team_id INT PRIMARY KEY,
    conference VARCHAR(10),
    division VARCHAR(40),
    city VARCHAR(50),
    name VARCHAR(50),
    full_name VARCHAR(100),
    abbreviation VARCHAR(10)
);
"""

create_player_table = """
CREATE TABLE IF NOT EXISTS player (
    player_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    position VARCHAR(10),
    height VARCHAR(6),
    weight INT,
    jersey_number INT,
    college VARCHAR(50),
    country VARCHAR(50),
    draft_year INT,
    draft_round INT,
    draft_number INT,
    team_id INT,
    FOREIGN KEY (team_id) REFERENCES team (team_id)
);
"""

create_game_table = """
CREATE TABLE IF NOT EXISTS game (
    game_id INT PRIMARY KEY,
    date DATE,
    season INT,
    postseason BOOL,
    home_team_score INT,
    away_team_score INT,
    home_team_id INT,
    away_team_id INT,
    FOREIGN KEY (home_team_id) REFERENCES team (team_id),
    FOREIGN KEY (away_team_id) REFERENCES team (team_id)
);
"""

create_player_game_table = """
CREATE TABLE IF NOT EXISTS player_game (
    player_id int,
    game_id int,
    pie FLOAT,
    pace FLOAT,
    assist_percentage FLOAT,
    assist_ratio FLOAT,
    assist_to_turnover FLOAT,
    defensive_rating FLOAT,
    defensive_rebound_percentage FLOAT,
    effective_field_goal_percentage FLOAT,
    net_rating FLOAT,
    offensive_rating FLOAT,
    offensive_rebound_percentage FLOAT,
    true_shooting_percentage FLOAT,
    turnover_ratio FLOAT,
    usage_percentage FLOAT,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (player_id) REFERENCES player (player_id),
    FOREIGN KEY (game_id) REFERENCES game (game_id)
);
"""

# Function to check if table exists
def check_table_exists(table_name):
    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
    return cursor.fetchone()[0]

# Create tables and print messages
if not check_table_exists('team'):
    cursor.execute(create_team_table)
    print("Table 'player' created successfully.")
else:
    print("Table 'player' already exists.")

if not check_table_exists('player'):
    cursor.execute(create_player_table)
    print("Table 'team' created successfully.")
else:
    print("Table 'team' already exists.")

if not check_table_exists('game'):
    cursor.execute(create_game_table)
    print("Table 'game' created successfully.")
else:
    print("Table 'game' already exists.")

if not check_table_exists('player_game'):
    cursor.execute(create_player_game_table)
    print("Table 'player_game' created successfully.")
else:
    print("Table 'player_game' already exists.")

# Commit the transaction
conn.commit()

# CLose the connection
cursor.close()
conn.close()

print("--------------------------------------------")
print("Database and tables created successfully.")