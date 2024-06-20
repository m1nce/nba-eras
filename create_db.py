import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# PostgreSQL connection details
admin_conn_str = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"

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

admin_cursor.close()
admin_conn.close()

# Connection details for the new database
conn_str = f"dbname={database_name} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"

# Connect to new database
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

# SQL commands to create tables