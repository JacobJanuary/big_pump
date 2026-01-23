import psycopg
import os
from pathlib import Path
from dotenv import load_dotenv

DATABASE = {
    'dbname': 'postgres',
    'user': 'elcrypto',
    'password': 'LohNeMamont@!21',
    'host': 'localhost',
    'port': '5435'
}

def check():
    conn_str = " ".join([f"{k}={v}" for k,v in DATABASE.items()])
    try:
        conn = psycopg.connect(conn_str)
        print("Connected to 'postgres'. Listing databases:")
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        dbs = [row[0] for row in cur.fetchall()]
        print(dbs)
        
        # Check permissions/schemas
        cur.execute("SELECT nspname FROM pg_catalog.pg_namespace;")
        schemas = [row[0] for row in cur.fetchall()]
        print(f"Schemas in 'postgres': {schemas}")
        
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check()
