
import sys
from pathlib import Path

# Add config directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import psycopg
import settings

DB_CONFIG = settings.DATABASE

def get_db_connection():
    conn_params = [
        f"host={DB_CONFIG['host']}",
        f"port={DB_CONFIG['port']}",
        f"dbname={DB_CONFIG['dbname']}",
        f"user={DB_CONFIG['user']}",
        "sslmode=disable"
    ]
    if DB_CONFIG.get('password'):
        conn_params.append(f"password={DB_CONFIG['password']}")
    conn_str = " ".join(conn_params)
    return psycopg.connect(conn_str)

def list_tables():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                    ORDER BY table_schema, table_name;
                """)
                tables = cur.fetchall()
                for schema, table in tables:
                    print(f"{schema}.{table}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_tables()
