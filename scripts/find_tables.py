
import sys
from pathlib import Path
import psycopg

# Add config directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import settings

DB_CONFIG = settings.DATABASE
print("--- Debug: DB Config ---")
for k, v in DB_CONFIG.items():
    masked_v = '***' if k == 'password' and v else v
    print(f"{k}: {masked_v}")
print("------------------------")

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

def find_tables():
    print("Searching for tables...")
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
                print("\n--- Available Tables ---")
                for schema, table in tables:
                    print(f"{schema}.{table}")
                print("------------------------\n")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    find_tables()
