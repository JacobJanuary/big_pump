import sys
from pathlib import Path
import pandas as pd
from pump_analysis_lib import get_db_connection

def main():
    conn = get_db_connection()
    try:
        # Check tables in bybit_trade schema
        print("Checking tables in bybit_trade schema...")
        tables = pd.read_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bybit_trade'",
            conn
        )
        print(tables)
        
        # Check if 'trades' or similar table exists
        if 'candles_1s' in tables['table_name'].values:
             print("\nChecking candles_1s range:")
             stats = pd.read_sql(
                 """SELECT listing_id, count(*) as count, 
                           min(timestamp_s) as start_ts, 
                           max(timestamp_s) as end_ts,
                           (max(timestamp_s) - min(timestamp_s))/3600.0 as hours
                    FROM bybit_trade.candles_1s 
                    GROUP BY listing_id LIMIT 5""",
                 conn
             )
             print(stats)
             
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
