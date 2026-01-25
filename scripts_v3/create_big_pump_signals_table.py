
import psycopg
import sys
from pathlib import Path

# Add config directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import settings

def create_table():
    db_config = settings.DATABASE
    conn_params = [
        f"host={db_config['host']}",
        f"port={db_config['port']}",
        f"dbname={db_config['dbname']}",
        f"user={db_config['user']}",
        "sslmode=disable"
    ]
    if db_config.get('password'):
        conn_params.append(f"password={db_config['password']}")
    conn_str = " ".join(conn_params)

    try:
        print(f"Connecting to database {db_config['dbname']}...")
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                print("Creating table web.big_pump_signals...")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS web.big_pump_signals (
                        id SERIAL PRIMARY KEY,
                        signal_timestamp timestamp with time zone NOT NULL,
                        pair_symbol character varying(20) NOT NULL,
                        trading_pair_id integer NOT NULL,
                        total_score integer NOT NULL,
                        rsi_threshold integer NOT NULL,
                        volume_zscore integer NOT NULL,
                        oi_delta integer NOT NULL,
                        entry_time timestamp with time zone NOT NULL,
                        entry_price numeric(20,8) NOT NULL,
                        time_to_sl_10_pr_seconds integer NOT NULL,
                        max_price numeric(20,8) NOT NULL,
                        max_grow_pr integer NOT NULL,
                        time_to_max_price_seconds integer NOT NULL,
                        created_at timestamp with time zone DEFAULT now(),
                        analysis_window_hours integer DEFAULT 24
                    );
                """)
                
                # Grant permissions (just in case the user running this has grant option, 
                # or so elcrypto/elcrypto_readonly can access it if created by another user)
                # Note: If created by elcrypto, it owns it.
                # But explicit GRANTS are good practice if we were superuser.
                # Assuming current user is elcrypto, it will own the table.
                
                pass
                
            conn.commit()
            print("Table web.big_pump_signals created successfully.")
            
    except Exception as e:
        print(f"Error creating table: {e}")
        # Print SQL for manual execution
        print("\nIf you lack permissions, please execute this SQL manually:")
        print("""
        CREATE TABLE IF NOT EXISTS web.big_pump_signals (
            id SERIAL PRIMARY KEY,
            signal_timestamp timestamp with time zone NOT NULL,
            pair_symbol character varying(20) NOT NULL,
            trading_pair_id integer NOT NULL,
            total_score integer NOT NULL,
            rsi_threshold integer NOT NULL,
            volume_zscore integer NOT NULL,
            oi_delta integer NOT NULL,
            entry_time timestamp with time zone NOT NULL,
            entry_price numeric(20,8) NOT NULL,
            time_to_sl_10_pr_seconds integer NOT NULL,
            max_price numeric(20,8) NOT NULL,
            max_grow_pr integer NOT NULL,
            time_to_max_price_seconds integer NOT NULL,
            created_at timestamp with time zone DEFAULT now(),
            analysis_window_hours integer DEFAULT 24
        );
        GRANT ALL PRIVILEGES ON TABLE web.big_pump_signals TO elcrypto;
        GRANT SELECT ON TABLE web.big_pump_signals TO elcrypto_readonly;
        GRANT USAGE, SELECT ON SEQUENCE web.big_pump_signals_id_seq TO elcrypto;
        """)

if __name__ == "__main__":
    create_table()
