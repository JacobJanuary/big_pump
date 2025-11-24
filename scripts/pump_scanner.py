import sys
import os
from pathlib import Path

# Add config directory to path to import settings directly (avoiding config.py conflict)
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import psycopg
from psycopg.rows import dict_row
import urllib.request
import json
from datetime import datetime, timedelta
import settings

# --- Configuration from settings ---
DB_CONFIG = settings.DATABASE
TELEGRAM_CONFIG = settings.TELEGRAM

# Scan settings
SCORE_THRESHOLD = 250
TARGET_PATTERNS = ['SQUEEZE_IGNITION', 'OI_EXPLOSION']
LOOKBACK_MINUTES = 30 # Keep 20m to cover cron gaps

def get_db_connection():
    # Construct params list to handle optional password (for .pgpass support)
    conn_params = [
        f"host={DB_CONFIG['host']}",
        f"port={DB_CONFIG['port']}",
        f"dbname={DB_CONFIG['dbname']}",
        f"user={DB_CONFIG['user']}",
        "sslmode=disable"
    ]
    
    # Only add password if it exists and is not empty
    if DB_CONFIG.get('password'):
        conn_params.append(f"password={DB_CONFIG['password']}")
        
    conn_str = " ".join(conn_params)
    return psycopg.connect(conn_str)

def send_telegram_alert(message):
    token = TELEGRAM_CONFIG['bot_token']
    # Use 'all' channel by default as per request
    chat_id = TELEGRAM_CONFIG['channels']['all']
    
    if not token or not chat_id:
        print("Telegram token or chat_id missing in settings.")
        return

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        data_json = json.dumps(data).encode('utf-8')
        req = urllib.request.Request(url, data=data_json, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req) as response:
            print(f"Telegram alert sent. Status: {response.getcode()}")
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

def scan_for_signals():
    print(f"Scanning for signals in the last {LOOKBACK_MINUTES} minutes...")
    
    try:
        with get_db_connection() as conn:
            # 1. High Scores
            query_scores = f"""
                SELECT trading_pair_id, pair_symbol, timestamp, total_score
                FROM fas_v2.scoring_history
                WHERE total_score > {SCORE_THRESHOLD}
                  AND timestamp >= NOW() - INTERVAL '{LOOKBACK_MINUTES} minutes'
            """
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query_scores)
                high_scores = cur.fetchall()
                
            if not high_scores:
                print("No high score signals found.")
                return

            print(f"Found {len(high_scores)} high score candidates.")
            
            # 2. Check for Patterns
            for score in high_scores:
                pair_id = score['trading_pair_id']
                ts = score['timestamp']
                symbol = score['pair_symbol']
                total_score = score['total_score']
                
                placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
                query_pattern = f"""
                    SELECT pattern_type, details
                    FROM fas_v2.signal_patterns
                    WHERE trading_pair_id = %s
                      AND pattern_type IN ({placeholders})
                      AND timestamp >= %s - INTERVAL '1 hour'
                      AND timestamp <= %s + INTERVAL '1 hour'
                    LIMIT 1
                """
                
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query_pattern, (pair_id, ts, ts))
                    pattern = cur.fetchone()
                    
                if pattern:
                    pattern_type = pattern['pattern_type']
                    print(f"MATCH FOUND: {symbol} | Score: {total_score} | Pattern: {pattern_type}")
                    
                    # Construct Message
                    msg = (
                        f"🐋 *WHALE ALERT* 🐋\n\n"
                        f"🚀 *{symbol}* is primed for a pump!\n\n"
                        f"📊 *Score*: `{total_score:.0f}`\n"
                        f"🔥 *Pattern*: `{pattern_type}`\n"
                        f"⏰ *Time*: `{ts.strftime('%H:%M UTC')}`\n\n"
                        f"⚠️ _High volatility expected. Stop-loss recommended at -8%._"
                    )
                    
                    send_telegram_alert(msg)
                else:
                    print(f"Candidate {symbol} has score {total_score} but no matching pattern.")

    except Exception as e:
        print(f"Error during scan: {e}")

if __name__ == "__main__":
    scan_for_signals()
