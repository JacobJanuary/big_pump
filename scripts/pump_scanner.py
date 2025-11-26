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
            # Single robust query matching pump_analysis_lib.py logic
            placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
            
            query = f"""
                SELECT 
                    sh.trading_pair_id, 
                    sh.pair_symbol, 
                    sh.timestamp, 
                    sh.total_score,
                    sp.pattern_type
                FROM fas_v2.scoring_history sh
                JOIN fas_v2.signal_patterns sp ON sh.trading_pair_id = sp.trading_pair_id 
                    AND sp.timestamp BETWEEN sh.timestamp - INTERVAL '1 hour' AND sh.timestamp + INTERVAL '1 hour'
                JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
                WHERE sh.total_score > {SCORE_THRESHOLD}
                  AND sh.timestamp >= NOW() - INTERVAL '{LOOKBACK_MINUTES} minutes'
                  AND sp.pattern_type IN ({placeholders})
                  AND tp.contract_type_id = 1
                  AND tp.exchange_id = 1
                  AND tp.is_active = TRUE
                ORDER BY sh.timestamp DESC
            """
            
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                signals = cur.fetchall()
                
            if not signals:
                print("No signals found.")
                return

            print(f"Found {len(signals)} confirmed signals.")
            
            # Process signals (deduplicate if needed, but for scanner we might want to alert on all new ones)
            # For now, just alert on them. 
            # Note: In a real scanner, we'd check if we already alerted for this specific signal ID or timestamp to avoid spam.
            # But based on current logic, it just scans the last X minutes. 
            # To avoid spamming the same signal every minute, we should probably track alerted signals in memory or DB.
            # The original code didn't seem to have robust deduping other than the short lookback window? 
            # Actually, the original code ran every X minutes and looked back X minutes. 
            # If it runs every 1 minute and looks back 30, it WILL duplicate alerts.
            # I will keep the alerting logic simple for now as requested, just fixing the query.
            
            for signal in signals:
                symbol = signal['pair_symbol']
                total_score = signal['total_score']
                pattern_type = signal['pattern_type']
                ts = signal['timestamp']
                
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


    except Exception as e:
        print(f"Error during scan: {e}")

if __name__ == "__main__":
    scan_for_signals()
