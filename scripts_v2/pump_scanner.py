import sys
import os
from pathlib import Path

# Add config directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import urllib.request
import json
from datetime import datetime
import settings

# Import from local scripts
from populate_signal_analysis import populate_signal_analysis
from pump_analysis_lib import EXCHANGE_FILTER

# --- Configuration ---
TELEGRAM_CONFIG = settings.TELEGRAM

def send_telegram_alert(message):
    token = TELEGRAM_CONFIG['bot_token']
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

def scan_and_alert():
    """
    Main scanner function:
    1. Calls populate_signal_analysis to fetch and store new signals (deduplicated)
    2. Alerts on any newly inserted signals
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting scan...")
    print(f"Exchange Filter: {EXCHANGE_FILTER}")
    
    # Run population script (look back 60 mins to be safe, cron runs every 1-5 mins)
    # It returns list of newly inserted signals
    new_signals = populate_signal_analysis(days=0.042, limit=None, force_refresh=False) # 0.042 days = ~1 hour
    
    if not new_signals:
        print("No new signals found.")
        return

    print(f"Found {len(new_signals)} NEW signals! Sending alerts...")
    
    for signal in new_signals:
        symbol = signal['pair_symbol']
        total_score = signal['total_score']
        # pattern_type might not be in the signal dict from populate, let's check
        # populate uses fetch_signals which returns pattern_type
        pattern_type = signal.get('pattern_type', 'Unknown')
        ts = signal['timestamp']
        
        print(f"ALERTING: {symbol} | Score: {total_score} | Pattern: {pattern_type}")
        
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

if __name__ == "__main__":
    scan_and_alert()
