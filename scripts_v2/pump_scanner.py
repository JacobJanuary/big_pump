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
# Import from local scripts
from populate_signal_analysis import populate_signal_analysis
from pump_analysis_lib import (
    EXCHANGE_FILTER,
    get_db_connection,
    fetch_signals,
    SCORE_THRESHOLD,
    SCORE_THRESHOLD_MAX,
    COOLDOWN_HOURS,
    DEFAULT_SCAN_WINDOW_HOURS
)

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
    1. Loads state of already-sent signals
    2. Fetches current signals from DB  
    3. Sends Telegram alerts for new signals not yet notified
    4. Updates state file
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting scan...")
    print(f"Exchange Filter: {EXCHANGE_FILTER}")
    
    # State file to track sent signals (per symbol, with timestamp)
    state_file = current_dir / 'scanner_state.json'
    sent_signals = {}
    
    # Load existing state
    if state_file.exists():
        try:
            with open(state_file, 'r') as f:
                data = json.load(f)
                # Convert string timestamps back to datetime
                for symbol, ts_str in data.items():
                    sent_signals[symbol] = datetime.fromisoformat(ts_str)
            print(f"Loaded state: {len(sent_signals)} symbols previously notified")
        except Exception as e:
            print(f"Warning: Failed to load state file: {e}")
    
    # Run population to ensure DB is up-to-date
    # This also applies deduplication, but we handle Telegram separately
    populate_signal_analysis(
        days=0.042,  # ~1 hour
        limit=None, 
        force_refresh=False,
        cooldown_hours=COOLDOWN_HOURS
    )
    
    # Now fetch ALL current signals from DB (last 12 hours to match cooldown)
    # We used imported functions instead of local import inside function
    
    conn = get_db_connection()
    try:
        # Get signals from last X hours (matching cooldown)
        # Using 0.5 days is 12 hours, which matches DEFAULT_SCAN_WINDOW_HOURS if it is 12
        scan_days = DEFAULT_SCAN_WINDOW_HOURS / 24.0
        
        all_signals = fetch_signals(conn, days=scan_days, limit=None, min_score=SCORE_THRESHOLD, max_score=SCORE_THRESHOLD_MAX)
        
        if not all_signals:
            print("No signals found in database.")
            return
        
        print(f"Found {len(all_signals)} total signals in DB (last {DEFAULT_SCAN_WINDOW_HOURS}h)")
        
        # Filter for signals we haven't sent yet
        # Use cooldown from config
        new_to_send = []
        
        for signal in all_signals:
            symbol = signal['pair_symbol']
            signal_ts = signal['timestamp']
            
            # Check if we already sent notification for this symbol recently
            if symbol in sent_signals:
                last_sent = sent_signals[symbol]
                time_diff_hours = (signal_ts - last_sent).total_seconds() / 3600
                
                if time_diff_hours < COOLDOWN_HOURS:
                    # Too soon, skip
                    continue
            
            # This is a new signal to send!
            new_to_send.append(signal)
        
        if not new_to_send:
            print("No new signals to send (all already notified).")
            return
        
        print(f"Found {len(new_to_send)} NEW signals to send!")
        
        # Send alerts
        for signal in new_to_send:
            symbol = signal['pair_symbol']
            total_score = signal['total_score']
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
            
            # Update sent state
            sent_signals[symbol] = ts
        
        # Save updated state
        try:
            state_to_save = {symbol: ts.isoformat() for symbol, ts in sent_signals.items()}
            with open(state_file, 'w') as f:
                json.dump(state_to_save, f, indent=2)
            print(f"State saved: {len(sent_signals)} symbols tracked")
        except Exception as e:
            print(f"Warning: Failed to save state: {e}")
            
    finally:
        conn.close()


if __name__ == "__main__":
    scan_and_alert()
