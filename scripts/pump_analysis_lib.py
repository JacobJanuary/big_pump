"""
Common library for pump analysis scripts
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import psycopg
from psycopg.rows import dict_row
import requests
import time

# Add config directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import settings

# --- Configuration ---
DB_CONFIG = settings.DATABASE
BINANCE_BASE_URL = "https://fapi.binance.com"
REQUEST_DELAY = 0.15  # Rate limiting

SCORE_THRESHOLD = 250
TARGET_PATTERNS = ['SQUEEZE_IGNITION', 'OI_EXPLOSION']

def get_db_connection():
    """Get database connection"""
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

def get_binance_price_at_time(symbol, timestamp_ms):
    """
    Get price from Binance at specific timestamp
    Returns: float or None
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'startTime': timestamp_ms,
            'limit': 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                # Return open price of that minute
                open_price = float(data[0][1])
                return open_price
        return None
            
    except Exception as e:
        print(f"  Error getting Binance price for {symbol}: {e}")
        return None

def fetch_signals(conn, days=30, limit=None):
    """
    Fetch signals from database
    Returns: list of signal dicts
    """
    placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    query_signals = f"""
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
          AND sh.timestamp >= NOW() - INTERVAL '{days} days'
          AND sp.pattern_type IN ({placeholders})
          AND tp.contract_type_id = 1
          AND tp.exchange_id = 1
          AND tp.is_active = TRUE
        ORDER BY sh.timestamp ASC
        {limit_clause}
    """
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query_signals)
        signals = cur.fetchall()
    
    return signals

def fetch_candles_5m(conn, pair_id, start_time_ms, end_time_ms):
    """
    Fetch 5-minute candles from database
    Returns: list of candle dicts
    """
    query_candles = """
        SELECT open_time, open_price, high_price, low_price, close_price
        FROM public.candles
        WHERE trading_pair_id = %s
          AND interval_id = 1
          AND open_time >= %s
          AND open_time <= %s
        ORDER BY open_time ASC
    """
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query_candles, (pair_id, start_time_ms, end_time_ms))
        candles = cur.fetchall()
    
    return candles

def deduplicate_signals(signals, cooldown_hours=24):
    """
    Remove duplicate signals within cooldown period
    Returns: list of unique signals
    """
    last_signal_time = {}
    unique_signals = []
    
    for signal in signals:
        symbol = signal['pair_symbol']
        signal_ts = signal['timestamp']
        
        if symbol in last_signal_time:
            last_ts = last_signal_time[symbol]
            if (signal_ts - last_ts).total_seconds() < cooldown_hours * 3600:
                continue
        
        last_signal_time[symbol] = signal_ts
        unique_signals.append(signal)
    
    return unique_signals

def get_entry_price_and_candles(conn, signal, analysis_hours=24, entry_offset_minutes=17):
    """
    Get entry price from Binance API and fetch analysis candles
    
    Args:
        conn: Database connection
        signal: Signal dict
        analysis_hours: Hours of candles to fetch after entry
        entry_offset_minutes: Minutes after signal for entry
    
    Returns: (entry_price, candles, entry_time_dt) or (None, None, None)
    """
    pair_id = signal['trading_pair_id']
    signal_ts = signal['timestamp']
    symbol = signal['pair_symbol']
    
    # Entry time
    entry_time_dt = signal_ts + timedelta(minutes=entry_offset_minutes)
    entry_time_ms = int(entry_time_dt.timestamp() * 1000)
    
    # Get entry price from Binance API
    entry_price = get_binance_price_at_time(symbol, entry_time_ms)
    time.sleep(REQUEST_DELAY)
    
    if entry_price is None:
        return None, None, None
    
    # Fetch 5-minute candles for analysis
    end_time_dt = entry_time_dt + timedelta(hours=analysis_hours)
    end_time_ms = int(end_time_dt.timestamp() * 1000)
    
    candles = fetch_candles_5m(conn, pair_id, entry_time_ms, end_time_ms)
    
    if not candles:
        return None, None, None
    
    return entry_price, candles, entry_time_dt
