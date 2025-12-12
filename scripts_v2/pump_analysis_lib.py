"""
Common library for pump analysis scripts (Multi-Exchange Support)
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
BYBIT_BASE_URL = "https://api.bybit.com"
REQUEST_DELAY = 0.15  # Rate limiting

# --- Signal Filter Configuration ---
# Minimum total score for high-quality signals
SCORE_THRESHOLD = 118

# Target patterns to filter for
TARGET_PATTERNS = ['SQUEEZE_IGNITION', 'OI_EXPLOSION']

# Exchange Configuration
# Options: 'ALL', 'BINANCE', 'BYBIT'
EXCHANGE_FILTER = 'BINANCE' 

EXCHANGE_IDS = {
    'BINANCE': 1,
    'BYBIT': 2
}

# --- Indicator Filter Configuration ---
# Set to 0 to disable a specific filter
INDICATOR_FILTERS = {
    'rsi_threshold': 1,            # DISABLED
    'volume_zscore_threshold': 12,  # Volume Z-Score > 12
    'oi_delta_threshold': 40,       # OI Delta % > 40
}


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
    import time
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'startTime': timestamp_ms,
            'limit': 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        # Rate limiting: Binance allows ~1200 requests/min, play it safe with ~4 req/sec
        time.sleep(0.25)  # 250ms delay between requests
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                # Return open price of that minute
                open_price = float(data[0][1])
                return open_price
        elif response.status_code == 429:
            print(f"  Binance API error for {symbol}: 429 (Rate limit exceeded)")
            print(f"  Waiting 60 seconds before retry...")
            time.sleep(60)  # Wait 1 minute if we hit rate limit
            return None
        return None
            
    except Exception as e:
        print(f"  Error getting Binance price for {symbol}: {e}")
        return None

def get_bybit_price_at_time(symbol, timestamp_ms):
    """
    Get price from Bybit at specific timestamp
    Returns: float or None
    """
    import time
    try:
        url = f"{BYBIT_BASE_URL}/v5/market/kline"
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': '1',  # 1 minute
            'start': timestamp_ms,
            'limit': 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        # Rate limiting: Similar to Binance, add delay
        time.sleep(0.25)  # 250ms delay between requests
        
        if response.status_code == 200:
            data = response.json()
            if data.get('retCode') == 0:
                list_data = data.get('result', {}).get('list', [])
                if list_data and len(list_data) > 0:
                    # Bybit returns [timestamp, open, high, low, close, volume, turnover]
                    open_price = float(list_data[0][1])
                    return open_price
        elif response.status_code == 429:
            print(f"  Bybit API error for {symbol}: 429 (Rate limit exceeded)")
            print(f"  Waiting 60 seconds before retry...")
            time.sleep(60)
            return None
        return None
            
    except Exception as e:
        print(f"  Error getting Bybit price for {symbol}: {e}")
        return None

def fetch_signals(conn, days=30, limit=None, min_score=None, max_score=None):
    """
    Fetch signals from database with exchange filtering and configurable indicator filters
    Returns: list of signal dicts
    
    Filters:
    - total_score between min_score and max_score (default min=SCORE_THRESHOLD, max=None)
    - Patterns: SQUEEZE_IGNITION or OI_EXPLOSION
    - Timeframe: 15m, 1h, or 4h
    - Exchange: Binance (PERPETUAL/Futures only)
    - Indicators: Configurable via INDICATOR_FILTERS (set to 0 to disable)
    """
    placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Defaults
    if min_score is None:
        min_score = SCORE_THRESHOLD
    
    score_condition = f"AND sh.total_score >= {min_score}"
    if max_score is not None:
        score_condition += f" AND sh.total_score <= {max_score}"

    
    # Exchange filter logic
    exchange_condition = ""
    if EXCHANGE_FILTER == 'BINANCE':
        exchange_condition = f"AND tp.exchange_id = {EXCHANGE_IDS['BINANCE']}"
    elif EXCHANGE_FILTER == 'BYBIT':
        exchange_condition = f"AND tp.exchange_id = {EXCHANGE_IDS['BYBIT']}"
    # If 'ALL', no extra condition needed
    
    # Build dynamic indicator filters (skip if threshold is 0)
    indicator_conditions = []
    if INDICATOR_FILTERS['rsi_threshold'] > 0:
        indicator_conditions.append(f"AND i.rsi > {INDICATOR_FILTERS['rsi_threshold']}")
    if INDICATOR_FILTERS['volume_zscore_threshold'] > 0:
        indicator_conditions.append(f"AND i.volume_zscore > {INDICATOR_FILTERS['volume_zscore_threshold']}")
    if INDICATOR_FILTERS['oi_delta_threshold'] > 0:
        indicator_conditions.append(f"AND i.oi_delta_pct > {INDICATOR_FILTERS['oi_delta_threshold']}")
    
    indicator_filter_clause = "\n          ".join(indicator_conditions)
    
    # Updated query with dynamic indicator filters
    # Note: DISTINCT ON (sh.id) to avoid duplicates when signal has patterns on multiple timeframes
    query_signals = f"""
        SELECT DISTINCT ON (sh.id)
            sh.id,
            sh.trading_pair_id, 
            sh.pair_symbol, 
            sh.timestamp, 
            sh.total_score,
            tp.exchange_id,
            sp.pattern_type,
            sp.timeframe,
            i.rsi,
            i.volume_zscore,
            i.oi_delta_pct
        FROM fas_v2.scoring_history sh
        JOIN fas_v2.sh_patterns shp ON shp.scoring_history_id = sh.id
        JOIN fas_v2.signal_patterns sp ON sp.id = shp.signal_patterns_id
        JOIN fas_v2.sh_indicators shi ON shi.scoring_history_id = sh.id
        JOIN fas_v2.indicators i ON (
            i.trading_pair_id = shi.indicators_trading_pair_id 
            AND i.timestamp = shi.indicators_timestamp 
            AND i.timeframe = shi.indicators_timeframe
        )
        JOIN public.trading_pairs tp ON tp.id = sh.trading_pair_id
        WHERE 1=1
          {score_condition}
          AND sh.timestamp >= NOW() - INTERVAL '{days} days'
          AND sp.pattern_type IN ({placeholders})
          AND sp.timeframe IN ('15m', '1h', '4h')
          AND tp.contract_type_id = 1  -- PERPETUAL (Futures)
          AND tp.is_active = TRUE
          AND shi.indicators_timeframe = sp.timeframe  -- Match indicator timeframe to pattern timeframe
          {indicator_filter_clause}
          {exchange_condition}
        ORDER BY sh.id, sh.timestamp ASC
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

def deduplicate_signals(signals, cooldown_hours=24, initial_state=None):
    """
    Remove duplicate signals within cooldown period
    
    Args:
        signals: List of signal dicts
        cooldown_hours: Cooldown in hours (default 24, use 12 for populate_signal_analysis)
        initial_state: Dict of {symbol: last_timestamp} to seed deduplication
    Returns: list of unique signals
    """
    last_signal_time = initial_state.copy() if initial_state else {}
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
    Get entry price from appropriate API and fetch analysis candles
    
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
    exchange_id = signal.get('exchange_id', EXCHANGE_IDS['BINANCE']) # Default to Binance if missing
    
    # Entry time
    entry_time_dt = signal_ts + timedelta(minutes=entry_offset_minutes)
    entry_time_ms = int(entry_time_dt.timestamp() * 1000)
    
    # Get entry price from appropriate API
    entry_price = None
    
    if exchange_id == EXCHANGE_IDS['BINANCE']:
        entry_price = get_binance_price_at_time(symbol, entry_time_ms)
    elif exchange_id == EXCHANGE_IDS['BYBIT']:
        entry_price = get_bybit_price_at_time(symbol, entry_time_ms)
    else:
        print(f"  Unknown exchange ID: {exchange_id} for {symbol}")
        return None, None, None
        
    time.sleep(REQUEST_DELAY)
    
    if entry_price is None:
        return None, None, None
    
    # Fetch 5-minute candles for analysis (from DB)
    # Note: This assumes DB has candles for both exchanges in public.candles
    end_time_dt = entry_time_dt + timedelta(hours=analysis_hours)
    end_time_ms = int(end_time_dt.timestamp() * 1000)
    
    candles = fetch_candles_5m(conn, pair_id, entry_time_ms, end_time_ms)
    
    if not candles:
        return None, None, None
    
    return entry_price, candles, entry_time_dt
