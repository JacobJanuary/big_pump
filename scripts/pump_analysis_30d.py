import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import psycopg
from psycopg.rows import dict_row

# Add config directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import settings

# --- Configuration ---
DB_CONFIG = settings.DATABASE

SCORE_THRESHOLD = 250
TARGET_PATTERNS = ['SQUEEZE_IGNITION', 'OI_EXPLOSION']
LOOKBACK_DAYS = 30
ANALYSIS_WINDOW_HOURS = 24

def get_db_connection():
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

import argparse

def analyze_pumps(days=30, limit=None):
    print(f"Analyzing pumps for the last {days} days...")
    
    try:
        with get_db_connection() as conn:
            # 1. Fetch Signals
            placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            # Sort ASC to find the first signal of a pump
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
                  AND tp.contract_type_id = 1  -- Futures
                  AND tp.exchange_id = 1       -- Binance
                  AND tp.is_active = TRUE
                ORDER BY sh.timestamp ASC
                {limit_clause}
            """
            
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query_signals)
                signals = cur.fetchall()
                
            if not signals:
                print("No signals found.")
                return

            print(f"Found {len(signals)} signals. Fetching price data...")
            
            results = []
            last_signal_time = {} # symbol -> datetime
            COOLDOWN_HOURS = 24
            
            for i, signal in enumerate(signals, 1):
                if i % 10 == 0:
                    print(f"Processing {i}/{len(signals)}...", end='\r')
                    
                pair_id = signal['trading_pair_id']
                signal_ts = signal['timestamp'] # datetime with timezone
                symbol = signal['pair_symbol']
                
                # Deduplication Logic
                if symbol in last_signal_time:
                    last_ts = last_signal_time[symbol]
                    if (signal_ts - last_ts).total_seconds() < COOLDOWN_HOURS * 3600:
                        continue
                
                last_signal_time[symbol] = signal_ts
                
                # Entry is 15 minutes after signal
                entry_time_dt = signal_ts + timedelta(minutes=15)
                end_time_dt = entry_time_dt + timedelta(hours=ANALYSIS_WINDOW_HOURS)
                
                # Convert to milliseconds for candles table
                entry_time_ms = int(entry_time_dt.timestamp() * 1000)
                end_time_ms = int(end_time_dt.timestamp() * 1000)
                
                # Fetch candles (15m interval = 2)
                query_candles = """
                    SELECT open_time, open_price, high_price, low_price, close_price
                    FROM public.candles
                    WHERE trading_pair_id = %s
                      AND interval_id = 2
                      AND open_time >= %s
                      AND open_time <= %s
                    ORDER BY open_time ASC
                """
                
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query_candles, (pair_id, entry_time_ms, end_time_ms))
                    candles = cur.fetchall()
                    
                if not candles:
                    continue
                    
                # Analysis
                entry_price = float(candles[0]['open_price'])
                
                max_price = -1.0
                max_price_time_ms = None
                
                for candle in candles:
                    high = float(candle['high_price'])
                    ts = candle['open_time']
                    
                    if high > max_price:
                        max_price = high
                        max_price_time_ms = ts
                
                # Re-scan to find lowest low before max_price_time
                lowest_low = entry_price
                for candle in candles:
                    if candle['open_time'] > max_price_time_ms:
                        break
                    low = float(candle['low_price'])
                    if low < lowest_low:
                        lowest_low = low
                        
                max_growth_pct = ((max_price - entry_price) / entry_price) * 100
                max_drawdown_pct = ((lowest_low - entry_price) / entry_price) * 100
                
                # Time to peak
                peak_time_dt = datetime.fromtimestamp(max_price_time_ms / 1000, tz=timezone.utc)
                time_to_growth = peak_time_dt - entry_time_dt
                
                results.append({
                    'date': signal_ts.strftime('%Y-%m-%d'),
                    'time': signal_ts.strftime('%H:%M'),
                    'symbol': symbol,
                    'score': signal['total_score'],
                    'drawdown': max_drawdown_pct,
                    'growth': max_growth_pct,
                    'time_to_peak': str(time_to_growth)
                })
            
            print("\n" + "="*80)
            # Print Table
            # Header
            print(f"{'Date':<12} {'Time':<8} {'Symbol':<12} {'Score':<6} {'Drawdown %':<12} {'Growth %':<10} {'Time to Peak':<15}")
            print("-" * 80)
            
            for r in results:
                print(f"{r['date']:<12} {r['time']:<8} {r['symbol']:<12} {r['score']:<6.0f} {r['drawdown']:<12.2f} {r['growth']:<10.2f} {r['time_to_peak']:<15}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze pump signals.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of signals to process')
    args = parser.parse_args()
    
    analyze_pumps(days=args.days, limit=args.limit)
