import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
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
    if DB_CONFIG.get('password'):
        conn_params.append(f"password={DB_CONFIG['password']}")
    conn_str = " ".join(conn_params)
    return psycopg.connect(conn_str)

def analyze_pumps():
    print(f"Analyzing pumps for the last {LOOKBACK_DAYS} days...")
    
    try:
        with get_db_connection() as conn:
            # 1. Fetch Signals
            placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
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
                WHERE sh.total_score > {SCORE_THRESHOLD}
                  AND sh.timestamp >= NOW() - INTERVAL '{LOOKBACK_DAYS} days'
                  AND sp.pattern_type IN ({placeholders})
                ORDER BY sh.timestamp DESC
            """
            
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query_signals)
                signals = cur.fetchall()
                
            if not signals:
                print("No signals found.")
                return

            print(f"Found {len(signals)} signals. Fetching price data...")
            
            results = []
            
            for signal in signals:
                pair_id = signal['trading_pair_id']
                signal_ts = signal['timestamp']
                symbol = signal['pair_symbol']
                
                # Entry is 15 minutes after signal
                entry_time = signal_ts + timedelta(minutes=15)
                end_time = entry_time + timedelta(hours=ANALYSIS_WINDOW_HOURS)
                
                # Fetch candles
                query_candles = """
                    SELECT timestamp, open, high, low, close
                    FROM market_data.candles
                    WHERE trading_pair_id = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    ORDER BY timestamp ASC
                """
                
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query_candles, (pair_id, entry_time, end_time))
                    candles = cur.fetchall()
                    
                if not candles:
                    # print(f"No price data for {symbol} at {entry_time}")
                    continue
                    
                # Analysis
                entry_price = float(candles[0]['open'])
                
                max_price = -1.0
                max_price_time = None
                min_price_before_peak = entry_price
                
                for candle in candles:
                    high = float(candle['high'])
                    low = float(candle['low'])
                    ts = candle['timestamp']
                    
                    if high > max_price:
                        max_price = high
                        max_price_time = ts
                        # Reset min price tracking when we find a new high? 
                        # No, we want max drawdown *before* the peak.
                        # Actually, "max drawdown before pump" usually means the lowest point between entry and the peak.
                        # So we track min_price only up to max_price_time.
                
                # Re-scan to find lowest low before max_price_time
                lowest_low = entry_price
                for candle in candles:
                    if candle['timestamp'] > max_price_time:
                        break
                    low = float(candle['low'])
                    if low < lowest_low:
                        lowest_low = low
                        
                max_growth_pct = ((max_price - entry_price) / entry_price) * 100
                max_drawdown_pct = ((lowest_low - entry_price) / entry_price) * 100
                time_to_growth = max_price_time - entry_time
                
                results.append({
                    'date': signal_ts.strftime('%Y-%m-%d'),
                    'time': signal_ts.strftime('%H:%M'),
                    'symbol': symbol,
                    'score': signal['total_score'],
                    'drawdown': max_drawdown_pct,
                    'growth': max_growth_pct,
                    'time_to_peak': str(time_to_growth)
                })
            
            # Print Table
            # Header
            print(f"{'Date':<12} {'Time':<8} {'Symbol':<12} {'Score':<6} {'Drawdown %':<12} {'Growth %':<10} {'Time to Peak':<15}")
            print("-" * 80)
            
            for r in results:
                print(f"{r['date']:<12} {r['time']:<8} {r['symbol']:<12} {r['score']:<6.0f} {r['drawdown']:<12.2f} {r['growth']:<10.2f} {r['time_to_peak']:<15}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_pumps()
