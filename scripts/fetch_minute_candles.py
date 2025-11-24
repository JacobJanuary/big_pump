"""
Fetch and store 1-minute candles from Binance API for each signal
"""
import sys
import os
from pathlib import Path
import requests
import time

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, BINANCE_BASE_URL

# Binance limits:
# - Klines: Weight 1, max 1500 candles per request
# - Rate limit: 1200 requests/minute (use 400 for safety)
REQUEST_DELAY = 0.15  # 400 req/min

def fetch_minute_candles_from_binance(symbol, start_time_ms, end_time_ms):
    """
    Fetch 1-minute candles from Binance
    
    Returns: list of candles or None
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'startTime': start_time_ms,
            'endTime': end_time_ms,
            'limit': 1500  # Max allowed by Binance
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # Kline format: [open_time, open, high, low, close, volume, close_time, ...]
            candles = [{
                'open_time': int(k[0]),
                'open_price': float(k[1]),
                'high_price': float(k[2]),
                'low_price': float(k[3]),
                'close_price': float(k[4]),
                'volume': float(k[5])
            } for k in data]
            return candles
        else:
            print(f"  API error for {symbol}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  Error fetching {symbol}: {e}")
        return None

def populate_minute_candles():
    """
    Fetch minute candles for all signals in signal_analysis table
    """
    print("Fetching 1-minute candles from Binance for all signals...")
    print("="*120)
    
    try:
        with get_db_connection() as conn:
            # Get all signals
            query = """
                SELECT id, pair_symbol, entry_time
                FROM web.signal_analysis
                ORDER BY id
            """
            
            with conn.cursor() as cur:
                cur.execute(query)
                signals = cur.fetchall()
            
            if not signals:
                print("No signals found in web.signal_analysis table.")
                return
            
            print(f"Found {len(signals)} signals. Fetching minute candles...")
            
            for i, signal in enumerate(signals, 1):
                signal_id = signal[0]
                symbol = signal[1]
                entry_time = signal[2]
                
                print(f"[{i}/{len(signals)}] {symbol} (ID: {signal_id})...", end=' ')
                
                # Calculate time range (24 hours from entry)
                start_time_ms = int(entry_time.timestamp() * 1000)
                end_time_ms = start_time_ms + (24 * 3600 * 1000)
                
                # Check if candles already exist
                check_query = """
                    SELECT COUNT(*) FROM web.minute_candles
                    WHERE signal_analysis_id = %s
                """
                with conn.cursor() as cur:
                    cur.execute(check_query, (signal_id,))
                    count = cur.fetchone()[0]
                
                if count > 0:
                    print(f"Skip (already has {count} candles)")
                    continue
                
                # Fetch from Binance
                candles = fetch_minute_candles_from_binance(symbol, start_time_ms, end_time_ms)
                time.sleep(REQUEST_DELAY)  # Rate limiting
                
                if not candles:
                    print("Failed to fetch")
                    continue
                
                # Insert into database
                insert_query = """
                    INSERT INTO web.minute_candles (
                        signal_analysis_id, pair_symbol,
                        open_time, open_price, high_price, low_price, close_price, volume
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                with conn.cursor() as cur:
                    for candle in candles:
                        cur.execute(insert_query, (
                            signal_id, symbol,
                            candle['open_time'],
                            candle['open_price'],
                            candle['high_price'],
                            candle['low_price'],
                            candle['close_price'],
                            candle['volume']
                        ))
                
                conn.commit()
                print(f"✓ Inserted {len(candles)} candles")
                
                # Progress report every 10 signals
                if i % 10 == 0:
                    print(f"  Progress: {i}/{len(signals)} ({i/len(signals)*100:.1f}%)")
            
            print("\n" + "="*120)
            print("Minute candles population complete!")
            
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    populate_minute_candles()
