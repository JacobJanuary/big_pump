"""
Fetch minute candles for signals in web.signal_analysis
"""
import sys
import os
from pathlib import Path
import requests
import time
from datetime import datetime, timezone

# Add parent scripts directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
scripts_dir = parent_dir / 'scripts_v2'
sys.path.append(str(scripts_dir))

from pump_analysis_lib import get_db_connection, BINANCE_BASE_URL, BYBIT_BASE_URL, EXCHANGE_IDS

# Rate limits
# Binance: 1200 req/min -> ~0.05s delay (safe 0.1s)
# Bybit: 100 req/min -> 0.6s delay (safe 0.7s)
BINANCE_DELAY = 0.1
BYBIT_DELAY = 0.7

def fetch_minute_candles_from_binance(symbol, start_time_ms, end_time_ms):
    """
    Fetch 1-minute candles from Binance
    """
    import time
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
        
        # Rate limiting: Add delay to prevent 429 errors
        time.sleep(0.3)  # 300ms delay for larger data fetches
        
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
        elif response.status_code == 429:
            print(f"  Binance API rate limit exceeded for {symbol}")
            print(f"  Waiting 60 seconds...")
            time.sleep(60)
            return None
        else:
            print(f"  Binance API error for {symbol}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  Error fetching {symbol} from Binance: {e}")
        return None

def fetch_minute_candles_from_bybit(symbol, start_time_ms, end_time_ms):
    """
    Fetch 1-minute candles from Bybit
    """
    import time
    try:
        url = f"{BYBIT_BASE_URL}/v5/market/kline"
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': '1',
            'start': start_time_ms,
            'end': end_time_ms,
            'limit': 1000  # Max allowed by Bybit
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        # Rate limiting: Add delay to prevent 429 errors
        time.sleep(0.3)  # 300ms delay for larger data fetches
        
        if response.status_code == 200:
            data = response.json()
            if data.get('retCode') == 0:
                # Bybit returns list in REVERSE order (newest first) usually, but check docs
                # Format: [startTime, open, high, low, close, volume, turnover]
                list_data = data.get('result', {}).get('list', [])
                candles = [{
                    'open_time': int(k[0]),
                    'open_price': float(k[1]),
                    'high_price': float(k[2]),
                    'low_price': float(k[3]),
                    'close_price': float(k[4]),
                    'volume': float(k[5])
                } for k in list_data]
                # Reverse to get chronological order
                candles.reverse()
                return candles
            else:
                print(f"  Bybit API error for {symbol}: retCode {data.get('retCode')}")
                return None
        elif response.status_code == 429:
            print(f"  Bybit API rate limit exceeded for {symbol}")
            print(f"  Waiting 60 seconds...")
            time.sleep(60)
            return None
        else:
            print(f"  Bybit API HTTP error for {symbol}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  Error fetching {symbol} from Bybit: {e}")
        return None

def populate_minute_candles():
    """
    Fetch minute candles for all signals in signal_analysis table
    """
    print("Fetching 1-minute candles for all signals...")
    print("="*120)
    
    try:
        with get_db_connection() as conn:
            # Get all signals with exchange info
            # Need to join with trading_pairs to get exchange_id
            query = """
                SELECT sa.id, sa.pair_symbol, sa.entry_time, tp.exchange_id
                FROM web.signal_analysis sa
                JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
                ORDER BY sa.id
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
                exchange_id = signal[3]
                
                exchange_name = "BINANCE" if exchange_id == EXCHANGE_IDS['BINANCE'] else "BYBIT"
                print(f"[{i}/{len(signals)}] {symbol} ({exchange_name})...", end=' ')
                
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
                
                # Logic for re-fetching incomplete signals
                is_mature = (datetime.now(timezone.utc) - entry_time).total_seconds() > 24 * 3600
                
                if count >= 1430:
                    print(f"Skip (already has {count} candles)")
                    continue
                elif count > 0 and not is_mature:
                    print(f"Skip (has {count} candles, signal too young for full 24h)")
                    continue
                elif count > 0 and is_mature:
                    print(f"Refetching (has {count} candles, but signal is mature >24h). Deleting old...")
                    delete_query = "DELETE FROM web.minute_candles WHERE signal_analysis_id = %s"
                    with conn.cursor() as cur:
                        cur.execute(delete_query, (signal_id,))
                        conn.commit()
                # If count == 0, proceed to fetch
                
                # Fetch based on exchange
                candles = None
                if exchange_id == EXCHANGE_IDS['BINANCE']:
                    candles = fetch_minute_candles_from_binance(symbol, start_time_ms, end_time_ms)
                    time.sleep(BINANCE_DELAY)
                elif exchange_id == EXCHANGE_IDS['BYBIT']:
                    candles = fetch_minute_candles_from_bybit(symbol, start_time_ms, end_time_ms)
                    time.sleep(BYBIT_DELAY)
                else:
                    print(f"Unknown exchange ID: {exchange_id}")
                    continue
                
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
