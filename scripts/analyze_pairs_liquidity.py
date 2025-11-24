import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_row
import requests
import time
import argparse

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

# Binance Futures API
BINANCE_BASE_URL = "https://fapi.binance.com"

# Rate limiting
# Binance Futures: 1200 requests/minute
# Use 400 requests/minute = 6.67 requests/second
# With 2 requests per pair: 60s / 400 req = 0.15s per request
REQUEST_DELAY = 0.15  # seconds between requests (400 req/min)

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

def get_historical_volume(symbol, timestamp_ms):
    """
    Get 1-hour volume at specific timestamp from Binance Futures
    """
    try:
        # Convert to Binance format (remove USDT suffix, add USDT back)
        # BTCUSDT stays BTCUSDT
        
        # Get 1-hour candle at specific time
        url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1h',
            'startTime': timestamp_ms,
            'limit': 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                # Kline format: [open_time, open, high, low, close, volume, ...]
                volume = float(data[0][5])  # Volume in base asset
                quote_volume = float(data[0][7])  # Quote asset volume (USDT)
                return quote_volume  # Return USDT volume
            else:
                return None
        else:
            print(f"  Warning: Failed to get volume for {symbol}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  Error getting volume for {symbol}: {e}")
        return None

def get_current_open_interest(symbol):
    """
    Get current Open Interest from Binance Futures
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/openInterest"
        params = {'symbol': symbol}
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # Response: {"openInterest": "value", "symbol": "BTCUSDT", "time": timestamp}
            oi = float(data.get('openInterest', 0))
            return oi
        else:
            print(f"  Warning: Failed to get OI for {symbol}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  Error getting OI for {symbol}: {e}")
        return None

def analyze_pairs_liquidity(days=30, limit=None):
    """
    Analyze volume and OI for unique pairs from signals
    """
    print(f"Analyzing liquidity for unique pairs from the last {days} days...")
    print(f"Rate limit: 1 request per {REQUEST_DELAY} seconds (33% of Binance limit)")
    print("="*120)
    
    try:
        with get_db_connection() as conn:
            # Fetch unique pairs from signals
            placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query_signals = f"""
                SELECT DISTINCT
                    sh.pair_symbol, 
                    sh.timestamp
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
                ORDER BY sh.timestamp DESC
                {limit_clause}
            """
            
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query_signals)
                signals = cur.fetchall()
                
            if not signals:
                print("No signals found.")
                return

            print(f"Found {len(signals)} unique pair-timestamp combinations.")
            
            # Get unique pairs
            unique_pairs = {}
            for signal in signals:
                symbol = signal['pair_symbol']
                timestamp = signal['timestamp']
                
                # Keep first (latest) timestamp for each pair
                if symbol not in unique_pairs:
                    unique_pairs[symbol] = timestamp
            
            print(f"Processing {len(unique_pairs)} unique pairs...\n")
            
            results = []
            
            for i, (symbol, timestamp) in enumerate(unique_pairs.items(), 1):
                print(f"[{i}/{len(unique_pairs)}] Processing {symbol}...", end=' ')
                
                # Convert timestamp to milliseconds
                timestamp_ms = int(timestamp.timestamp() * 1000)
                
                # Get historical volume at signal time
                volume_1h = get_historical_volume(symbol, timestamp_ms)
                
                # Rate limit
                time.sleep(REQUEST_DELAY)
                
                # Get current Open Interest
                open_interest = get_current_open_interest(symbol)
                
                # Rate limit
                time.sleep(REQUEST_DELAY)
                
                if volume_1h is not None and open_interest is not None:
                    print(f"✓ Volume: ${volume_1h:,.0f}, OI: {open_interest:,.0f}")
                else:
                    print(f"✗ Failed to fetch data")
                
                results.append({
                    'symbol': symbol,
                    'signal_time': timestamp,
                    'volume_1h_usdt': volume_1h,
                    'current_oi': open_interest
                })
            
            # Print results table
            print("\n" + "="*120)
            print("LIQUIDITY ANALYSIS RESULTS:")
            print("="*120)
            print(f"{'Symbol':<12} {'Signal Time':<20} {'1H Volume (USDT)':<20} {'Current OI':<20} {'Status':<10}")
            print("-"*120)
            
            for r in results:
                status = "✓ OK" if r['volume_1h_usdt'] and r['current_oi'] else "✗ Failed"
                vol_str = f"${r['volume_1h_usdt']:,.0f}" if r['volume_1h_usdt'] else "N/A"
                oi_str = f"{r['current_oi']:,.0f}" if r['current_oi'] else "N/A"
                time_str = r['signal_time'].strftime('%Y-%m-%d %H:%M')
                
                print(f"{r['symbol']:<12} {time_str:<20} {vol_str:<20} {oi_str:<20} {status:<10}")
            
            # Summary statistics
            valid_results = [r for r in results if r['volume_1h_usdt'] and r['current_oi']]
            
            if valid_results:
                avg_volume = sum(r['volume_1h_usdt'] for r in valid_results) / len(valid_results)
                avg_oi = sum(r['current_oi'] for r in valid_results) / len(valid_results)
                min_volume = min(r['volume_1h_usdt'] for r in valid_results)
                max_volume = max(r['volume_1h_usdt'] for r in valid_results)
                min_oi = min(r['current_oi'] for r in valid_results)
                max_oi = max(r['current_oi'] for r in valid_results)
                
                print("\n" + "="*120)
                print("SUMMARY:")
                print("="*120)
                print(f"Total Pairs Analyzed: {len(results)}")
                print(f"Successfully Fetched: {len(valid_results)} ({len(valid_results)/len(results)*100:.1f}%)")
                print(f"\n1H Volume (USDT):")
                print(f"  Average: ${avg_volume:,.0f}")
                print(f"  Min: ${min_volume:,.0f}")
                print(f"  Max: ${max_volume:,.0f}")
                print(f"\nCurrent Open Interest:")
                print(f"  Average: {avg_oi:,.0f}")
                print(f"  Min: {min_oi:,.0f}")
                print(f"  Max: {max_oi:,.0f}")
                print("="*120)
                
                # Filter by minimum liquidity thresholds
                print("\nPAIRS BELOW LIQUIDITY THRESHOLDS:")
                print("-"*120)
                
                MIN_VOLUME = 1_000_000  # $1M per hour
                MIN_OI = 1_000_000  # 1M in OI
                
                low_liquidity = [r for r in valid_results 
                                if r['volume_1h_usdt'] < MIN_VOLUME or r['current_oi'] < MIN_OI]
                
                if low_liquidity:
                    print(f"{'Symbol':<12} {'1H Volume':<20} {'Current OI':<20} {'Issue':<30}")
                    print("-"*120)
                    for r in low_liquidity:
                        issues = []
                        if r['volume_1h_usdt'] < MIN_VOLUME:
                            issues.append(f"Low Volume (<$1M)")
                        if r['current_oi'] < MIN_OI:
                            issues.append(f"Low OI (<1M)")
                        
                        print(f"{r['symbol']:<12} ${r['volume_1h_usdt']:>18,.0f} {r['current_oi']:>19,.0f} {', '.join(issues):<30}")
                else:
                    print("All pairs meet minimum liquidity thresholds ✓")
                
                print("="*120)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze liquidity (volume & OI) for signal pairs.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of pairs to process')
    args = parser.parse_args()
    
    analyze_pairs_liquidity(days=args.days, limit=args.limit)
