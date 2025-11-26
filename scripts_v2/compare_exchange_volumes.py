import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_row
import requests
import time
import argparse

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import (
    get_db_connection, 
    EXCHANGE_FILTER, 
    EXCHANGE_IDS, 
    BINANCE_BASE_URL, 
    BYBIT_BASE_URL,
    REQUEST_DELAY
)

# --- Configuration ---
SCORE_THRESHOLD = 250
TARGET_PATTERNS = ['SQUEEZE_IGNITION', 'OI_EXPLOSION']

# Rate limiting
# Bybit: 100 requests/minute (most restrictive)
# Use 80 requests/minute to be safe: 60s / 80 = 0.75s per request
REQUEST_DELAY = 0.75  # seconds between requests

def get_binance_volume(symbol, timestamp_ms):
    """
    Get 1-hour volume at specific timestamp from Binance Futures
    """
    try:
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
                # Quote asset volume (USDT)
                quote_volume = float(data[0][7])
                return quote_volume
            else:
                return None
        else:
            return None
            
    except Exception as e:
        print(f"  Error getting Binance volume for {symbol}: {e}")
        return None

def get_bybit_volume(symbol, timestamp_ms):
    """
    Get 1-hour volume at specific timestamp from Bybit Futures
    """
    try:
        url = f"{BYBIT_BASE_URL}/v5/market/kline"
        params = {
            'category': 'linear',  # USDT Perpetual
            'symbol': symbol,
            'interval': '60',  # 60 minutes
            'start': timestamp_ms,
            'limit': 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('retCode') == 0:
                klines = data.get('result', {}).get('list', [])
                if klines and len(klines) > 0:
                    # Bybit kline format: [startTime, open, high, low, close, volume, turnover]
                    # turnover = volume in USDT
                    turnover = float(klines[0][6])
                    return turnover
                else:
                    return None
            else:
                return None
        else:
            return None
            
    except Exception as e:
        print(f"  Error getting Bybit volume for {symbol}: {e}")
        return None

def compare_exchange_volumes(days=30, limit=None):
    """
    Compare 1-hour trading volumes between Binance and Bybit at signal time
    """
    print(f"Comparing trading volumes on Binance and Bybit...")
    print(f"Exchange Filter: {EXCHANGE_FILTER}")
    print(f"Rate limit: {60/REQUEST_DELAY:.0f} requests/minute (respecting Bybit's 100 req/min limit)")
    print("="*140)
    
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
                  AND tp.is_active = TRUE
                  AND (
                    '{EXCHANGE_FILTER}' = 'ALL' 
                    OR ('{EXCHANGE_FILTER}' = 'BINANCE' AND tp.exchange_id = {EXCHANGE_IDS['BINANCE']})
                    OR ('{EXCHANGE_FILTER}' = 'BYBIT' AND tp.exchange_id = {EXCHANGE_IDS['BYBIT']})
                  )
                ORDER BY sh.timestamp DESC
                {limit_clause}
            """
            
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query_signals)
                signals = cur.fetchall()
                
            if not signals:
                print("No signals found.")
                return

            # Get unique pairs
            unique_pairs = {}
            for signal in signals:
                symbol = signal['pair_symbol']
                timestamp = signal['timestamp']
                
                if symbol not in unique_pairs:
                    unique_pairs[symbol] = timestamp
            
            print(f"Processing {len(unique_pairs)} unique pairs...\n")
            
            results = []
            
            for i, (symbol, timestamp) in enumerate(unique_pairs.items(), 1):
                print(f"[{i}/{len(unique_pairs)}] {symbol}...", end=' ')
                
                timestamp_ms = int(timestamp.timestamp() * 1000)
                
                # Get Binance volume
                binance_vol = get_binance_volume(symbol, timestamp_ms)
                time.sleep(REQUEST_DELAY)
                
                # Get Bybit volume
                bybit_vol = get_bybit_volume(symbol, timestamp_ms)
                time.sleep(REQUEST_DELAY)
                
                if binance_vol is not None and bybit_vol is not None:
                    diff_pct = ((binance_vol - bybit_vol) / bybit_vol * 100) if bybit_vol > 0 else 0
                    print(f"Binance: ${binance_vol:,.0f}, Bybit: ${bybit_vol:,.0f} ({diff_pct:+.1f}%)")
                elif binance_vol is not None:
                    print(f"Binance: ${binance_vol:,.0f}, Bybit: N/A")
                elif bybit_vol is not None:
                    print(f"Binance: N/A, Bybit: ${bybit_vol:,.0f}")
                else:
                    print(f"Both: N/A")
                
                results.append({
                    'symbol': symbol,
                    'signal_time': timestamp,
                    'binance_volume': binance_vol,
                    'bybit_volume': bybit_vol
                })
            
            # Print comparison table
            print("\n" + "="*140)
            print("VOLUME COMPARISON:")
            print("="*140)
            print(f"{'Symbol':<12} {'Signal Time':<20} {'Binance (USDT)':<18} {'Bybit (USDT)':<18} {'Difference':<15} {'Higher':<10}")
            print("-"*140)
            
            for r in results:
                time_str = r['signal_time'].strftime('%Y-%m-%d %H:%M')
                
                binance_str = f"${r['binance_volume']:,.0f}" if r['binance_volume'] else "N/A"
                bybit_str = f"${r['bybit_volume']:,.0f}" if r['bybit_volume'] else "N/A"
                
                if r['binance_volume'] and r['bybit_volume']:
                    diff_pct = ((r['binance_volume'] - r['bybit_volume']) / r['bybit_volume'] * 100)
                    diff_str = f"{diff_pct:+.1f}%"
                    higher = "Binance" if r['binance_volume'] > r['bybit_volume'] else "Bybit"
                else:
                    diff_str = "N/A"
                    higher = "N/A"
                
                print(f"{r['symbol']:<12} {time_str:<20} {binance_str:<18} {bybit_str:<18} {diff_str:<15} {higher:<10}")
            
            # Summary statistics
            valid_results = [r for r in results if r['binance_volume'] and r['bybit_volume']]
            
            if valid_results:
                binance_higher = sum(1 for r in valid_results if r['binance_volume'] > r['bybit_volume'])
                bybit_higher = sum(1 for r in valid_results if r['bybit_volume'] > r['binance_volume'])
                equal = len(valid_results) - binance_higher - bybit_higher
                
                avg_binance = sum(r['binance_volume'] for r in valid_results) / len(valid_results)
                avg_bybit = sum(r['bybit_volume'] for r in valid_results) / len(valid_results)
                
                # Average difference
                avg_diff = sum(abs(r['binance_volume'] - r['bybit_volume']) for r in valid_results) / len(valid_results)
                avg_diff_pct = (avg_diff / avg_bybit * 100) if avg_bybit > 0 else 0
                
                print("\n" + "="*140)
                print("SUMMARY:")
                print("="*140)
                print(f"Total Pairs Analyzed: {len(results)}")
                print(f"Both Exchanges Available: {len(valid_results)} ({len(valid_results)/len(results)*100:.1f}%)")
                print(f"Binance Only: {sum(1 for r in results if r['binance_volume'] and not r['bybit_volume'])}")
                print(f"Bybit Only: {sum(1 for r in results if r['bybit_volume'] and not r['binance_volume'])}")
                print(f"\nVolume Comparison (pairs available on both):")
                print(f"  Binance Higher: {binance_higher} pairs ({binance_higher/len(valid_results)*100:.1f}%)")
                print(f"  Bybit Higher: {bybit_higher} pairs ({bybit_higher/len(valid_results)*100:.1f}%)")
                print(f"  Equal: {equal} pairs")
                print(f"\nAverage 1H Volume:")
                print(f"  Binance: ${avg_binance:,.0f}")
                print(f"  Bybit: ${avg_bybit:,.0f}")
                print(f"  Average Difference: ${avg_diff:,.0f} ({avg_diff_pct:.1f}%)")
                print("="*140)
                
                # Show pairs with significant volume differences
                print("\nPAIRS WITH >50% VOLUME DIFFERENCE:")
                print("-"*140)
                print(f"{'Symbol':<12} {'Binance':<18} {'Bybit':<18} {'Difference':<15}")
                print("-"*140)
                
                large_diff = []
                for r in valid_results:
                    diff_pct = abs((r['binance_volume'] - r['bybit_volume']) / r['bybit_volume'] * 100)
                    if diff_pct > 50:
                        large_diff.append((r['symbol'], r['binance_volume'], r['bybit_volume'], diff_pct))
                
                if large_diff:
                    large_diff.sort(key=lambda x: x[3], reverse=True)
                    for symbol, binance, bybit, diff in large_diff:
                        print(f"{symbol:<12} ${binance:>16,.0f} ${bybit:>16,.0f} {diff:>13.1f}%")
                else:
                    print("No pairs with >50% difference")
                
                print("="*140)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compare trading volumes between Binance and Bybit.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of pairs to process')
    args = parser.parse_args()
    
    compare_exchange_volumes(days=args.days, limit=args.limit)
