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

# Exchange APIs
BINANCE_BASE_URL = "https://fapi.binance.com"
BYBIT_BASE_URL = "https://api.bybit.com"

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

def get_bybit_futures_symbols():
    """
    Get all available Bybit USDT Perpetual futures symbols
    """
    try:
        url = f"{BYBIT_BASE_URL}/v5/market/instruments-info"
        params = {
            'category': 'linear',  # USDT Perpetual
            'status': 'Trading',   # Only active trading pairs
            'limit': 1000
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('retCode') == 0:
                instruments = data.get('result', {}).get('list', [])
                # Extract symbols
                symbols = set()
                for inst in instruments:
                    symbol = inst.get('symbol')
                    if symbol:
                        symbols.add(symbol)
                return symbols
            else:
                print(f"Error from Bybit: {data.get('retMsg')}")
                return set()
        else:
            print(f"Failed to fetch Bybit symbols: {response.status_code}")
            return set()
            
    except Exception as e:
        print(f"Error getting Bybit symbols: {e}")
        return set()

def check_pairs_on_exchanges(days=30, limit=None):
    """
    Check which signal pairs are available on Binance and Bybit
    """
    print(f"Checking pair availability on Binance and Bybit...")
    print("="*120)
    
    try:
        with get_db_connection() as conn:
            # Fetch unique pairs from signals
            placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query_signals = f"""
                SELECT DISTINCT sh.pair_symbol
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
                ORDER BY sh.pair_symbol
                {limit_clause}
            """
            
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query_signals)
                signals = cur.fetchall()
                
            if not signals:
                print("No signals found.")
                return

            signal_pairs = [s['pair_symbol'] for s in signals]
            print(f"Found {len(signal_pairs)} unique pairs from signals.")
            
            # Get Bybit symbols
            print("\nFetching Bybit Futures symbols...")
            bybit_symbols = get_bybit_futures_symbols()
            print(f"Bybit has {len(bybit_symbols)} USDT Perpetual futures available.\n")
            
            # Compare
            results = []
            for symbol in signal_pairs:
                on_bybit = symbol in bybit_symbols
                results.append({
                    'symbol': symbol,
                    'binance': True,  # All our signals are from Binance
                    'bybit': on_bybit
                })
            
            # Print results
            print("="*120)
            print("EXCHANGE AVAILABILITY:")
            print("="*120)
            print(f"{'Symbol':<15} {'Binance Futures':<20} {'Bybit Futures':<20} {'Status':<20}")
            print("-"*120)
            
            on_both = []
            binance_only = []
            
            for r in results:
                binance_status = "✓ Available" if r['binance'] else "✗ Not Available"
                bybit_status = "✓ Available" if r['bybit'] else "✗ Not Available"
                
                if r['bybit']:
                    status = "Both Exchanges"
                    on_both.append(r['symbol'])
                else:
                    status = "Binance Only"
                    binance_only.append(r['symbol'])
                
                print(f"{r['symbol']:<15} {binance_status:<20} {bybit_status:<20} {status:<20}")
            
            # Summary
            print("\n" + "="*120)
            print("SUMMARY:")
            print("="*120)
            print(f"Total Signal Pairs: {len(signal_pairs)}")
            print(f"Available on Both Exchanges: {len(on_both)} ({len(on_both)/len(signal_pairs)*100:.1f}%)")
            print(f"Binance Only: {len(binance_only)} ({len(binance_only)/len(signal_pairs)*100:.1f}%)")
            print("="*120)
            
            if binance_only:
                print("\nPAIRS NOT AVAILABLE ON BYBIT:")
                print("-"*120)
                for i, symbol in enumerate(binance_only, 1):
                    print(f"{i:3}. {symbol}")
                print("="*120)
            
            if on_both:
                print(f"\n✓ {len(on_both)} pairs are available on both Binance and Bybit Futures")
                print(f"✗ {len(binance_only)} pairs are only available on Binance Futures")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check pair availability on Binance and Bybit Futures.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of pairs to check')
    args = parser.parse_args()
    
    check_pairs_on_exchanges(days=args.days, limit=args.limit)
