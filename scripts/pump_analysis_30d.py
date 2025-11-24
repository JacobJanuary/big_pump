import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add scripts directory to path for library import
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# Import common library
from pump_analysis_lib import (
    get_db_connection,
    fetch_signals,
    deduplicate_signals,
    get_entry_price_and_candles
)

import argparse

ANALYSIS_WINDOW_HOURS = 24

def analyze_pumps(days=30, limit=None):
    print(f"Analyzing pumps for the last {days} days...")
    
    try:
        with get_db_connection() as conn:
            # Fetch Signals
            signals = fetch_signals(conn, days=days, limit=limit)
                
            if not signals:
                print("No signals found.")
                return

            print(f"Found {len(signals)} signals. Fetching price data...")
            
            # Deduplicate signals
            unique_signals = deduplicate_signals(signals, cooldown_hours=24)
            print(f"After deduplication: {len(unique_signals)} unique signals.")
            
            results = []
            
            for i, signal in enumerate(unique_signals, 1):
                if i % 10 == 0:
                    print(f"Processing {i}/{len(unique_signals)}...", end='\r')
                    
                symbol = signal['pair_symbol']
                signal_ts = signal['timestamp']
                
                # Get entry price and candles
                entry_price, candles, entry_time_dt = get_entry_price_and_candles(
                    conn, signal, 
                    analysis_hours=ANALYSIS_WINDOW_HOURS,
                    entry_offset_minutes=17
                )
                
                if entry_price is None or not candles:
                    continue
                
                # Calculate max price and time
                max_price = -1.0
                max_price_time_ms = None
                
                for candle in candles:
                    high = float(candle['high_price'])
                    ts = candle['open_time']
                    
                    if high > max_price:
                        max_price = high
                        max_price_time_ms = ts
                
                # Find lowest low before max_price_time
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
            print(f"{'Date':<12} {'Time':<8} {'Symbol':<12} {'Score':<6} {'Drawdown %':<12} {'Growth %':<10} {'Time to Peak':<15}")
            print("-" * 80)
            
            for r in results:
                print(f"{r['date']:<12} {r['time']:<8} {r['symbol']:<12} {r['score']:<6.0f} {r['drawdown']:<12.2f} {r['growth']:<10.2f} {r['time_to_peak']:<15}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze pump signals.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of signals to process')
    args = parser.parse_args()
    
    analyze_pumps(days=args.days, limit=args.limit)
