import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# Import from pump_analysis_lib
from pump_analysis_lib import (
    get_db_connection,
    fetch_signals,
    get_entry_price_and_candles,
    get_binance_price_at_time,
    get_bybit_price_at_time,
    EXCHANGE_IDS,
    REQUEST_DELAY
)

# Constants
SCORE_THRESHOLD = 250
ENTRY_OFFSET_MINUTES = 17

def get_historical_price(symbol, exchange_id, timestamp_ms):
    """Fetch price at a specific timestamp using appropriate API"""
    if exchange_id == EXCHANGE_IDS['BINANCE']:
        return get_binance_price_at_time(symbol, timestamp_ms)
    elif exchange_id == EXCHANGE_IDS['BYBIT']:
        return get_bybit_price_at_time(symbol, timestamp_ms)
    return None

def analyze_overheating(days=30):
    print(f"🔥 Analyzing Overheating vs Performance for last {days} days...")
    
    try:
        with get_db_connection() as conn:
            # 1. Fetch Signals
            print("Fetching signals...")
            signals = fetch_signals(conn, days=days)
            print(f"Found {len(signals)} signals.")
            
            results = []
            
            for i, sig in enumerate(signals):
                if i % 10 == 0:
                    print(f"Processing {i}/{len(signals)}...", end='\r')
                    
                signal_ts = sig['timestamp']
                symbol = sig['pair_symbol']
                exchange_id = sig.get('exchange_id', EXCHANGE_IDS['BINANCE'])
                
                # Entry time
                entry_time_dt = signal_ts + timedelta(minutes=ENTRY_OFFSET_MINUTES)
                entry_time_ms = int(entry_time_dt.timestamp() * 1000)
                
                # --- 1. Calculate Overheating (Pre-Entry) ---
                # We need prices at: Entry, Entry-5m, Entry-15m, Entry-60m
                
                # Get Entry Price (Current)
                # We can reuse get_entry_price_and_candles for this + post-analysis
                entry_price, candles, _ = get_entry_price_and_candles(
                    conn, sig, analysis_hours=24, entry_offset_minutes=ENTRY_OFFSET_MINUTES
                )
                
                if entry_price is None or not candles:
                    continue
                    
                # Helper to get past price
                def get_past_price_change(minutes_before):
                    target_ts_ms = entry_time_ms - (minutes_before * 60 * 1000)
                    past_price = get_historical_price(symbol, exchange_id, target_ts_ms)
                    time.sleep(REQUEST_DELAY) # Rate limit
                    
                    if past_price:
                        return (entry_price - past_price) / past_price * 100
                    return None

                oh_5m = get_past_price_change(5)
                oh_15m = get_past_price_change(15)
                oh_1h = get_past_price_change(60)
                
                # --- 2. Calculate Performance (Post-Entry) ---
                # Candles are already fetched by get_entry_price_and_candles
                # They start from entry_time
                
                max_high = entry_price
                min_low = entry_price
                
                for candle in candles:
                    high = float(candle['high_price'])
                    low = float(candle['low_price'])
                    if high > max_high: max_high = high
                    if low < min_low: min_low = low
                
                max_drawdown = (min_low - entry_price) / entry_price * 100
                max_profit = (max_high - entry_price) / entry_price * 100
                
                results.append({
                    'symbol': symbol,
                    'oh_5m': oh_5m,
                    'oh_15m': oh_15m,
                    'oh_1h': oh_1h,
                    'max_profit': max_profit,
                    'max_drawdown': max_drawdown
                })
                
            # --- 3. Analysis & Reporting ---
            if not results:
                print("No results to analyze.")
                return

            df_res = pd.DataFrame(results)
            
            print("\n" + "="*80)
            print(f"📊 OVERHEATING ANALYSIS REPORT ({len(df_res)} signals)")
            print("="*80)
            
            # Define buckets
            buckets = [-np.inf, -2, 0, 1, 2, 3, 5, np.inf]
            labels = ['< -2%', '-2% to 0%', '0% to 1%', '1% to 2%', '2% to 3%', '3% to 5%', '> 5%']
            
            for interval, col_name in [('5 min', 'oh_5m'), ('15 min', 'oh_15m'), ('1 Hour', 'oh_1h')]:
                print(f"\n🔹 Interval: {interval} Before Entry")
                print("-" * 95)
                print(f"{'Range':<15} | {'Count':<5} | {'Avg Profit':<10} | {'Avg DD':<10} | {'WinRate (>1%)':<12} | {'Risk/Reward':<10}")
                print("-" * 95)
                
                # Filter out None values
                df_clean = df_res.dropna(subset=[col_name])
                
                if df_clean.empty:
                    print("No data for this interval.")
                    continue

                df_clean['bucket'] = pd.cut(df_clean[col_name], bins=buckets, labels=labels)
                grouped = df_clean.groupby('bucket', observed=False)
                
                for name, group in grouped:
                    if group.empty:
                        continue
                    count = len(group)
                    avg_profit = group['max_profit'].mean()
                    avg_dd = group['max_drawdown'].mean()
                    win_rate = (group['max_profit'] > 1.0).mean() * 100
                    rr_ratio = abs(avg_profit / avg_dd) if avg_dd != 0 else 0
                    
                    print(f"{name:<15} | {count:<5} | {avg_profit:>9.2f}% | {avg_dd:>9.2f}% | {win_rate:>11.1f}% | {rr_ratio:>9.2f}")
                    
                # Correlation
                corr_profit = df_clean[col_name].corr(df_clean['max_profit'])
                corr_dd = df_clean[col_name].corr(df_clean['max_drawdown'])
                print(f"\nCorrelation with Profit: {corr_profit:.2f}")
                print(f"Correlation with Drawdown: {corr_dd:.2f}")
                print("." * 95)

    except Exception as e:
        print(f"Critical Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_overheating()
