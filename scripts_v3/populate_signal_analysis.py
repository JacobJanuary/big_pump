"""
Populate BIG PUMP signal analysis table with preprocessed data
Target Table: web.big_pump_signals
"""
import sys
import os
from pathlib import Path
import json
from datetime import datetime, timezone, timedelta

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import (
    get_db_connection,
    fetch_signals,
    get_entry_price_and_candles,
    get_binance_price_at_time,
    get_bybit_price_at_time,
    EXCHANGE_IDS,
    REQUEST_DELAY
)
import time

def populate_big_pump_signals(days=30, limit=None, force_refresh=False):
    """
    Fetch signals and store in web.big_pump_signals table
    
    Args:
        days: Number of days to look back
        limit: Limit number of signals
        force_refresh: If True, DELETE and repopulate.
    """
    print(f"Populating web.big_pump_signals for the last {days} days...")
    
    try:
        with get_db_connection() as conn:
            if force_refresh:
                print("Force refresh: Deleting existing data from web.big_pump_signals...")
                with conn.cursor() as cur:
                    try:
                        cur.execute("DELETE FROM web.big_pump_signals;")
                        conn.commit()
                        print("Old data deleted.")
                    except Exception as e:
                        conn.rollback()
                        print(f"Error deleting data (maybe table doesn't exist?): {e}")
                        return

            # Fetch signals (now includes RSI, Vol, OI)
            signals = fetch_signals(conn, days=days, limit=limit)
            
            if not signals:
                print("No signals found.")
                return
            
            print(f"Found {len(signals)} signals from database.")
            
            # Insert into database
            inserted = 0
            skipped = 0
            
            for i, signal in enumerate(signals, 1):
                if i % 50 == 0:
                    print(f"Processing {i}/{len(signals)}... (inserted: {inserted}, skipped: {skipped})", end='\r')
                
                # --- 1. Prepare Data ---
                
                # Entry Logic: Signal Time + 18 minutes
                signal_ts = signal['timestamp']
                entry_offset_minutes = 18
                entry_time_dt = signal_ts + timedelta(minutes=entry_offset_minutes)
                entry_time_ms = int(entry_time_dt.timestamp() * 1000)
                
                # Get Entry Price (1m candle at entry time)
                entry_price = None
                pair_symbol = signal['pair_symbol']
                exchange_id = signal.get('exchange_id', 1) 
                
                if exchange_id == EXCHANGE_IDS['BINANCE']:
                    entry_price = get_binance_price_at_time(pair_symbol, entry_time_ms)
                elif exchange_id == EXCHANGE_IDS['BYBIT']:
                    entry_price = get_bybit_price_at_time(pair_symbol, entry_time_ms)
                
                time.sleep(REQUEST_DELAY) # Rate limit
                
                if entry_price is None:
                    # Try fetching from DB candles if API fails or for backfill?
                    # For now, if API fails, skip or use 0? Prompt implies NOT NULL.
                    # Let's try to get it from DB 5m candles if possible, or skip.
                    # User wants clean dataset. Skipping is safer than 0 price.
                    # print(f"  ❌ SKIPPED (no entry price): {pair_symbol} at {entry_time_dt}")
                    skipped += 1
                    continue

                # --- 2. Insert ---
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO web.big_pump_signals (
                            signal_timestamp, 
                            pair_symbol, 
                            trading_pair_id, 
                            total_score,
                            rsi_threshold,
                            volume_zscore,
                            oi_delta,
                            entry_time, 
                            entry_price,
                            time_to_sl_10_pr_seconds,
                            max_price,
                            max_grow_pr,
                            time_to_max_price_seconds,
                            analysis_window_hours
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, 
                            0, 0, 0, 0, 
                            24
                        )
                    """, (
                        signal['timestamp'], 
                        signal['pair_symbol'], 
                        signal['trading_pair_id'], 
                        signal['total_score'],
                        int(signal['rsi']),          # rsi_threshold -> rsi (from query)
                        float(signal['volume_zscore']),
                        float(signal['oi_delta_pct']),
                        entry_time_dt, 
                        float(entry_price)
                    ))
                
                inserted += 1
            
            conn.commit()
            print(f"\n\nSuccessfully inserted {inserted} new signals into web.big_pump_signals")
            if skipped > 0:
                print(f"Skipped {skipped} signals (missing entry price)")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Populate BIG PUMP signals table.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of signals')
    parser.add_argument('--force-refresh', action='store_true', help='Force delete exiting data')
    args = parser.parse_args()
    
    populate_big_pump_signals(days=args.days, limit=args.limit, force_refresh=args.force_refresh)
