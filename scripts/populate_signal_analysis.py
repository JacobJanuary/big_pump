"""
Populate signal analysis table with preprocessed data
"""
import sys
import os
from pathlib import Path
import json

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import (
    get_db_connection,
    fetch_signals,
    deduplicate_signals,
    get_entry_price_and_candles
)

def populate_signal_analysis(days=30, limit=None, force_refresh=False):
    """
    Fetch signals, preprocess them, and store in web.signal_analysis table
    
    Args:
        days: Number of days to look back
        limit: Limit number of signals
        force_refresh: If True, TRUNCATE and repopulate. If False, only add new signals.
    """
    print(f"Populating signal analysis table for the last {days} days...")
    
    try:
        with get_db_connection() as conn:
            if force_refresh:
                # Clear existing data
                print("Force refresh: Clearing existing data from web.signal_analysis...")
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE web.signal_analysis CASCADE")
                conn.commit()
            else:
                print("Incremental mode: Only adding new signals...")
            
            # Fetch signals
            signals = fetch_signals(conn, days=days, limit=limit)
            
            if not signals:
                print("No signals found.")
                return
            
            print(f"Found {len(signals)} signals from database.")
            
            # Deduplicate
            unique_signals = deduplicate_signals(signals, cooldown_hours=24)
            print(f"After deduplication: {len(unique_signals)} unique signals.")
            
            # Get existing signal timestamps to skip
            if not force_refresh:
                existing_query = """
                    SELECT signal_timestamp, pair_symbol
                    FROM web.signal_analysis
                """
                with conn.cursor() as cur:
                    cur.execute(existing_query)
                    existing = set((row[0], row[1]) for row in cur.fetchall())
                print(f"Found {len(existing)} existing signals in database.")
            else:
                existing = set()
            
            # Process each signal
            inserted = 0
            skipped = 0
            for i, signal in enumerate(unique_signals, 1):
                if i % 10 == 0:
                    print(f"Processing {i}/{len(unique_signals)}... (inserted: {inserted}, skipped: {skipped})", end='\r')
                
                # Skip if already exists (incremental mode)
                if (signal['timestamp'], signal['pair_symbol']) in existing:
                    skipped += 1
                    continue
                
                # Get entry price and candles
                entry_price, candles, entry_time_dt = get_entry_price_and_candles(
                    conn, signal,
                    analysis_hours=24,
                    entry_offset_minutes=17
                )
                
                if entry_price is None or not candles:
                    continue
                
                # Calculate metrics
                entry_time_ms = candles[0]['open_time']
                max_price = entry_price
                max_price_time_ms = entry_time_ms
                min_price = entry_price
                
                for candle in candles:
                    high = float(candle['high_price'])
                    low = float(candle['low_price'])
                    
                    if high > max_price:
                        max_price = high
                        max_price_time_ms = candle['open_time']
                    
                    if low < min_price:
                        min_price = low
                
                max_growth_pct = ((max_price - entry_price) / entry_price) * 100
                max_drawdown_pct = ((min_price - entry_price) / entry_price) * 100
                time_to_peak_seconds = int((max_price_time_ms - entry_time_ms) / 1000)
                
                from datetime import datetime, timezone
                max_price_time_dt = datetime.fromtimestamp(max_price_time_ms / 1000, tz=timezone.utc)
                
                # Convert candles to JSON (store only essential data)
                candles_json = json.dumps([{
                    'time': c['open_time'],
                    'o': float(c['open_price']),
                    'h': float(c['high_price']),
                    'l': float(c['low_price']),
                    'c': float(c['close_price'])
                } for c in candles])
                
                # Insert into database
                insert_query = """
                    INSERT INTO web.signal_analysis (
                        signal_timestamp, pair_symbol, trading_pair_id, total_score,
                        entry_time, entry_price,
                        max_price, max_price_time, min_price,
                        max_growth_pct, max_drawdown_pct,
                        time_to_peak_seconds,
                        candles_data, analysis_window_hours
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s,
                        %s::jsonb, %s
                    )
                """
                
                with conn.cursor() as cur:
                    cur.execute(insert_query, (
                        signal['timestamp'], signal['pair_symbol'], signal['trading_pair_id'], signal['total_score'],
                        entry_time_dt, entry_price,
                        max_price, max_price_time_dt, min_price,
                        max_growth_pct, max_drawdown_pct,
                        time_to_peak_seconds,
                        candles_json, 24
                    ))
                
                inserted += 1
                
                # Commit every 50 signals
                if inserted % 50 == 0:
                    conn.commit()
            
            # Final commit
            conn.commit()
            
            print(f"\n\nSuccessfully populated {inserted} new signals into web.signal_analysis")
            if skipped > 0:
                print(f"Skipped {skipped} existing signals")
            
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Populate signal analysis table.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of signals')
    parser.add_argument('--force-refresh', action='store_true', help='Force full refresh (TRUNCATE and repopulate)')
    args = parser.parse_args()
    
    populate_signal_analysis(days=args.days, limit=args.limit, force_refresh=args.force_refresh)
