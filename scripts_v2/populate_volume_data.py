#!/usr/bin/env python3
"""
Populate Volume Data for Signal Analysis
Fetches 1-minute and 1-hour volume from Binance API and updates web.signal_analysis table
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts_v2.pump_analysis_lib import get_db_connection
from datetime import datetime, timedelta, timezone
import requests
import time
import argparse

def get_binance_klines(symbol, interval, start_time, end_time, limit=1):
    """
    Get klines from Binance API
    interval: '1m', '1h', etc.
    Returns list of klines or None on error
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': limit
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        return None

def get_volume_at_entry(symbol, entry_time):
    """
    Get 1m volume at entry time and 1h volume before entry
    entry_time: datetime object (timezone aware)
    Returns: (volume_1m_usdt, volume_1h_usdt) or (None, None)
    """
    entry_ts = int(entry_time.timestamp() * 1000)
    
    # 1-minute volume at entry
    kline_1m = get_binance_klines(symbol, '1m', entry_ts, entry_ts + 60000, limit=1)
    volume_1m = None
    if kline_1m and len(kline_1m) > 0:
        volume_1m = float(kline_1m[0][7])  # Quote asset volume (USDT)
    
    # 1-hour volume before entry
    hour_before = entry_ts - 3600000  # 1 hour before
    kline_1h = get_binance_klines(symbol, '1h', hour_before, entry_ts, limit=1)
    volume_1h = None
    if kline_1h and len(kline_1h) > 0:
        volume_1h = float(kline_1h[0][7])
    
    return volume_1m, volume_1h

def populate_volume_data(force=False, limit=None):
    """
    Populate volume data for signals without it
    force: if True, repopulate even if data exists
    limit: if set, process only N signals (for testing)
    """
    print("="*80)
    print("POPULATE VOLUME DATA FOR SIGNAL ANALYSIS")
    print("="*80)
    
    conn = get_db_connection()
    
    # Query signals without volume data (or all if force=True)
    if force:
        query = """
            SELECT id, pair_symbol, signal_timestamp
            FROM web.signal_analysis
            ORDER BY signal_timestamp DESC
        """
        print("Mode: FORCE - Repopulating all signals")
    else:
        query = """
            SELECT id, pair_symbol, signal_timestamp
            FROM web.signal_analysis
            WHERE volume_1m_usdt IS NULL OR volume_1h_usdt IS NULL
            ORDER BY signal_timestamp DESC
        """
        print("Mode: INCREMENTAL - Processing signals without volume data")
    
    if limit:
        query += f" LIMIT {limit}"
        print(f"Limit: {limit} signals")
    
    with conn.cursor() as cur:
        cur.execute(query)
        signals = cur.fetchall()
    
    if not signals:
        print("\nNo signals to process. All volume data is up to date!")
        conn.close()
        return
    
    print(f"\nProcessing {len(signals)} signals...")
    print(f"Rate limit: 0.7s delay between signals (~85 signals/min)")
    print(f"Estimated time: {len(signals) * 0.7 / 60:.1f} minutes")
    print("-"*80)
    
    # Statistics
    success_count = 0
    skip_count = 0
    error_count = 0
    
    # Rate limiting setup
    delay_between_signals = 0.7  # seconds (safe for 200 API calls/min)
    
    for i, signal in enumerate(signals, 1):
        signal_id = signal[0]
        symbol = signal[1]
        signal_ts = signal[2]
        
        # Calculate entry time (signal_timestamp + 17 minutes)
        if signal_ts.tzinfo is None:
            signal_ts = signal_ts.replace(tzinfo=timezone.utc)
        
        entry_time = signal_ts + timedelta(minutes=17)
        
        # Fetch volume data from Binance
        vol_1m, vol_1h = get_volume_at_entry(symbol, entry_time)
        
        if vol_1m is not None and vol_1h is not None:
            # Update database
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE web.signal_analysis
                        SET volume_1m_usdt = %s, volume_1h_usdt = %s
                        WHERE id = %s
                    """, (vol_1m, vol_1h, signal_id))
                conn.commit()
                success_count += 1
                
                # Progress logging (every 10 signals)
                if i % 10 == 0 or i == len(signals):
                    pct = (i / len(signals)) * 100
                    print(f"Progress: {i}/{len(signals)} ({pct:.1f}%) | Last: {symbol} | Vol1m=${vol_1m/1000:.1f}K Vol1h=${vol_1h/1000000:.2f}M")
            except Exception as e:
                print(f"  ⚠️ DB Update failed for {symbol}: {e}")
                error_count += 1
        else:
            print(f"  ⚠️ API failed for {symbol} (signal_id={signal_id})")
            skip_count += 1
        
        # Rate limiting (skip for last signal)
        if i < len(signals):
            time.sleep(delay_between_signals)
    
    conn.close()
    
    # Final statistics
    print("\n" + "="*80)
    print("POPULATION COMPLETE")
    print("="*80)
    print(f"Total processed:  {len(signals)}")
    print(f"  ✅ Success:     {success_count}")
    print(f"  ⚠️  Skipped:     {skip_count}")
    if error_count > 0:
        print(f"  ❌ Errors:      {error_count}")
    print(f"\nSuccess rate: {success_count/len(signals)*100:.1f}%")
    
    if skip_count > 0:
        print(f"\nNote: {skip_count} signals skipped due to API errors (can retry later)")

def main():
    parser = argparse.ArgumentParser(
        description='Populate volume data for signal analysis from Binance API'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Repopulate data even if it already exists'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of signals to process (for testing)'
    )
    
    args = parser.parse_args()
    
    populate_volume_data(force=args.force, limit=args.limit)

if __name__ == "__main__":
    main()
