"""
Analyze 1-minute candles for each signal to determine outcome metrics.
Updates web.big_pump_signals with:
- time_to_sl_10_pr_seconds
- max_price
- max_grow_pr
- time_to_max_price_seconds
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timezone
import math

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

def analyze_outcomes():
    print("Analyzing signal outcomes (SL 10% and Max Growth)...")
    
    try:
        with get_db_connection() as conn:
            # 1. Fetch all signals
            # We need id, entry_price, entry_time
            query_signals = """
                SELECT id, entry_price, entry_time, pair_symbol
                FROM web.big_pump_signals
                ORDER BY id
            """
            
            with conn.cursor() as cur:
                cur.execute(query_signals)
                signals = cur.fetchall()
            
            if not signals:
                print("No signals found in web.big_pump_signals.")
                return
                
            print(f"Found {len(signals)} signals. Processing...")
            
            updated_count = 0
            
            for i, signal in enumerate(signals, 1):
                sig_id = signal[0]
                entry_price = float(signal[1])
                entry_time_dt = signal[2] # datetime with tz
                symbol = signal[3]
                
                # Convert entry time to ms for comparison with candle timestamps
                entry_time_ms = int(entry_time_dt.timestamp() * 1000)
                
                # Calculate SL Price (10% drop)
                sl_price = entry_price * 0.90
                
                # 2. Fetch candles for this signal
                # Ordered by time ASC
                query_candles = """
                    SELECT open_time, high_price, low_price
                    FROM web.big_pump_minute_candles
                    WHERE signal_analysis_id = %s
                    AND open_time >= %s -- distinct check, candles should be >= entry
                    ORDER BY open_time ASC
                """
                
                with conn.cursor() as cur:
                    cur.execute(query_candles, (sig_id, entry_time_ms))
                    candles = cur.fetchall()
                
                if not candles:
                    print(f"  Warning: No candles found for signal {sig_id} ({symbol}). Skipping.")
                    continue
                    
                # --- Step 1: Find time to SL ---
                time_to_sl_seconds = 86400 # Default to 24h if not hit
                sl_hit_index = -1
                
                for idx, candle in enumerate(candles):
                    # candle: 0=open_time, 1=high, 2=low
                    c_time = candle[0]
                    c_low = float(candle[2])
                    
                    if c_low <= sl_price:
                        sl_hit_index = idx
                        # Calculate time delta
                        # candle open time is start of minute. 
                        # technically SL could happen anytime in that minute. 
                        # using open_time difference is close enough.
                        time_to_sl_seconds = int((c_time - entry_time_ms) / 1000)
                        # Ensure non-negative (if candle time < entry time slightly due to alignment)
                        if time_to_sl_seconds < 0: time_to_sl_seconds = 0
                        break
                
                # If SL not hit, time_to_sl_seconds remains 86400 (or duration of available data?)
                # Requirement: "Analyze minute candles in interval between entry_time and entry_time+time_to_sl"
                # If SL not hit, we analyze all available candles (up to 24h).
                
                # --- Step 2: Find Max Price in Window ---
                # Window is candles[0] to candles[sl_hit_index] (inclusive of the SL candle? 
                # Yes, price could pump then dump to SL in same candle. Ideally we check high first?
                # But requirement says "interval... entry to SL". 
                # Let's assume we scan up to the SL candle inclusive.
                
                scan_candles = candles
                if sl_hit_index != -1:
                    scan_candles = candles[:sl_hit_index+1]
                
                max_price = -1.0
                max_price_time_ms = entry_time_ms
                
                for candle in scan_candles:
                    c_time = candle[0]
                    c_high = float(candle[1])
                    
                    if c_high > max_price:
                        max_price = c_high
                        max_price_time_ms = c_time
                
                # If for some reason max_price is still -1 (empty slice?), start from entry
                if max_price == -1.0:
                    max_price = entry_price
                    max_price_time_ms = entry_time_ms

                # Metrics
                max_grow_pct = ((max_price - entry_price) / entry_price) * 100
                max_grow_pr_int = int(round(max_grow_pct))
                
                time_to_max_seconds = int((max_price_time_ms - entry_time_ms) / 1000)
                if time_to_max_seconds < 0: time_to_max_seconds = 0
                
                # --- Update DB ---
                update_query = """
                    UPDATE web.big_pump_signals
                    SET 
                        time_to_sl_10_pr_seconds = %s,
                        max_price = %s,
                        max_grow_pr = %s,
                        time_to_max_price_seconds = %s
                    WHERE id = %s
                """
                
                with conn.cursor() as cur:
                    cur.execute(update_query, (
                        time_to_sl_seconds,
                        max_price,
                        max_grow_pr_int,
                        time_to_max_seconds,
                        sig_id
                    ))
                
                updated_count += 1
                
                if i % 50 == 0:
                    print(f"  Processed {i}/{len(signals)} signals...", end='\r')

            conn.commit()
            print(f"\nSuccessfully analyzed and updated {updated_count} signals.")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_outcomes()
