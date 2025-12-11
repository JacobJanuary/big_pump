#!/usr/bin/env python3
"""
Step 1: Calculate is_win for all signals in web.signal_analysis

Populates is_win field:
- true: TP hit (+10%)
- false: SL hit (-10%)
- NULL: timeout (24 hours)

Entry: signal timestamp + 18 minutes
Uses 1-minute candles from public.candles
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add current directory to path
sys.path.append(str(Path(__file__).resolve().parent))
from pump_analysis_lib import get_db_connection

# Trading parameters
ENTRY_DELAY_MINUTES = 18
STOP_LOSS_PCT = -10.0
TAKE_PROFIT_PCT = 20.0
MAX_HOLDING_HOURS = 24

def get_entry_price(conn, trading_pair_id, entry_time):
    """Get entry price from 1-minute candles"""
    query = """
        SELECT open_price
        FROM public.candles
        WHERE trading_pair_id = %s
            AND interval_id = 1
            AND open_time = %s
        LIMIT 1
    """
    
    with conn.cursor() as cur:
        # DB uses Milliseconds!
        cur.execute(query, (trading_pair_id, int(entry_time.timestamp() * 1000)))
        result = cur.fetchone()
        return float(result[0]) if result else None

def simulate_trade(conn, trading_pair_id, entry_time, entry_price):
    """
    Simulate trade and return result
    Returns: True (win), False (loss), or None (timeout)
    """
    if not entry_price:
        return None
    
    sl_price = float(entry_price) * (1 + STOP_LOSS_PCT / 100)
    tp_price = float(entry_price) * (1 + TAKE_PROFIT_PCT / 100)
    end_time = entry_time + timedelta(hours=MAX_HOLDING_HOURS)
    
    # Fetch candles
    query = """
        SELECT open_time, low_price, high_price
        FROM public.candles
        WHERE trading_pair_id = %s
            AND interval_id = 1
            AND open_time > %s
            AND open_time <= %s
        ORDER BY open_time ASC
    """
    
    # Convert to unix timestamp MS (BIGINT) for public.candles
    entry_ts = int(entry_time.timestamp() * 1000)
    end_ts = int(end_time.timestamp() * 1000)
    
    with conn.cursor() as cur:
        cur.execute(query, (trading_pair_id, entry_ts, end_ts))
        candles = cur.fetchall()
    
    if not candles:
        return None
    
    # Check each candle for SL or TP
    for candle_time, low, high in candles:
        # Check SL first
        if low <= sl_price:
            return False  # Loss
        # Then TP
        if high >= tp_price:
            return True  # Win
    
    return None  # Timeout

def calculate_is_win_for_all():
    """Calculate is_win for all signals"""
    conn = get_db_connection()
    
    print("="*100)
    print("CALCULATING IS_WIN FOR ALL SIGNALS")
    print("="*100)
    print(f"\nParameters:")
    print(f"  Entry: Signal timestamp + {ENTRY_DELAY_MINUTES} minutes")
    print(f"  Stop Loss: {STOP_LOSS_PCT}%")
    print(f"  Take Profit: {TAKE_PROFIT_PCT}%")
    print(f"  Max Holding: {MAX_HOLDING_HOURS} hours")
    
    # Check if is_win column exists
    check_column_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'web' 
            AND table_name = 'signal_analysis' 
            AND column_name = 'is_win'
    """
    
    with conn.cursor() as cur:
        cur.execute(check_column_query)
        if not cur.fetchone():
            print("\n⚠️  Column 'is_win' does not exist. Creating...")
            cur.execute("""
                ALTER TABLE web.signal_analysis 
                ADD COLUMN is_win BOOLEAN DEFAULT NULL
            """)
            conn.commit()
            print("✅ Column created")
    
    # Get all signals
    query = """
        SELECT id, trading_pair_id, signal_timestamp, entry_price
        FROM web.signal_analysis
        WHERE is_win IS NULL
        ORDER BY signal_timestamp DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        signals = cur.fetchall()
    
    total = len(signals)
    print(f"\n📊 Found {total:,} signals to process")
    
    if total == 0:
        print("✅ All signals already processed")
        conn.close()
        return
    
    # Process signals
    wins = 0
    losses = 0
    timeouts = 0
    processed = 0
    
    print(f"\n{'='*100}")
    print("PROCESSING SIGNALS")
    print("="*100)
    
        
    # Collect updates for batch processing
    updates = []
    BATCH_SIZE = 1000
    
    print(f"\n{'='*100}")
    print("PROCESSING SIGNALS (BATCHED)")
    print("="*100)
    
    try:
        from psycopg2.extras import execute_batch
    except ImportError:
        # Fallback for psycopg 3 or if extras not avail
        def execute_batch(cur, sql, args_list):
            for args in args_list:
                cur.execute(sql, args)
    
    for sig_id, trading_pair_id, signal_ts, stored_entry_price in signals:
        processed += 1
        
        # Calculate entry time
        entry_time = signal_ts + timedelta(minutes=ENTRY_DELAY_MINUTES)
        
        # Get entry price (use stored if available)
        entry_price = stored_entry_price if stored_entry_price else get_entry_price(conn, trading_pair_id, entry_time)
        
        if entry_price is None:
            if processed <= 5: # Only print first few errors
                print(f"\nDEBUG: Failed to get entry price for PairID {trading_pair_id}, Time {entry_time} "
                      f"(TS: {int(entry_time.timestamp() * 1000)})")
            timeouts += 1
            updates.append((None, sig_id))
        else:
            # Simulate trade
            is_win = simulate_trade(conn, trading_pair_id, entry_time, entry_price)
            updates.append((is_win, sig_id))
        
            # Track stats
            if is_win is True:
                wins += 1
            elif is_win is False:
                losses += 1
            else:
                timeouts += 1
        
        # Execute batch
        if len(updates) >= BATCH_SIZE:
            with conn.cursor() as cur:
                execute_batch(cur, "UPDATE web.signal_analysis SET is_win = %s WHERE id = %s", updates)
            conn.commit()
            updates = []
            
            print(f"Progress: {processed:,}/{total:,} ({processed/total*100:.1f}%) - "
                  f"W:{wins} L:{losses} T:{timeouts}", end='\r')
    
    # Process remaining
    if updates:
        with conn.cursor() as cur:
             execute_batch(cur, "UPDATE web.signal_analysis SET is_win = %s WHERE id = %s", updates)
        conn.commit()
    
    # Final commit
    conn.commit()
    conn.close()
    
    print(f"\n\n{'='*100}")
    print("CALCULATION COMPLETE")
    print("="*100)
    print(f"\nProcessed: {processed:,} signals")
    print(f"Wins: {wins:,} ({wins/processed*100:.2f}%)")
    print(f"Losses: {losses:,} ({losses/processed*100:.2f}%)")
    print(f"Timeouts: {timeouts:,} ({timeouts/processed*100:.2f}%)")
    
    trades = wins + losses
    if trades > 0:
        win_rate = wins / trades * 100
        print(f"\nWin Rate (excluding timeouts): {win_rate:.2f}%")

if __name__ == '__main__':
    calculate_is_win_for_all()
