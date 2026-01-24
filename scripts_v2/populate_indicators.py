#!/usr/bin/env python3
"""
Backfill script: Populates RSI, Volume Z-Score, and OI Delta 
into web.signal_analysis from fas_v2 tables
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.append(str(Path(__file__).resolve().parent))
from pump_analysis_lib import get_db_connection

def populate_indicators():
    conn = get_db_connection()
    
    print("Populating missing indicators in web.signal_analysis...")
    
    # Update query using JOIN
    # We join web.signal_analysis with scoring_history ON pair and timestamp
    # Then join indicators
    
    query = """
    UPDATE web.signal_analysis w
    SET 
        rsi = i.rsi,
        volume_zscore = i.volume_zscore,
        oi_delta_pct = i.oi_delta_pct
    FROM fas_v2.scoring_history sh
    JOIN fas_v2.sh_indicators shi ON shi.scoring_history_id = sh.id
    JOIN fas_v2.indicators i ON (
        i.trading_pair_id = shi.indicators_trading_pair_id 
        AND i.timestamp = shi.indicators_timestamp 
        AND i.timeframe = shi.indicators_timeframe
    )
    WHERE w.trading_pair_id = sh.trading_pair_id 
        AND w.signal_timestamp = sh.timestamp
        AND w.rsi = 0 -- Only update empty ones
    """
    
    print("Executing UPDATE batch (this might take a moment)...")
    with conn.cursor() as cur:
        cur.execute(query)
        updated_rows = cur.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"✅ Updated {updated_rows} rows with indicator data.")

if __name__ == "__main__":
    populate_indicators()
