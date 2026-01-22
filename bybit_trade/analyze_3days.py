#!/usr/bin/env python3
"""
3-Day History Analysis (Patterns & Extremes).
Analyzes the first 72h of 1-second candles to find:
1. Extremum Timing (Time to Low / Time to High).
2. Patterns (V-Shape recovery, Slow Bleed, or Instant Pump).
3. Volume Profile (where volume is concentrated).

Run:
    python bybit_trade/analyze_3days.py
"""

import sys
import warnings
from pathlib import Path
import pandas as pd
import numpy as np

# Add scripts_v3 to path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root / 'scripts_v3'))

from pump_analysis_lib import get_db_connection

warnings.simplefilter("ignore", UserWarning)

def load_data(conn, listing_id: int, hours=72) -> pd.DataFrame:
    """Load first 72 hours of data."""
    sql = """
        SELECT timestamp_s, open_price, high_price, low_price, close_price, volume
        FROM bybit_trade.candles_1s
        WHERE listing_id = %s
        ORDER BY timestamp_s ASC
        LIMIT %s
    """
    # 72 hours * 3600 seconds = 259200 rows
    limit = hours * 3600
    df = pd.read_sql(sql, conn, params=(listing_id, limit))
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp_s"], unit="s")
        df.set_index("timestamp", inplace=True)
    return df

def analyze_extremums(df: pd.DataFrame, listing_date: pd.Timestamp):
    """Find Min/Max and their timing relative to listing."""
    if df.empty: return {}
    
    # Global Min/Max
    min_price = df['low_price'].min()
    max_price = df['high_price'].max()
    
    # Times
    min_time = df['low_price'].idxmin()
    max_time = df['high_price'].idxmax()
    
    # Deltas
    hours_to_min = (min_time - listing_date).total_seconds() / 3600.0
    hours_to_max = (max_time - listing_date).total_seconds() / 3600.0
    
    start_price = df.iloc[0]['open_price']
    
    return {
        'min_price': min_price,
        'max_price': max_price,
        'hours_to_min': round(hours_to_min, 1),
        'hours_to_max': round(hours_to_max, 1),
        'max_drawdown_pct': round((min_price - start_price) / start_price * 100, 2),
        'max_pump_pct': round((max_price - start_price) / start_price * 100, 2),
    }

def detect_pattern(df: pd.DataFrame, stats: dict) -> str:
    """Classify 3-day pattern."""
    if df.empty: return "No Data"
    
    h_min = stats['hours_to_min']
    h_max = stats['hours_to_max']
    
    # Pattern Logic
    if h_max < 1.0 and h_min > 24.0:
        return "Pump & Bleed" # Pumped early, then bled out
    
    if h_min < 1.0 and h_max > 24.0:
        return "Dip & Rip" # Dipped early, then grew
        
    if h_min < 6.0 and h_max < 6.0 and abs(h_max - h_min) < 2.0:
        return "Volatile Start" # All action in first hours
    
    if stats['max_drawdown_pct'] < -50 and h_min > 48:
        return "Slow Death" # Just keeps dropping
        
    return "Mixed"

def main():
    conn = get_db_connection()
    try:
        listings = pd.read_sql(
            "SELECT id, symbol, listing_date FROM bybit_trade.listings WHERE data_fetched = TRUE ORDER BY listing_date DESC LIMIT 23",
            conn
        )
        
        report = []
        
        for _, row in listings.iterrows():
            lid, symbol = row['id'], row['symbol']
            listing_date = pd.to_datetime(row['listing_date'], unit='s')
            
            df = load_data(conn, lid)
            if df.empty: continue
            
            # Resample to 1min for noise reduction if needed, but 1s is fine for extremes
            stats = analyze_extremums(df, df.index[0]) # Use actual data start as listing time
            pattern = detect_pattern(df, stats)
            
            stats['symbol'] = symbol
            stats['pattern'] = pattern
            stats['hours_avail'] = round((df.index[-1] - df.index[0]).total_seconds()/3600, 1)
            
            report.append(stats)
            
        # Output
        res = pd.DataFrame(report)
        cols = ['symbol', 'pattern', 'hours_to_min', 'max_drawdown_pct', 'hours_to_max', 'max_pump_pct', 'hours_avail']
        print("\n📊 3-DAY PATTERN ANALYSIS")
        print("-" * 80)
        print(res[cols].to_string())
        
        # Aggregation
        print("\n🧠 INSIGHTS")
        print(f"Avg Time to Bottom: {res['hours_to_min'].mean():.1f} hours")
        print(f"Avg Time to ATH:    {res['hours_to_max'].mean():.1f} hours")
        print("\nPattern Distribution:")
        print(res['pattern'].value_counts())

    finally:
        conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
