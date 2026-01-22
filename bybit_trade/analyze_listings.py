#!/usr/bin/env python3
"""
Explore Bybit Listing Data (EDA).
Calculates key metrics for new token listings to identify trading patterns.
"""

import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir / 'scripts_v3'))
sys.path.append(str(parent_dir / 'config'))

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir / 'scripts_v3'))
sys.path.append(str(parent_dir / 'config'))

from pump_analysis_lib import get_db_connection
import warnings

# Suppress pandas/sql alchemy warning
warnings.simplefilter(action='ignore', category=UserWarning)

OUTPUT_DIR = current_dir / "analysis_results"
OUTPUT_DIR.mkdir(exist_ok=True)

# removed get_db_engine since we use pump_analysis_lib

def calculate_metrics(df, symbol):
    """Calculate key listing metrics from DataFrame."""
    if df.empty:
        return None
    
    # Normalize start time (t=0 is listing time)
    start_time = df.index.min()
    
    # 1. Opening stats
    first_1m = df.iloc[:60]
    # Sometimes opening price might be missing or 0, take first valid
    opening_price = df.iloc[0]['open_price']
    
    # Max pump in first 1m
    max_price_1m = first_1m['high_price'].max()
    max_pump_1m_pct = (max_price_1m - opening_price) / opening_price * 100
    
    # 2. First 1 Hour stats
    first_1h = df.iloc[:3600]
    max_price_1h = first_1h['high_price'].max()
    max_pump_1h_pct = (max_price_1h - opening_price) / opening_price * 100
    
    # Time to ATH in first hour (seconds from start)
    ath_idx = first_1h['high_price'].idxmax()
    
    # Calculate difference
    diff = ath_idx - start_time
    
    # Handle both Timestamp (timedelta result) and integer/float (seconds result)
    if hasattr(diff, 'total_seconds'):
        time_to_ath_s = diff.total_seconds()
    else:
        time_to_ath_s = float(diff)
    
    # 3. First Dip (Max drawdown from ATH in first hour)
    # We look for the lowest low AFTER the ATH
    post_ath_data = first_1h.loc[ath_idx:]
    if not post_ath_data.empty:
        min_price_post_ath = post_ath_data['low_price'].min()
        first_dip_drawdown_pct = (max_price_1h - min_price_post_ath) / max_price_1h * 100
    else:
        first_dip_drawdown_pct = 0
        
    # 4. Volume Profile (Approximate)
    total_vol = first_1h['volume'].sum()
    buy_vol = first_1h['buy_volume'].sum()
    sell_vol = first_1h['sell_volume'].sum()
    buy_ratio = buy_vol / total_vol if total_vol > 0 else 0
    
    # 5. Volatility & Swings (The "Super-Trader" metrics)
    # We want to find how many times we could have entered/exited.
    # Count swings > 3%
    
    swing_count_3pct = 0
    swing_count_5pct = 0
    total_swing_pnl = 0.0
    
    # Simple ZigZag-like algorithm to count swings
    last_swing_low = df.iloc[0]['low_price']
    last_swing_high = df.iloc[0]['high_price']
    direction = 1 # 1 = searching for high, -1 = searching for low
    
    # Downsample to 1m for speed (or use 1s but iterate carefully)
    # Using 1s for precision since we have it
    prices = df['close_price'].values
    
    current_leg_start = prices[0]
    
    for p in prices:
        move_pct = (p - current_leg_start) / current_leg_start * 100
        
        if direction == 1: # Uptrend look
            if move_pct > 3: # Confirmed up swing > 3%
                # Potential profit locked? No, we wait for reversal.
                pass
            if move_pct < -3: # Reversal down > 3%
                # Previous up swing ended.
                swing_count_3pct += 1
                if min(abs(move_pct), abs((p - last_swing_high)/last_swing_high*100)) > 5:
                    swing_count_5pct += 1
                
                direction = -1
                current_leg_start = p
                last_swing_high = p # Reset high marker
        
        elif direction == -1: # Downtrend look
            if move_pct > 3: # Reversal up > 3%
                swing_count_3pct += 1
                if move_pct > 5:
                    swing_count_5pct += 1
                
                direction = 1
                current_leg_start = p
                last_swing_low = p # Reset low marker

    return {
        'symbol': symbol,
        'opening_price': opening_price,
        'max_pump_1h_pct': round(max_pump_1h_pct, 2),
        'max_pump_1h_pct': round(max_pump_1h_pct, 2),
        'swings_gt_3pct': swing_count_3pct,
        'swings_gt_5pct': swing_count_5pct,
        'time_to_ath_s': int(time_to_ath_s),
        'first_dip_drawdown_pct': round(first_dip_drawdown_pct, 2),
        'buy_vol_ratio': round(buy_ratio, 2),
    }

def main():
    print("🚀 Deep Analysis: Counting Tradable Swings (1s Data)")
    
    conn = None
    try:
        conn = get_db_connection()
        
        listings_df = pd.read_sql(
            "SELECT id, symbol, listing_date FROM bybit_trade.listings WHERE data_fetched = TRUE", 
            conn
        )
        
        results = []
        
        for _, row in listings_df.iterrows():
            lid = row['id']
            symbol = row['symbol']
            
            # Using 1s data for precision
            df = pd.read_sql(
                f"SELECT timestamp_s, open_price, close_price, high_price, low_price, volume, buy_volume, sell_volume FROM bybit_trade.candles_1s WHERE listing_id = {lid} ORDER BY timestamp_s ASC",
                conn
            )
            
            if df.empty: continue
            
            metrics = calculate_metrics(df, symbol)
            if metrics:
                results.append(metrics)
                print(f"  ✓ {symbol:<10} Swings >3%: {metrics['swings_gt_3pct']:<3} | >5%: {metrics['swings_gt_5pct']:<3} (Potential Trades)")

        # Create DataFrame from results
        res_df = pd.DataFrame(results)
        
        print("\n📊 POTENTIAL TRADES (Volatility Swings):")
        print("-" * 60)
        print(f"Avg 'Swings > 3%' per token: {res_df['swings_gt_3pct'].mean():.1f}")
        print(f"Avg 'Swings > 5%' per token: {res_df['swings_gt_5pct'].mean():.1f}")
        print("-" * 60)
        
        top_volatile = res_df.sort_values('swings_gt_3pct', ascending=False).head(10)
        print("\nTOP 10 VOLATILE TOKENS (Most Opportunities):")
        print(top_volatile[['symbol', 'swings_gt_3pct', 'swings_gt_5pct', 'max_pump_1h_pct']])
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()
