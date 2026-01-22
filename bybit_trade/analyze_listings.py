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
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

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
    time_to_ath_s = (ath_idx - start_time).total_seconds()
    
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
    
    # 5. Volatility
    # Resample to 1m
    resampled_1m = df['close_price'].resample('1min').ohlc()
    
    # Fix: access columns directly from DataFrame
    resampled_1m['volatility_pct'] = (resampled_1m['high'] - resampled_1m['low']) / resampled_1m['open'] * 100
    
    if len(resampled_1m) >= 15:
        avg_volatility_first_15m = resampled_1m.iloc[:15]['volatility_pct'].mean()
    else:
        avg_volatility_first_15m = resampled_1m['volatility_pct'].mean()
        
    if len(resampled_1m) >= 60:
        avg_volatility_next_45m = resampled_1m.iloc[15:60]['volatility_pct'].mean()
    else:
        avg_volatility_next_45m = resampled_1m.iloc[15:]['volatility_pct'].mean()

    return {
        'symbol': symbol,
        'opening_price': opening_price,
        'max_pump_1m_pct': round(max_pump_1m_pct, 2),
        'max_pump_1h_pct': round(max_pump_1h_pct, 2),
        'time_to_ath_s': int(time_to_ath_s),
        'first_dip_drawdown_pct': round(first_dip_drawdown_pct, 2),
        'buy_vol_ratio': round(buy_ratio, 2),
        'volatility_15m': round(avg_volatility_first_15m, 2),
        'volatility_rest': round(avg_volatility_next_45m, 2)
    }

def main():
    print("🚀 Analyzing Bybit Listings (1s Data)")
    
    conn = None
    try:
        conn = get_db_connection()
        
        # Get all listings
        listings_df = pd.read_sql(
            "SELECT id, symbol, listing_date FROM bybit_trade.listings WHERE data_fetched = TRUE", 
            conn
        )
        
        print(f"Found {len(listings_df)} listings with data.")
        
        results = []
        
        for _, row in listings_df.iterrows():
            lid = row['id']
            symbol = row['symbol']
            
            # Use conn for pandas read_sql (will trigger warning but work)
            query = f"""
                SELECT timestamp_s, open_price, high_price, low_price, close_price, 
                       volume, buy_volume, sell_volume, trade_count
                FROM bybit_trade.candles_1s
                WHERE listing_id = {lid}
                ORDER BY timestamp_s ASC
            """
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print(f"⚠️ {symbol}: No data")
                continue
            
            df['timestamp'] = pd.to_datetime(df['timestamp_s'], unit='s')
            df.set_index('timestamp', inplace=True)
                
            metrics = calculate_metrics(df, symbol)
            if metrics:
                results.append(metrics)
                pump_str = f"{metrics['max_pump_1h_pct']}%"
                dip_str = f"-{metrics['first_dip_drawdown_pct']}%"
                print(f"  ✓ {symbol:<10} Pump: {pump_str:>8} | Dip: {dip_str:>8}")
        
        # Create DataFrame from results
        res_df = pd.DataFrame(results)
        
        # Save results
        json_path = OUTPUT_DIR / "listing_metrics.json"
        csv_path = OUTPUT_DIR / "listing_metrics.csv"
        
        res_df.to_json(json_path, orient='records', indent=2)
        res_df.to_csv(csv_path, index=False)
        
        print(f"\n[DONE] Saved metrics to {json_path}")
        
        if not res_df.empty:
            print("\n📊 SUMMARY STATISTICS (First 1 Hour):")
            print("-" * 50)
            print(f"Average Pump 1H:      {res_df['max_pump_1h_pct'].mean():.2f}%")
            print(f"Average Dip from ATH: {res_df['first_dip_drawdown_pct'].mean():.2f}%")
            print(f"Avg Time to ATH:      {res_df['time_to_ath_s'].mean() / 60:.1f} minutes")
            print(f"Avg Buy Pressure:     {res_df['buy_vol_ratio'].mean():.2f}")
            print("-" * 50)
            
            print("\nTOP 5 PUMPS:")
            print(res_df.sort_values('max_pump_1h_pct', ascending=False)[['symbol', 'max_pump_1h_pct', 'time_to_ath_s']].head(5))
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
