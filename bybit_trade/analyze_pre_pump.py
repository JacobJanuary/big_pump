#!/usr/bin/env python3
"""
Deep Dive Analysis: Pre-Pump Conditions
Compare "Winners" (SKR, ZKP, MONPRO, PIEVERSE, STABLE) vs "Losers" (SCOR, MMT, LITKEY, EAT, COMMON).

Goal: Identify distinguishing features *before* the main move.
Metrics:
1. Volume Decay: Does volume dry up before the second pump?
2. Volatility Compression: Is there a "squeeze"?
3. Delta: Is smart money accumulating? (Buy/Sell ratio)
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

WINNERS = ['SKR', 'ZKP', 'MONPRO', 'PIEVERSE', 'STABLE']
LOSERS = ['SCOR', 'MMT', 'LITKEY', 'EAT', 'COMMON']

def load_data(conn, symbol: str) -> pd.DataFrame:
    sql = """
        SELECT c.timestamp_s, c.open_price, c.high_price, c.low_price, c.close_price, c.volume, c.buy_volume, c.sell_volume
        FROM bybit_trade.candles_1s c
        JOIN bybit_trade.listings l ON c.listing_id = l.id
        WHERE l.symbol = %s
        ORDER BY c.timestamp_s ASC
        LIMIT 86400 
    """
    df = pd.read_sql(sql, conn, params=(symbol,))
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp_s'], unit='s')
        df.set_index('timestamp', inplace=True)
        # Resample to 5min for clearer patterns
        df_5m = df.resample('5min').agg({
            'open_price': 'first',
            'high_price': 'max',
            'low_price': 'min',
            'close_price': 'last',
            'volume': 'sum',
            'buy_volume': 'sum',
            'sell_volume': 'sum'
        })
        return df_5m
    return df

def analyze_token(df, name):
    if df.empty: return None
    
    # 1. Normalize Price (Start = 100)
    start_price = df.iloc[0]['open_price']
    df['norm_price'] = df['close_price'] / start_price * 100
    
    # 2. Volume Profile
    # Avg volume in first hour vs hours 2-4
    vol_h1 = df.iloc[:12]['volume'].mean() # First 12 5-min candles
    vol_h2_4 = df.iloc[12:48]['volume'].mean() # Next 3 hours
    vol_decay = vol_h2_4 / vol_h1 if vol_h1 > 0 else 0
    
    # 3. Volatility (Std Dev of returns in hours 2-4)
    df['returns'] = df['close_price'].pct_change()
    volatility_h2_4 = df.iloc[12:48]['returns'].std() * 100
    
    # 4. Delta / Accumulation (Buy/Sell Ratio in hours 2-4)
    # Are they buying the dip/chop?
    subset = df.iloc[12:48]
    buy_vol = subset['buy_volume'].sum()
    sell_vol = subset['sell_volume'].sum()
    delta_ratio = buy_vol / sell_vol if sell_vol > 0 else 0
    
    # 5. Price Trend (Linear Regression Slope of hours 2-4)
    # Is it slowly grinding up or down?
    y = subset['norm_price'].values
    x = np.arange(len(y))
    if len(y) > 0:
        slope, _ = np.polyfit(x, y, 1)
    else:
        slope = 0
        
    return {
        'Symbol': name,
        'Vol_Decay': round(vol_decay, 3), # Lower = dried up more
        'Volatility': round(volatility_h2_4, 3), # Lower = more compression
        'Delta_Ratio': round(delta_ratio, 2), # >1 means buying pressure
        'Trend_Slope': round(slope, 3) # >0 means drift up, <0 means drift down
    }

def main():
    conn = get_db_connection()
    try:
        results = []
        
        print(f"Analyzing {len(WINNERS)} Winners vs {len(LOSERS)} Losers...")
        print("Focus Period: Hours 2-4 after listing (Consolidation Phase)\n")
        
        for sym in WINNERS:
            df = load_data(conn, sym)
            res = analyze_token(df, sym)
            if res: 
                res['Type'] = 'WINNER'
                results.append(res)
                
        for sym in LOSERS:
            df = load_data(conn, sym)
            res = analyze_token(df, sym)
            if res: 
                res['Type'] = 'LOSER'
                results.append(res)
                
        df_res = pd.DataFrame(results)
        
        cols = ['Symbol', 'Type', 'Vol_Decay', 'Volatility', 'Delta_Ratio', 'Trend_Slope']
        print(df_res[cols].sort_values(by='Type', ascending=False).to_string(index=False))
        
        # Averages
        print("\n📊 AVERAGES BY GROUP")
        print(df_res.groupby('Type')[['Vol_Decay', 'Volatility', 'Delta_Ratio', 'Trend_Slope']].mean())
        
        print("\n🔍 HYPOTHESIS CHECK:")
        print("1. Winners should have LOWER Volatility (Quiet accumulation)?")
        print("2. Winners should have HIGHER Delta Ratio (Hidden buying)?")
        print("3. Winners should have stable/positive Trend Slope?")

    finally:
        conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
