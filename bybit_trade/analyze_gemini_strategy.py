#!/usr/bin/env python3
"""
Backtest: Gemini AI Optimization Strategy.
Reverse-engineered logic from "Winning Algorithm" analysis.

Rules:
1. Entry Window: 60-300 mins.
2. Survival Check: Price > 0.8 * Listing Open.
3. Smart Money: DeltaRatio SMA(3) > 1.15 OR Max Delta(30m) > 2.5.
4. Volume Decay: Volume < 0.5 * Peak Volume.
5. Trigger: Price Cross > VWAP (1h).

Exit:
1. Invalidation: Delta < 0.6 AND Red Candle.
2. Trailing Stop: Close < 0.95 * VWAP (1h).
"""

import sys
import warnings
from pathlib import Path
from dataclasses import dataclass
import pandas as pd
import numpy as np
import pandas_ta as ta

# Add scripts_v3 to path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root / 'scripts_v3'))

from pump_analysis_lib import get_db_connection

warnings.simplefilter("ignore", UserWarning)

@dataclass
class Trade:
    symbol: str
    entry_time: pd.Timestamp
    entry_price: float
    exit_time: pd.Timestamp = None
    exit_price: float = None
    pnl_pct: float = 0.0
    exit_reason: str = ""
    duration_m: int = 0
    max_pnl_seen: float = 0.0

def load_data_1m(conn, listing_id: int) -> pd.DataFrame:
    """Load first 6 hours of 1-minute candles (enough for 300m window)."""
    # Loading 1-second data and resampling locally to ensure accurate indicators
    query = """
        SELECT timestamp_s, open_price, high_price, low_price, close_price, volume, buy_volume, sell_volume
        FROM bybit_trade.candles_1s
        WHERE listing_id = %s
        ORDER BY timestamp_s ASC
        LIMIT 43200 
    """ # 12 hours = 43200 seconds
    
    df = pd.read_sql(query, conn, params=(listing_id,))
    if df.empty: return df
    
    df['timestamp'] = pd.to_datetime(df['timestamp_s'], unit='s')
    df.set_index('timestamp', inplace=True)
    
    # Resample to 1 minute
    df_1m = df.resample('1min').agg({
        'open_price': 'first',
        'high_price': 'max',
        'low_price': 'min',
        'close_price': 'last',
        'volume': 'sum',
        'buy_volume': 'sum',
        'sell_volume': 'sum'
    })
    
    # Derived Metrics
    # Delta Ratio (Handle div by zero)
    df_1m['sell_volume'] = df_1m['sell_volume'].replace(0, 0.0001)
    df_1m['delta_ratio'] = df_1m['buy_volume'] / df_1m['sell_volume']
    
    # Indicators
    # VWAP 1h (Rolling 60 periods) - Approximation
    # Proper VWAP resets, but rolling VWAP is fine for this context
    # Standard: (Cum(Price*Vol) / Cum(Vol)) over window
    pv = df_1m['close_price'] * df_1m['volume']
    df_1m['vwap_1h'] = pv.rolling(60).sum() / df_1m['volume'].rolling(60).sum()
    
    # Delta SMA 3
    df_1m['delta_sma_3'] = df_1m['delta_ratio'].rolling(3).mean()
    
    # Max Delta Last 30m
    df_1m['max_delta_30m'] = df_1m['delta_ratio'].rolling(30).max()
    
    # Max Vol Session (expanding max)
    df_1m['max_vol_session'] = df_1m['volume'].expanding().max()
    
    # Listing Open (First Open)
    df_1m['listing_open'] = df_1m.iloc[0]['open_price']
    
    return df_1m

def run_gemini_strategy(df: pd.DataFrame, symbol: str) -> list[Trade]:
    trades = []
    in_trade = False
    current_trade = None
    
    start_time = df.index[0]
    
    # Iterate
    # Skip first 60 mins for indicators to warm up + entry rule
    for t, row in df.iloc[60:].iterrows():
        
        # Calculate time offset in minutes
        time_offset_m = (t - start_time).total_seconds() / 60
        
        # 3. EXIT LOGIC (Check first if in trade)
        if in_trade:
            current_trade.max_pnl_seen = max(current_trade.max_pnl_seen, 
                                           (row['high_price'] - current_trade.entry_price) / current_trade.entry_price * 100)
            
            should_exit = False
            exit_reason = ""
            
            # Rule 1: Invalidation (Delta Bearish Flip)
            # "If DeltaRatio < 0.6 and Close < Open"
            if row['delta_ratio'] < 0.6 and row['close_price'] < row['open_price']:
                should_exit = True
                exit_reason = "Delta Flip (<0.6)"
            
            # Rule 2: Trailing Stop (Close < 0.95 * VWAP)
            elif row['close_price'] < (row['vwap_1h'] * 0.95):
                should_exit = True
                exit_reason = "Lost VWAP (-5%)"
            
            # Rule 3: End of Data
            if should_exit:
                current_trade.exit_time = t
                current_trade.exit_price = row['close_price']
                current_trade.exit_reason = exit_reason
                current_trade.pnl_pct = (current_trade.exit_price - current_trade.entry_price) / current_trade.entry_price * 100
                current_trade.duration_m = int(time_offset_m - (current_trade.entry_time - start_time).total_seconds()/60)
                
                trades.append(current_trade)
                in_trade = False
                current_trade = None
                continue
                
        # 1. ENTRY LOGIC
        if not in_trade:
            # 1. TIME FILTER: 60 - 300 mins
            if time_offset_m > 300:
                continue # Too late
                
            # 2. DRAWDOWN LIMIT: Close > 0.8 * Listing Open
            if row['close_price'] < (0.8 * row['listing_open']):
                continue
                
            # 3. SMART MONEY: SMA(3) > 1.15 OR Max(30m) > 2.5
            is_accumulating = (row['delta_sma_3'] > 1.15) or (row['max_delta_30m'] > 2.5)
            if not is_accumulating:
                continue
                
            # 4. VOLUME DECAY: Vol < 0.5 * Max Session
            if row['volume'] > (row['max_vol_session'] * 0.5):
                # But wait, breakout often has high volume. 
                # The Gemini rule implies "Volume should have cooled off from opening chaos".
                # If this is the breakout candle, volume might spike. 
                # Let's check previous candle for decay, or assume strictly adhering to rule.
                # Rule says: "Do not enter if Volume > ..." at entry time.
                # Let's allow Entry if volume IS spiking now (breakout), so maybe check volume of *previous* period?
                # Sticking to strict AI rule for now: Current Vol < 50% Peak.
                continue
                
            # 5. TRIGGER: Cross > VWAP 1h
            # Check previous close was below VWAP (crossover)
            # We need previous row.
            prev_idx = df.index.get_loc(t) - 1
            if prev_idx < 0: continue
            prev_row = df.iloc[prev_idx]
            
            # Cross Over
            if row['close_price'] > row['vwap_1h'] and prev_row['close_price'] <= prev_row['vwap_1h']:
                # BUY!
                in_trade = True
                current_trade = Trade(symbol, t, row['close_price'])
                current_trade.max_pnl_seen = 0.0
                
    # Force close at end
    if in_trade:
        last = df.iloc[-1]
        current_trade.exit_time = df.index[-1]
        current_trade.exit_price = last['close_price']
        current_trade.exit_reason = "End of Session"
        current_trade.pnl_pct = (current_trade.exit_price - current_trade.entry_price) / current_trade.entry_price * 100
        trades.append(current_trade)
        
    return trades

def main():
    conn = get_db_connection()
    try:
        listings = pd.read_sql(
            "SELECT id, symbol FROM bybit_trade.listings WHERE data_fetched = TRUE ORDER BY listing_date DESC LIMIT 25",
            conn
        )
        
        all_trades = []
        print("\n🧠 RUNNING GEMINI AI STRATEGY BACKTEST...")
        
        for _, row in listings.iterrows():
            lid, symbol = row['id'], row['symbol']
            df = load_data_1m(conn, lid)
            if df.empty or len(df) < 100: continue
            
            trades = run_gemini_strategy(df, symbol)
            all_trades.extend(trades)
            
        if not all_trades:
            print("No trades triggered by AI rules.")
            return
            
        res = pd.DataFrame([vars(t) for t in all_trades])
        
        cols = ['symbol', 'entry_time', 'pnl_pct', 'max_pnl_seen', 'duration_m', 'exit_reason']
        print("\n📄 AI TRADE LOG:")
        print(res[cols].to_string())
        
        print("\n💰 SUMMARY:")
        print(f"Total Trades: {len(res)}")
        win_rate = (res['pnl_pct'] > 0).mean()
        print(f"Win Rate:     {win_rate:.1%}")
        print(f"Avg PnL:      {res['pnl_pct'].mean():.2f}%")
        print(f"Total PnL:    {res['pnl_pct'].sum():.2f}%")

    finally:
        conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
