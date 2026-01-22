#!/usr/bin/env python3
"""
Full Day (24h) Backtest for Bybit Listings.
Focus: Capturing large pumps that happen hours after listing (e.g. SKR, ZKP) 
while managing risk on "Pump & Bleed" coins.

Strategy: "Pure Momentum" (Price Only)
- Entry: Quick pump in first 30s-1m.
- Exit: Trailing Stop ONLY (no fixed time exit).
- Duration: Up to 24 hours.
"""

import sys
import warnings
from pathlib import Path
from dataclasses import dataclass
import pandas as pd
import numpy as np

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
    duration_s: int = 0
    max_price_seen: float = 0.0
    max_pnl_seen: float = 0.0

def load_candles_24h(conn, listing_id: int) -> pd.DataFrame:
    """Load first 24 hours of 1s candles."""
    query = """
        SELECT timestamp_s, open_price, high_price, low_price, close_price, volume
        FROM bybit_trade.candles_1s
        WHERE listing_id = %s
        ORDER BY timestamp_s ASC
        LIMIT 86400
    """
    df = pd.read_sql(query, conn, params=(listing_id,))
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp_s'], unit='s')
        df.set_index('timestamp', inplace=True)
    return df

def pure_momentum_24h(df: pd.DataFrame, symbol: str, 
                      pump_pct=2.0,      # Entry: +2% in 30s
                      trailing_stop_pct=0.10 # Exit: 10% trailing stop (looser to survive volatility)
                      ) -> list[Trade]:
    """
    Enter on initial momentum, hold until trailing stop hits or 24h end.
    """
    trades = []
    in_trade = False
    entry_price = 0.0
    entry_time = None
    highest_price = 0.0
    
    # 1. Check Entry Condition (First minute only)
    start_price = df.iloc[0]['open_price']
    
    # Iterate first 60 seconds for entry
    # Note: This is a simplification. We could scan the whole day, but we want to catch the "listing pump"
    # and ride it. If valid entry missed in min 1, we ignore (for now).
    
    entry_window = df.iloc[:60]
    
    for t, row in entry_window.iterrows():
        cur_price = row['close_price']
        change_from_start = (cur_price - start_price) / start_price * 100
        
        if change_from_start >= pump_pct:
            # BUY SIGNAL
            in_trade = True
            entry_price = cur_price
            entry_time = t
            highest_price = cur_price
            break
            
    if not in_trade:
        return []
        
    # 2. Manage Trade (Rest of the data)
    # Slice df from entry_time onwards
    trade_data = df.loc[entry_time:].iloc[1:] # Skip entry candle
    
    trade = Trade(symbol, entry_time, entry_price)
    trade.max_price_seen = entry_price
    
    for t, row in trade_data.iterrows():
        cur_price = row['close_price']
        
        # Update High
        if cur_price > highest_price:
            highest_price = cur_price
            trade.max_price_seen = highest_price
            
        # Trailing Stop Check
        # Stop price rises with highest_price
        stop_price = highest_price * (1.0 - trailing_stop_pct)
        
        if cur_price <= stop_price:
            # EXIT: Trailing Stop
            trade.exit_time = t
            trade.exit_price = cur_price
            trade.exit_reason = f"Trailing Stop (-{trailing_stop_pct:.0%})"
            break
            
    # End of Data Exit (if still open)
    if trade.exit_time is None:
        last_row = df.iloc[-1]
        trade.exit_time = df.index[-1]
        trade.exit_price = last_row['close_price']
        trade.exit_reason = "End of 24h"

    # Calculate PnL
    trade.pnl_pct = (trade.exit_price - trade.entry_price) / trade.entry_price * 100
    trade.duration_s = int((trade.exit_time - trade.entry_time).total_seconds())
    trade.max_pnl_seen = (trade.max_price_seen - trade.entry_price) / trade.entry_price * 100
    
    return [trade]

def main():
    conn = get_db_connection()
    try:
        listings = pd.read_sql(
            """
            SELECT id, symbol FROM bybit_trade.listings 
            WHERE data_fetched = TRUE 
            ORDER BY listing_date DESC LIMIT 23
            """, 
            conn
        )
        
        all_trades = []
        print("\n🚀 RUNNING 24H BACKTEST (Pure Momentum, 10% Trailing Stop)...")
        
        for _, row in listings.iterrows():
            lid, symbol = row['id'], row['symbol']
            
            # Load Data
            df = load_candles_24h(conn, lid)
            if df.empty: continue
            
            # Run Strategy
            trades = pure_momentum_24h(df, symbol, pump_pct=2.0, trailing_stop_pct=0.10)
            all_trades.extend(trades)
            
        # Report
        if not all_trades:
            print("No trades found.")
            return

        res = pd.DataFrame([vars(t) for t in all_trades])
        
        # Format for display
        display_cols = ['symbol', 'entry_price', 'exit_price', 'pnl_pct', 'max_pnl_seen', 'duration_s', 'exit_reason']
        res_display = res[display_cols].copy()
        res_display['pnl_pct'] = res_display['pnl_pct'].round(2)
        res_display['max_pnl_seen'] = res_display['max_pnl_seen'].round(2)
        res_display['duration_h'] = (res_display['duration_s'] / 3600).round(1)
        
        print("\n📄 TRADE LOG:")
        print(res_display[['symbol', 'pnl_pct', 'max_pnl_seen', 'duration_h', 'exit_reason']].to_string())
        
        print("\n💰 SUMMARY:")
        print(f"Total Trades: {len(res)}")
        print(f"Win Rate:     {(res['pnl_pct'] > 0).mean():.1%}")
        print(f"Avg PnL:      {res['pnl_pct'].mean():.2f}%")
        print(f"Total PnL:    {res['pnl_pct'].sum():.2f}% (uncompounded)")
        
        # Save to file
        out_path = project_root / 'analysis_results' / 'trades_24h.csv'
        out_path.parent.mkdir(exist_ok=True, parents=True)
        res.to_csv(out_path, index=False)
        print(f"\nSaved detailed results to {out_path}")

    finally:
        conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
