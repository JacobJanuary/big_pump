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
    # Strategy State
    trades = []
    in_trade = False
    current_trade = None
    
    # Session State
    session_high = df.iloc[0]['open_price']
    
    # We iterate through the ENTIRE dataframe for signals
    # However, for performance, we can just loop once.
    
    start_time = df.index[0]
    
    for t, row in df.iterrows():
        cur_price = row['close_price']
        
        # 1. Update Session High
        if cur_price > session_high:
            session_high = cur_price
            
        # 2. Manage Existing Trade
        if in_trade:
            # Update Trade High
            if cur_price > current_trade.max_price_seen:
                current_trade.max_price_seen = cur_price
                
            # Trailing Stop Check
            stop_price = current_trade.max_price_seen * (1.0 - trailing_stop_pct)
            
            if cur_price <= stop_price:
                # EXIT
                current_trade.exit_time = t
                current_trade.exit_price = cur_price
                current_trade.exit_reason = f"Trailing Stop (-{trailing_stop_pct:.0%})"
                current_trade.pnl_pct = (cur_price - current_trade.entry_price) / current_trade.entry_price * 100
                current_trade.duration_s = int((t - current_trade.entry_time).total_seconds())
                current_trade.max_pnl_seen = (current_trade.max_price_seen - current_trade.entry_price) / current_trade.entry_price * 100
                
                trades.append(current_trade)
                in_trade = False
                current_trade = None
                continue # Don't re-enter on the same candle
                
        # 3. Entry Logic (If not in trade)
        if not in_trade:
            # Condition A: Initial Pump (First 60s)
            time_since_start = (t - start_time).total_seconds()
            is_start_window = time_since_start <= 60
            
            buy_signal = False
            
            if is_start_window:
                # Check for +2% from open
                open_price = df.iloc[0]['open_price']
                if (cur_price - open_price) / open_price * 100 >= pump_pct:
                    buy_signal = True
                    
            else:
                # Condition B: Breakout Re-Entry (Price > Session High)
                # We need to be careful not to buy the *exact* same high that stopped us out unless it breaks higher.
                # Logic: If Current Close > Previous Session High (implies breakout)
                # But we just updated session_high above. 
                # Let's verify if this candle *made* the new high.
                if cur_price >= session_high and cur_price > df.iloc[0]['open_price']: # Ensure it's not just the start price
                     # To avoid buying top of every candle, maybe require % clearance? 
                     # For now, simplistic Breakout of Session High.
                     # But we must ensure we didn't JUST exit. 
                     # In this loop logic, exit happens first, so we won't re-enter same tick usually unless low then high.
                     buy_signal = True
            
            if buy_signal:
                in_trade = True
                current_trade = Trade(symbol, t, cur_price)
                current_trade.max_price_seen = cur_price
                # Update session high if this entry is the new high
                if cur_price > session_high:
                    session_high = cur_price

    # End of session handling
    if in_trade:
        current_trade.exit_time = df.index[-1]
        current_trade.exit_price = df.iloc[-1]['close_price']
        current_trade.exit_reason = "End of 24h"
        current_trade.pnl_pct = (current_trade.exit_price - current_trade.entry_price) / current_trade.entry_price * 100
        current_trade.duration_s = int((current_trade.exit_time - current_trade.entry_time).total_seconds())
        current_trade.max_pnl_seen = (current_trade.max_price_seen - current_trade.entry_price) / current_trade.entry_price * 100
        trades.append(current_trade)
        
    return trades

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
