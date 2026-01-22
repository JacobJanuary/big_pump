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
    cooldown_until = None  # Timestamp to wait until before re-entering
    
    # Session State
    session_high = df.iloc[0]['open_price']
    
    start_time = df.index[0]
    
    for t, row in df.iterrows():
        # Prices for this second
        close_price = row['close_price']
        low_price = row['low_price']
        high_price = row['high_price']
        
        # 1. Update Session High (using High of candle)
        if high_price > session_high:
            session_high = high_price
            
        # 2. Cooldown Check
        if cooldown_until and t < cooldown_until:
            continue
        if cooldown_until and t >= cooldown_until:
            cooldown_until = None
            
        # 3. Manage Existing Trade
        if in_trade:
            # Update Trade High
            if high_price > current_trade.max_price_seen:
                current_trade.max_price_seen = high_price
                
            # Trailing Stop Check (Use LOW price to see if we got wicked out)
            stop_price = current_trade.max_price_seen * (1.0 - trailing_stop_pct)
            
            # If Low dipped below Stop, we exit at Stop Price (or Open if gap down, but 1s is granular)
            # worst case: exit at stop_price. 
            # Note: In real life, slippage happens. We assume fill at stop_price.
            if low_price <= stop_price:
                # EXIT
                current_trade.exit_time = t
                current_trade.exit_price = stop_price # Assume fill at trigger
                current_trade.exit_reason = f"Trailing Stop (-{trailing_stop_pct:.0%})"
                current_trade.pnl_pct = (current_trade.exit_price - current_trade.entry_price) / current_trade.entry_price * 100
                current_trade.duration_s = int((t - current_trade.entry_time).total_seconds())
                current_trade.max_pnl_seen = (current_trade.max_price_seen - current_trade.entry_price) / current_trade.entry_price * 100
                
                trades.append(current_trade)
                in_trade = False
                current_trade = None
                
                # Set Cooldown (e.g., 5 minutes or 300s) to avoid whipsaw
                # volatility means we might enter again instantly and die.
                cooldown_until = t + pd.Timedelta(minutes=15)
                continue 
                
        # 4. Entry Logic (If not in trade)
        if not in_trade:
            # Condition A: Initial Pump (First 60s)
            time_since_start = (t - start_time).total_seconds()
            
            buy_signal = False
            entry_price = close_price # Default entry at close of signal candle
            
            if time_since_start <= 60:
                # Check for +2% from open
                open_price = df.iloc[0]['open_price']
                if (close_price - open_price) / open_price * 100 >= pump_pct:
                    buy_signal = True
            else:
                # Condition B: Breakout Re-Entry
                # Enter if Close passes the Session High (confirmed breakout)
                # To be safer, maybe ensure we aren't just hugging the high.
                # Logic: We want to catch the run above the previous peak.
                if close_price >= session_high and close_price > df.iloc[0]['open_price']:
                     buy_signal = True
            
            if buy_signal:
                in_trade = True
                current_trade = Trade(symbol, t, entry_price)
                current_trade.max_price_seen = high_price # Can be high of this candle too
                # Update session high if needed
                if high_price > session_high:
                    session_high = high_price

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
