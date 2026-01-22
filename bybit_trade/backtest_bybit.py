#!/usr/bin/env python3
"""
Backtesting Engine for Bybit Listings.
Simulates trading strategies on 1-second candle data.
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Callable

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir / 'scripts_v3'))
sys.path.append(str(parent_dir / 'config'))

from pump_analysis_lib import get_db_connection

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

class Backtester:
    def __init__(self, conn):
        self.conn = conn
        self.listings = self._load_listings()
        self.results: List[Trade] = []
        
    def _load_listings(self):
        """Load listings meta data."""
        return pd.read_sql(
            "SELECT id, symbol, listing_date FROM bybit_trade.listings WHERE data_fetched = TRUE", 
            self.conn
        )
        
    def _load_data(self, listing_id: int) -> pd.DataFrame:
        """Load 1s data for a listing."""
        query = """
            SELECT 
                timestamp_s, open_price, high_price, low_price, close_price, 
                volume, buy_volume, sell_volume
            FROM bybit_trade.candles_1s
            WHERE listing_id = %s
            ORDER BY timestamp_s ASC
        """
        df = pd.read_sql(query, self.conn, params=(listing_id,))
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp_s'], unit='s')
            df.set_index('timestamp', inplace=True)
            
            # Calculate basic indicators
            df['vwap'] = (df['volume'] * df['close_price']).cumsum() / df['volume'].cumsum()
            df['cum_buy'] = df['buy_volume'].cumsum()
            df['cum_sell'] = df['sell_volume'].cumsum()
            df['delta_ratio'] = df['cum_buy'] / df['cum_sell']
            
        return df

    def run_strategy(self, strategy_func: Callable, **kwargs):
        """Run a strategy function on all listings."""
        print(f"🔄 Running Strategy: {strategy_func.__name__}...")
        self.results = []
        
        for _, row in self.listings.iterrows():
            lid = row['id']
            symbol = row['symbol']
            
            df = self._load_data(lid)
            if df.empty or len(df) < 60:
                continue
                
            trade = strategy_func(df, symbol, **kwargs)
            if trade:
                self.results.append(trade)
                print(f"  ✓ Trade on {symbol}: {trade.pnl_pct:.2f}% ({trade.exit_reason})")
        
        self._print_summary()
        
    def _print_summary(self):
        if not self.results:
            print("No trades executed.")
            return
            
        df = pd.DataFrame([t.__dict__ for t in self.results])
        total_pnl = df['pnl_pct'].sum()
        win_rate = (df['pnl_pct'] > 0).mean() * 100
        
        print("\n📊 BACKTEST RESULTS")
        print("-" * 40)
        print(f"Total Trades: {len(df)}")
        print(f"Win Rate:     {win_rate:.1f}%")
        print(f"Total PnL:    {total_pnl:.2f}%")
        print(f"Avg PnL:      {df['pnl_pct'].mean():.2f}%")
        print(f"Best Trade:   {df['pnl_pct'].max():.2f}% ({df.loc[df['pnl_pct'].idxmax(), 'symbol']})")
        print(f"Worst Trade:  {df['pnl_pct'].min():.2f}% ({df.loc[df['pnl_pct'].idxmin(), 'symbol']})")
        print("-" * 40)

# --- STRATEGIES ---

def strategy_opening_breakout(df: pd.DataFrame, symbol: str, wait_seconds=300, breakout_multiplier=1.01):
    """
    Wait 5 minutes. If price breaks the High of the first 5 mins -> Buy.
    Stop Loss: Low of first 5 mins.
    Take Profit: 3x risk.
    """
    # 1. Define range (first X seconds)
    if len(df) < wait_seconds + 60:
        return None
        
    initial_window = df.iloc[:wait_seconds]
    high_level = initial_window['high_price'].max()
    low_level = initial_window['low_price'].min()
    
    # 2. Look for breakout
    # Look at data AFTER the wait window
    trading_window = df.iloc[wait_seconds:]
    
    entry_price = 0
    entry_time = None
    
    for idx, row in trading_window.iterrows():
        if row['close_price'] > high_level * breakout_multiplier:
            entry_price = row['close_price']
            entry_time = idx
            break
            
    if not entry_price:
        return None
        
    # 3. Simulate Trade
    # TP/SL based on range
    risk = entry_price - low_level
    if risk <= 0: # Should not happen if entry > high > low
        return None
        
    tp_price = entry_price + (risk * 3) # 1:3 R:R
    sl_price = low_level
    
    # Check subsequent data for outcome
    trade_data = df.loc[entry_time:].iloc[1:] # Candles after entry
    
    for idx, row in trade_data.iterrows():
        # Check SL first (conservative)
        if row['low_price'] <= sl_price:
            return Trade(symbol, entry_time, entry_price, idx, sl_price, 
                        (sl_price - entry_price)/entry_price*100, "Stop Loss")
            
        # Check TP
        if row['high_price'] >= tp_price:
            return Trade(symbol, entry_time, entry_price, idx, tp_price, 
                        (tp_price - entry_price)/entry_price*100, "Take Profit")
                        
    # End of data exit
    final_price = trade_data.iloc[-1]['close_price']
    return Trade(symbol, entry_time, entry_price, trade_data.index[-1], final_price, 
                (final_price - entry_price)/entry_price*100, "End of Data")


def main():
    try:
        conn = get_db_connection()
        bt = Backtester(conn)
        
        # Test Strategy 1: Opening Breakout (5 min wait)
        bt.run_strategy(strategy_opening_breakout, wait_seconds=300)
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
