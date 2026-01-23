#!/usr/bin/env python3
"""
Backtest: Gemini "Hybrid Runner" Strategy.
Logic:
1. Entry: Volume > 2x Avg, Delta > 1.2, Price > VWAP.
2. Management:
   - Partial TP: Sell 50% at +30% gain.
   - Panic Sell: Delta < 0.6 AND Price < Entry.
   - Dynamic Trailing Stop: High - (3 * ATR).
3. Re-Entry: Price > Last Exit Price AND Delta > 1.5.

Focus: Surviving 25-30% shakeouts (SKR) while cutting losers (ELSA, LIT) fast.
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
class TradePosition:
    symbol: str
    entry_time: pd.Timestamp
    entry_price: float
    size_pct: float = 1.0  # 1.0 = 100% position
    realized_pnl: float = 0.0
    
    # State tracking
    highest_price: float = 0.0
    tp_hit: bool = False
    
    exit_time: pd.Timestamp = None
    exit_price: float = None
    exit_reason: str = ""
    
    def calculate_total_pnl(self, current_price):
        # Value of remaining position
        unrealized = (current_price - self.entry_price) / self.entry_price * 100 * self.size_pct
        return self.realized_pnl + unrealized

def load_data_1m(conn, listing_id: int) -> pd.DataFrame:
    """Load first 12 hours of 1-minute candles."""
    query = """
        SELECT timestamp_s, open_price, high_price, low_price, close_price, volume, buy_volume, sell_volume
        FROM bybit_trade.candles_1s
        WHERE listing_id = %s
        ORDER BY timestamp_s ASC
        LIMIT 43200 
    """
    df = pd.read_sql(query, conn, params=(listing_id,))
    if df.empty: return df
    
    df['timestamp'] = pd.to_datetime(df['timestamp_s'], unit='s')
    df.set_index('timestamp', inplace=True)
    
    # Resample
    df_1m = df.resample('1min').agg({
        'open_price': 'first',
        'high_price': 'max',
        'low_price': 'min',
        'close_price': 'last',
        'volume': 'sum',
        'buy_volume': 'sum',
        'sell_volume': 'sum'
    })
    
    # Metrics
    df_1m['sell_volume'] = df_1m['sell_volume'].replace(0, 0.0001)
    df_1m['delta_ratio'] = df_1m['buy_volume'] / df_1m['sell_volume']
    
    # Indicators
    # VWAP
    pv = df_1m['close_price'] * df_1m['volume']
    df_1m['vwap'] = pv.rolling(60).sum() / df_1m['volume'].rolling(60).sum()
    
    # Avg Volume (Last 10 mins)
    df_1m['vol_avg_10'] = df_1m['volume'].rolling(10).mean()
    
    # ATR (Simplified: High - Low)
    df_1m['atr'] = df_1m['high_price'] - df_1m['low_price']
    # Smooth ATR
    df_1m['atr_sma'] = df_1m['atr'].rolling(5).mean()
    
    # Listing Open
    df_1m['listing_open'] = df_1m.iloc[0]['open_price']
    
    return df_1m

def run_hybrid_strategy(df: pd.DataFrame, symbol: str) -> list[TradePosition]:
    trades = []
    active_trade = None
    
    # Configuration
    VOL_THRESHOLD = 5.0 # Increased to 5.0 (Only massive breakouts)
    DELTA_BUY = 1.2
    DELTA_REENTRY = 1.5
    DELTA_PANIC = 0.6
    ATR_MULT = 3.0
    TP_PCT = 0.30
    
    # State
    panic_counter = 0 # Count consecutive low delta candles
    cooldown_until = None
    
    start_time = df.index[0]
    
    # Warmup
    for t, row in df.iloc[10:].iterrows():
        
        # Cooldown check
        if cooldown_until and t < cooldown_until:
            continue
            
        # 1. Manage Active Trade
        if active_trade:
            # Update High
            if row['high_price'] > active_trade.highest_price:
                active_trade.highest_price = row['high_price']
                
            current_pnl_pct = (row['close_price'] - active_trade.entry_price) / active_trade.entry_price
            
            should_exit = False
            exit_reason = ""
            current_atr = row['atr_sma'] if not np.isnan(row['atr_sma']) else (row['high_price'] - row['low_price'])
            
            # A. Panic Sell (Invalidation) - CONFIRMED
            # Only panic if Delta < 0.6 for 3 consecutive candles while Price < Entry
            if row['delta_ratio'] < DELTA_PANIC:
                panic_counter += 1
            else:
                panic_counter = 0
                
            if row['close_price'] < active_trade.entry_price and panic_counter >= 3:
                should_exit = True
                exit_reason = "Panic: Delta < 0.6 (3x)"
                
            # B. Partial Take Profit
            elif not active_trade.tp_hit and current_pnl_pct >= TP_PCT:
                # Sell 50%
                active_trade.tp_hit = True
                active_trade.realized_pnl += current_pnl_pct * 100 * 0.5
                active_trade.size_pct = 0.5
            
            # C. Dynamic Stop (ATR or Breakeven)
            if active_trade.tp_hit:
                # BREAKEVEN STOP: If we hit TP, move stop to Entry to secure win
                stop_price = max(active_trade.entry_price * 1.005, active_trade.highest_price - (current_atr * ATR_MULT))
            else:
                # Standard ATR Stop
                stop_price = active_trade.highest_price - (current_atr * ATR_MULT)
            
            if row['low_price'] < stop_price:
                # We hit the stop
                should_exit = True
                exit_reason = f"Stop Hit ({stop_price:.4f})" if active_trade.tp_hit else f"ATR Stop ({stop_price:.4f})"
                exit_price = min(row['close_price'], stop_price) 
            
            if should_exit:
                active_trade.exit_time = t
                if 'exit_price' not in locals(): exit_price = row['close_price']
                
                active_trade.exit_price = exit_price
                active_trade.exit_reason = exit_reason
                
                # Close remaining position
                remaining_pnl = (active_trade.exit_price - active_trade.entry_price) / active_trade.entry_price * 100 * active_trade.size_pct
                active_trade.realized_pnl += remaining_pnl
                
                # Check outcome for cooldown
                total_trade_pnl = active_trade.realized_pnl 
                if total_trade_pnl < 0:
                    cooldown_until = t + pd.Timedelta(minutes=15) # Pulse check
                
                active_trade.size_pct = 0.0 
                trades.append(active_trade)
                active_trade = None
                panic_counter = 0 # Reset
                continue
        
        # 2. Entry Logic (If no active trade)
        if not active_trade:
            # Check conditions
            is_entry = False
            entry_reason = ""
            
            # Check basic filters first
            vol_ok = row['volume'] > (row['vol_avg_10'] * VOL_THRESHOLD)
            delta_ok = row['delta_ratio'] > DELTA_BUY
            px_vwap_ok = row['close_price'] > row['vwap']
            
            # Last closed trade for Re-Entry logic
            last_exit_price = trades[-1].exit_price if trades else 999999
            
            # A. Fresh Entry (Standard)
            time_m = (t - start_time).total_seconds() / 60
            if 60 <= time_m <= 300:
                if vol_ok and delta_ok and px_vwap_ok:
                    is_entry = True
                    entry_reason = "Standard Entry"
            
            # B. Re-Entry (Pyramid / Recovery)
            # Re-Entry must clean the level clearly (+1%)
            elif trades and row['close_price'] > (last_exit_price * 1.01):
                 if row['delta_ratio'] > DELTA_REENTRY and vol_ok: # Require volume for re-entry too
                     is_entry = True
                     entry_reason = "Re-Entry Breakout"
            
            if is_entry:
                active_trade = TradePosition(symbol, t, row['close_price'])
                active_trade.highest_price = row['close_price']

    # Close at end
    if active_trade:
        active_trade.exit_time = df.index[-1]
        active_trade.exit_price = df.iloc[-1]['close_price']
        active_trade.exit_reason = "End of Data"
        remaining_pnl = (active_trade.exit_price - active_trade.entry_price) / active_trade.entry_price * 100 * active_trade.size_pct
        active_trade.realized_pnl += remaining_pnl
        trades.append(active_trade)
        
    return trades

def main():
    conn = get_db_connection()
    try:
        listings = pd.read_sql(
            "SELECT id, symbol FROM bybit_trade.listings WHERE data_fetched = TRUE ORDER BY listing_date DESC LIMIT 25",
            conn
        )
        
        all_trades = []
        print("\n🧬 RUNNING HYBRID RUNNER STRATEGY (Partial TP + ATR Stop)...")
        
        for _, row in listings.iterrows():
            lid, symbol = row['id'], row['symbol']
            df = load_data_1m(conn, lid)
            if df.empty or len(df) < 100: continue
            
            trades = run_hybrid_strategy(df, symbol)
            all_trades.extend(trades)
            
        if not all_trades:
            print("No trades triggered.")
            return
            
        res = pd.DataFrame([vars(t) for t in all_trades])
        
        # Display
        cols = ['symbol', 'entry_time', 'realized_pnl', 'exit_reason', 'tp_hit']
        res['realized_pnl'] = res['realized_pnl'].round(2)
        print("\n📄 HYBRID TRADE LOG:")
        print(res[cols].to_string())
        
        print("\n💰 SUMMARY:")
        print(f"Total Trades: {len(res)}")
        # Win if Realized PnL > 0 (even if stopped out later, TP might have saved it)
        win_rate = (res['realized_pnl'] > 0).mean()
        print(f"Win Rate:     {win_rate:.1%}")
        # Note: This PnL assumes fixed capital per trade, no compounding
        print(f"Avg PnL:      {res['realized_pnl'].mean():.2f}%")
        print(f"Total PnL:    {res['realized_pnl'].sum():.2f}%")

    finally:
        conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
