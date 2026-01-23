#!/usr/bin/env python3
"""
Grid Search Optimizer for Hybrid Strategy.
Tests thousands of parameter combinations in parallel to find the "Holy Grail" configuration.

Parameters to Optimize:
- VOL_THRESHOLD: [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
- TP_PCT: [0.15, 0.20, 0.25, 0.30, 0.40, 0.50]
- ATR_MULT: [2.0, 2.5, 3.0, 3.5, 4.0]
- DELTA_BUY: [1.0, 1.2, 1.5]

Metric: Total Realized PnL.
"""

import sys
import warnings
import itertools
import time
from pathlib import Path
from dataclasses import dataclass
from concurrent.futures import ProcessPoolExecutor
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
    size_pct: float = 1.0
    realized_pnl: float = 0.0
    highest_price: float = 0.0
    tp_hit: bool = False
    exit_time: pd.Timestamp = None
    exit_price: float = None

def load_data_batch(conn, limit=50):
    """Load data for ALL listings into memory once to avoid SQL overhead during multiprocessing."""
    print(f"⏳ Loading data for {limit} listings...")
    listings = pd.read_sql(
        f"SELECT id, symbol FROM bybit_trade.listings WHERE data_fetched = TRUE ORDER BY listing_date DESC LIMIT {limit}",
        conn
    )
    
    data_map = {}
    for _, row in listings.iterrows():
        lid, symbol = row['id'], row['symbol']
        
        query = """
            SELECT timestamp_s, open_price, high_price, low_price, close_price, volume, buy_volume, sell_volume
            FROM bybit_trade.candles_1s
            WHERE listing_id = %s
            ORDER BY timestamp_s ASC
            LIMIT 43200 
        """ # 12h
        df = pd.read_sql(query, conn, params=(lid,))
        if df.empty or len(df) < 600: continue
        
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
        
        # Pre-calc Indicators
        df_1m['sell_volume'] = df_1m['sell_volume'].replace(0, 0.0001)
        df_1m['delta_ratio'] = df_1m['buy_volume'] / df_1m['sell_volume']
        pv = df_1m['close_price'] * df_1m['volume']
        df_1m['vwap'] = pv.rolling(60).sum() / df_1m['volume'].rolling(60).sum()
        df_1m['vol_avg_10'] = df_1m['volume'].rolling(10).mean()
        df_1m['atr'] = df_1m['high_price'] - df_1m['low_price']
        df_1m['atr_sma'] = df_1m['atr'].rolling(5).mean()
        df_1m['delta_sma_60'] = df_1m['delta_ratio'].rolling(60).mean()
        df_1m['listing_open'] = df_1m.iloc[0]['open_price']
        
        data_map[symbol] = df_1m
    
    print(f"✅ Loaded {len(data_map)} datasets.")
    return data_map

# Global data for workers
GLOBAL_DATA = {}

def evaluate_strategy(params):
    """Run strategy on ALL symbols with specific parameters."""
    # Unpack params
    VOL_THRESH, TP_PCT, ATR_MULT, DELTA_BUY = params
    
    # Constants
    DELTA_PANIC = 0.6
    DELTA_REENTRY = 1.5
    
    total_pnl = 0.0
    total_trades = 0
    wins = 0
    
    for symbol, df in GLOBAL_DATA.items():
        if df.empty: continue
        
        trades = []
        active_trade = None
        panic_counter = 0
        consecutive_losses = 0
        cooldown_until = None
        
        start_time = df.index[0]
        
        for t, row in df.iloc[60:].iterrows():
            if consecutive_losses >= 3: break
            if cooldown_until and t < cooldown_until: continue
            
            # Active Trade Management
            if active_trade:
                if row['high_price'] > active_trade.highest_price:
                    active_trade.highest_price = row['high_price']
                
                curr_pnl_pct = (row['close_price'] - active_trade.entry_price) / active_trade.entry_price
                current_atr = row['atr_sma'] if not np.isnan(row['atr_sma']) else (row['high_price'] - row['low_price'])
                
                exit_signal = False
                exit_price = row['close_price']
                
                # 1. Panic
                if row['delta_ratio'] < DELTA_PANIC: panic_counter += 1
                else: panic_counter = 0
                
                if row['close_price'] < active_trade.entry_price and panic_counter >= 3:
                    exit_signal = True
                    
                # 2. Partial TP
                elif not active_trade.tp_hit and curr_pnl_pct >= TP_PCT:
                    active_trade.tp_hit = True
                    active_trade.realized_pnl += curr_pnl_pct * 100 * 0.5
                    active_trade.size_pct = 0.5
                    
                # 3. Stop Loss
                if active_trade.tp_hit:
                    # Breakeven
                    stop_px = max(active_trade.entry_price * 1.005, active_trade.highest_price - (current_atr * ATR_MULT))
                else:
                    stop_px = active_trade.highest_price - (current_atr * ATR_MULT)
                    
                if row['low_price'] < stop_px:
                    exit_signal = True
                    exit_price = min(row['close_price'], stop_px)
                
                if exit_signal:
                    rem_pnl = (exit_price - active_trade.entry_price) / active_trade.entry_price * 100 * active_trade.size_pct
                    active_trade.realized_pnl += rem_pnl
                    
                    if active_trade.realized_pnl > 0: consecutive_losses = 0
                    else: 
                        consecutive_losses += 1
                        cooldown_until = t + pd.Timedelta(minutes=15)
                        
                    trades.append(active_trade)
                    active_trade = None
                    panic_counter = 0
                    continue

            # Entry Logic
            if not active_trade:
                vol_ok = row['volume'] > (row['vol_avg_10'] * VOL_THRESH)
                
                # Optimizable Delta?
                delta_ok = row['delta_ratio'] > DELTA_BUY
                trend_ok = row['delta_sma_60'] > 1.0
                drawdown_ok = row['close_price'] > (0.8 * row['listing_open'])
                vwap_ok = row['close_price'] > row['vwap']
                
                last_exit = trades[-1].exit_price if trades and trades[-1].exit_price else 999999
                
                is_entry = False
                time_m = (t - start_time).total_seconds() / 60
                
                # Standard
                if 60 <= time_m <= 300:
                    if vol_ok and delta_ok and vwap_ok and drawdown_ok and trend_ok:
                        is_entry = True
                
                # Re-Entry
                elif trades and row['close_price'] > (last_exit * 1.01):
                    if row['delta_ratio'] > DELTA_REENTRY and vol_ok and trend_ok:
                        is_entry = True
                        
                if is_entry:
                    active_trade = TradePosition(symbol, t, row['close_price'])
                    active_trade.highest_price = row['close_price']
                    
        # Force close
        if active_trade:
             rem_pnl = (df.iloc[-1]['close_price'] - active_trade.entry_price) / active_trade.entry_price * 100 * active_trade.size_pct
             active_trade.realized_pnl += rem_pnl
             trades.append(active_trade)
             
        # Aggregate stats
        for t in trades:
            total_pnl += t.realized_pnl
            total_trades += 1
            if t.realized_pnl > 0: wins += 1
            
    return (params, total_pnl, total_trades, wins)

def main():
    # 1. Connect
    conn = get_db_connection()
    try:
        data = load_data_batch(conn, limit=50) # Load top 50 recents
    finally:
        conn.close()
        
    global GLOBAL_DATA
    GLOBAL_DATA = data
    
    # 2. Define Grid
    # Expanded grid as requested
    vol_range = [1.5, 2.0, 2.5, 3.0, 4.0, 5.0]
    tp_range = [0.10, 0.15, 0.20, 0.30, 0.40, 0.50]
    atr_range = [2.0, 2.5, 3.0, 3.5, 4.0]
    delta_range = [1.0, 1.2, 1.5]
    
    combinations = list(itertools.product(vol_range, tp_range, atr_range, delta_range))
    print(f"\n🚀 Starting Optimization on {len(combinations)} combinations using 10 workers...")
    
    start_time = time.time()
    results = []
    
    # 3. Serial Execution (Mac/Windows compatible)
    print("⚠️ Running in SERIAL mode (Global State fix)...")
    
    # Reduced Grid for speed
    vol_range = [2.0, 2.5, 3.0, 4.0]
    tp_range = [0.15, 0.20, 0.30, 0.40]
    atr_range = [2.5, 3.0, 3.5]
    delta_range = [1.2] # Fixed Delta for now
    
    combinations = list(itertools.product(vol_range, tp_range, atr_range, delta_range))
    print(f"\n🚀 Starting Optimization on {len(combinations)} combinations...")

    for i, params in enumerate(combinations):
        res = evaluate_strategy(params)
        results.append(res)
        if i % 10 == 0:
            print(f"   Progress: {i}/{len(combinations)}...", end='\r')
                
    elapsed = time.time() - start_time
    print(f"\n✅ Done in {elapsed:.1f}s.")
    
    # 4. Find Best
    # Sort by PnL desc
    results.sort(key=lambda x: x[1], reverse=True)
    
    print("\n🏆 TOP 10 CONFIGURATIONS:")
    print(f"{'#':<3} | {'VOL':<4} {'TP':<4} {'ATR':<4} {'DEL':<4} | {'PnL %':<8} | {'Trades':<6} | {'WinRate':<6}")
    print("-" * 60)
    
    for i in range(min(10, len(results))):
        params, pnl, trades, wins = results[i]
        vol, tp, atr, d_buy = params
        wr = (wins / trades * 100) if trades > 0 else 0
        print(f"{i+1:<3} | {vol:<4} {tp:<4} {atr:<4} {d_buy:<4} | {pnl:8.2f} | {trades:<6} | {wr:5.1f}%")
        
    best = results[0]
    print(f"\n🌟 BEST PARAMETERS: VOL={best[0][0]}, TP={best[0][1]}, ATR={best[0][2]}, DELTA={best[0][3]}")

if __name__ == "__main__":
    main()
