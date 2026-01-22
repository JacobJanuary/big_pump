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

# ... imports ...
import pandas_ta as ta # Ensure pandas_ta is in requirements

# --- INDICATORS ---
def add_indicators(df):
    """Add technical indicators for strategies."""
    # RSI
    df['rsi'] = ta.rsi(df['close_price'], length=14)
    
    # Bollinger Bands
    bb = ta.bbands(df['close_price'], length=20, std=2)
    if bb is not None:
        df['bb_upper'] = bb['BBU_20_2.0']
        df['bb_lower'] = bb['BBL_20_2.0']
        
    # EMA
    df['ema_short'] = ta.ema(df['close_price'], length=9)
    df['ema_long'] = ta.ema(df['close_price'], length=21)
    
    # Volume Spikes
    df['vol_ma'] = df['volume'].rolling(window=60).mean() # 1 min avg volume
    df['vol_spike'] = df['volume'] / df['vol_ma']
    
    return df

# --- STRATEGIES ---

def strategy_trend_momentum(df: pd.DataFrame, symbol: str, **kwargs):
    """
    Strategy 1: Momentum / Trend Following.
    Buy when price breaks above 5-min High with Volume.
    Hold with Trailing Stop.
    Best for: SKR, ZKP (Big pumps).
    """
    trades = []
    
    # Parameters
    lookback = kwargs.get('lookback', 300) # 5 mins
    vol_thresh = kwargs.get('vol_thresh', 3.0)
    trailing_stop_pct = kwargs.get('trailing_stop', 0.05) # 5% trailing
    
    # Skip initial lookback
    if len(df) < lookback: return []
    
    # Rolling high
    df['rolling_high'] = df['high_price'].rolling(window=lookback).max().shift(1)
    
    position = None
    highest_price = 0
    
    for i in range(lookback, len(df)):
        row = df.iloc[i]
        
        # ENTRY
        if position is None:
            # Breakout + Volume Spike
            if row['close_price'] > row['rolling_high'] and row['vol_spike'] > vol_thresh:
                position = {
                    'entry_time': df.index[i],
                    'entry_price': row['close_price'],
                    'symbol': symbol,
                    'type': 'Momentum'
                }
                highest_price = row['close_price']
                
        # EXIT (Trailing Stop)
        elif position:
            highest_price = max(highest_price, row['high_price'])
            stop_price = highest_price * (1 - trailing_stop_pct)
            
            if row['low_price'] < stop_price:
                # Stopped out
                exit_price = stop_price
                pnl = (exit_price - position['entry_price']) / position['entry_price'] * 100
                trades.append(Trade(
                    symbol=symbol,
                    entry_time=position['entry_time'],
                    entry_price=position['entry_price'],
                    exit_time=df.index[i],
                    exit_price=exit_price,
                    pnl_pct=pnl,
                    exit_reason="Trailing Stop",
                    duration_s=(df.index[i] - position['entry_time']).total_seconds()
                ))
                position = None
                
    return trades

def strategy_mean_reversion(df: pd.DataFrame, symbol: str, **kwargs):
    """
    Strategy 2: Mean Reversion / Scalping.
    Buy when RSI < 30 and Price < Lower BB.
    Sell when RSI > 70 or Price > Upper BB.
    Best for: WET, ELSA (Choppy/Volatile).
    """
    trades = []
    position = None
    
    for i in range(20, len(df)):
        row = df.iloc[i]
        
        # Need indicators
        if pd.isna(row['bb_lower']) or pd.isna(row['rsi']): continue
        
        # ENTRY: Oversold
        if position is None:
            if row['rsi'] < 30 and row['close_price'] < row['bb_lower']:
                position = {
                    'entry_time': df.index[i],
                    'entry_price': row['close_price']
                }
                
        # EXIT: Overbought or TP
        elif position:
            pnl_current = (row['close_price'] - position['entry_price']) / position['entry_price'] * 100
            
            # PnL Targets
            if pnl_current > 5 or row['rsi'] > 70: # Take profit quickly
                trades.append(Trade(
                    symbol=symbol,
                    entry_time=position['entry_time'],
                    entry_price=position['entry_price'],
                    exit_time=df.index[i],
                    exit_price=row['close_price'],
                    pnl_pct=pnl_current,
                    exit_reason="Scalp Target",
                    duration_s=(df.index[i] - position['entry_time']).total_seconds()
                ))
                position = None
            elif pnl_current < -5: # Stop loss
                trades.append(Trade(
                    symbol=symbol,
                    entry_time=position['entry_time'],
                    entry_price=position['entry_price'],
                    exit_time=df.index[i],
                    exit_price=row['close_price'],
                    pnl_pct=pnl_current,
                    exit_reason="Stop Loss",
                    duration_s=(df.index[i] - position['entry_time']).total_seconds()
                ))
                position = None
                
    return trades

class Backtester:
    # ... (previous init/load methods) ...

    def run_all_strategies(self):
        """Run multiple strategies comparison."""
        strategies = [
            (strategy_trend_momentum, {'lookback': 300, 'vol_thresh': 3.0, 'trailing_stop': 0.05}),
            (strategy_trend_momentum, {'lookback': 60, 'vol_thresh': 2.0, 'trailing_stop': 0.03}), # Aggressive Trend
            (strategy_mean_reversion, {}),
        ]
        
        overall_report = []
        
        for strat_func, params in strategies:
            strat_name = f"{strat_func.__name__}_{params.get('lookback', '')}"
            print(f"\n🧪 Testing {strat_name}...")
            
            all_trades = []
            
            for _, row in self.listings.iterrows():
                lid = row['id']
                symbol = row['symbol']
                
                df = self._load_data(lid)
                if df.empty or len(df) < 300: continue
                
                # Add indicators
                df = add_indicators(df)
                
                # Run strategy
                # Note: changed run_strategy return type to list of trades
                trades = strat_func(df, symbol, **params)
                all_trades.extend(trades)
                
            # Calcs
            if not all_trades:
                print("  No trades.")
                continue
                
            df_res = pd.DataFrame([t.__dict__ for t in all_trades])
            total_pnl = df_res['pnl_pct'].sum()
            win_rate = (df_res['pnl_pct'] > 0).mean() * 100
            
            print(f"  Trades: {len(df_res)} | Win Rate: {win_rate:.1f}% | Total PnL: {total_pnl:.2f}%")
            overall_report.append({
                'strategy': strat_name,
                'trades': len(df_res),
                'win_rate': win_rate,
                'total_pnl': total_pnl
            })
            
        return overall_report

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
        
        # Run Comprehensive Backtest
        print("🚀 Starting Super-Trader Strategy Comparison...")
        reports = bt.run_all_strategies()
        
        print("\n🏆 FINAL LEADERBOARD")
        print("Strategy              | Trades | Win Rate | Total PnL")
        print("-" * 60)
        for r in sorted(reports, key=lambda x: x['total_pnl'], reverse=True):
            print(f"{r['strategy']:<21} | {r['trades']:<6} | {r['win_rate']:>7.1f}% | {r['total_pnl']:>8.2f}%")
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
