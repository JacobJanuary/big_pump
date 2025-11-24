import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import argparse
from itertools import product

# Add scripts directory to path for library import
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# Import common library
from pump_analysis_lib import (
    get_db_connection,
    fetch_signals,
    deduplicate_signals,
    get_entry_price_and_candles
)

ANALYSIS_WINDOW_HOURS = 24

def simulate_sl_only(candles, entry_price, sl_pct):
    """Simulate Stop-Loss only strategy"""
    sl_price = entry_price * (1 + sl_pct / 100)
    
    for candle in candles:
        low = float(candle['low_price'])
        if low <= sl_price:
            return sl_pct  # Hit SL, return loss
    
    # Position still open at end, use last close
    final_price = float(candles[-1]['close_price'])
    return ((final_price - entry_price) / entry_price) * 100

def simulate_trailing_stop(candles, entry_price, activation_pct, callback_pct):
    """Simulate Trailing Stop strategy"""
    activation_price = entry_price * (1 + activation_pct / 100)
    ts_activated = False
    peak_price = entry_price
    
    for candle in candles:
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        
        # Check if TS should activate
        if not ts_activated and high >= activation_price:
            ts_activated = True
            peak_price = high
        
        if ts_activated:
            # Update peak
            if high > peak_price:
                peak_price = high
            
            # Check callback from peak
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                return ((ts_exit_price - entry_price) / entry_price) * 100
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    return ((final_price - entry_price) / entry_price) * 100

def simulate_combined(candles, entry_price, sl_pct, activation_pct, callback_pct):
    """Simulate combined SL + TS strategy"""
    sl_price = entry_price * (1 + sl_pct / 100)
    activation_price = entry_price * (1 + activation_pct / 100)
    ts_activated = False
    peak_price = entry_price
    
    for candle in candles:
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        
        # Check SL (only before TS activation)
        if not ts_activated and low <= sl_price:
            return sl_pct  # Hit SL
        
        # Check if TS should activate
        if not ts_activated and high >= activation_price:
            ts_activated = True
            peak_price = high
        
        if ts_activated:
            # Update peak
            if high > peak_price:
                peak_price = high
            
            # Check callback from peak
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                return ((ts_exit_price - entry_price) / entry_price) * 100
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    return ((final_price - entry_price) / entry_price) * 100

def optimize_parameters(days=30, limit=None):
    print(f"Starting SL/TS optimization for the last {days} days...")
    
    # Parameter ranges
    sl_levels = [-3, -5, -8, -10, -15]
    activation_levels = [5, 10, 15, 20]
    callback_rates = [1, 2, 3, 5, 8]
    
    try:
        with get_db_connection() as conn:
            # Fetch signals using common library
            signals = fetch_signals(conn, days=days, limit=limit)
                
            if not signals:
                print("No signals found.")
                return

            print(f"Found {len(signals)} signals. Fetching candle data...")
            
            # Deduplicate signals
            unique_signals = deduplicate_signals(signals, cooldown_hours=24)
            print(f"After deduplication: {len(unique_signals)} unique signals.")
            
            # Fetch candle data for all signals
            signal_data = []
            
            for i, signal in enumerate(unique_signals, 1):
                if i % 10 == 0:
                    print(f"Fetching {i}/{len(unique_signals)}...", end='\r')
                
                # Get entry price and candles using common library
                entry_price, candles, entry_time_dt = get_entry_price_and_candles(
                    conn, signal,
                    analysis_hours=ANALYSIS_WINDOW_HOURS,
                    entry_offset_minutes=17
                )
                
                if entry_price is None or not candles:
                    continue
                
                signal_data.append({
                    'symbol': signal['pair_symbol'],
                    'timestamp': signal['timestamp'],
                    'entry_price': entry_price,
                    'candles': candles
                })
            
            print(f"\nProcessing {len(signal_data)} unique signals...")
            
            # Test all parameter combinations
            results = []
            
            # 1. SL Only strategies
            print("\nTesting SL-only strategies...")
            for sl in sl_levels:
                profits = []
                for sig in signal_data:
                    pnl = simulate_sl_only(sig['candles'], sig['entry_price'], sl)
                    profits.append(pnl)
                
                win_rate = len([p for p in profits if p > 0]) / len(profits) * 100
                avg_profit = sum(profits) / len(profits)
                total_profit = sum(profits)
                
                results.append({
                    'strategy': f"SL Only",
                    'sl': sl,
                    'activation': None,
                    'callback': None,
                    'win_rate': win_rate,
                    'avg_profit': avg_profit,
                    'total_profit': total_profit,
                    'trades': len(profits)
                })
            
            # 2. TS Only strategies
            print("Testing TS-only strategies...")
            for activation, callback in product(activation_levels, callback_rates):
                profits = []
                for sig in signal_data:
                    pnl = simulate_trailing_stop(sig['candles'], sig['entry_price'], activation, callback)
                    profits.append(pnl)
                
                win_rate = len([p for p in profits if p > 0]) / len(profits) * 100
                avg_profit = sum(profits) / len(profits)
                total_profit = sum(profits)
                
                results.append({
                    'strategy': f"TS Only",
                    'sl': None,
                    'activation': activation,
                    'callback': callback,
                    'win_rate': win_rate,
                    'avg_profit': avg_profit,
                    'total_profit': total_profit,
                    'trades': len(profits)
                })
            
            # 3. Combined SL + TS strategies
            print("Testing combined SL+TS strategies...")
            for sl, activation, callback in product(sl_levels, activation_levels, callback_rates):
                profits = []
                for sig in signal_data:
                    pnl = simulate_combined(sig['candles'], sig['entry_price'], sl, activation, callback)
                    profits.append(pnl)
                
                win_rate = len([p for p in profits if p > 0]) / len(profits) * 100
                avg_profit = sum(profits) / len(profits)
                total_profit = sum(profits)
                
                results.append({
                    'strategy': f"Combined",
                    'sl': sl,
                    'activation': activation,
                    'callback': callback,
                    'win_rate': win_rate,
                    'avg_profit': avg_profit,
                    'total_profit': total_profit,
                    'trades': len(profits)
                })
            
            # Sort by total profit
            results.sort(key=lambda x: x['total_profit'], reverse=True)
            
            # Print results
            print("\n" + "="*120)
            print("TOP 20 STRATEGIES BY TOTAL PROFIT:")
            print("="*120)
            print(f"{'Strategy':<12} {'SL %':<8} {'Activ %':<9} {'CB %':<7} {'Win Rate %':<12} {'Avg Profit %':<14} {'Total Profit %':<15} {'Trades':<8}")
            print("-"*120)
            
            for r in results[:20]:
                sl_str = f"{r['sl']:.1f}" if r['sl'] is not None else "N/A"
                act_str = f"{r['activation']:.1f}" if r['activation'] is not None else "N/A"
                cb_str = f"{r['callback']:.1f}" if r['callback'] is not None else "N/A"
                
                print(f"{r['strategy']:<12} {sl_str:<8} {act_str:<9} {cb_str:<7} {r['win_rate']:<12.2f} {r['avg_profit']:<14.2f} {r['total_profit']:<15.2f} {r['trades']:<8}")
            
            # Print best strategy summary
            best = results[0]
            print("\n" + "="*120)
            print("OPTIMAL STRATEGY:")
            print(f"  Type: {best['strategy']}")
            if best['sl'] is not None:
                print(f"  Stop-Loss: {best['sl']:.1f}%")
            if best['activation'] is not None:
                print(f"  TS Activation: {best['activation']:.1f}%")
            if best['callback'] is not None:
                print(f"  TS Callback: {best['callback']:.1f}%")
            print(f"  Win Rate: {best['win_rate']:.2f}%")
            print(f"  Average Profit: {best['avg_profit']:.2f}%")
            print(f"  Total Profit: {best['total_profit']:.2f}%")
            print(f"  Number of Trades: {best['trades']}")
            print("="*120)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Optimize SL and TS parameters.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of signals to process')
    args = parser.parse_args()
    
    optimize_parameters(days=args.days, limit=args.limit)
