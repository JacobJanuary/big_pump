"""
Advanced optimization with 1-minute candles for precision
Uses preprocessed data from web.signal_analysis and web.minute_candles
"""
import sys
import os
from pathlib import Path
from itertools import product

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection
from optimization_lib import (
    simulate_combined,
    calculate_peak_time_stats
)

def load_signals_with_minute_candles():
    """Load signals with 1-minute candles from database"""
    with get_db_connection() as conn:
        query = """
            SELECT 
                sa.id, sa.signal_timestamp, sa.pair_symbol, sa.entry_price
            FROM web.signal_analysis sa
            WHERE EXISTS (
                SELECT 1 FROM web.minute_candles mc
                WHERE mc.signal_analysis_id = sa.id
            )
            ORDER BY sa.signal_timestamp ASC
        """
        
        with conn.cursor() as cur:
            cur.execute(query)
            signals = cur.fetchall()
        
        signals_data = []
        
        for signal in signals:
            signal_id = signal[0]
            
            # Fetch minute candles for this signal
            candles_query = """
                SELECT open_time, open_price, high_price, low_price, close_price, volume
                FROM web.minute_candles
                WHERE signal_analysis_id = %s
                ORDER BY open_time ASC
            """
            
            with conn.cursor() as cur:
                cur.execute(candles_query, (signal_id,))
                candles_rows = cur.fetchall()
            
            candles = [{
                'open_time': int(row[0]),
                'open_price': float(row[1]),
                'high_price': float(row[2]),
                'low_price': float(row[3]),
                'close_price': float(row[4])
            } for row in candles_rows]
            
            if candles:
                signals_data.append({
                    'symbol': signal[2],
                    'timestamp': signal[1],
                    'entry_price': float(signal[3]),
                    'candles': candles
                })
        
        return signals_data

def optimize_advanced():
    """
    Run advanced optimization with 1-minute precision
    """
    print("="*120)
    print("ADVANCED STRATEGY OPTIMIZATION (1-minute precision)")
    print("="*120)
    
    # Load preprocessed data
    print("\nLoading signals with 1-minute candles from database...")
    signals_data = load_signals_with_minute_candles()
    
    if not signals_data:
        print("No signals with minute candles found.")
        print("Please run:")
        print("  1. python3 scripts/populate_signal_analysis.py")
        print("  2. python3 scripts/fetch_minute_candles.py")
        return
    
    print(f"Loaded {len(signals_data)} signals with minute candles.")
    
    # Calculate time-to-peak statistics
    print("\nCalculating time-to-peak statistics...")
    peak_stats = calculate_peak_time_stats(signals_data)
    
    print("\nTime to Peak Statistics (hours):")
    print(f"  Median: {peak_stats['median']:.2f}")
    print(f"  Mean: {peak_stats['mean']:.2f}")
    print(f"  25th percentile: {peak_stats['p25']:.2f}")
    print(f"  75th percentile: {peak_stats['p75']:.2f}")
    print(f"  90th percentile: {peak_stats['p90']:.2f}")
    print(f"  Min: {peak_stats['min']:.2f}, Max: {peak_stats['max']:.2f}")
    
    # Determine optimal timeout (use 90th percentile)
    optimal_timeout = int(peak_stats['p90']) + 1  # Round up
    print(f"\nRecommended timeout: {optimal_timeout} hours (90th percentile + buffer)")
    
    # Parameter ranges
    sl_levels = list(range(-10, 0))  # -10% to -1%, step 1%
    activation_levels = list(range(3, 51))  # 3% to 50%, step 1%
    callback_rates = list(range(1, 11))  # 1% to 10%, step 1%
    timeout_options = [optimal_timeout]  # Use calculated optimal timeout
    
    print(f"\nParameter Search Space:")
    print(f"  SL: {len(sl_levels)} values ({min(sl_levels)}% to {max(sl_levels)}%)")
    print(f"  TS Activation: {len(activation_levels)} values ({min(activation_levels)}% to {max(activation_levels)}%)")
    print(f"  TS Callback: {len(callback_rates)} values ({min(callback_rates)}% to {max(callback_rates)}%)")
    print(f"  Timeout: {timeout_options} hours")
    print(f"  Total combinations: {len(sl_levels) * len(activation_levels) * len(callback_rates) * len(timeout_options)}")
    
    print("\nTesting all combinations...")
    results = []
    total_combos = len(sl_levels) * len(activation_levels) * len(callback_rates) * len(timeout_options)
    current = 0
    
    for sl, activation, callback, timeout in product(sl_levels, activation_levels, callback_rates, timeout_options):
        current += 1
        if current % 500 == 0:
            print(f"Progress: {current}/{total_combos} ({current/total_combos*100:.1f}%)...")
        
        profits = []
        for sig in signals_data:
            pnl = simulate_combined(
                sig['candles'], 
                sig['entry_price'], 
                sl, 
                activation, 
                callback,
                timeout_hours=timeout
            )
            profits.append(pnl)
        
        win_rate = len([p for p in profits if p > 0]) / len(profits) * 100 if profits else 0
        avg_profit = sum(profits) / len(profits) if profits else 0
        total_profit = sum(profits)
        
        results.append({
            'sl': sl,
            'activation': activation,
            'callback': callback,
            'timeout': timeout,
            'win_rate': win_rate,
            'avg_profit': avg_profit,
            'total_profit': total_profit,
            'trades': len(profits)
        })
    
    # Sort by average profit (more meaningful than total)
    results.sort(key=lambda x: x['avg_profit'], reverse=True)
    
    print("\n\n" + "="*140)
    print("TOP 20 STRATEGIES BY AVERAGE PROFIT PER TRADE:")
    print("="*140)
    print(f"{'SL %':<6} {'Activ %':<9} {'CB %':<7} {'Timeout (h)':<13} {'Win Rate %':<12} {'Avg Profit %':<15} {'Total %':<12} {'Trades':<8}")
    print("-"*140)
    
    for r in results[:20]:
        print(f"{r['sl']:<6} {r['activation']:<9} {r['callback']:<7} {r['timeout']:<13} "
              f"{r['win_rate']:<12.2f} {r['avg_profit']:<15.2f} {r['total_profit']:<12.2f} {r['trades']:<8}")
    
    # Print best strategy summary
    best = results[0]
    print("\n" + "="*140)
    print("OPTIMAL STRATEGY (by Avg Profit):")
    print("="*140)
    print(f"  Stop-Loss: {best['sl']}%")
    print(f"  TS Activation: {best['activation']}%")
    print(f"  TS Callback: {best['callback']}%")
    print(f"  Timeout: {best['timeout']} hours")
    print(f"  Win Rate: {best['win_rate']:.2f}%")
    print(f"  Average Profit per Trade: {best['avg_profit']:.2f}%")
    print(f"  Total Profit (sum of %): {best['total_profit']:.2f}%")
    print(f"  Number of Trades: {best['trades']}")
    print("="*140)
    
    # Also show top by total profit for comparison
    results_by_total = sorted(results, key=lambda x: x['total_profit'], reverse=True)
    best_total = results_by_total[0]
    print("\nBest Strategy by Total Profit:")
    print(f"  SL: {best_total['sl']}%, TS: {best_total['activation']}%, CB: {best_total['callback']}%, "
          f"Timeout: {best_total['timeout']}h")
    print(f"  Avg Profit: {best_total['avg_profit']:.2f}%, Win Rate: {best_total['win_rate']:.2f}%")

if __name__ == "__main__":
    optimize_advanced()
