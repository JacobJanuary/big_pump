"""
Advanced optimization with volume filters
Based on optimize_advanced.py with added volume-based signal filtering
"""
import sys
import os
from pathlib import Path
from itertools import product
import argparse

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, EXCHANGE_FILTER, EXCHANGE_IDS
from optimization_lib import (
    simulate_combined,
    calculate_peak_time_stats
)

def load_signals_with_volume_filter(min_volume_1m=0, min_volume_1h=0):
    """Load signals with volume filters and 1-minute candles from database"""
    with get_db_connection() as conn:
        query = """
            SELECT 
                sa.id, sa.signal_timestamp, sa.pair_symbol, sa.entry_price,
                sa.volume_1m_usdt, sa.volume_1h_usdt
            FROM web.signal_analysis sa
            JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
            WHERE EXISTS (
                SELECT 1 FROM web.minute_candles mc
                WHERE mc.signal_analysis_id = sa.id
            )
            AND sa.volume_1m_usdt >= {min_vol_1m}
            AND sa.volume_1h_usdt >= {min_vol_1h}
            AND (
                '{filter}' = 'ALL' 
                OR ('{filter}' = 'BINANCE' AND tp.exchange_id = {binance_id})
                OR ('{filter}' = 'BYBIT' AND tp.exchange_id = {bybit_id})
            )
            ORDER BY sa.signal_timestamp ASC
        """.format(
            min_vol_1m=min_volume_1m,
            min_vol_1h=min_volume_1h,
            filter=EXCHANGE_FILTER,
            binance_id=EXCHANGE_IDS['BINANCE'],
            bybit_id=EXCHANGE_IDS['BYBIT']
        )
        
        with conn.cursor() as cur:
            cur.execute(query)
            signals = cur.fetchall()
        
        # Get total signals without filter for comparison
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM web.signal_analysis sa
                JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
                WHERE EXISTS (
                    SELECT 1 FROM web.minute_candles mc
                    WHERE mc.signal_analysis_id = sa.id
                )
            """)
            total_signals = cur.fetchone()[0]
        
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
                    'volume_1m': float(signal[4]) if signal[4] else 0,
                    'volume_1h': float(signal[5]) if signal[5] else 0,
                    'candles': candles
                })
        
        return signals_data, total_signals

def optimize_with_volume(min_volume_1m=50000, min_volume_1h=1000000):
    """
    Run optimization with volume filters
    """
    print("="*120)
    print("ADVANCED STRATEGY OPTIMIZATION WITH VOLUME FILTERS")
    print("="*120)
    print(f"Volume Filters:")
    print(f"  Min 1m Volume: ${min_volume_1m/1000:.2f}K USDT")
    print(f"  Min 1h Volume: ${min_volume_1h/1000000:.2f}M USDT")
    
    # Load preprocessed data with volume filter
    print("\nLoading signals with volume filters from database...")
    signals_data, total_signals = load_signals_with_volume_filter(min_volume_1m, min_volume_1h)
    
    if not signals_data:
        print("No signals found matching volume criteria.")
        print(f"Total signals in database: {total_signals}")
        print("\nTry lower volume thresholds or run:")
        print("  python3 scripts_v2/populate_volume_data.py")
        return
    
    filtered_pct = (len(signals_data) / total_signals * 100) if total_signals > 0 else 0
    print(f"Loaded {len(signals_data)} signals (filtered {len(signals_data)}/{total_signals} = {filtered_pct:.1f}%)")
    
    # Calculate volume statistics
    avg_vol_1m = sum(s['volume_1m'] for s in signals_data) / len(signals_data)
    avg_vol_1h = sum(s['volume_1h'] for s in signals_data) / len(signals_data)
    print(f"Average volumes: 1m=${avg_vol_1m/1000:.1f}K, 1h=${avg_vol_1h/1000000:.2f}M")
    
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
            'total_profit': total_profit
        })
    
    # Sort by total profit (descending)
    results.sort(key=lambda x: x['total_profit'], reverse=True)
    
    print("\n" + "="*120)
    print("TOP 10 CONFIGURATIONS (Volume-Filtered)")
    print("="*120)
    print(f"{'#':<4} {'SL':<6} {'Activation':<12} {'Callback':<10} {'Timeout':<9} {'Win%':<8} {'Avg%':<8} {'Total%'}")
    print("-"*120)
    
    for i, r in enumerate(results[:10], 1):
        print(f"{i:<4} {r['sl']:<6} {r['activation']:<12} {r['callback']:<10} "
              f"{r['timeout']:<9} {r['win_rate']:.2f}%{' '*2} "
              f"{r['avg_profit']:+.2f}%{' '*2} {r['total_profit']:+.2f}%")
    
    print("\n" + "="*120)
    print(f"Volume Filter Impact:")
    print(f"  Signals used: {len(signals_data)} / {total_signals} ({filtered_pct:.1f}%)")
    print(f"  Avg 1m Volume: ${avg_vol_1m/1000:.1f}K USDT")
    print(f"  Avg 1h Volume: ${avg_vol_1h/1000000:.2f}M USDT")
    print("="*120)

def main():
    parser = argparse.ArgumentParser(
        description='Advanced optimization with volume filters'
    )
    parser.add_argument(
        '--min-volume-1m',
        type=float,
        default=50000,
        help='Minimum 1-minute volume in USDT (default: 50000 = $50K)'
    )
    parser.add_argument(
        '--min-volume-1h',
        type=float,
        default=1000000,
        help='Minimum 1-hour volume in USDT (default: 1000000 = $1M)'
    )
    
    args = parser.parse_args()
    
    optimize_with_volume(
        min_volume_1m=args.min_volume_1m,
        min_volume_1h=args.min_volume_1h
    )

if __name__ == "__main__":
    main()
