"""
Volume-Based & Dynamic Callback Optimization

Tests 4 configurations:
1. Baseline: No volume analysis, static callback
2. Volume Only: Volume exits enabled, static callback
3. Dynamic Only: No volume exits, dynamic callback
4. Full Advanced: Both features enabled

Optimizes volume weakness threshold and compares performance.
"""
import sys
import os
from pathlib import Path
from itertools import product

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, EXCHANGE_FILTER, EXCHANGE_IDS
from optimization_lib import (
    simulate_partial_close,
    simulate_partial_close_advanced,
    simulate_combined,
    calculate_peak_time_stats
)

def load_signals_with_minute_candles():
    """Load signals with 1-minute candles including volume"""
    with get_db_connection() as conn:
        query = """
            SELECT 
                sa.id, sa.signal_timestamp, sa.pair_symbol, sa.entry_price
            FROM web.signal_analysis sa
            JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
            WHERE EXISTS (
                SELECT 1 FROM web.minute_candles mc
                WHERE mc.signal_analysis_id = sa.id
            )
            AND (
                '{filter}' = 'ALL' 
                OR ('{filter}' = 'BINANCE' AND tp.exchange_id = {binance_id})
                OR ('{filter}' = 'BYBIT' AND tp.exchange_id = {bybit_id})
            )
            ORDER BY sa.signal_timestamp ASC
        """.format(
            filter=EXCHANGE_FILTER,
            binance_id=EXCHANGE_IDS['BINANCE'],
            bybit_id=EXCHANGE_IDS['BYBIT']
        )
        
        with conn.cursor() as cur:
            cur.execute(query)
            signals = cur.fetchall()
        
        signals_data = []
        
        for signal in signals:
            signal_id = signal[0]
            
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
                'close_price': float(row[4]),
                'volume': float(row[5])
            } for row in candles_rows]
            
            if candles:
                signals_data.append({
                    'symbol': signal[2],
                    'timestamp': signal[1],
                    'entry_price': float(signal[3]),
                    'candles': candles
                })
        
        return signals_data

def run_configuration(config_name, signals_data, ts_configs, sl, timeout, 
                      use_volume, use_dynamic, volume_threshold):
    """
    Run simulation for a specific configuration
    
    Returns dict with performance metrics
    """
    profits = []
    exit_reasons = []
    
    for sig in signals_data:
        if use_volume or use_dynamic:
            # Use advanced simulation
            pnl, portions = simulate_partial_close_advanced(
                sig['candles'],
                sig['entry_price'],
                sl,
                ts_configs,
                timeout_hours=timeout,
                use_volume_exit=use_volume,
                use_dynamic_callback=use_dynamic,
                volume_weakness_threshold=volume_threshold
            )
            # Track exit reasons
            for p in portions:
                exit_reasons.append(p['exit_reason'])
        else:
            # Use baseline simulation
            pnl = simulate_partial_close(
                sig['candles'],
                sig['entry_price'],
                sl,
                ts_configs,
                timeout_hours=timeout
            )
        
        profits.append(pnl)
    
    # Calculate metrics
    win_rate = len([p for p in profits if p > 0]) / len(profits) * 100 if profits else 0
    avg_profit = sum(profits) / len(profits) if profits else 0
    total_profit = sum(profits)
    
    # Exit reason distribution
    exit_dist = {}
    if exit_reasons:
        total_exits = len(exit_reasons)
        for reason in set(exit_reasons):
            count = exit_reasons.count(reason)
            exit_dist[reason] = (count / total_exits) * 100
    
    return {
        'config': config_name,
        'win_rate': win_rate,
        'avg_profit': avg_profit,
        'total_profit': total_profit,
        'trades': len(profits),
        'exit_distribution': exit_dist
    }

def optimize_volume_dynamic():
    """
    Main optimization function with A/B testing
    """
    print("="*140)
    print("VOLUME-BASED EXITS & DYNAMIC CALLBACK OPTIMIZATION")
    print("="*140)
    
    # Load data
    print("\nLoading signals with volume data...")
    signals_data = load_signals_with_minute_candles()
    
    if not signals_data:
        print("No signals found. Please run data preparation scripts first.")
        return
    
    print(f"Loaded {len(signals_data)} signals with 1-minute candles + volume.")
    
    # Calculate timeout
    peak_stats = calculate_peak_time_stats(signals_data)
    optimal_timeout = int(peak_stats['p90']) + 1
    print(f"Using timeout: {optimal_timeout} hours (90th percentile + buffer)")
    
    # Default parameters (from previous optimization)
    default_sl = -5
    default_ts_configs = [
        {'portion': 0.30, 'activation_pct': 10.0, 'callback_pct': 2.0},
        {'portion': 0.30, 'activation_pct': 20.0, 'callback_pct': 2.0},
        {'portion': 0.40, 'activation_pct': 60.0, 'callback_pct': 5.0}
    ]
    
    # Volume threshold options
    volume_thresholds = [0.10, 0.15, 0.20, 0.25]
    
    # STAGE 1: A/B TESTING - 4 CONFIGURATIONS
    print("\n" + "="*140)
    print("STAGE 1: CONFIGURATION COMPARISON")
    print("="*140)
    print("\nTesting 4 configurations with default parameters...")
    
    configs_to_test = [
        ("Baseline", False, False, 0.15),
        ("Volume Only", True, False, 0.15),
        ("Dynamic Only", False, True, 0.15),
        ("Full Advanced", True, True, 0.15)
    ]
    
    stage1_results = []
    
    for config_name, use_vol, use_dyn, vol_thresh in configs_to_test:
        print(f"\n  Testing '{config_name}'...")
        result = run_configuration(
            config_name, signals_data, default_ts_configs, 
            default_sl, optimal_timeout, use_vol, use_dyn, vol_thresh
        )
        stage1_results.append(result)
        print(f"    Win Rate: {result['win_rate']:.2f}%, Avg Profit: {result['avg_profit']:.2f}%")
    
    # Print comparison
    print("\n" + "="*140)
    print("CONFIGURATION COMPARISON RESULTS:")
    print("="*140)
    print(f"{'Configuration':<20} {'Win Rate %':<15} {'Avg Profit %':<15} {'Total %':<15} {'Trades':<10}")
    print("-" * 80)
    
    for r in stage1_results:
        print(f"{r['config']:<20} {r['win_rate']:<15.2f} {r['avg_profit']:<15.2f} "
              f"{r['total_profit']:<15.2f} {r['trades']:<10}")
    
    # Find best config
    best_config = max(stage1_results, key=lambda x: x['avg_profit'])
    print(f"\n✨ Best Configuration: {best_config['config']} "
          f"(Avg Profit: {best_config['avg_profit']:.2f}%)")
    
    # STAGE 2: VOLUME THRESHOLD OPTIMIZATION
    if best_config['config'] in ['Volume Only', 'Full Advanced']:
        print("\n" + "="*140)
        print("STAGE 2: VOLUME THRESHOLD OPTIMIZATION")
        print("="*140)
        print(f"\nOptimizing volume threshold for '{best_config['config']}'...")
        
        use_volume = best_config['config'] in ['Volume Only', 'Full Advanced']
        use_dynamic = best_config['config'] in ['Dynamic Only', 'Full Advanced']
        
        threshold_results = []
        
        for thresh in volume_thresholds:
            print(f"\n  Testing threshold {thresh:.2f}...")
            result = run_configuration(
                f"Threshold {thresh:.2f}", signals_data, default_ts_configs,
                default_sl, optimal_timeout, use_volume, use_dynamic, thresh
            )
            threshold_results.append(result)
            print(f"    Avg Profit: {result['avg_profit']:.2f}%")
        
        # Find best threshold
        best_threshold_result = max(threshold_results, key=lambda x: x['avg_profit'])
        best_threshold = volume_thresholds[threshold_results.index(best_threshold_result)]
        
        print(f"\n✨ Optimal Volume Threshold: {best_threshold:.2f} "
              f"(Avg Profit: {best_threshold_result['avg_profit']:.2f}%)")
        
        # Print threshold comparison
        print("\n" + "="*140)
        print("VOLUME THRESHOLD COMPARISON:")
        print("="*140)
        print(f"{'Threshold':<15} {'Win Rate %':<15} {'Avg Profit %':<15} {'Total %':<15}")
        print("-" * 65)
        
        for i, r in enumerate(threshold_results):
            print(f"{volume_thresholds[i]:<15.2f} {r['win_rate']:<15.2f} "
                  f"{r['avg_profit']:<15.2f} {r['total_profit']:<15.2f}")
    else:
        best_threshold = None
    
    # STAGE 3: EXIT REASON ANALYSIS
    print("\n" + "="*140)
    print("STAGE 3: EXIT REASON ANALYSIS")
    print("="*140)
    
    for r in stage1_results:
        if r['exit_distribution']:
            print(f"\n{r['config']} - Exit Reason Distribution:")
            for reason, pct in sorted(r['exit_distribution'].items(), 
                                     key=lambda x: x[1], reverse=True):
                print(f"  {reason:<20}: {pct:>6.2f}%")
    
    # FINAL RESULTS
    print("\n" + "="*140)
    print("FINAL OPTIMAL CONFIGURATION")
    print("="*140)
    
    print(f"\nOptimal Strategy: {best_config['config']}")
    
    if best_threshold is not None:
        print(f"Volume Threshold: {best_threshold:.2f}")
    
    print(f"\nTS Configuration:")
    for i, ts in enumerate(default_ts_configs, 1):
        print(f"  TS{i} ({ts['portion']*100:.0f}%): "
              f"Activation {ts['activation_pct']}%, Callback {ts['callback_pct']}%")
    
    print(f"\nPerformance:")
    print(f"  Win Rate: {best_config['win_rate']:.2f}%")
    print(f"  Average Profit per Trade: {best_config['avg_profit']:.2f}%")
    print(f"  Total Profit (sum %): {best_config['total_profit']:.2f}%")
    print(f"  Number of Trades: {best_config['trades']}")
    
    # Improvement over baseline
    baseline = [r for r in stage1_results if r['config'] == 'Baseline'][0]
    
    print(f"\nImprovement over Baseline:")
    print(f"  Win Rate: {best_config['win_rate'] - baseline['win_rate']:+.2f}%")
    print(f"  Avg Profit: {best_config['avg_profit'] - baseline['avg_profit']:+.2f}%")
    print(f"  Total Profit: {best_config['total_profit'] - baseline['total_profit']:+.2f}%")
    
    # Python config
    print("\n" + "="*140)
    print("PYTHON CONFIGURATION:")
    print("="*140)
    
    use_volume_final = best_config['config'] in ['Volume Only', 'Full Advanced']
    use_dynamic_final = best_config['config'] in ['Dynamic Only', 'Full Advanced']
    
    print(f"""
# Optimal Configuration
USE_VOLUME_EXITS = {use_volume_final}
USE_DYNAMIC_CALLBACKS = {use_dynamic_final}
VOLUME_WEAKNESS_THRESHOLD = {best_threshold if best_threshold else 0.15}

POSITION_PORTIONS = [
    {{'size': {default_ts_configs[0]['portion']:.2f}, 'ts_activation': {default_ts_configs[0]['activation_pct']}, 'ts_callback': {default_ts_configs[0]['callback_pct']}}},
    {{'size': {default_ts_configs[1]['portion']:.2f}, 'ts_activation': {default_ts_configs[1]['activation_pct']}, 'ts_callback': {default_ts_configs[1]['callback_pct']}}},
    {{'size': {default_ts_configs[2]['portion']:.2f}, 'ts_activation': {default_ts_configs[2]['activation_pct']}, 'ts_callback': {default_ts_configs[2]['callback_pct']}}}
]

STOP_LOSS_PCT = {default_sl}
TIMEOUT_HOURS = {optimal_timeout}
""")
    
    print("="*140)

if __name__ == "__main__":
    optimize_volume_dynamic()
