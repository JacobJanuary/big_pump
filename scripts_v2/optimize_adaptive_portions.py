"""
Adaptive Portion Size Optimizer for 3-Tier Partial Closing

This script automatically finds:
1. Optimal portion sizes (e.g., 30%/30%/40% vs 20%/50%/30%)
2. Optimal TS parameters for each portion

Uses multi-level optimization:
- Stage 1: Coarse portion size search (step=10%)
- Stage 2: TS optimization for top splits
- Stage 3: Fine-tuning best split (step=5%)
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

def generate_portion_splits(step=10, min_portion=10):
    """
    Generate all valid 3-way portion splits that sum to 100%
    
    Args:
        step: Step size for iteration (%)
        min_portion: Minimum size for any portion (%)
    
    Returns:
        List of tuples (p1, p2, p3) as decimals
    """
    splits = []
    for p1 in range(min_portion, 100 - min_portion, step):
        for p2 in range(min_portion, 100 - p1, step):
            p3 = 100 - p1 - p2
            if p3 >= min_portion:
                splits.append((p1/100, p2/100, p3/100))
    return splits

def get_ts_ranges_for_portion(portion_index):
    """
    Get TS parameter ranges based on portion index
    
    Args:
        portion_index: 0=early, 1=mid, 2=late
    
    Returns:
        (activation_range, callback_range)
    """
    if portion_index == 0:  # Early exit
        activation = [round(x * 0.5, 1) for x in range(10, 31)]  # 5-15%
        callback = [round(x * 0.5, 1) for x in range(1, 5)]      # 0.5-2%
    elif portion_index == 1:  # Mid exit
        activation = [round(x * 0.5, 1) for x in range(30, 81)]  # 15-40%
        callback = [round(x * 0.5, 1) for x in range(1, 7)]      # 0.5-3%
    else:  # Late exit
        activation = [round(x, 1) for x in range(40, 101)]       # 40-100%
        callback = [round(x, 1) for x in range(1, 11)]           # 1-10%
    
    return activation, callback

def evaluate_with_default_ts(portions, signals_data, default_sl, timeout_hours):
    """
    Quick evaluation with default TS parameters
    
    Args:
        portions: Tuple of (p1, p2, p3)
        signals_data: List of signals
        default_sl: Default stop-loss %
        timeout_hours: Timeout in hours
    
    Returns:
        Average profit %
    """
    # Default TS configs based on portion index
    ts_configs = [
        {'portion': portions[0], 'activation_pct': 10.0, 'callback_pct': 1.0},
        {'portion': portions[1], 'activation_pct': 25.0, 'callback_pct': 1.5},
        {'portion': portions[2], 'activation_pct': 60.0, 'callback_pct': 5.0}
    ]
    
    profits = []
    for sig in signals_data:
        pnl = simulate_partial_close(
            sig['candles'], 
            sig['entry_price'], 
            default_sl,
            ts_configs,
            timeout_hours=timeout_hours
        )
        profits.append(pnl)
    
    return sum(profits) / len(profits) if profits else 0

def optimize_ts_for_portions(portions, signals_data, default_sl, timeout_hours):
    """
    Staged TS optimization for given portion split
    
    Args:
        portions: Tuple of (p1, p2, p3)
        signals_data: List of signals
        default_sl: Default stop-loss %
        timeout_hours: Timeout in hours
    
    Returns:
        Dict with optimized TS params and performance
    """
    # Initialize with defaults
    best_ts = [
        {'portion': portions[0], 'activation_pct': 10.0, 'callback_pct': 1.0},
        {'portion': portions[1], 'activation_pct': 25.0, 'callback_pct': 1.5},
        {'portion': portions[2], 'activation_pct': 60.0, 'callback_pct': 5.0}
    ]
    
    # Optimize each TS sequentially
    for ts_idx in range(3):
        print(f"    Optimizing TS{ts_idx+1} ({portions[ts_idx]*100:.0f}%)...")
        
        activation_range, callback_range = get_ts_ranges_for_portion(ts_idx)
        best_avg = float('-inf')
        best_params = None
        
        total_combos = len(activation_range) * len(callback_range)
        current = 0
        
        for act, cb in product(activation_range, callback_range):
            current += 1
            if total_combos > 100 and current % 50 == 0:
                print(f"      Progress: {current}/{total_combos} ({current/total_combos*100:.1f}%)...")
            
            # Update current TS config
            test_ts = best_ts.copy()
            test_ts[ts_idx] = {'portion': portions[ts_idx], 'activation_pct': act, 'callback_pct': cb}
            
            profits = []
            for sig in signals_data:
                pnl = simulate_partial_close(
                    sig['candles'], 
                    sig['entry_price'], 
                    default_sl,
                    test_ts,
                    timeout_hours=timeout_hours
                )
                profits.append(pnl)
            
            avg_profit = sum(profits) / len(profits) if profits else 0
            
            if avg_profit > best_avg:
                best_avg = avg_profit
                best_params = (act, cb)
        
        # Update with best found
        best_ts[ts_idx] = {'portion': portions[ts_idx], 'activation_pct': best_params[0], 'callback_pct': best_params[1]}
        print(f"      Best: Activation {best_params[0]}%, Callback {best_params[1]}%, Avg Profit: {best_avg:.2f}%")
    
    # Final evaluation
    profits = []
    for sig in signals_data:
        pnl = simulate_partial_close(
            sig['candles'], 
            sig['entry_price'], 
            default_sl,
            best_ts,
            timeout_hours=timeout_hours
        )
        profits.append(pnl)
    
    win_rate = len([p for p in profits if p > 0]) / len(profits) * 100 if profits else 0
    avg_profit = sum(profits) / len(profits) if profits else 0
    total_profit = sum(profits)
    
    return {
        'ts_configs': best_ts,
        'win_rate': win_rate,
        'avg_profit': avg_profit,
        'total_profit': total_profit
    }

def optimize_adaptive_portions():
    """
    Main optimization function
    """
    print("="*140)
    print("ADAPTIVE PORTION SIZE OPTIMIZATION")
    print("="*140)
    
    # Load data
    print("\nLoading signals with 1-minute candles from database...")
    signals_data = load_signals_with_minute_candles()
    
    if not signals_data:
        print("No signals with minute candles found.")
        print("Please run:")
        print("  1. python3 scripts/populate_signal_analysis.py")
        print("  2. python3 scripts/fetch_minute_candles.py")
        return
    
    print(f"Loaded {len(signals_data)} signals with minute candles.")
    
    # Calculate timeout
    print("\nCalculating time-to-peak statistics...")
    peak_stats = calculate_peak_time_stats(signals_data)
    optimal_timeout = int(peak_stats['p90']) + 1
    print(f"Using timeout: {optimal_timeout} hours (90th percentile + buffer)")
    
    default_sl = -5
    
    # STAGE 1: Coarse Portion Size Search
    print("\n" + "="*140)
    print("STAGE 1: COARSE PORTION SIZE SEARCH (step=10%)")
    print("="*140)
    
    portion_candidates = generate_portion_splits(step=10, min_portion=10)
    print(f"\nGenerated {len(portion_candidates)} valid portion splits")
    print("Testing with default TS parameters...\n")
    
    results_stage1 = []
    for idx, portions in enumerate(portion_candidates, 1):
        if idx % 5 == 0:
            print(f"  Testing split {idx}/{len(portion_candidates)}...")
        
        avg_profit = evaluate_with_default_ts(portions, signals_data, default_sl, optimal_timeout)
        results_stage1.append({
            'portions': portions,
            'avg_profit': avg_profit
        })
    
    # Sort by avg profit
    results_stage1.sort(key=lambda x: x['avg_profit'], reverse=True)
    
    print(f"\nTop 10 Portion Splits from Stage 1:")
    print(f"{'Rank':<6} {'Split':<20} {'Avg Profit %':<15}")
    print("-" * 50)
    for idx, result in enumerate(results_stage1[:10], 1):
        p1, p2, p3 = result['portions']
        split_str = f"{p1*100:.0f}%/{p2*100:.0f}%/{p3*100:.0f}%"
        print(f"{idx:<6} {split_str:<20} {result['avg_profit']:<15.2f}")
    
    # STAGE 2: TS Optimization for Top 5
    print("\n" + "="*140)
    print("STAGE 2: TS OPTIMIZATION FOR TOP 5 SPLITS")
    print("="*140)
    
    top_n = 5
    results_stage2 = []
    
    for idx, result in enumerate(results_stage1[:top_n], 1):
        portions = result['portions']
        p1, p2, p3 = portions
        print(f"\n[{idx}/{top_n}] Optimizing {p1*100:.0f}%/{p2*100:.0f}%/{p3*100:.0f}%...")
        
        optimized = optimize_ts_for_portions(portions, signals_data, default_sl, optimal_timeout)
        
        results_stage2.append({
            'portions': portions,
            'ts_configs': optimized['ts_configs'],
            'win_rate': optimized['win_rate'],
            'avg_profit': optimized['avg_profit'],
            'total_profit': optimized['total_profit']
        })
        
        print(f"  Final: Avg Profit {optimized['avg_profit']:.2f}%, Win Rate {optimized['win_rate']:.2f}%")
    
    # Sort by avg profit
    results_stage2.sort(key=lambda x: x['avg_profit'], reverse=True)
    
    # STAGE 3: Fine-tuning around best
    print("\n" + "="*140)
    print("STAGE 3: FINE-TUNING BEST SPLIT (step=5%)")
    print("="*140)
    
    best_from_stage2 = results_stage2[0]
    base_portions = best_from_stage2['portions']
    p1, p2, p3 = base_portions
    
    print(f"\nBest from Stage 2: {p1*100:.0f}%/{p2*100:.0f}%/{p3*100:.0f}%")
    print(f"Avg Profit: {best_from_stage2['avg_profit']:.2f}%")
    print("\nFine-tuning around this split (±10%, step=5%)...")
    
    # Generate fine-tuned splits around best
    fine_splits = []
    for delta1 in [-10, -5, 0, 5, 10]:
        for delta2 in [-10, -5, 0, 5, 10]:
            new_p1 = max(10, min(50, int(p1*100) + delta1)) / 100
            new_p2 = max(10, min(50, int(p2*100) + delta2)) / 100
            new_p3 = 1.0 - new_p1 - new_p2
            
            if new_p3 >= 0.10 and new_p3 <= 0.80:
                if (new_p1, new_p2, new_p3) not in fine_splits:
                    fine_splits.append((new_p1, new_p2, new_p3))
    
    print(f"Testing {len(fine_splits)} fine-tuned splits...")
    
    results_stage3 = []
    for idx, portions in enumerate(fine_splits, 1):
        if idx % 3 == 0:
            print(f"  Progress: {idx}/{len(fine_splits)}...")
        
        optimized = optimize_ts_for_portions(portions, signals_data, default_sl, optimal_timeout)
        
        results_stage3.append({
            'portions': portions,
            'ts_configs': optimized['ts_configs'],
            'win_rate': optimized['win_rate'],
            'avg_profit': optimized['avg_profit'],
            'total_profit': optimized['total_profit']
        })
    
    # Sort by avg profit
    results_stage3.sort(key=lambda x: x['avg_profit'], reverse=True)
    
    # FINAL RESULTS
    print("\n" + "="*140)
    print("FINAL OPTIMAL CONFIGURATION")
    print("="*140)
    
    best_overall = results_stage3[0]
    p1, p2, p3 = best_overall['portions']
    ts_configs = best_overall['ts_configs']
    
    print(f"\nOptimal Portion Split: {p1*100:.0f}%/{p2*100:.0f}%/{p3*100:.0f}%")
    print(f"\nTS1 ({p1*100:.0f}%): Activation {ts_configs[0]['activation_pct']}%, Callback {ts_configs[0]['callback_pct']}%")
    print(f"TS2 ({p2*100:.0f}%): Activation {ts_configs[1]['activation_pct']}%, Callback {ts_configs[1]['callback_pct']}%")
    print(f"TS3 ({p3*100:.0f}%): Activation {ts_configs[2]['activation_pct']}%, Callback {ts_configs[2]['callback_pct']}%")
    
    print(f"\nPerformance:")
    print(f"  Win Rate: {best_overall['win_rate']:.2f}%")
    print(f"  Average Profit per Trade: {best_overall['avg_profit']:.2f}%")
    print(f"  Total Profit (sum of %): {best_overall['total_profit']:.2f}%")
    print(f"  Number of Trades: {len(signals_data)}")
    
    # Comparison with single TS
    print("\n" + "="*140)
    print("COMPARISON: Adaptive Portions vs Single TS")
    print("="*140)
    
    # Weighted average activation/callback
    avg_activation = sum(ts_configs[i]['activation_pct'] * best_overall['portions'][i] for i in range(3))
    avg_callback = sum(ts_configs[i]['callback_pct'] * best_overall['portions'][i] for i in range(3))
    
    print(f"\nRunning single TS baseline: Activation {avg_activation:.1f}%, Callback {avg_callback:.1f}%...")
    
    single_profits = []
    for sig in signals_data:
        pnl = simulate_combined(
            sig['candles'], 
            sig['entry_price'], 
            default_sl,
            int(avg_activation),
            int(avg_callback),
            timeout_hours=optimal_timeout
        )
        single_profits.append(pnl)
    
    single_win_rate = len([p for p in single_profits if p > 0]) / len(single_profits) * 100 if single_profits else 0
    single_avg = sum(single_profits) / len(single_profits) if single_profits else 0
    single_total = sum(single_profits)
    
    print(f"\nSingle TS Results:")
    print(f"  Win Rate: {single_win_rate:.2f}%")
    print(f"  Average Profit: {single_avg:.2f}%")
    print(f"  Total Profit: {single_total:.2f}%")
    
    print(f"\nAdaptive Portions Results:")
    print(f"  Win Rate: {best_overall['win_rate']:.2f}%")
    print(f"  Average Profit: {best_overall['avg_profit']:.2f}%")
    print(f"  Total Profit: {best_overall['total_profit']:.2f}%")
    
    print(f"\nImprovement:")
    print(f"  Win Rate: {best_overall['win_rate'] - single_win_rate:+.2f}%")
    print(f"  Average Profit: {best_overall['avg_profit'] - single_avg:+.2f}%")
    print(f"  Total Profit: {best_overall['total_profit'] - single_total:+.2f}%")
    
    # Python config
    print("\n" + "="*140)
    print("CONFIGURATION FOR PRODUCTION:")
    print("="*140)
    print(f"""
POSITION_PORTIONS_ADAPTIVE = [
    {{'size': {p1:.2f}, 'ts_activation': {ts_configs[0]['activation_pct']}, 'ts_callback': {ts_configs[0]['callback_pct']}}},
    {{'size': {p2:.2f}, 'ts_activation': {ts_configs[1]['activation_pct']}, 'ts_callback': {ts_configs[1]['callback_pct']}}},
    {{'size': {p3:.2f}, 'ts_activation': {ts_configs[2]['activation_pct']}, 'ts_callback': {ts_configs[2]['callback_pct']}}}
]
STOP_LOSS_PCT = {default_sl}
TIMEOUT_HOURS = {optimal_timeout}
""")
    print("="*140)
    
    # Summary of all top configurations
    print("\n" + "="*140)
    print("TOP 5 CONFIGURATIONS FROM FINE-TUNING:")
    print("="*140)
    print(f"{'Rank':<6} {'Split':<20} {'Avg Profit %':<15} {'Win Rate %':<15}")
    print("-" * 65)
    for idx, result in enumerate(results_stage3[:5], 1):
        p1, p2, p3 = result['portions']
        split_str = f"{p1*100:.0f}%/{p2*100:.0f}%/{p3*100:.0f}%"
        print(f"{idx:<6} {split_str:<20} {result['avg_profit']:<15.2f} {result['win_rate']:<15.2f}")
    print("="*140)

if __name__ == "__main__":
    optimize_adaptive_portions()
