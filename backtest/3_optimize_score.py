#!/usr/bin/env python3
"""
Backtest: Optimize strategy parameters for different score thresholds
For each score threshold, finds best SL/TS/Callback combination
"""
import sys
from pathlib import Path
import json
from datetime import datetime, timezone
from itertools import product

# Add parent scripts directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
scripts_dir = parent_dir / 'scripts_v2'
sys.path.append(str(scripts_dir))

from pump_analysis_lib import get_db_connection, EXCHANGE_FILTER, EXCHANGE_IDS
from optimization_lib import get_candle_direction

# Import simulation function from report_enhanced
sys.path.insert(0, str(scripts_dir))
from report_enhanced import simulate_signal_exit

def deduplicate_signals_by_cooldown(signals, cooldown_hours=12):
    """Deduplicate signals: keep highest score per pair within cooldown"""
    from datetime import timedelta
    
    sorted_signals = sorted(signals, key=lambda x: x['signal_timestamp'])
    last_signal_time = {}
    deduplicated = []
    
    for signal in sorted_signals:
        symbol = signal['pair_symbol']
        timestamp = signal['signal_timestamp']
        score = signal['total_score']
        
        if symbol not in last_signal_time:
            last_signal_time[symbol] = timestamp
            deduplicated.append(signal)
        else:
            time_diff = (timestamp - last_signal_time[symbol]).total_seconds() / 3600
            
            if time_diff >= cooldown_hours:
                last_signal_time[symbol] = timestamp
                deduplicated.append(signal)
            else:
                # Within cooldown - replace if higher score
                for i in range(len(deduplicated) - 1, -1, -1):
                    if deduplicated[i]['pair_symbol'] == symbol:
                        if score > deduplicated[i]['total_score']:
                            deduplicated[i] = signal
                            last_signal_time[symbol] = timestamp
                        break
    
    return deduplicated

def optimize_for_score_threshold(score_threshold, cooldown_hours=12):
    """
    Optimize SL/TS/Callback parameters for signals with given score threshold
    Returns best parameters and results
    """
    print(f"\n{'='*120}")
    print(f"OPTIMIZING FOR SCORE >= {score_threshold}")
    print(f"{'='*120}")
    
    # Fetch signals
    conn = get_db_connection()
    
    query = f"""
        SELECT 
            sa.pair_symbol,
            sa.signal_timestamp,
            sa.total_score,
            sa.entry_price,
            sa.candles_data,
            sa.entry_time
        FROM web.signal_analysis sa
        JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
        WHERE sa.total_score >= {score_threshold}
        AND (
            '{EXCHANGE_FILTER}' = 'ALL' 
            OR ('{EXCHANGE_FILTER}' = 'BINANCE' AND tp.exchange_id = {EXCHANGE_IDS['BINANCE']})
            OR ('{EXCHANGE_FILTER}' = 'BYBIT' AND tp.exchange_id = {EXCHANGE_IDS['BYBIT']})
        )
        ORDER BY sa.signal_timestamp ASC
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    
    conn.close()
    
    if not rows:
        print(f"No signals found for score >= {score_threshold}")
        return None
    
    print(f"Found {len(rows)} raw signals")
    
    # Convert and deduplicate
    all_signals = [{
        'pair_symbol': row[0],
        'signal_timestamp': row[1],
        'total_score': row[2],
        'entry_price': float(row[3]),
        'candles_data': row[4],
        'entry_time_dt': row[5]
    } for row in rows]
    
    signals = deduplicate_signals_by_cooldown(all_signals, cooldown_hours)
    print(f"After {cooldown_hours}h deduplication: {len(signals)} unique signals")
    
    if len(signals) < 10:
        print(f"⚠️  Too few signals ({len(signals)}) for reliable optimization")
        return None
    
    # Parameter ranges (like optimize_advanced.py)
    sl_levels = list(range(-10, 0))  # -10% to -1%
    activation_levels = list(range(5, 31, 5))  # 5%, 10%, 15%, 20%, 25%, 30%
    callback_rates = list(range(5, 16, 5))  # 5%, 10%, 15%
    timeout_options = [20]  # Fixed 20h
    
    total_combos = len(sl_levels) * len(activation_levels) * len(callback_rates)
    print(f"\nParameter space:")
    print(f"  SL: {sl_levels}")
    print(f"  Activation: {activation_levels}")
    print(f"  Callback: {callback_rates}")
    print(f"  Total combinations: {total_combos}")
    
    print(f"\nTesting all combinations...")
    results = []
    current = 0
    
    for sl, activation, callback, timeout in product(sl_levels, activation_levels, callback_rates, timeout_options):
        current += 1
        if current % 50 == 0:
            print(f"  Progress: {current}/{total_combos} ({current/total_combos*100:.1f}%)...", end='\r')
        
        profits = []
        
        for signal in signals:
            candles_data = signal['candles_data']
            
            if not candles_data:
                continue
            
            if isinstance(candles_data, str):
                candles = json.loads(candles_data)
            else:
                candles = candles_data
            
            # Simulate
            (exit_reason, exit_time_dt, pnl_pct, max_dd_pct, max_dd_time, 
             max_pump_pct, max_pump_time, ts_activation_time) = simulate_signal_exit(
                candles, signal['entry_price'], signal['entry_time_dt'],
                sl, activation, callback, timeout
            )
            
            profits.append(pnl_pct)
        
        if not profits:
            continue
        
        win_rate = len([p for p in profits if p > 0]) / len(profits) * 100
        avg_profit = sum(profits) / len(profits)
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
    
    print()  # New line after progress
    
    # Sort by average profit
    results.sort(key=lambda x: x['avg_profit'], reverse=True)
    
    # Print top 10
    print(f"\nTop 10 strategies:")
    print(f"{'SL':<5} {'Activ':<7} {'CB':<5} {'Win%':<7} {'AvgPnL':<10} {'TotalPnL':<10}")
    print("-"*50)
    for r in results[:10]:
        print(f"{r['sl']:<5} {r['activation']:<7} {r['callback']:<5} "
              f"{r['win_rate']:<7.1f} {r['avg_profit']:<10.2f} {r['total_profit']:<10.2f}")
    
    best = results[0]
    print(f"\n✅ Best: SL={best['sl']}%, TS={best['activation']}%, CB={best['callback']}%")
    print(f"   Win Rate: {best['win_rate']:.1f}%, Avg PnL: {best['avg_profit']:.2f}%")
    
    return {
        'score_threshold': score_threshold,
        'signals_count': len(signals),
        'best_params': {
            'sl': best['sl'],
            'activation': best['activation'],
            'callback': best['callback'],
            'timeout': best['timeout']
        },
        'best_metrics': {
            'win_rate': best['win_rate'],
            'avg_profit': best['avg_profit'],
            'total_profit': best['total_profit'],
            'trades': best['trades']
        },
        'all_results': results[:20]  # Keep top 20
    }

def backtest_score_thresholds(min_score=150, max_score=300, step=50, cooldown_hours=12):
    """
    Optimize parameters for each score threshold
    """
    print("="*120)
    print("SCORE THRESHOLD OPTIMIZATION WITH PARAMETER SEARCH")
    print(f"Testing thresholds: {min_score} to {max_score}, step {step}")
    print(f"Deduplication: {cooldown_hours}h cooldown")
    print("="*120)
    
    all_results = []
    
    for score_threshold in range(min_score, max_score + 1, step):
        result = optimize_for_score_threshold(score_threshold, cooldown_hours)
        if result:
            all_results.append(result)
    
    # Summary table
    print("\n" + "="*120)
    print("SUMMARY: BEST PARAMETERS FOR EACH SCORE THRESHOLD")
    print("="*120)
    print(f"{'Score':<8} {'Signals':<10} {'Best SL':<10} {'Best TS':<10} {'Best CB':<10} "
          f"{'Win%':<8} {'AvgPnL':<10} {'TotalPnL':<10}")
    print("-"*120)
    
    for r in all_results:
        print(f"{r['score_threshold']:<8} {r['signals_count']:<10} "
              f"{r['best_params']['sl']:<10} {r['best_params']['activation']:<10} "
              f"{r['best_params']['callback']:<10} "
              f"{r['best_metrics']['win_rate']:<8.1f} "
              f"{r['best_metrics']['avg_profit']:<10.2f} "
              f"{r['best_metrics']['total_profit']:<10.2f}")
    
    print("="*120)
    
    # Save results
    output_file = current_dir / f"score_optimization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\nResults saved to: {output_file}")
    
    return all_results

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Optimize parameters for different score thresholds')
    parser.add_argument('--min-score', type=int, default=150, help='Minimum score threshold')
    parser.add_argument('--max-score', type=int, default=300, help='Maximum score threshold')
    parser.add_argument('--step', type=int, default=10, help='Step size (default: 10)')
    parser.add_argument('--cooldown', type=float, default=12, help='Deduplication cooldown hours')
    args = parser.parse_args()
    
    backtest_score_thresholds(
        min_score=args.min_score,
        max_score=args.max_score,
        step=args.step,
        cooldown_hours=args.cooldown
    )
