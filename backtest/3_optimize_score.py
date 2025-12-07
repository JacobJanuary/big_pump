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
from optimization_lib import simulate_combined  # Use same function as optimize_advanced.py!

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
            sa.id,
            sa.pair_symbol,
            sa.signal_timestamp,
            sa.total_score,
            sa.entry_price
        FROM web.signal_analysis sa
        JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
        WHERE sa.total_score >= {score_threshold}
        AND EXISTS (
            SELECT 1 FROM web.minute_candles mc
            WHERE mc.signal_analysis_id = sa.id
        )
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
    
    if not rows:
        print(f"No signals found for score >= {score_threshold}")
        conn.close()
        return None
    
    print(f"Found {len(rows)} raw signals")
    
    # Load signals with candles from minute_candles table (like optimize_advanced.py)
    signals_data = []
    
    for row in rows:
        signal_id = row[0]
        
        # Fetch minute candles for this signal
        candles_query = """
            SELECT open_time, open_price, high_price, low_price, close_price
            FROM web.minute_candles
            WHERE signal_analysis_id = %s
            ORDER BY open_time ASC
        """
        
        with conn.cursor() as cur:
            cur.execute(candles_query, (signal_id,))
            candles_rows = cur.fetchall()
        
        candles = [{
            'open_time': int(c[0]),
            'open_price': float(c[1]),
            'high_price': float(c[2]),
            'low_price': float(c[3]),
            'close_price': float(c[4])
        } for c in candles_rows]
        
        if candles:
            signals_data.append({
                'signal_id': signal_id,
                'pair_symbol': row[1],
                'signal_timestamp': row[2],
                'total_score': row[3],
                'entry_price': float(row[4]),
                'candles': candles
            })
    
    conn.close()
    
    print(f"Loaded {len(signals_data)} signals with minute candles")
    
    # Deduplicate
    deduplicated_signals = deduplicate_signals_by_cooldown(signals_data, cooldown_hours)
    print(f"After {cooldown_hours}h deduplication: {len(deduplicated_signals)} unique signals")
    
    if len(deduplicated_signals) < 10:
        print(f"⚠️  Too few signals ({len(deduplicated_signals)}) for reliable optimization")
        return None
    
    # Parameter ranges (EXACTLY like optimize_advanced.py)
    sl_levels = list(range(-10, 0))  # -10% to -1%, step 1% = 10 values
    activation_levels = list(range(3, 51))  # 3% to 50%, step 1% = 48 values
    callback_rates = list(range(1, 11))  # 1% to 10%, step 1% = 10 values
    timeout_options = [20]  # Fixed 20h
    
    total_combos = len(sl_levels) * len(activation_levels) * len(callback_rates)
    print(f"\nParameter space:")
    print(f"  SL: {len(sl_levels)} values ({min(sl_levels)}% to {max(sl_levels)}%)")
    print(f"  Activation: {len(activation_levels)} values ({min(activation_levels)}% to {max(activation_levels)}%)")
    print(f"  Callback: {len(callback_rates)} values ({min(callback_rates)}% to {max(callback_rates)}%)")
    print(f"  Total combinations: {total_combos}")
    
    print(f"\nTesting all combinations...")
    results = []
    current = 0
    
    for sl, activation, callback, timeout in product(sl_levels, activation_levels, callback_rates, timeout_options):
        current += 1
        if current % 500 == 0:
            print(f"  Progress: {current}/{total_combos} ({current/total_combos*100:.1f}%)...", end='\r')
        
        profits = []
        
        for signal in deduplicated_signals:
            # Simulate using same function as optimize_advanced.py
            pnl_pct = simulate_combined(
                signal['candles'],
                signal['entry_price'],
                sl,
                activation,
                callback,
                timeout_hours=timeout
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
