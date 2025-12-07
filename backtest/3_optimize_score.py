#!/usr/bin/env python3
"""
Backtest: Optimize strategy across different score thresholds
Tests score thresholds from 150 to 300 with step 10
"""
import sys
from pathlib import Path
import json
from datetime import datetime, timezone

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
    """
    Deduplicate signals: keep only one signal per pair within cooldown period
    If multiple signals for same pair within cooldown, keep the one with highest score
    
    Args:
        signals: List of signal dicts with 'pair_symbol', 'signal_timestamp', 'total_score'
        cooldown_hours: Cooldown period in hours (default: 12)
    
    Returns:
        List of deduplicated signals
    """
    from datetime import timedelta
    
    # Sort by timestamp
    sorted_signals = sorted(signals, key=lambda x: x['signal_timestamp'])
    
    # Track last signal time for each pair
    last_signal_time = {}
    deduplicated = []
    
    for signal in sorted_signals:
        symbol = signal['pair_symbol']
        timestamp = signal['signal_timestamp']
        score = signal['total_score']
        
        if symbol not in last_signal_time:
            # First signal for this pair
            last_signal_time[symbol] = timestamp
            deduplicated.append(signal)
        else:
            # Check if cooldown period has passed
            time_diff = (timestamp - last_signal_time[symbol]).total_seconds() / 3600
            
            if time_diff >= cooldown_hours:
                # Cooldown passed, add this signal
                last_signal_time[symbol] = timestamp
                deduplicated.append(signal)
            else:
                # Within cooldown - check if this signal has higher score
                # Find the last added signal for this pair
                for i in range(len(deduplicated) - 1, -1, -1):
                    if deduplicated[i]['pair_symbol'] == symbol:
                        if score > deduplicated[i]['total_score']:
                            # Replace with higher score signal
                            deduplicated[i] = signal
                            last_signal_time[symbol] = timestamp
                        break
    
    return deduplicated

def backtest_with_score_threshold(min_score=150, max_score=300, step=10,
                                   sl_pct=-7, activation_pct=14, callback_pct=9, timeout_hours=20,
                                   cooldown_hours=12):
    """
    Test strategy performance across different score thresholds
    
    Args:
        min_score: Minimum score threshold to test
        max_score: Maximum score threshold to test
        step: Step size for score thresholds
        sl_pct: Stop-loss percentage
        activation_pct: TS activation percentage
        callback_pct: TS callback percentage
        timeout_hours: Timeout in hours
        cooldown_hours: Deduplication cooldown in hours (default: 12)
    """
    print("="*120)
    print("SCORE THRESHOLD OPTIMIZATION")
    print(f"Testing score thresholds from {min_score} to {max_score} with step {step}")
    print(f"Strategy: SL={sl_pct}%, TS Activation={activation_pct}%, Callback={callback_pct}%, Timeout={timeout_hours}h")
    print(f"Deduplication: {cooldown_hours}h cooldown per pair")
    print("="*120)
    
    results = []
    
    # Test each score threshold
    for score_threshold in range(min_score, max_score + 1, step):
        print(f"\n{'='*120}")
        print(f"Testing Score Threshold: {score_threshold}")
        print(f"{'='*120}")
        
        # Fetch signals with this threshold
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
            continue
        
        print(f"Found {len(rows)} signals with score >= {score_threshold}")
        
        # Convert to signal dicts for deduplication
        all_signals = [{
            'pair_symbol': row[0],
            'signal_timestamp': row[1],
            'total_score': row[2],
            'entry_price': float(row[3]),
            'candles_data': row[4],
            'entry_time_dt': row[5]
        } for row in rows]
        
        # Deduplicate: one signal per pair within cooldown period
        deduplicated_signals = deduplicate_signals_by_cooldown(all_signals, cooldown_hours)
        print(f"After {cooldown_hours}h deduplication: {len(deduplicated_signals)} unique signals")
        
        # Simulate trades
        trades = []
        exit_reasons = {'TS': 0, 'SL': 0, 'LIQUIDATION': 0, 'TIMEOUT': 0, 'EOD': 0}
        
        for signal in deduplicated_signals:
            symbol = signal['pair_symbol']
            signal_ts = signal['signal_timestamp']
            total_score = signal['total_score']
            entry_price = signal['entry_price']
            candles_data = signal['candles_data']
            entry_time_dt = signal['entry_time_dt']
            
            if not candles_data:
                continue
            
            # Parse candles
            if isinstance(candles_data, str):
                candles = json.loads(candles_data)
            else:
                candles = candles_data
            
            # Simulate trade
            (exit_reason, exit_time_dt, pnl_pct, max_dd_pct, max_dd_time, 
             max_pump_pct, max_pump_time, ts_activation_time) = simulate_signal_exit(
                candles, entry_price, entry_time_dt,
                sl_pct, activation_pct, callback_pct, timeout_hours
            )

            
            exit_reasons[exit_reason] += 1
            
            trades.append({
                'symbol': symbol,
                'timestamp': signal_ts,
                'score': total_score,
                'entry_price': entry_price,
                'exit_reason': exit_reason,
                'pnl_pct': pnl_pct
            })
        
        # Calculate metrics
        winning_trades = [t for t in trades if t['pnl_pct'] > 0]
        total_pnl = sum(t['pnl_pct'] for t in trades)
        avg_pnl = total_pnl / len(trades) if trades else 0
        win_rate = (len(winning_trades) / len(trades) * 100) if trades else 0
        
        # Calculate max drawdown
        cumulative_pnl = 0
        peak = 0
        max_dd = 0
        for t in trades:
            cumulative_pnl += t['pnl_pct']
            if cumulative_pnl > peak:
                peak = cumulative_pnl
            dd = peak - cumulative_pnl
            if dd > max_dd:
                max_dd = dd

        
        # Store results
        result = {
            'score_threshold': score_threshold,
            'total_signals': len(rows),
            'total_trades': len(trades),
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'win_rate': win_rate,
            'max_dd': max_dd,
            'exit_reasons': exit_reasons
        }
        results.append(result)
        
        # Print summary
        print(f"\nResults for Score >= {score_threshold}:")
        print(f"  Total Signals: {len(rows)}")
        print(f"  Total Trades: {len(trades)}")
        print(f"  Total PnL: {total_pnl:.2f}%")
        print(f"  Avg PnL: {avg_pnl:.2f}%")
        print(f"  Win Rate: {win_rate:.1f}%")
        print(f"  Max Drawdown: {max_dd:.2f}%")

        print(f"\n  Exit Breakdown:")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
            pct = (count / len(trades) * 100) if len(trades) > 0 else 0
            print(f"    {reason:<12}: {count:>3} ({pct:>5.1f}%)")
    
    # Print comparison table
    print("\n" + "="*120)
    print("COMPARISON TABLE")
    print("="*120)
    print(f"{'Score':<8} {'Signals':<10} {'Trades':<10} {'Total PnL':<12} {'Avg PnL':<10} {'Win Rate':<10} {'TS':<8} {'SL':<8} {'LIQ':<8}")
    print("-"*120)
    
    for r in results:
        print(f"{r['score_threshold']:<8} {r['total_signals']:<10} {r['total_trades']:<10} "
              f"{r['total_pnl']:>10.2f}%  {r['avg_pnl']:>8.2f}%  {r['win_rate']:>8.1f}%  "
              f"{r['exit_reasons']['TS']:<8} {r['exit_reasons']['SL']:<8} {r['exit_reasons']['LIQUIDATION']:<8}")
    
    # Find best threshold
    if results:
        best_by_total_pnl = max(results, key=lambda x: x['total_pnl'])
        best_by_avg_pnl = max(results, key=lambda x: x['avg_pnl'])
        best_by_win_rate = max(results, key=lambda x: x['win_rate'])
        
        print("\n" + "="*120)
        print("BEST THRESHOLDS")
        print("="*120)
        print(f"Best Total PnL: Score >= {best_by_total_pnl['score_threshold']} ({best_by_total_pnl['total_pnl']:.2f}%)")
        print(f"Best Avg PnL: Score >= {best_by_avg_pnl['score_threshold']} ({best_by_avg_pnl['avg_pnl']:.2f}%)")
        print(f"Best Win Rate: Score >= {best_by_win_rate['score_threshold']} ({best_by_win_rate['win_rate']:.1f}%)")
        print("="*120)
    
    # Save results to file
    output_file = current_dir / f"score_optimization_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nResults saved to: {output_file}")
    
    return results

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Optimize strategy across score thresholds')
    parser.add_argument('--min-score', type=int, default=150, help='Minimum score threshold')
    parser.add_argument('--max-score', type=int, default=300, help='Maximum score threshold')
    parser.add_argument('--step', type=int, default=10, help='Step size for thresholds')
    parser.add_argument('--sl', type=float, default=-7, help='Stop-loss percentage')
    parser.add_argument('--activation', type=float, default=14, help='TS activation percentage')
    parser.add_argument('--callback', type=float, default=9, help='TS callback percentage')
    parser.add_argument('--timeout', type=float, default=20, help='Timeout hours')
    parser.add_argument('--cooldown', type=float, default=12, help='Deduplication cooldown hours')
    args = parser.parse_args()
    
    backtest_with_score_threshold(
        min_score=args.min_score,
        max_score=args.max_score,
        step=args.step,
        sl_pct=args.sl,
        activation_pct=args.activation,
        callback_pct=args.callback,
        timeout_hours=args.timeout,
        cooldown_hours=args.cooldown
    )

