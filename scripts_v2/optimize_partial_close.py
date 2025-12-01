"""
Partial Position Closing Optimization with 4-part strategy
Splits position into 4x25% portions with independent trailing stops

TS1 (25%): 5-10% activation, 0.5-3% callback, step 0.5%
TS2 (25%): 10-20% activation, 0.5-3% callback, step 0.5%
TS3 (25%): 20-30% activation, 0.5-3% callback, step 0.5%
TS4 (25%): 30-50% activation, 1-10% callback, step 1%
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

def optimize_partial_close():
    """
    Run staged optimization for 4-part partial closing strategy
    """
    print("="*140)
    print("PARTIAL POSITION CLOSING OPTIMIZATION (4-part strategy)")
    print("="*140)
    
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
    print(f"  90th percentile: {peak_stats['p90']:.2f}")
    
    # Determine optimal timeout
    optimal_timeout = int(peak_stats['p90']) + 1
    print(f"\nUsing timeout: {optimal_timeout} hours (90th percentile + buffer)")
    
    # Define parameter ranges for each TS
    print("\n" + "="*140)
    print("PARAMETER RANGES:")
    print("="*140)
    
    # TS1: 25% at early profit (5-10% activation, 0.5-3% callback, step 0.5%)
    ts1_activation = [round(x * 0.5, 1) for x in range(10, 21)]  # 5.0, 5.5, ..., 10.0
    ts1_callback = [round(x * 0.5, 1) for x in range(1, 7)]      # 0.5, 1.0, ..., 3.0
    print(f"TS1 (25%): Activation {min(ts1_activation)}-{max(ts1_activation)}%, Callback {min(ts1_callback)}-{max(ts1_callback)}%")
    
    # TS2: 25% at moderate profit (10-20% activation, 0.5-3% callback, step 0.5%)
    ts2_activation = [round(x * 0.5, 1) for x in range(20, 41)]  # 10.0, 10.5, ..., 20.0
    ts2_callback = [round(x * 0.5, 1) for x in range(1, 7)]      # 0.5, 1.0, ..., 3.0
    print(f"TS2 (25%): Activation {min(ts2_activation)}-{max(ts2_activation)}%, Callback {min(ts2_callback)}-{max(ts2_callback)}%")
    
    # TS3: 25% at good profit (20-30% activation, 0.5-3% callback, step 0.5%)
    ts3_activation = [round(x * 0.5, 1) for x in range(40, 61)]  # 20.0, 20.5, ..., 30.0
    ts3_callback = [round(x * 0.5, 1) for x in range(1, 7)]      # 0.5, 1.0, ..., 3.0
    print(f"TS3 (25%): Activation {min(ts3_activation)}-{max(ts3_activation)}%, Callback {min(ts3_callback)}-{max(ts3_callback)}%")
    
    # TS4: 25% at exceptional profit (30-50% activation, 1-10% callback, step 1%)
    ts4_activation = list(range(30, 51))                         # 30, 31, ..., 50
    ts4_callback = list(range(1, 11))                            # 1, 2, ..., 10
    print(f"TS4 (25%): Activation {min(ts4_activation)}-{max(ts4_activation)}%, Callback {min(ts4_callback)}-{max(ts4_callback)}%")
    
    # Stop-loss options
    sl_levels = list(range(-10, 0))  # -10% to -1%, step 1%
    print(f"\nStop-Loss: {min(sl_levels)}% to {max(sl_levels)}%, step 1%")
    
    # STAGED OPTIMIZATION
    print("\n" + "="*140)
    print("STAGE 1: INDEPENDENT TS OPTIMIZATION")
    print("="*140)
    
    # Default TS config (will be updated as we optimize each tier)
    default_ts1 = {'portion': 0.25, 'activation_pct': 7.5, 'callback_pct': 1.5}
    default_ts2 = {'portion': 0.25, 'activation_pct': 15.0, 'callback_pct': 1.5}
    default_ts3 = {'portion': 0.25, 'activation_pct': 25.0, 'callback_pct': 1.5}
    default_ts4 = {'portion': 0.25, 'activation_pct': 40.0, 'callback_pct': 5.0}
    default_sl = -5  # Default SL
    
    # Stage 1.1: Optimize TS1
    print("\n--- Optimizing TS1 (25% early exits) ---")
    print(f"Total combinations: {len(ts1_activation) * len(ts1_callback)}")
    
    best_ts1_result = None
    best_ts1_avg = float('-inf')
    
    for act, cb in product(ts1_activation, ts1_callback):
        ts_configs = [
            {'portion': 0.25, 'activation_pct': act, 'callback_pct': cb},
            default_ts2,
            default_ts3,
            default_ts4
        ]
        
        profits = []
        for sig in signals_data:
            pnl = simulate_partial_close(
                sig['candles'], 
                sig['entry_price'], 
                default_sl,
                ts_configs,
                timeout_hours=optimal_timeout
            )
            profits.append(pnl)
        
        avg_profit = sum(profits) / len(profits) if profits else 0
        
        if avg_profit > best_ts1_avg:
            best_ts1_avg = avg_profit
            best_ts1_result = {'activation': act, 'callback': cb, 'avg_profit': avg_profit}
    
    print(f"Best TS1: Activation {best_ts1_result['activation']}%, Callback {best_ts1_result['callback']}%, Avg Profit: {best_ts1_result['avg_profit']:.2f}%")
    default_ts1 = {'portion': 0.25, 'activation_pct': best_ts1_result['activation'], 'callback_pct': best_ts1_result['callback']}
    
    # Stage 1.2: Optimize TS2
    print("\n--- Optimizing TS2 (25% moderate exits) ---")
    print(f"Total combinations: {len(ts2_activation) * len(ts2_callback)}")
    
    best_ts2_result = None
    best_ts2_avg = float('-inf')
    
    for act, cb in product(ts2_activation, ts2_callback):
        ts_configs = [
            default_ts1,
            {'portion': 0.25, 'activation_pct': act, 'callback_pct': cb},
            default_ts3,
            default_ts4
        ]
        
        profits = []
        for sig in signals_data:
            pnl = simulate_partial_close(
                sig['candles'], 
                sig['entry_price'], 
                default_sl,
                ts_configs,
                timeout_hours=optimal_timeout
            )
            profits.append(pnl)
        
        avg_profit = sum(profits) / len(profits) if profits else 0
        
        if avg_profit > best_ts2_avg:
            best_ts2_avg = avg_profit
            best_ts2_result = {'activation': act, 'callback': cb, 'avg_profit': avg_profit}
    
    print(f"Best TS2: Activation {best_ts2_result['activation']}%, Callback {best_ts2_result['callback']}%, Avg Profit: {best_ts2_result['avg_profit']:.2f}%")
    default_ts2 = {'portion': 0.25, 'activation_pct': best_ts2_result['activation'], 'callback_pct': best_ts2_result['callback']}
    
    # Stage 1.3: Optimize TS3
    print("\n--- Optimizing TS3 (25% good profit exits) ---")
    print(f"Total combinations: {len(ts3_activation) * len(ts3_callback)}")
    
    best_ts3_result = None
    best_ts3_avg = float('-inf')
    
    for act, cb in product(ts3_activation, ts3_callback):
        ts_configs = [
            default_ts1,
            default_ts2,
            {'portion': 0.25, 'activation_pct': act, 'callback_pct': cb},
            default_ts4
        ]
        
        profits = []
        for sig in signals_data:
            pnl = simulate_partial_close(
                sig['candles'], 
                sig['entry_price'], 
                default_sl,
                ts_configs,
                timeout_hours=optimal_timeout
            )
            profits.append(pnl)
        
        avg_profit = sum(profits) / len(profits) if profits else 0
        
        if avg_profit > best_ts3_avg:
            best_ts3_avg = avg_profit
            best_ts3_result = {'activation': act, 'callback': cb, 'avg_profit': avg_profit}
    
    print(f"Best TS3: Activation {best_ts3_result['activation']}%, Callback {best_ts3_result['callback']}%, Avg Profit: {best_ts3_result['avg_profit']:.2f}%")
    default_ts3 = {'portion': 0.25, 'activation_pct': best_ts3_result['activation'], 'callback_pct': best_ts3_result['callback']}
    
    # Stage 1.4: Optimize TS4
    print("\n--- Optimizing TS4 (25% exceptional profit exits) ---")
    print(f"Total combinations: {len(ts4_activation) * len(ts4_callback)}")
    
    best_ts4_result = None
    best_ts4_avg = float('-inf')
    
    for act, cb in product(ts4_activation, ts4_callback):
        ts_configs = [
            default_ts1,
            default_ts2,
            default_ts3,
            {'portion': 0.25, 'activation_pct': act, 'callback_pct': cb}
        ]
        
        profits = []
        for sig in signals_data:
            pnl = simulate_partial_close(
                sig['candles'], 
                sig['entry_price'], 
                default_sl,
                ts_configs,
                timeout_hours=optimal_timeout
            )
            profits.append(pnl)
        
        avg_profit = sum(profits) / len(profits) if profits else 0
        
        if avg_profit > best_ts4_avg:
            best_ts4_avg = avg_profit
            best_ts4_result = {'activation': act, 'callback': cb, 'avg_profit': avg_profit}
    
    print(f"Best TS4: Activation {best_ts4_result['activation']}%, Callback {best_ts4_result['callback']}%, Avg Profit: {best_ts4_result['avg_profit']:.2f}%")
    default_ts4 = {'portion': 0.25, 'activation_pct': best_ts4_result['activation'], 'callback_pct': best_ts4_result['callback']}
    
    # STAGE 2: OPTIMIZE STOP-LOSS
    print("\n" + "="*140)
    print("STAGE 2: STOP-LOSS OPTIMIZATION")
    print("="*140)
    print(f"Total combinations: {len(sl_levels)}")
    
    best_sl_result = None
    best_sl_avg = float('-inf')
    
    for sl in sl_levels:
        ts_configs = [default_ts1, default_ts2, default_ts3, default_ts4]
        
        profits = []
        for sig in signals_data:
            pnl = simulate_partial_close(
                sig['candles'], 
                sig['entry_price'], 
                sl,
                ts_configs,
                timeout_hours=optimal_timeout
            )
            profits.append(pnl)
        
        win_rate = len([p for p in profits if p > 0]) / len(profits) * 100 if profits else 0
        avg_profit = sum(profits) / len(profits) if profits else 0
        total_profit = sum(profits)
        
        if avg_profit > best_sl_avg:
            best_sl_avg = avg_profit
            best_sl_result = {
                'sl': sl,
                'avg_profit': avg_profit,
                'win_rate': win_rate,
                'total_profit': total_profit
            }
    
    print(f"Best SL: {best_sl_result['sl']}%, Avg Profit: {best_sl_result['avg_profit']:.2f}%, Win Rate: {best_sl_result['win_rate']:.2f}%")
    
    # FINAL RESULTS
    print("\n" + "="*140)
    print("OPTIMAL PARTIAL CLOSING STRATEGY:")
    print("="*140)
    print(f"\nStop-Loss: {best_sl_result['sl']}%")
    print(f"\nTS1 (25%): Activation {best_ts1_result['activation']}%, Callback {best_ts1_result['callback']}%")
    print(f"TS2 (25%): Activation {best_ts2_result['activation']}%, Callback {best_ts2_result['callback']}%")
    print(f"TS3 (25%): Activation {best_ts3_result['activation']}%, Callback {best_ts3_result['callback']}%")
    print(f"TS4 (25%): Activation {best_ts4_result['activation']}%, Callback {best_ts4_result['callback']}%")
    print(f"\nTimeout: {optimal_timeout} hours")
    print(f"\nPerformance:")
    print(f"  Win Rate: {best_sl_result['win_rate']:.2f}%")
    print(f"  Average Profit per Trade: {best_sl_result['avg_profit']:.2f}%")
    print(f"  Total Profit (sum of %): {best_sl_result['total_profit']:.2f}%")
    print(f"  Number of Trades: {len(signals_data)}")
    
    # COMPARISON WITH SINGLE TS
    print("\n" + "="*140)
    print("COMPARISON: Partial Close vs Single TS")
    print("="*140)
    
    # Run single TS with similar activation (average of our TS configs)
    avg_activation = (best_ts1_result['activation'] + best_ts2_result['activation'] + 
                      best_ts3_result['activation'] + best_ts4_result['activation']) / 4
    avg_callback = (best_ts1_result['callback'] + best_ts2_result['callback'] + 
                    best_ts3_result['callback'] + best_ts4_result['callback']) / 4
    
    print(f"\nRunning single TS baseline: Activation {avg_activation:.1f}%, Callback {avg_callback:.1f}%...")
    
    single_profits = []
    for sig in signals_data:
        pnl = simulate_combined(
            sig['candles'], 
            sig['entry_price'], 
            best_sl_result['sl'],
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
    
    print(f"\nPartial Close Results:")
    print(f"  Win Rate: {best_sl_result['win_rate']:.2f}%")
    print(f"  Average Profit: {best_sl_result['avg_profit']:.2f}%")
    print(f"  Total Profit: {best_sl_result['total_profit']:.2f}%")
    
    print(f"\nImprovement:")
    print(f"  Win Rate: {best_sl_result['win_rate'] - single_win_rate:+.2f}%")
    print(f"  Average Profit: {best_sl_result['avg_profit'] - single_avg:+.2f}%")
    print(f"  Total Profit: {best_sl_result['total_profit'] - single_total:+.2f}%")
    
    print("="*140)

if __name__ == "__main__":
    optimize_partial_close()
