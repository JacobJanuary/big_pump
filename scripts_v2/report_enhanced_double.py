#!/usr/bin/env python3
"""
Enhanced Report with Doubling Comparison
Shows side-by-side comparison of standard strategy vs doubling strategy for each signal
"""
import sys
from pathlib import Path
import argparse
import json
from datetime import datetime, timezone

current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, EXCHANGE_FILTER, EXCHANGE_IDS
from optimization_lib import get_candle_direction

def format_time_diff(start_dt, end_dt):
    """Format time difference as HH:MM"""
    diff_seconds = (end_dt - start_dt).total_seconds()
    hours = int(diff_seconds // 3600)
    minutes = int((diff_seconds % 3600) // 60)
    return f"{hours:02d}:{minutes:02d}"

def simulate_standard(candles, entry_price, sl_pct, activation_pct, callback_pct, timeout_hours):
    """Standard strategy without doubling"""
    sl_price = entry_price * (1 + sl_pct / 100)
    activation_price = entry_price * (1 + activation_pct / 100)
    liquidation_price = entry_price * (1 - 0.08)  # -8% liquidation
    
    ts_activated = False
    peak_price = entry_price
    entry_time_ms = candles[0]['time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    for candle in candles:
        high = float(candle['h'])
        low = float(candle['l'])
        close = float(candle['c'])
        candle_time_ms = candle['time']
        candle_time_dt = datetime.fromtimestamp(candle_time_ms / 1000, tz=timezone.utc)
        candle_dir = get_candle_direction({'open_price': candle['o'], 'close_price': candle['c']})
        
        # Liquidation
        if low <= liquidation_price:
            pnl_pct = -10
            return ('LIQUIDATION', candle_time_dt, pnl_pct)
        
        # SL/TS
        if candle_dir == 'bullish':
            if not ts_activated and low <= sl_price:
                return ('SL', candle_time_dt, sl_pct)
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated and high > peak_price:
                peak_price = high
        else:
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated and high > peak_price:
                peak_price = high
            if not ts_activated and low <= sl_price:
                return ('SL', candle_time_dt, sl_pct)
        
        # TS callback
        if ts_activated:
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                pnl_pct = ((ts_exit_price - entry_price) / entry_price) * 100
                return ('TS', candle_time_dt, pnl_pct)
        
        # Timeout
        if not ts_activated and timeout_ms and candle_time_ms >= timeout_ms:
            pnl_pct = ((close - entry_price) / entry_price) * 100
            return ('TIMEOUT', candle_time_dt, pnl_pct)
    
    # EOD
    final_price = float(candles[-1]['c'])
    pnl_pct = ((final_price - entry_price) / entry_price) * 100
    final_time = datetime.fromtimestamp(candles[-1]['time'] / 1000, tz=timezone.utc)
    return ('EOD', final_time, pnl_pct)

def simulate_with_doubling(candles, entry_price, sl_pct, activation_pct, callback_pct, 
                           timeout_hours, double_at_pct=-1):
    """Strategy with position doubling at -1%"""
    sl_price = entry_price * (1 + sl_pct / 100)
    activation_price = entry_price * (1 + activation_pct / 100)
    liquidation_price = entry_price * (1 - 0.08)
    double_price = entry_price * (1 + double_at_pct / 100)
    
    position_size = 100
    avg_entry_price = entry_price
    doubled = False
    double_time = None
    
    ts_activated = False
    peak_price = entry_price
    entry_time_ms = candles[0]['time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    for candle in candles:
        high = float(candle['h'])
        low = float(candle['l'])
        close = float(candle['c'])
        candle_time_ms = candle['time']
        candle_time_dt = datetime.fromtimestamp(candle_time_ms / 1000, tz=timezone.utc)
        candle_dir = get_candle_direction({'open_price': candle['o'], 'close_price': candle['c']})
        
        # Check for doubling
        if not doubled and low <= double_price:
            doubled = True
            double_time = candle_time_dt
            avg_entry_price = (entry_price + double_price) / 2
            position_size = 200
            # Recalculate levels
            sl_price = avg_entry_price * (1 + sl_pct / 100)
            activation_price = avg_entry_price * (1 + activation_pct / 100)
            liquidation_price = avg_entry_price * (1 - 0.08)
        
        # Liquidation
        if low <= liquidation_price:
            pnl_pct = -10
            pnl_dollars = (pnl_pct / 100) * position_size
            return ('LIQUIDATION', candle_time_dt, pnl_dollars, doubled, double_time)
        
        # SL/TS
        if candle_dir == 'bullish':
            if not ts_activated and low <= sl_price:
                pnl_pct = ((sl_price - avg_entry_price) / avg_entry_price) * 100
                pnl_dollars = (pnl_pct / 100) * position_size
                return ('SL', candle_time_dt, pnl_dollars, doubled, double_time)
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated and high > peak_price:
                peak_price = high
        else:
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated and high > peak_price:
                peak_price = high
            if not ts_activated and low <= sl_price:
                pnl_pct = ((sl_price - avg_entry_price) / avg_entry_price) * 100
                pnl_dollars = (pnl_pct / 100) * position_size
                return ('SL', candle_time_dt, pnl_dollars, doubled, double_time)
        
        # TS callback
        if ts_activated:
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                pnl_pct = ((ts_exit_price - avg_entry_price) / avg_entry_price) * 100
                pnl_dollars = (pnl_pct / 100) * position_size
                return ('TS', candle_time_dt, pnl_dollars, doubled, double_time)
        
        # Timeout
        if not ts_activated and timeout_ms and candle_time_ms >= timeout_ms:
            pnl_pct = ((close - avg_entry_price) / avg_entry_price) * 100
            pnl_dollars = (pnl_pct / 100) * position_size
            return ('TIMEOUT', candle_time_dt, pnl_dollars, doubled, double_time)
    
    # EOD
    final_price = float(candles[-1]['c'])
    pnl_pct = ((final_price - avg_entry_price) / avg_entry_price) * 100
    pnl_dollars = (pnl_pct / 100) * position_size
    final_time = datetime.fromtimestamp(candles[-1]['time'] / 1000, tz=timezone.utc)
    return ('EOD', final_time, pnl_dollars, doubled, double_time)

def generate_comparison_report(days=30, sl_pct=-7, activation_pct=14, callback_pct=9, timeout_hours=20):
    """Generate comparison report"""
    print("="*180)
    print(f"STRATEGY COMPARISON REPORT - Standard vs Doubling at -1%")
    print(f"Parameters: SL={sl_pct}%, TS={activation_pct}%, CB={callback_pct}%, Timeout={timeout_hours}h")
    print("="*180)
    print(f"{'Signal':<18} {'Symbol':<12} {'Standard':<35} {'With Doubling (-1%)':<70} {'Diff':<10}")
    print(f"{'':<18} {'':<12} {'Exit':<8} {'Time':<8} {'PnL':<8} {'Exit':<8} {'Time':<8} {'PnL':<10} {'Doubled?':<15} {'PnL':<10}")
    print("-" * 180)
    
    conn = get_db_connection()
    
    query = f"""
        SELECT 
            sa.pair_symbol,
            sa.signal_timestamp,
            sa.entry_price,
            sa.candles_data,
            sa.entry_time
        FROM web.signal_analysis sa
        JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
        WHERE sa.signal_timestamp >= NOW() - INTERVAL '{days} days'
        AND (
            '{EXCHANGE_FILTER}' = 'ALL' 
            OR ('{EXCHANGE_FILTER}' = 'BINANCE' AND tp.exchange_id = {EXCHANGE_IDS['BINANCE']})
            OR ('{EXCHANGE_FILTER}' = 'BYBIT' AND tp.exchange_id = {EXCHANGE_IDS['BYBIT']})
        )
        ORDER BY sa.signal_timestamp DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    
    conn.close()
    
    # Stats
    total_standard_pnl = 0
    total_double_pnl = 0
    better_with_double = 0
    worse_with_double = 0
    same = 0
    
    for row in rows:
        symbol = row[0]
        signal_ts = row[1]
        entry_price = float(row[2])
        candles_data = row[3]
        
        if not candles_data:
            continue
        
        if isinstance(candles_data, str):
            candles = json.loads(candles_data)
        else:
            candles = candles_data
        
        # Simulate both strategies
        std_exit, std_time, std_pnl_pct = simulate_standard(
            candles, entry_price, sl_pct, activation_pct, callback_pct, timeout_hours
        )
        
        dbl_exit, dbl_time, dbl_pnl_dollars, doubled, double_time = simulate_with_doubling(
            candles, entry_price, sl_pct, activation_pct, callback_pct, timeout_hours, -1
        )
        
        # Convert standard PnL to dollars
        std_pnl_dollars = (std_pnl_pct / 100) * 100  # $100 position
        
        # Calculate difference
        diff_dollars = dbl_pnl_dollars - std_pnl_dollars
        
        # Update stats
        total_standard_pnl += std_pnl_dollars
        total_double_pnl += dbl_pnl_dollars
        
        if diff_dollars > 0.5:
            better_with_double += 1
            diff_marker = "✅"
        elif diff_dollars < -0.5:
            worse_with_double += 1
            diff_marker = "❌"
        else:
            same += 1
            diff_marker = "="
        
        # Format output
        ts_str = signal_ts.strftime('%Y-%m-%d %H:%M')
        std_time_str = format_time_diff(signal_ts, std_time)
        dbl_time_str = format_time_diff(signal_ts, dbl_time)
        
        doubled_str = "YES" if doubled else "NO"
        if doubled and double_time:
            double_time_str = format_time_diff(signal_ts, double_time)
            doubled_str = f"YES @{double_time_str}"
        
        print(f"{ts_str:<18} {symbol:<12} "
              f"{std_exit:<8} {std_time_str:<8} ${std_pnl_dollars:>6.2f}  "
              f"{dbl_exit:<8} {dbl_time_str:<8} ${dbl_pnl_dollars:>8.2f}  {doubled_str:<15} "
              f"{diff_marker} ${diff_dollars:>6.2f}")
    
    # Summary
    total_signals = better_with_double + worse_with_double + same
    
    print("\n" + "="*180)
    print("SUMMARY:")
    print("="*180)
    print(f"Total Signals: {total_signals}")
    print(f"\nStandard Strategy:")
    print(f"  Total PnL: ${total_standard_pnl:.2f}")
    print(f"  Avg PnL: ${total_standard_pnl/total_signals:.2f}" if total_signals > 0 else "  Avg PnL: N/A")
    print(f"\nDoubling Strategy (-1%):")
    print(f"  Total PnL: ${total_double_pnl:.2f}")
    print(f"  Avg PnL: ${total_double_pnl/total_signals:.2f}" if total_signals > 0 else "  Avg PnL: N/A")
    print(f"\nComparison:")
    print(f"  Better with Doubling: {better_with_double} ({better_with_double/total_signals*100:.1f}%)" if total_signals > 0 else "  Better with Doubling: 0")
    print(f"  Worse with Doubling: {worse_with_double} ({worse_with_double/total_signals*100:.1f}%)" if total_signals > 0 else "  Worse with Doubling: 0")
    print(f"  Same: {same} ({same/total_signals*100:.1f}%)" if total_signals > 0 else "  Same: 0")
    print(f"\n  Total Difference: ${total_double_pnl - total_standard_pnl:.2f} ({(total_double_pnl - total_standard_pnl)/total_standard_pnl*100:+.1f}%)" if total_standard_pnl != 0 else "")
    print("="*180)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compare standard vs doubling strategy')
    parser.add_argument('--days', type=int, default=30, help='Days to look back')
    parser.add_argument('--sl', type=float, default=-7, help='Stop-Loss %')
    parser.add_argument('--activation', type=float, default=14, help='TS Activation %')
    parser.add_argument('--callback', type=float, default=9, help='TS Callback %')
    parser.add_argument('--timeout', type=float, default=20, help='Timeout hours')
    args = parser.parse_args()
    
    generate_comparison_report(
        days=args.days,
        sl_pct=args.sl,
        activation_pct=args.activation,
        callback_pct=args.callback,
        timeout_hours=args.timeout
    )
