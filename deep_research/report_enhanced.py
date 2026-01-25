#!/usr/bin/env python3
"""
Enhanced Detailed Report with Backtest-like Simulation
Shows actual exit for each signal based on SL/TS/Timeout parameters
"""
import sys
from pathlib import Path
import argparse
import json
from datetime import datetime, timezone

# Add scripts directory to path
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

def simulate_signal_exit(candles, entry_price, entry_time_dt, sl_pct, activation_pct, callback_pct, timeout_hours):
    """
    Simulate signal with exact backtest logic
    Returns: (exit_reason, exit_time, pnl_pct, max_dd_pct, max_dd_time, max_pump_pct, max_pump_time, ts_activation_time)
    """
    sl_price = entry_price * (1 + sl_pct / 100)
    activation_price = entry_price * (1 + activation_pct / 100)
    ts_activated = False
    ts_activation_time = None
    peak_price = entry_price
    entry_time_ms = candles[0]['time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    # Track max drawdown and pump
    min_price = entry_price
    min_price_time = entry_time_dt
    max_price = entry_price
    max_price_time = entry_time_dt
    
    # Liquidation threshold (10x leverage)
    LIQUIDATION_THRESHOLD = 1.0  # 100% of margin (correct for 10x leverage = -10% price drop)
    LEVERAGE = 10
    liquidation_price = entry_price * (1 - (LIQUIDATION_THRESHOLD / LEVERAGE))
    
    for candle in candles:
        high = float(candle['h'])
        low = float(candle['l'])
        close = float(candle['c'])
        candle_time_ms = candle['time']
        candle_time_dt = datetime.fromtimestamp(candle_time_ms / 1000, tz=timezone.utc)
        candle_dir = get_candle_direction({'open_price': candle['o'], 'close_price': candle['c']})
        
        # Track max drawdown
        if low < min_price:
            min_price = low
            min_price_time = candle_time_dt
        
        # Track max pump
        if high > max_price:
            max_price = high
            max_price_time = candle_time_dt
        
        # Check liquidation (before any other exit)
        if low <= liquidation_price:
            pnl_pct = -10  # Loss of entire margin
            max_dd_pct = ((min_price - entry_price) / entry_price) * 100
            max_pump_pct = ((max_price - entry_price) / entry_price) * 100
            return ('LIQUIDATION', candle_time_dt, pnl_pct, max_dd_pct, min_price_time, 
                    max_pump_pct, max_price_time, ts_activation_time)
        
        # Process SL/TS based on candle direction
        if candle_dir == 'bullish':
            # LOW first, then HIGH
            if not ts_activated and low <= sl_price:
                pnl_pct = sl_pct
                max_dd_pct = ((min_price - entry_price) / entry_price) * 100
                max_pump_pct = ((max_price - entry_price) / entry_price) * 100
                return ('SL', candle_time_dt, pnl_pct, max_dd_pct, min_price_time,
                        max_pump_pct, max_price_time, ts_activation_time)
            
            if not ts_activated and high >= activation_price:
                ts_activated = True
                ts_activation_time = candle_time_dt
                peak_price = high
            elif ts_activated:
                if high > peak_price:
                    peak_price = high
        else:  # bearish
            # HIGH first, then LOW
            if not ts_activated and high >= activation_price:
                ts_activated = True
                ts_activation_time = candle_time_dt
                peak_price = high
            elif ts_activated and high > peak_price:
                peak_price = high
            
            if not ts_activated and low <= sl_price:
                pnl_pct = sl_pct
                max_dd_pct = ((min_price - entry_price) / entry_price) * 100
                max_pump_pct = ((max_price - entry_price) / entry_price) * 100
                return ('SL', candle_time_dt, pnl_pct, max_dd_pct, min_price_time,
                        max_pump_pct, max_price_time, ts_activation_time)
        
        # Check TS callback
        if ts_activated:
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                pnl_pct = ((ts_exit_price - entry_price) / entry_price) * 100
                max_dd_pct = ((min_price - entry_price) / entry_price) * 100
                max_pump_pct = ((max_price - entry_price) / entry_price) * 100
                return ('TS', candle_time_dt, pnl_pct, max_dd_pct, min_price_time,
                        max_pump_pct, max_price_time, ts_activation_time)
        
        # Check timeout (if TS not activated)
        if not ts_activated and timeout_ms and candle_time_ms >= timeout_ms:
            pnl_pct = ((close - entry_price) / entry_price) * 100
            max_dd_pct = ((min_price - entry_price) / entry_price) * 100
            max_pump_pct = ((max_price - entry_price) / entry_price) * 100
            return ('TIMEOUT', candle_time_dt, pnl_pct, max_dd_pct, min_price_time,
                    max_pump_pct, max_price_time, ts_activation_time)
    
    # Position still open at end
    final_price = float(candles[-1]['c'])
    pnl_pct = ((final_price - entry_price) / entry_price) * 100
    max_dd_pct = ((min_price - entry_price) / entry_price) * 100
    max_pump_pct = ((max_price - entry_price) / entry_price) * 100
    final_time = datetime.fromtimestamp(candles[-1]['time'] / 1000, tz=timezone.utc)
    return ('EOD', final_time, pnl_pct, max_dd_pct, min_price_time,
            max_pump_pct, max_price_time, ts_activation_time)

def generate_enhanced_report(days=30, sl_pct=-7, activation_pct=14, callback_pct=9, timeout_hours=20):
    """Generate detailed report with backtest-like simulation"""
    print("="*160)
    print(f"ENHANCED DETAILED SIGNAL ANALYSIS (LAST {days} DAYS)")
    print(f"Parameters: SL={sl_pct}%, TS Activation={activation_pct}%, TS Callback={callback_pct}%, Timeout={timeout_hours}h")
    print("="*160)
    print(f"{'Entry Time':<18} {'Symbol':<12} {'Exit':<12} {'Time':<8} {'PnL':<8} {'Max DD':<15} {'Max Pump':<15} {'Details':<50}")
    print("-" * 160)
    
    with get_db_connection() as conn:
        # Query signals
        query = f"""
            SELECT 
                sa.id,
                sa.pair_symbol,
                sa.signal_timestamp,
                sa.total_score,
                sa.entry_price,
                sa.entry_time
            FROM web.signal_analysis sa
            JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
            WHERE sa.signal_timestamp >= NOW() - INTERVAL '{days} days'
            AND EXISTS (
                SELECT 1 FROM web.minute_candles mc
                WHERE mc.signal_analysis_id = sa.id
            )
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
        
        if not rows:
            print("No signals found.")
            return
        
        # Statistics
        exit_counts = {'SL': 0, 'TS': 0, 'LIQUIDATION': 0, 'TIMEOUT': 0, 'EOD': 0}
        total_pnl = 0
        
        for row in rows:
            signal_id = row[0]
            symbol = row[1]
            signal_ts = row[2]
            score = row[3]
            entry_price = float(row[4])
            entry_time_dt = row[5]
            
            # Load minute candles from table (like backtest_portfolio_realistic.py)
            candles_query = """
                SELECT open_time, open_price, high_price, low_price, close_price
                FROM web.minute_candles
                WHERE signal_analysis_id = %s
                ORDER BY open_time ASC
            """
            
            with conn.cursor() as cur:
                cur.execute(candles_query, (signal_id,))
                candles_rows = cur.fetchall()
            
            if not candles_rows:
                continue
            
            # Convert to format expected by simulate_signal_exit
            candles = [{
                'time': int(c[0]),
                'o': float(c[1]),
                'h': float(c[2]),
                'l': float(c[3]),
                'c': float(c[4])
            } for c in candles_rows]
            
            # Simulate exit
            (exit_reason, exit_time, pnl_pct, max_dd_pct, max_dd_time, 
             max_pump_pct, max_pump_time, ts_activation_time) = simulate_signal_exit(
                candles, entry_price, entry_time_dt, sl_pct, activation_pct, callback_pct, timeout_hours
            )
            
            # Update stats
            exit_counts[exit_reason] += 1
            total_pnl += pnl_pct
            
            # Format output
            entry_str = entry_time_dt.strftime('%Y-%m-%d %H:%M')
            exit_time_str = format_time_diff(entry_time_dt, exit_time)
            pnl_str = f"{pnl_pct:>6.2f}%"
            
            # Max drawdown info
            dd_time_str = format_time_diff(entry_time_dt, max_dd_time)
            dd_str = f"{max_dd_pct:>6.2f}% @{dd_time_str}"
            
            # Max pump info
            pump_time_str = format_time_diff(entry_time_dt, max_pump_time)
            pump_str = f"{max_pump_pct:>6.2f}% @{pump_time_str}"
            
            # Details based on exit reason
            if exit_reason == 'TS':
                ts_act_str = format_time_diff(entry_time_dt, ts_activation_time)
                details = f"TS activated @{ts_act_str}, peak {max_pump_pct:.2f}%"
            elif exit_reason == 'SL':
                details = f"Hit SL at {sl_pct}%"
            elif exit_reason == 'LIQUIDATION':
                details = f"Liquidated at -10%"
            elif exit_reason == 'TIMEOUT':
                details = f"Timeout after {timeout_hours}h, TS not activated"
            else:
                details = f"Position still open"
            
            print(f"{entry_str:<18} {symbol:<12} {exit_reason:<12} {exit_time_str:<8} {pnl_str:<8} {dd_str:<15} {pump_str:<15} {details:<50}")
        
        # Summary
        print("\n" + "="*160)
        print("SUMMARY:")
        print("="*160)
        total_signals = sum(exit_counts.values())
        print(f"Total Signals: {total_signals}")
        print(f"Total PnL: {total_pnl:.2f}%")
        print(f"Average PnL: {total_pnl/total_signals:.2f}%" if total_signals > 0 else "Average PnL: N/A")
        print(f"\nExit Breakdown:")
        for reason, count in sorted(exit_counts.items(), key=lambda x: -x[1]):
            pct = (count / total_signals * 100) if total_signals > 0 else 0
            print(f"  {reason:<12}: {count:>3} ({pct:>5.1f}%)")
        print("="*160)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enhanced detailed signal analysis with backtest simulation.')
    parser.add_argument('--days', type=int, default=30, help='Days to look back')
    parser.add_argument('--sl', type=float, default=-7, help='Stop-Loss percentage (negative)')
    parser.add_argument('--activation', type=float, default=14, help='TS Activation percentage (positive)')
    parser.add_argument('--callback', type=float, default=9, help='TS Callback percentage (positive)')
    parser.add_argument('--timeout', type=float, default=20, help='Timeout in hours')
    args = parser.parse_args()
    
    generate_enhanced_report(
        days=args.days,
        sl_pct=args.sl,
        activation_pct=args.activation,
        callback_pct=args.callback,
        timeout_hours=args.timeout
    )
