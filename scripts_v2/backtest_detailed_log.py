"""
Detailed Backtest Log
Shows step-by-step execution for each signal:
- Exit Reason (SL, TS, TIMEOUT, LIQUIDATION)
- Duration (HH:MM)
- PnL (USDT)
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
import argparse

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, EXCHANGE_FILTER, EXCHANGE_IDS
from optimization_lib import get_candle_direction

# Trading parameters
POSITION_SIZE = 1000  # $1000 per trade
LEVERAGE = 10
MARGIN_PER_POSITION = POSITION_SIZE / LEVERAGE  # $100

# Realistic trading costs
TAKER_FEE = 0.0005  # 0.05%
SLIPPAGE = 0.001    # 0.1%
LIQUIDATION_THRESHOLD = 0.8  # 80%
FUNDING_RATE = 0.0001  # 0.01% per 8 hours

def simulate_trade_with_exit_time(candles, entry_price, sl_pct, activation_pct, callback_pct, timeout_hours):
    """
    Simulate trade and return (pnl_pct, exit_time_ms, exit_reason)
    """
    sl_price = entry_price * (1 + sl_pct / 100)
    activation_price = entry_price * (1 + activation_pct / 100)
    ts_activated = False
    peak_price = entry_price
    entry_time_ms = candles[0]['open_time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    for candle in candles:
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        close = float(candle['close_price'])
        candle_time = candle['open_time']
        candle_dir = get_candle_direction(candle)
        
        # Check liquidation
        liquidation_price = entry_price * (1 - (LIQUIDATION_THRESHOLD / LEVERAGE))
        if low <= liquidation_price:
            pnl_pct = -(MARGIN_PER_POSITION / POSITION_SIZE) * 100
            return pnl_pct, candle_time, 'LIQUIDATION'
        
        # Process SL/TS
        if candle_dir == 'bullish':
            if not ts_activated and low <= sl_price:
                return sl_pct, candle_time, 'SL'
            
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated:
                if high > peak_price:
                    peak_price = high
        else:  # bearish
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated and high > peak_price:
                peak_price = high
            
            if not ts_activated and low <= sl_price:
                return sl_pct, candle_time, 'SL'
        
        # Check TS callback
        if ts_activated:
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                pnl_pct = ((ts_exit_price - entry_price) / entry_price) * 100
                return pnl_pct, candle_time, 'TS'
        
        # Check timeout
        if not ts_activated and timeout_ms and candle_time >= timeout_ms:
            pnl_pct = ((close - entry_price) / entry_price) * 100
            return pnl_pct, candle_time, 'TIMEOUT'
    
    # EOD
    final_price = float(candles[-1]['close_price'])
    pnl_pct = ((final_price - entry_price) / entry_price) * 100
    return pnl_pct, candles[-1]['open_time'], 'EOD'

def load_signals_with_minute_candles():
    """Load signals with 1-minute candles from database"""
    with get_db_connection() as conn:
        query = """
            SELECT 
                sa.id, sa.signal_timestamp, sa.pair_symbol, sa.entry_price, sa.entry_time
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
        print(f"Loading {len(signals)} signals...")
        
        for i, signal in enumerate(signals, 1):
            if i % 50 == 0:
                print(f"Loading {i}/{len(signals)}...", end='\r')
            
            signal_id = signal[0]
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
                'open_time': int(row[0]),
                'open_price': float(row[1]),
                'high_price': float(row[2]),
                'low_price': float(row[3]),
                'close_price': float(row[4])
            } for row in candles_rows]
            
            if candles:
                signals_data.append({
                    'signal_timestamp': signal[1],
                    'symbol': signal[2],
                    'entry_price': float(signal[3]),
                    'entry_time': signal[4],
                    'candles': candles
                })
        
        print(f"\nLoaded {len(signals_data)} signals.")
        return signals_data

def format_duration(ms):
    seconds = int(ms / 1000)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours:02d}:{minutes:02d}"

def run_detailed_backtest(sl_pct, activation_pct, callback_pct, timeout_hours):
    print("="*100)
    print(f"DETAILED BACKTEST LOG")
    print(f"Params: SL={sl_pct}%, TS={activation_pct}/{callback_pct}%, Timeout={timeout_hours}h")
    print("="*100)
    print(f"{'Time':<20} {'Symbol':<12} {'Exit Reason':<15} {'Duration':<10} {'PnL ($)':<12} {'PnL (%)':<10}")
    print("-" * 100)

    signals_data = load_signals_with_minute_candles()
    if not signals_data:
        print("No signals found.")
        return

    total_pnl = 0
    wins = 0
    losses = 0

    for sig in signals_data:
        pnl_pct, exit_time_ms, exit_reason = simulate_trade_with_exit_time(
            sig['candles'],
            sig['entry_price'],
            sl_pct,
            activation_pct,
            callback_pct,
            timeout_hours
        )

        # Apply realistic costs
        if pnl_pct > 0:
            pnl_pct -= SLIPPAGE * 100
        else:
            pnl_pct -= SLIPPAGE * 100
        
        pnl_dollars = (pnl_pct / 100) * POSITION_SIZE
        
        # Fees
        fees = POSITION_SIZE * TAKER_FEE * 2 # Entry + Exit
        
        # Funding
        entry_time_ms = sig['candles'][0]['open_time']
        duration_ms = exit_time_ms - entry_time_ms
        holding_hours = duration_ms / (1000 * 3600)
        funding_periods = int(holding_hours / 8)
        funding_cost = POSITION_SIZE * FUNDING_RATE * funding_periods if funding_periods > 0 else 0
        
        net_pnl = pnl_dollars - fees - funding_cost
        net_pnl_pct = (net_pnl / POSITION_SIZE) * 100
        
        total_pnl += net_pnl
        if net_pnl > 0:
            wins += 1
        else:
            losses += 1
            
        # Output row
        entry_dt = sig['entry_time'].strftime('%Y-%m-%d %H:%M')
        duration_str = format_duration(duration_ms)
        
        # Color coding (if terminal supports it, but keep simple for now)
        pnl_str = f"${net_pnl:+.2f}"
        
        print(f"{entry_dt:<20} {sig['symbol']:<12} {exit_reason:<15} {duration_str:<10} {pnl_str:<12} {net_pnl_pct:+.2f}%")

    print("="*100)
    print(f"TOTAL RESULT:")
    print(f"Net Profit: ${total_pnl:,.2f}")
    print(f"Trades: {len(signals_data)} (Wins: {wins}, Losses: {losses})")
    print("="*100)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--sl', type=float, default=-8)
    parser.add_argument('--activation', type=float, default=20)
    parser.add_argument('--callback', type=float, default=1)
    parser.add_argument('--timeout', type=float, default=12)
    args = parser.parse_args()
    
    run_detailed_backtest(args.sl, args.activation, args.callback, args.timeout)
