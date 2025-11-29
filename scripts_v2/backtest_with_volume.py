"""
Realistic Portfolio Backtest with Volume Filters
Based on backtest_portfolio_realistic.py with added volume-based signal filtering
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
import argparse
from collections import defaultdict

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, EXCHANGE_FILTER, EXCHANGE_IDS
from optimization_lib import simulate_combined, get_candle_direction

# Trading parameters
POSITION_SIZE = 1000  # $1000 per trade
LEVERAGE = 10
MARGIN_PER_POSITION = POSITION_SIZE / LEVERAGE  # $100

# Realistic trading costs
TAKER_FEE = 0.0005  # 0.05% (Binance Futures)
SLIPPAGE = 0.001    # 0.1% average slippage
LIQUIDATION_THRESHOLD = 0.8  # 80% of margin
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
        
        # Process SL/TS based on candle direction
        if candle_dir == 'bullish':
            if not ts_activated and low <= sl_price:
                return sl_pct, candle_time, 'SL'
            
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated:
                if high > peak_price:
                    peak_price = high
        else:
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
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    pnl_pct = ((final_price - entry_price) / entry_price) * 100
    return pnl_pct, candles[-1]['open_time'], 'EOD'

def load_signals_with_volume_filter(min_volume_1m=0, min_volume_1h=0):
    """Load signals with volume filters and minute candles from database"""
    with get_db_connection() as conn:
        query = """
            SELECT 
                sa.id, sa.signal_timestamp, sa.pair_symbol, sa.entry_price, sa.entry_time,
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
        
        signals_data = []
        
        print(f"Loading {len(signals)} signals with minute candles...")
        
        for i, signal in enumerate(signals, 1):
            if i % 50 == 0:
                print(f"Loading {i}/{len(signals)}...", end='\r')
            
            signal_id = signal[0]
            
            # Fetch minute candles
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
                    'volume_1m': float(signal[5]) if signal[5] else 0,
                    'volume_1h': float(signal[6]) if signal[6] else 0,
                    'candles': candles
                })
        
        print(f"\nLoaded {len(signals_data)} signals successfully.")
        return signals_data

def backtest_with_volume(sl_pct=-8, activation_pct=20, callback_pct=1, timeout_hours=12, 
                        min_volume_1m=50000, min_volume_1h=1000000):
    """
    Realistic backtest with fees, liquidation, slippage and volume filters
    """
    print("="*140)
    print("REALISTIC PORTFOLIO BACKTEST WITH VOLUME FILTERS")
    print("="*140)
    print(f"Strategy Parameters:")
    print(f"  Stop-Loss: {sl_pct}%")
    print(f"  TS Activation: {activation_pct}%")
    print(f"  TS Callback: {callback_pct}%")
    print(f"  Timeout: {timeout_hours} hours")
    print(f"\nVolume Filters:")
    print(f"  Min 1m Volume: ${min_volume_1m/1000:.2f}K USDT")
    print(f"  Min 1h Volume: ${min_volume_1h/1000000:.2f}M USDT")
    print(f"\nPortfolio Settings:")
    print(f"  Position Size: ${POSITION_SIZE}, Leverage: {LEVERAGE}x, Margin: ${MARGIN_PER_POSITION}")
    print(f"\nRealistic Costs:")
    print(f"  Taker Fee: {TAKER_FEE*100}%")
    print(f"  Slippage: {SLIPPAGE*100}%")
    print(f"  Liquidation Threshold: {LIQUIDATION_THRESHOLD*100}% of margin")
    print("="*140)
    
    try:
        # Load signals with volume filter
        signals_data = load_signals_with_volume_filter(min_volume_1m, min_volume_1h)
        
        if not signals_data:
            print("No signals found matching volume criteria.")
            return
        
        # Calculate volume statistics
        avg_vol_1m = sum(s['volume_1m'] for s in signals_data) / len(signals_data)
        avg_vol_1h = sum(s['volume_1h'] for s in signals_data) / len(signals_data)
        
        print(f"\nVolume Statistics:")
        print(f"  Signals: {len(signals_data)}")
        print(f"  Avg 1m Volume: ${avg_vol_1m/1000:.1f}K USDT")
        print(f"  Avg 1h Volume: ${avg_vol_1h/1000000:.2f}M USDT")
        print(f"\nSimulating {len(signals_data)} trades...")
        
        # Simulate all trades (rest of backtest logic from original script)
        events = []
        
        for sig in signals_data:
            pnl_pct, exit_time_ms, exit_reason = simulate_trade_with_exit_time(
                sig['candles'],
                sig['entry_price'],
                sl_pct,
                activation_pct,
                callback_pct,
                timeout_hours
            )
            
            # Apply trading costs
            entry_cost = (TAKER_FEE + SLIPPAGE) * POSITION_SIZE  # Entry fee + slippage
            exit_cost = (TAKER_FEE + SLIPPAGE) * POSITION_SIZE   # Exit fee + slippage
            total_cost_usd = entry_cost + exit_cost
            cost_as_pct_of_position = (total_cost_usd / POSITION_SIZE) * 100
            
            final_pnl_pct = pnl_pct - cost_as_pct_of_position
            
            absolute_pnl_usd = (POSITION_SIZE * final_pnl_pct / 100)
            
            # Track event
            events.append({
                'entry_time': sig['entry_time'],
                'exit_time': datetime.fromtimestamp(exit_time_ms / 1000, tz=timezone.utc),
                'symbol': sig['symbol'],
                'pnl_usd': absolute_pnl_usd,
                'pnl_pct': final_pnl_pct,
                'exit_reason': exit_reason,
                'volume_1m': sig['volume_1m'],
                'volume_1h': sig['volume_1h']
            })
        
        # Calculate metrics
        total_pnl = sum(e['pnl_usd'] for e in events)
        wins = [e for e in events if e['pnl_usd'] > 0]
        losses = [e for e in events if e['pnl_usd'] < 0]
        win_rate = (len(wins) / len(events) * 100) if events else 0
        avg_win = (sum(e['pnl_usd'] for e in wins) / len(wins)) if wins else 0
        avg_loss = (sum(e['pnl_usd'] for e in losses) / len(losses)) if losses else 0
        
        # Exit reason stats
        exit_reasons = defaultdict(int)
        for e in events:
            exit_reasons[e['exit_reason']] += 1
        
        # Print results
        print("\n" + "="*140)
        print("BACKTEST RESULTS")
        print("="*140)
        print(f"Total Trades: {len(events)}")
        print(f"Total PnL: ${total_pnl:.2f} ({total_pnl/POSITION_SIZE/len(events)*100:.2f}% per trade)")
        print(f"\nWin Rate: {win_rate:.2f}% ({len(wins)} wins / {len(losses)} losses)")
        print(f"Average Win: ${avg_win:.2f}")
        print(f"Average Loss: ${avg_loss:.2f}")
        if losses:
            print(f"Win/Loss Ratio: {abs(avg_win/avg_loss):.2f}")
        
        print(f"\nExit Reasons:")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"  {reason}: {count} ({count/len(events)*100:.1f}%)")
        
        print("="*140)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(description='Realistic portfolio backtest with volume filters')
    parser.add_argument('--sl', type=float, default=-8, help='Stop-loss percentage')
    parser.add_argument('--activation', type=float, default=20, help='TS activation percentage')
    parser.add_argument('--callback', type=float, default=1, help='TS callback percentage')
    parser.add_argument('--timeout', type=float, default=12, help='Timeout in hours')
    parser.add_argument('--min-volume-1m', type=float, default=50000, 
                        help='Minimum 1-minute volume in USDT (default: 50000 = $50K)')
    parser.add_argument('--min-volume-1h', type=float, default=1000000,
                        help='Minimum 1-hour volume in USDT (default: 1000000 = $1M)')
    
    args = parser.parse_args()
    
    backtest_with_volume(
        sl_pct=args.sl,
        activation_pct=args.activation,
        callback_pct=args.callback,
        timeout_hours=args.timeout,
        min_volume_1m=args.min_volume_1m,
        min_volume_1h=args.min_volume_1h
    )

if __name__ == "__main__":
    main()
