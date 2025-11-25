"""
Realistic Portfolio Backtest with:
- Trading fees
- Liquidation checks
- Slippage
- Precise exit time tracking
- Funding rate (optional)
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

from pump_analysis_lib import get_db_connection
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
        
        # Check liquidation (before any other exit)
        # For LONG positions: liquidation if price drops ~8% with 10x leverage
        liquidation_price = entry_price * (1 - (LIQUIDATION_THRESHOLD / LEVERAGE))
        if low <= liquidation_price:
            # Liquidation = loss of entire margin
            pnl_pct = -(MARGIN_PER_POSITION / POSITION_SIZE) * 100  # -10% loss
            return pnl_pct, candle_time, 'LIQUIDATION'
        
        # Process SL/TS based on candle direction
        if candle_dir == 'bullish':
            # LOW first, then HIGH
            if not ts_activated and low <= sl_price:
                return sl_pct, candle_time, 'SL'
            
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated:
                if high > peak_price:
                    peak_price = high
        else:  # bearish
            # HIGH first, then LOW
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
        
        # Check timeout (if TS not activated)
        if not ts_activated and timeout_ms and candle_time >= timeout_ms:
            pnl_pct = ((close - entry_price) / entry_price) * 100
            return pnl_pct, candle_time, 'TIMEOUT'
    
    # Position still open at end (shouldn't happen with proper timeout)
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
            WHERE EXISTS (
                SELECT 1 FROM web.minute_candles mc
                WHERE mc.signal_analysis_id = sa.id
            )
            ORDER BY sa.signal_timestamp ASC
        """
        
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
                    'candles': candles
                })
        
        print(f"\nLoaded {len(signals_data)} signals successfully.")
        return signals_data

def backtest_portfolio_realistic(sl_pct=-8, activation_pct=20, callback_pct=1, timeout_hours=12):
    """
    Realistic backtest with fees, liquidation, and slippage
    """
    print("="*140)
    print("REALISTIC PORTFOLIO BACKTEST - With Fees, Liquidation & Slippage")
    print("="*140)
    print(f"Strategy Parameters:")
    print(f"  Stop-Loss: {sl_pct}%")
    print(f"  TS Activation: {activation_pct}%")
    print(f"  TS Callback: {callback_pct}%")
    print(f"  Timeout: {timeout_hours} hours")
    print(f"  Position Size: ${POSITION_SIZE}, Leverage: {LEVERAGE}x, Margin: ${MARGIN_PER_POSITION}")
    print(f"\nRealistic Costs:")
    print(f"  Taker Fee: {TAKER_FEE*100}%")
    print(f"  Slippage: {SLIPPAGE*100}%")
    print(f"  Liquidation Threshold: {LIQUIDATION_THRESHOLD*100}% of margin")
    print("="*140)
    
    try:
        # Load signals
        signals_data = load_signals_with_minute_candles()
        
        if not signals_data:
            print("No signals found.")
            return
        
        print(f"\nSimulating {len(signals_data)} trades...")
        
        # Simulate all trades
        events = []
        trade_id = 0
        exit_reasons = defaultdict(int)
        
        for i, sig in enumerate(signals_data, 1):
            if i % 10 == 0:
                print(f"Simulating {i}/{len(signals_data)}...", end='\r')
            
            # Simulate trade with exit time
            pnl_pct, exit_time_ms, exit_reason = simulate_trade_with_exit_time(
                sig['candles'],
                sig['entry_price'],
                sl_pct,
                activation_pct,
                callback_pct,
                timeout_hours
            )
            
            exit_reasons[exit_reason] += 1
            
            # Apply slippage (worse execution)
            if pnl_pct > 0:
                pnl_pct -= SLIPPAGE * 100  # Reduce profit
            else:
                pnl_pct -= SLIPPAGE * 100  # Increase loss
            
            # Calculate PnL in dollars from position
            pnl_dollars = (pnl_pct / 100) * POSITION_SIZE
            
            # Apply fees (entry + exit)
            entry_fee = POSITION_SIZE * TAKER_FEE
            exit_fee = POSITION_SIZE * TAKER_FEE
            total_fees = entry_fee + exit_fee
            
            # Net PnL
            net_pnl = pnl_dollars - total_fees
            net_pnl_pct = (net_pnl / POSITION_SIZE) * 100
            
            # Convert times
            entry_time_dt = sig['entry_time']
            exit_time_dt = datetime.fromtimestamp(exit_time_ms / 1000, tz=timezone.utc)
            
            # Calculate holding time
            holding_hours = (exit_time_ms - sig['candles'][0]['open_time']) / (1000 * 3600)
            
            # Funding rate cost (if held > 8 hours)
            funding_periods = int(holding_hours / 8)
            funding_cost = POSITION_SIZE * FUNDING_RATE * funding_periods if funding_periods > 0 else 0
            net_pnl -= funding_cost
            
            # Create events
            events.append({
                'type': 'OPEN',
                'time': entry_time_dt,
                'trade_id': trade_id,
                'symbol': sig['symbol'],
                'margin': MARGIN_PER_POSITION
            })
            
            events.append({
                'type': 'CLOSE',
                'time': exit_time_dt,
                'trade_id': trade_id,
                'symbol': sig['symbol'],
                'pnl': net_pnl,
                'pnl_pct': net_pnl_pct,
                'exit_reason': exit_reason,
                'fees': total_fees,
                'funding': funding_cost
            })
            
            trade_id += 1
        
        print(f"\n\nSimulated {trade_id} trades. Processing event timeline...")
        
        # Sort events chronologically
        events.sort(key=lambda x: x['time'])
        
        # Process events
        balance = 0
        locked_margin = 0
        active_positions = 0
        capital_added = 0
        max_active_positions = 0
        total_fees = 0
        total_funding = 0
        
        daily_stats = defaultdict(lambda: {
            'opened': 0,
            'closed': 0,
            'pnl': 0,
            'fees': 0,
            'funding': 0,
            'balance_eod': 0,
            'locked_eod': 0,
            'free_eod': 0,
            'capital_added': 0
        })
        
        for event in events:
            event_date = event['time'].date()
            
            if event['type'] == 'OPEN':
                # Check capital needs
                free_balance = balance - locked_margin
                if free_balance < MARGIN_PER_POSITION:
                    needed = MARGIN_PER_POSITION - free_balance
                    balance += needed
                    capital_added += needed
                    daily_stats[event_date]['capital_added'] += needed
                
                # Lock margin
                locked_margin += MARGIN_PER_POSITION
                active_positions += 1
                daily_stats[event_date]['opened'] += 1
                
                if active_positions > max_active_positions:
                    max_active_positions = active_positions
            
            elif event['type'] == 'CLOSE':
                # Realize PnL
                balance += event['pnl']
                locked_margin -= MARGIN_PER_POSITION
                active_positions -= 1
                daily_stats[event_date]['closed'] += 1
                daily_stats[event_date]['pnl'] += event['pnl']
                daily_stats[event_date]['fees'] += event.get('fees', 0)
                daily_stats[event_date]['funding'] += event.get('funding', 0)
                
                total_fees += event.get('fees', 0)
                total_funding += event.get('funding', 0)
            
            # Update EOD  values
            daily_stats[event_date]['balance_eod'] = balance
            daily_stats[event_date]['locked_eod'] = locked_margin
            daily_stats[event_date]['free_eod'] = balance - locked_margin
        
        # Print daily report
        print("\n" + "="*160)
        print("DAILY BALANCE REPORT:")
        print("="*160)
        print(f"{'Date':<12} {'Opened':<8} {'Closed':<8} {'PnL':<12} {'Fees':<10} {'Funding':<10} {'Balance':<14} {'Locked':<12} {'Free':<14} {'Added':<12}")
        print("-"*160)
        
        for date in sorted(daily_stats.keys()):
            stats = daily_stats[date]
            print(f"{date.strftime('%Y-%m-%d'):<12} "
                  f"{stats['opened']:<8} "
                  f"{stats['closed']:<8} "
                  f"${stats['pnl']:>10,.2f} "
                  f"${stats['fees']:>8,.2f} "
                  f"${stats['funding']:>8,.2f} "
                  f"${stats['balance_eod']:>12,.2f} "
                  f"${stats['locked_eod']:>10,.2f} "
                  f"${stats['free_eod']:>12,.2f} "
                  f"${stats['capital_added']:>10,.2f}")
        
        # Summary
        final_balance = balance
        final_locked = locked_margin
        net_profit = final_balance - capital_added
        
        total_trades = trade_id
        winning_trades = len([e for e in events if e['type'] == 'CLOSE' and e['pnl'] > 0])
        losing_trades = len([e for e in events if e['type'] == 'CLOSE' and e['pnl'] <= 0])
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        print("\n" + "="*140)
        print("SUMMARY:")
        print("="*140)
        print(f"Strategy: SL={sl_pct}%, TS Activation={activation_pct}%, Callback={callback_pct}%, Timeout={timeout_hours}h")
        print(f"Initial Balance: $0.00")
        print(f"Capital Added: ${capital_added:,.2f}")
        print(f"Final Balance: ${final_balance:,.2f}")
        print(f"Final Locked Margin: ${final_locked:,.2f}")
        print(f"Net Profit: ${net_profit:,.2f} ({(net_profit/capital_added*100):+.2f}% ROI)" if capital_added > 0 else "Net Profit: $0.00")
        print(f"Total Fees Paid: ${total_fees:,.2f}")
        print(f"Total Funding Paid: ${total_funding:,.2f}")
        print(f"Max Active Positions: {max_active_positions}")
        print(f"Total Trades: {total_trades}")
        print(f"Winning Trades: {winning_trades} ({win_rate:.2f}%)")
        print(f"Losing Trades: {losing_trades}")
        print(f"\nExit Reasons:")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
            pct = (count / total_trades * 100) if total_trades > 0 else 0
            print(f"  {reason}: {count} ({pct:.1f}%)")
        print("="*140)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Realistic backtest with fees and slippage.')
    parser.add_argument('--sl', type=float, default=-8, help='Stop-Loss percentage (negative)')
    parser.add_argument('--activation', type=float, default=20, help='TS Activation percentage (positive)')
    parser.add_argument('--callback', type=float, default=1, help='TS Callback percentage (positive)')
    parser.add_argument('--timeout', type=float, default=12, help='Timeout in hours')
    args = parser.parse_args()
    
    backtest_portfolio_realistic(
        sl_pct=args.sl,
        activation_pct=args.activation,
        callback_pct=args.callback,
        timeout_hours=args.timeout
    )
