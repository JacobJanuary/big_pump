"""
Portfolio backtest with optimal strategy parameters
Uses 1-minute candles for accurate simulation
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timezone
import argparse
from collections import defaultdict

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection
from optimization_lib import simulate_combined

# Trading parameters
POSITION_SIZE = 1000  # $1000 per trade
LEVERAGE = 10
MARGIN_PER_POSITION = POSITION_SIZE / LEVERAGE  # $100

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

def backtest_portfolio(sl_pct=-8, activation_pct=20, callback_pct=1, timeout_hours=12):
    """
    Backtest portfolio with specified strategy parameters using event-based simulation
    
    Args:
        sl_pct: Stop-loss percentage (negative)
        activation_pct: TS activation percentage (positive)
        callback_pct: TS callback percentage (positive)
        timeout_hours: Timeout in hours if TS not activated
    """
    print("="*140)
    print("PORTFOLIO BACKTEST - Event-Based Simulation")
    print("="*140)
    print(f"Strategy Parameters:")
    print(f"  Stop-Loss: {sl_pct}%")
    print(f"  TS Activation: {activation_pct}%")
    print(f"  TS Callback: {callback_pct}%")
    print(f"  Timeout: {timeout_hours} hours")
    print(f"  Position Size: ${POSITION_SIZE}, Leverage: {LEVERAGE}x, Margin: ${MARGIN_PER_POSITION}")
    print("="*140)
    
    try:
        # Load signals with minute candles
        signals_data = load_signals_with_minute_candles()
        
        if not signals_data:
            print("No signals with minute candles found.")
            print("Please run scripts/fetch_minute_candles.py first.")
            return
        
        print(f"\nSimulating {len(signals_data)} trades...")
        
        # Simulate all trades and collect events
        events = []
        trade_id = 0
        
        for i, sig in enumerate(signals_data, 1):
            if i % 10 == 0:
                print(f"Simulating {i}/{len(signals_data)}...", end='\r')
            
            # Simulate trade
            pnl_pct = simulate_combined(
                sig['candles'],
                sig['entry_price'],
                sl_pct,
                activation_pct,
                callback_pct,
                timeout_hours=timeout_hours
            )
            
            # Calculate PnL in dollars
            pnl_dollars = (pnl_pct / 100) * POSITION_SIZE
            
            # Find exit time (simplified - use last candle for now)
            # TODO: Track exact exit time in simulate_combined
            entry_time_dt = sig['entry_time']
            exit_time_dt = datetime.fromtimestamp(
                sig['candles'][-1]['open_time'] / 1000, 
                tz=timezone.utc
            )
            
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
                'pnl': pnl_dollars,
                'pnl_pct': pnl_pct
            })
            
            trade_id += 1
        
        print(f"\n\nSimulated {trade_id} trades. Processing event timeline...")
        
        # Sort events chronologically
        events.sort(key=lambda x: x['time'])
        
        # Process events sequentially
        balance = 0
        locked_margin = 0
        active_positions = 0
        capital_added = 0
        max_active_positions = 0
        
        daily_stats = defaultdict(lambda: {
            'opened': 0,
            'closed': 0,
            'pnl': 0,
            'balance_eod': 0,
            'locked_eod': 0,
            'free_eod': 0,
            'capital_added': 0
        })
        
        for event in events:
            event_date = event['time'].date()
            
            if event['type'] == 'OPEN':
                # Check if we need more capital
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
        
        # Calculate end-of-day values
        balance_running = 0
        locked_running = 0
        
        for event in events:
            event_date = event['time'].date()
            
            if event['type'] == 'OPEN':
                free = balance_running - locked_running
                if free < MARGIN_PER_POSITION:
                    balance_running += (MARGIN_PER_POSITION - free)
                locked_running += MARGIN_PER_POSITION
            
            elif event['type'] == 'CLOSE':
                balance_running += event['pnl']
                locked_running -= MARGIN_PER_POSITION
            
            # Update end-of-day values
            daily_stats[event_date]['balance_eod'] = balance_running
            daily_stats[event_date]['locked_eod'] = locked_running
            daily_stats[event_date]['free_eod'] = balance_running - locked_running
        
        # Print daily report
        print("\n" + "="*140)
        print("DAILY BALANCE REPORT:")
        print("="*140)
        print(f"{'Date':<12} {'Opened':<8} {'Closed':<8} {'Daily PnL':<13} {'Balance':<14} {'Locked':<12} {'Free':<14} {'Added Today':<13}")
        print("-"*140)
        
        for date in sorted(daily_stats.keys()):
            stats = daily_stats[date]
            print(f"{date.strftime('%Y-%m-%d'):<12} "
                  f"{stats['opened']:<8} "
                  f"{stats['closed']:<8} "
                  f"${stats['pnl']:>11,.2f} "
                  f"${stats['balance_eod']:>12,.2f} "
                  f"${stats['locked_eod']:>10,.2f} "
                  f"${stats['free_eod']:>12,.2f} "
                  f"${stats['capital_added']:>11,.2f}")
        
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
        print(f"Max Active Positions: {max_active_positions}")
        print(f"Total Trades: {total_trades}")
        print(f"Winning Trades: {winning_trades} ({win_rate:.2f}%)")
        print(f"Losing Trades: {losing_trades}")
        print("="*140)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backtest portfolio with optimal strategy.')
    parser.add_argument('--sl', type=float, default=-8, help='Stop-Loss percentage (negative)')
    parser.add_argument('--activation', type=float, default=20, help='TS Activation percentage (positive)')
    parser.add_argument('--callback', type=float, default=1, help='TS Callback percentage (positive)')
    parser.add_argument('--timeout', type=float, default=12, help='Timeout in hours')
    args = parser.parse_args()
    
    backtest_portfolio(
        sl_pct=args.sl,
        activation_pct=args.activation,
        callback_pct=args.callback,
        timeout_hours=args.timeout
    )
