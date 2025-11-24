import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import psycopg
from psycopg.rows import dict_row
import argparse
from collections import defaultdict

# Add config directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import settings

# --- Configuration ---
DB_CONFIG = settings.DATABASE

SCORE_THRESHOLD = 250
TARGET_PATTERNS = ['SQUEEZE_IGNITION', 'OI_EXPLOSION']
ANALYSIS_WINDOW_HOURS = 24

# Trading parameters
POSITION_SIZE = 1000  # $1000 per trade
LEVERAGE = 10
MARGIN_PER_POSITION = POSITION_SIZE / LEVERAGE  # $100

def get_db_connection():
    conn_params = [
        f"host={DB_CONFIG['host']}",
        f"port={DB_CONFIG['port']}",
        f"dbname={DB_CONFIG['dbname']}",
        f"user={DB_CONFIG['user']}",
        "sslmode=disable"
    ]
    
    if DB_CONFIG.get('password'):
        conn_params.append(f"password={DB_CONFIG['password']}")
        
    conn_str = " ".join(conn_params)
    return psycopg.connect(conn_str)

def simulate_trade(candles, entry_price, sl_pct, activation_pct, callback_pct):
    """
    Simulate a trade with SL + TS strategy
    Returns: (exit_price, exit_time_ms, pnl_pct)
    """
    sl_price = entry_price * (1 + sl_pct / 100)
    activation_price = entry_price * (1 + activation_pct / 100)
    ts_activated = False
    peak_price = entry_price
    
    for candle in candles:
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        close = float(candle['close_price'])
        open_time = candle['open_time']
        
        # Check SL (only before TS activation)
        if not ts_activated and low <= sl_price:
            pnl_pct = sl_pct
            return sl_price, open_time, pnl_pct
        
        # Check if TS should activate
        if not ts_activated and high >= activation_price:
            ts_activated = True
            peak_price = high
        
        if ts_activated:
            # Update peak
            if high > peak_price:
                peak_price = high
            
            # Check callback from peak
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                pnl_pct = ((ts_exit_price - entry_price) / entry_price) * 100
                return ts_exit_price, open_time, pnl_pct
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    final_time = candles[-1]['open_time']
    pnl_pct = ((final_price - entry_price) / entry_price) * 100
    return final_price, final_time, pnl_pct

def backtest_portfolio(days=30, limit=None, sl_pct=-5, activation_pct=15, callback_pct=3, initial_balance=10000):
    """
    Backtest portfolio with given SL/TS parameters
    """
    print(f"Starting portfolio backtest for the last {days} days...")
    print(f"Parameters: SL={sl_pct}%, TS Activation={activation_pct}%, TS Callback={callback_pct}%")
    print(f"Position Size: ${POSITION_SIZE}, Leverage: {LEVERAGE}x, Margin per position: ${MARGIN_PER_POSITION}")
    print(f"Initial Balance: ${initial_balance:,.2f}")
    print("="*120)
    
    try:
        with get_db_connection() as conn:
            # Fetch Signals
            placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query_signals = f"""
                SELECT 
                    sh.trading_pair_id, 
                    sh.pair_symbol, 
                    sh.timestamp, 
                    sh.total_score
                FROM fas_v2.scoring_history sh
                JOIN fas_v2.signal_patterns sp ON sh.trading_pair_id = sp.trading_pair_id 
                    AND sp.timestamp BETWEEN sh.timestamp - INTERVAL '1 hour' AND sh.timestamp + INTERVAL '1 hour'
                JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
                WHERE sh.total_score > {SCORE_THRESHOLD}
                  AND sh.timestamp >= NOW() - INTERVAL '{days} days'
                  AND sp.pattern_type IN ({placeholders})
                  AND tp.contract_type_id = 1
                  AND tp.exchange_id = 1
                  AND tp.is_active = TRUE
                ORDER BY sh.timestamp ASC
                {limit_clause}
            """
            
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query_signals)
                signals = cur.fetchall()
                
            if not signals:
                print("No signals found.")
                return

            print(f"Found {len(signals)} signals. Simulating trades...")
            
            # Simulate all trades
            trades = []
            last_signal_time = {}
            COOLDOWN_HOURS = 24
            
            for i, signal in enumerate(signals, 1):
                if i % 10 == 0:
                    print(f"Processing {i}/{len(signals)}...", end='\r')
                
                pair_id = signal['trading_pair_id']
                signal_ts = signal['timestamp']
                symbol = signal['pair_symbol']
                
                # Deduplication
                if symbol in last_signal_time:
                    last_ts = last_signal_time[symbol]
                    if (signal_ts - last_ts).total_seconds() < COOLDOWN_HOURS * 3600:
                        continue
                
                last_signal_time[symbol] = signal_ts
                
                # Entry time
                entry_time_dt = signal_ts + timedelta(minutes=15)
                end_time_dt = entry_time_dt + timedelta(hours=ANALYSIS_WINDOW_HOURS)
                
                entry_time_ms = int(entry_time_dt.timestamp() * 1000)
                end_time_ms = int(end_time_dt.timestamp() * 1000)
                
                # Fetch candles
                query_candles = """
                    SELECT open_time, open_price, high_price, low_price, close_price
                    FROM public.candles
                    WHERE trading_pair_id = %s
                      AND interval_id = 2
                      AND open_time >= %s
                      AND open_time <= %s
                    ORDER BY open_time ASC
                """
                
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query_candles, (pair_id, entry_time_ms, end_time_ms))
                    candles = cur.fetchall()
                    
                if not candles:
                    continue
                
                entry_price = float(candles[0]['open_price'])
                
                # Simulate trade
                exit_price, exit_time_ms, pnl_pct = simulate_trade(
                    candles, entry_price, sl_pct, activation_pct, callback_pct
                )
                
                # Calculate PnL in dollars (with leverage)
                pnl_dollars = (pnl_pct / 100) * POSITION_SIZE
                
                # Convert timestamps to dates
                entry_date = entry_time_dt.date()
                exit_date = datetime.fromtimestamp(exit_time_ms / 1000, tz=timezone.utc).date()
                
                trades.append({
                    'symbol': symbol,
                    'entry_time': entry_time_dt,
                    'entry_date': entry_date,
                    'exit_date': exit_date,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'pnl_pct': pnl_pct,
                    'pnl_dollars': pnl_dollars,
                    'margin': MARGIN_PER_POSITION
                })
            
            print(f"\nSimulated {len(trades)} unique trades.")
            
            # Group by day and calculate daily metrics
            daily_stats = defaultdict(lambda: {
                'opened': 0,
                'closed': 0,
                'pnl': 0,
                'balance': initial_balance,
                'max_drawdown': 0
            })
            
            # Track cumulative balance
            balance = initial_balance
            max_balance = initial_balance
            
            # Get all dates in range
            if trades:
                start_date = min(t['entry_date'] for t in trades)
                end_date = max(t['exit_date'] for t in trades)
                
                current_date = start_date
                while current_date <= end_date:
                    daily_stats[current_date]  # Initialize
                    current_date += timedelta(days=1)
            
            # Process trades
            for trade in trades:
                # Mark as opened on entry date
                daily_stats[trade['entry_date']]['opened'] += 1
                
                # Mark as closed on exit date
                daily_stats[trade['exit_date']]['closed'] += 1
                daily_stats[trade['exit_date']]['pnl'] += trade['pnl_dollars']
            
            # Calculate cumulative balance for each day
            for date in sorted(daily_stats.keys()):
                balance += daily_stats[date]['pnl']
                daily_stats[date]['balance'] = balance
                
                if balance > max_balance:
                    max_balance = balance
                
                drawdown = balance - max_balance
                daily_stats[date]['max_drawdown'] = drawdown
            
            # Print daily report
            print("\n" + "="*120)
            print("DAILY BALANCE REPORT:")
            print("="*120)
            print(f"{'Date':<12} {'Opened':<8} {'Closed':<8} {'Daily PnL':<12} {'Balance':<14} {'Drawdown':<12} {'Need to Add':<12}")
            print("-"*120)
            
            for date in sorted(daily_stats.keys()):
                stats = daily_stats[date]
                need_to_add = max(0, -stats['max_drawdown']) if stats['max_drawdown'] < 0 else 0
                
                print(f"{date.strftime('%Y-%m-%d'):<12} "
                      f"{stats['opened']:<8} "
                      f"{stats['closed']:<8} "
                      f"${stats['pnl']:>10,.2f} "
                      f"${stats['balance']:>12,.2f} "
                      f"${stats['max_drawdown']:>10,.2f} "
                      f"${need_to_add:>10,.2f}")
            
            # Summary
            final_balance = balance
            total_profit = final_balance - initial_balance
            total_profit_pct = (total_profit / initial_balance) * 100
            max_drawdown = min(stats['max_drawdown'] for stats in daily_stats.values()) if daily_stats else 0
            
            winning_trades = len([t for t in trades if t['pnl_dollars'] > 0])
            losing_trades = len([t for t in trades if t['pnl_dollars'] <= 0])
            win_rate = (winning_trades / len(trades) * 100) if trades else 0
            
            print("\n" + "="*120)
            print("SUMMARY:")
            print("="*120)
            print(f"Initial Balance: ${initial_balance:,.2f}")
            print(f"Final Balance: ${final_balance:,.2f}")
            print(f"Total Profit: ${total_profit:,.2f} ({total_profit_pct:+.2f}%)")
            print(f"Max Drawdown: ${max_drawdown:,.2f}")
            print(f"Total Trades: {len(trades)}")
            print(f"Winning Trades: {winning_trades} ({win_rate:.2f}%)")
            print(f"Losing Trades: {losing_trades}")
            print("="*120)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backtest portfolio with optimal SL/TS parameters.')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of signals to process')
    parser.add_argument('--sl', type=float, default=-5, help='Stop-Loss percentage (negative)')
    parser.add_argument('--activation', type=float, default=15, help='TS Activation percentage (positive)')
    parser.add_argument('--callback', type=float, default=3, help='TS Callback percentage (positive)')
    parser.add_argument('--balance', type=float, default=10000, help='Initial balance in USD')
    args = parser.parse_args()
    
    backtest_portfolio(
        days=args.days,
        limit=args.limit,
        sl_pct=args.sl,
        activation_pct=args.activation,
        callback_pct=args.callback,
        initial_balance=args.balance
    )
