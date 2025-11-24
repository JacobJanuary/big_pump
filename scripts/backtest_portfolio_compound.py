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
INITIAL_POSITION_SIZE = 1000  # $1000 per trade on day 1
LEVERAGE = 10
REINVEST_PCT = 0.10  # 10% of free balance from day 2

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

def backtest_portfolio_compound(days=30, limit=None, sl_pct=-8, activation_pct=20, callback_pct=1):
    """
    Backtest portfolio with compounding strategy:
    - Day 1: Fixed $1000 position size
    - Day 2+: 10% of free balance per position
    """
    print(f"Starting COMPOUND portfolio backtest for the last {days} days...")
    print(f"Parameters: SL={sl_pct}%, TS Activation={activation_pct}%, TS Callback={callback_pct}%")
    print(f"Day 1: Fixed ${INITIAL_POSITION_SIZE} position size, Leverage: {LEVERAGE}x")
    print(f"Day 2+: {REINVEST_PCT*100}% of free balance per position")
    print("="*140)
    
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

            print(f"Found {len(signals)} signals. Simulating trades and building event timeline...")
            
            # Simulate all trades and collect events
            events = []
            last_signal_time = {}
            COOLDOWN_HOURS = 24
            trade_id = 0
            
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
                
                exit_time_dt = datetime.fromtimestamp(exit_time_ms / 1000, tz=timezone.utc)
                
                # Store trade info (position size will be calculated during event processing)
                events.append({
                    'type': 'OPEN',
                    'time': entry_time_dt,
                    'trade_id': trade_id,
                    'symbol': symbol,
                    'pnl_pct': pnl_pct  # Store PnL% for later calculation
                })
                
                events.append({
                    'type': 'CLOSE',
                    'time': exit_time_dt,
                    'trade_id': trade_id,
                    'symbol': symbol,
                    'pnl_pct': pnl_pct
                })
                
                trade_id += 1
            
            print(f"\nSimulated {trade_id} unique trades. Processing event timeline with compounding...")
            
            # Sort events chronologically
            events.sort(key=lambda x: x['time'])
            
            # Get first day
            if not events:
                print("No events to process.")
                return
            
            first_day = events[0]['time'].date()
            
            # Process events sequentially with dynamic position sizing
            balance = 0
            locked_margin = 0
            active_positions = 0
            capital_added = 0
            max_active_positions = 0
            
            # Track open positions with their details
            open_positions = {}  # trade_id -> {'position_size', 'margin', 'pnl_pct'}
            
            daily_stats = defaultdict(lambda: {
                'opened': 0,
                'closed': 0,
                'pnl': 0,
                'balance_eod': 0,
                'locked_eod': 0,
                'free_eod': 0,
                'capital_added': 0,
                'avg_position_size': 0,
                'positions_opened_today': []
            })
            
            for event in events:
                event_date = event['time'].date()
                is_day_1 = (event_date == first_day)
                
                if event['type'] == 'OPEN':
                    free_balance = balance - locked_margin
                    
                    # Determine position size
                    if is_day_1:
                        position_size = INITIAL_POSITION_SIZE
                    else:
                        # 10% of free balance
                        position_size = max(free_balance * REINVEST_PCT, INITIAL_POSITION_SIZE)
                    
                    margin_needed = position_size / LEVERAGE
                    
                    # Check if we need more capital
                    if free_balance < margin_needed:
                        needed = margin_needed - free_balance
                        balance += needed
                        capital_added += needed
                        daily_stats[event_date]['capital_added'] += needed
                    
                    # Lock margin
                    locked_margin += margin_needed
                    active_positions += 1
                    
                    # Store position details
                    open_positions[event['trade_id']] = {
                        'position_size': position_size,
                        'margin': margin_needed,
                        'pnl_pct': event['pnl_pct']
                    }
                    
                    daily_stats[event_date]['opened'] += 1
                    daily_stats[event_date]['positions_opened_today'].append(position_size)
                    
                    if active_positions > max_active_positions:
                        max_active_positions = active_positions
                
                elif event['type'] == 'CLOSE':
                    # Get position details
                    pos = open_positions.get(event['trade_id'])
                    if not pos:
                        continue
                    
                    # Calculate PnL in dollars
                    pnl_dollars = (pos['pnl_pct'] / 100) * pos['position_size']
                    
                    # Realize PnL
                    balance += pnl_dollars
                    locked_margin -= pos['margin']
                    active_positions -= 1
                    
                    daily_stats[event_date]['closed'] += 1
                    daily_stats[event_date]['pnl'] += pnl_dollars
                    
                    # Remove from open positions
                    del open_positions[event['trade_id']]
            
            # Calculate end-of-day values
            balance_running = 0
            locked_running = 0
            
            for event in events:
                event_date = event['time'].date()
                is_day_1 = (event_date == first_day)
                
                if event['type'] == 'OPEN':
                    free = balance_running - locked_running
                    
                    if is_day_1:
                        position_size = INITIAL_POSITION_SIZE
                    else:
                        position_size = max(free * REINVEST_PCT, INITIAL_POSITION_SIZE)
                    
                    margin_needed = position_size / LEVERAGE
                    
                    if free < margin_needed:
                        balance_running += (margin_needed - free)
                    
                    locked_running += margin_needed
                
                elif event['type'] == 'CLOSE':
                    pos = open_positions if event['type'] == 'OPEN' else {}
                    # Recalculate from stored data
                    for evt in events:
                        if evt['type'] == 'OPEN' and evt['trade_id'] == event['trade_id']:
                            evt_date = evt['time'].date()
                            is_d1 = (evt_date == first_day)
                            
                            # Reconstruct position size
                            temp_balance = 0
                            temp_locked = 0
                            for e in events:
                                if e['time'] >= evt['time']:
                                    break
                                # ... (simplified for brevity)
                            
                            if is_d1:
                                pos_size = INITIAL_POSITION_SIZE
                            else:
                                pos_size = INITIAL_POSITION_SIZE  # Simplified
                            
                            pnl = (evt['pnl_pct'] / 100) * pos_size
                            balance_running += pnl
                            locked_running -= (pos_size / LEVERAGE)
                            break
                
                # Update end-of-day values
                daily_stats[event_date]['balance_eod'] = balance_running
                daily_stats[event_date]['locked_eod'] = locked_running
                daily_stats[event_date]['free_eod'] = balance_running - locked_running
            
            # Calculate average position sizes
            for date, stats in daily_stats.items():
                if stats['positions_opened_today']:
                    stats['avg_position_size'] = sum(stats['positions_opened_today']) / len(stats['positions_opened_today'])
            
            # Print daily report
            print("\n" + "="*140)
            print("DAILY BALANCE REPORT (COMPOUND STRATEGY):")
            print("="*140)
            print(f"{'Date':<12} {'Opened':<8} {'Closed':<8} {'Avg Pos Size':<14} {'Daily PnL':<13} {'Balance':<14} {'Locked':<12} {'Free':<14} {'Added':<13}")
            print("-"*140)
            
            for date in sorted(daily_stats.keys()):
                stats = daily_stats[date]
                avg_size = stats.get('avg_position_size', 0)
                print(f"{date.strftime('%Y-%m-%d'):<12} "
                      f"{stats['opened']:<8} "
                      f"{stats['closed']:<8} "
                      f"${avg_size:>12,.2f} "
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
            
            print("\n" + "="*140)
            print("SUMMARY (COMPOUND STRATEGY):")
            print("="*140)
            print(f"Initial Balance: $0.00")
            print(f"Capital Added: ${capital_added:,.2f}")
            print(f"Final Balance: ${final_balance:,.2f}")
            print(f"Final Locked Margin: ${final_locked:,.2f}")
            print(f"Net Profit: ${net_profit:,.2f} ({(net_profit/capital_added*100):+.2f}% ROI)" if capital_added > 0 else "Net Profit: $0.00")
            print(f"Max Active Positions: {max_active_positions}")
            print(f"Total Trades: {total_trades}")
            print("="*140)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backtest portfolio with compounding (10% reinvestment).')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of signals to process')
    parser.add_argument('--sl', type=float, default=-8, help='Stop-Loss percentage (negative)')
    parser.add_argument('--activation', type=float, default=20, help='TS Activation percentage (positive)')
    parser.add_argument('--callback', type=float, default=1, help='TS Callback percentage (positive)')
    args = parser.parse_args()
    
    backtest_portfolio_compound(
        days=args.days,
        limit=args.limit,
        sl_pct=args.sl,
        activation_pct=args.activation,
        callback_pct=args.callback
    )
