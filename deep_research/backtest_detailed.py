#!/usr/bin/env python3
import csv
import sys
from pathlib import Path
from typing import List, Dict, Tuple

# Add parent directory to path to allow imports
sys.path.append(str(Path(__file__).parent.parent))

from scripts_v2.db_batch_utils import fetch_bars_batch, get_connection
from scripts_v2.optimize_combined_leverage import (
    run_strategy, 
    COMMISSION_PCT,
    BASE_ACTIVATION,
    BASE_CALLBACK,
    BASE_REENTRY_DROP,
    BASE_COOLDOWN,
    get_rolling_delta,
    get_avg_delta,
)

# ---------------------------------------------------------------------------
# Composite Strategy Rules (Hardcoded from Report)
# ---------------------------------------------------------------------------
RULES = [
    {
        "range": (100, 150),
        "filters": {"rsi": 45, "vol": 1, "oi": 2},
        "strategy": {"leverage": 10, "sl": 7, "window": 120, "threshold": 1.0}
    },
    {
        "range": (150, 200),
        "filters": {"rsi": 65, "vol": 1, "oi": 0},
        "strategy": {"leverage": 10, "sl": 3, "window": 10, "threshold": 1.0}
    },
    {
        "range": (200, 250),
        "filters": {"rsi": 50, "vol": 0, "oi": 0},
        "strategy": {"leverage": 10, "sl": 4, "window": 20, "threshold": 1.0}
    },
    {
        "range": (250, 300),
        "filters": {"rsi": 25, "vol": 0, "oi": 0},
        "strategy": {"leverage": 10, "sl": 3, "window": 10, "threshold": 1.0}
    }
]

def get_matching_rule(score, rsi, vol, oi):
    """Find the correct rule for a signal based on Score and Filters."""
    for rule in RULES:
        s_min, s_max = rule["range"]
        f = rule["filters"]
        
        # Check Score Range
        if s_min <= score < s_max:
            # Check Filters
            if rsi >= f["rsi"] and vol >= f["vol"] and oi >= f["oi"]:
                return rule["strategy"]
    return None

def run_simulation_detailed(bars, strategy_params):
    """Run a single signal simulation and return detailed trade information.

    This mirrors the logic from ``run_strategy`` in ``optimize_combined_leverage.py``
    but also records entry/exit timestamps, reason and PnL details.
    """
    if not bars:
        return None

    # Strategy parameters
    sl_pct = strategy_params["sl"]
    delta_window = strategy_params["window"]
    threshold_mult = strategy_params["threshold"]
    leverage = strategy_params["leverage"]

    # Initial state
    entry_price = bars[0][1]
    entry_ts = bars[0][0]
    max_price = entry_price
    in_position = True
    last_exit_ts = 0
    exit_price = entry_price
    exit_reason = "Timeout"
    exit_ts = bars[-1][0]

    comm_cost = COMMISSION_PCT * 2 * leverage

    for idx, bar in enumerate(bars):
        ts, price, delta, _, large_buy, large_sell = bar

        # Update max price while in position
        if in_position and price > max_price:
            max_price = price

        if in_position:
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100

            # Stop-loss
            if pnl_from_entry <= -sl_pct:
                exit_price = price
                exit_reason = "SL"
                exit_ts = ts
                in_position = False
                last_exit_ts = ts
                break

            # Trailing / momentum exit
            if pnl_from_entry >= BASE_ACTIVATION and drawdown_from_max >= BASE_CALLBACK:
                r_delta = get_rolling_delta(bars, idx, delta_window)
                a_delta = get_avg_delta(bars, idx)
                threshold = a_delta * threshold_mult
                if not (r_delta > threshold) and not (r_delta >= 0):
                    exit_price = price
                    exit_reason = "Trailing/Momentum"
                    exit_ts = ts
                    in_position = False
                    last_exit_ts = ts
                    break
        else:
            # Re-entry logic (mirrors optimize_combined_leverage)
            if ts - last_exit_ts >= BASE_COOLDOWN:
                if price < max_price:
                    drop_pct = (max_price - price) / max_price * 100
                    if drop_pct >= BASE_REENTRY_DROP:
                        if delta > 0 and large_buy > large_sell:
                            # Re-enter position
                            in_position = True
                            entry_price = price
                            entry_ts = ts
                            max_price = price
                            last_exit_ts = 0
                            continue

    # If still in position after loop, exit at last bar
    if in_position:
        exit_price = bars[-1][1]
        exit_reason = "Timeout"
        exit_ts = bars[-1][0]

    raw_pnl = (exit_price - entry_price) / entry_price * 100
    pnl_usd = raw_pnl * leverage - comm_cost

    return {
        "entry_price": entry_price,
        "exit_price": exit_price,
        "duration": exit_ts - entry_ts,
        "exit_reason": exit_reason,
        "pnl": pnl_usd,
    }


def main():
    conn = get_connection()
    
    print("Fetching signals...")
    # Fetch all signals that might match our Score Range (100-300)
    # We join with indicators to get RSI/Vol/OI at once
    # Fetch all signals that might match our Score Range (100-300)
    # We join with indicators to get RSI/Vol/OI at once
    # TABLE NAME FIX: Use sa.pair_symbol directly (denormalized column), remove risky join
    sql = """
        SELECT sa.id, sa.trading_pair_id, sa.signal_timestamp, sa.total_score,
               i.rsi, i.volume_zscore, i.oi_delta_pct, sa.pair_symbol
        FROM web.signal_analysis sa
        JOIN fas_v2.indicators i ON (i.trading_pair_id = sa.trading_pair_id AND i.timestamp = sa.signal_timestamp AND i.timeframe='15m')
        WHERE sa.total_score >= 100 AND sa.total_score < 300
        ORDER BY sa.signal_timestamp ASC
    """
    
    with conn.cursor() as cur:
        cur.execute(sql)
        signals = cur.fetchall()
        
    print(f"Found {len(signals)} candidates for backtest.")
    
    trades = []
    
    # Helper to batch fetch bars
    # We only fetch bars for signals that match filters
    batch_size = 100
    signals_to_process = []
    
    for row in signals:
        sid, pid, ts, score, rsi, vol, oi, symbol = row
        # Check if this signal matches any rule
        strategy = get_matching_rule(score, rsi, vol, oi)
        
        if strategy:
            signals_to_process.append((sid, strategy, symbol, ts, score))
            
    print(f"Signals passing filters: {len(signals_to_process)}")
    
    # Track active positions to prevent overlapping trades on same symbol
    active_positions = {}  # {symbol: exit_timestamp}
    skipped_due_to_position = 0
    
    # Process in batches
    for i in range(0, len(signals_to_process), batch_size):
        batch = signals_to_process[i : i+batch_size]
        sids = [b[0] for b in batch]
        
        # Fetch bars
        bars_dict = fetch_bars_batch(conn, sids)
        
        for item in batch:
            sid, strat, symbol, ts, score = item
            
            # Convert timestamp to epoch for comparison
            entry_epoch = ts.timestamp() if hasattr(ts, 'timestamp') else ts
            
            # Check if position is occupied
            if symbol in active_positions:
                if entry_epoch < active_positions[symbol]:
                    skipped_due_to_position += 1
                    continue  # Skip: position still open
            
            bars = bars_dict.get(sid, [])
            
            if not bars:
                continue
                
            # Limit bars based on window? The fetcher gets 2 hours.
            # We must slice if window < len(bars) strictly?
            # run_simulation_detailed handles logic.
            
            res = run_simulation_detailed(bars, strat)
            
            if res:
                # Calculate exit timestamp and update active_positions
                exit_epoch = entry_epoch + res["duration"]
                active_positions[symbol] = exit_epoch
                
                trades.append({
                    "Date": ts,
                    "Symbol": symbol,
                    "Score": score,
                    "Strategy": f"{strat['leverage']}x/SL{strat['sl']}",
                    "Entry": res["entry_price"],
                    "Exit": res["exit_price"],
                    "Duration(s)": res["duration"],
                    "Reason": res["exit_reason"],
                    "PnL%": round(res["pnl"], 2)
                })
                
    conn.close()
    
    # Sort trades by date
    trades.sort(key=lambda x: x["Date"])
    
    print(f"\nSkipped due to occupied position: {skipped_due_to_position}")

    # Console Output with Colors
    print("\n" + "="*100)
    print(f"{'DETAILED TRADE LOG':^100}")
    print("="*100)
    print(f"{'DATE':<20} | {'SYMBOL':<10} | {'SCORE':<5} | {'STRATEGY':<12} | {'ENTRY':<10} | {'EXIT':<10} | {'DUR(s)':<6} | {'PNL%':<8} | {'REASON'}")
    print("-" * 100)

    daily_pnl = {} # "YYYY-MM-DD": pnl
    
    total_pnl_cum = 0.0
    
    # ANSI Colors
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'

    for t in trades:
        date_str = str(t["Date"])[:19]
        day = str(t["Date"])[:10]
        pnl = t["PnL%"]
        
        # Accumulate Daily
        daily_pnl[day] = daily_pnl.get(day, 0.0) + pnl
        total_pnl_cum += pnl
        
        # Colorize PnL
        pnl_str = f"{pnl:+.2f}%"
        color = GREEN if pnl > 0 else RED
        
        print(f"{date_str:<20} | {t['Symbol']:<10} | {t['Score']:<5} | {t['Strategy']:<12} | {t['Entry']:<10.5f} | {t['Exit']:<10.5f} | {t['Duration(s)']:<6} | {color}{pnl_str:<8}{RESET} | {t['Reason']}")

    # -----------------------------------------------------------------------
    # 2. DAILY PERFORMANCE SUMMARY TABLE
    # -----------------------------------------------------------------------
    print("\n" + "="*80)
    print(f"{'DAILY PERFORMANCE SUMMARY':^80}")
    print("="*80)
    print(f"{'DATE':<12} | {'TRADES':<6} | {'WINS':<4} | {'LOSS':<4} | {'WR%':<6} | {'DAILY PNL%':<12} | {'CUMULATIVE'}")
    print("-" * 80)
    
    # Sort dates
    sorted_days = sorted(daily_pnl.keys())
    running_total = 0.0
    
    for day in sorted_days:
        day_trades = [t for t in trades if str(t["Date"]).startswith(day)]
        count = len(day_trades)
        wins = len([t for t in day_trades if t["PnL%"] > 0])
        losses = count - wins
        wr = (wins / count * 100) if count > 0 else 0
        
        d_pnl = daily_pnl[day]
        running_total += d_pnl
        
        d_pnl_str = f"{d_pnl:+.2f}%"
        cum_str = f"{running_total:+.2f}%"
        
        d_color = GREEN if d_pnl > 0 else RED
        c_color = GREEN if running_total > 0 else RED
        
        print(f"{day:<12} | {count:<6} | {wins:<4} | {losses:<4} | {wr:5.1f}% | {d_color}{d_pnl_str:<12}{RESET} | {c_color}{cum_str}{RESET}")
    print("="*80)

    # Save CSV
    csv_path = "backtest_trades.csv"
    keys = ["Date", "Symbol", "Score", "Strategy", "Entry", "Exit", "Duration(s)", "Reason", "PnL%"]
    
    with open(csv_path, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(trades)
        
    # Summary similar to backtest_portfolio_realistic
    total_days = len(daily_pnl)
    avg_daily = sum(daily_pnl.values()) / total_days if total_days > 0 else 0
    wins = [t for t in trades if t["PnL%"] > 0]
    wr = len(wins) / len(trades) if trades else 0
    
    # Calculate Max Drawdown (on cumulative PnL curve)
    # Simple check on daily sequence not strictly accurate for equity curve but approximate
    
    print("\n" + "="*60)
    print(f"{'PORTFOLIO SUMMARY (30 Days)':^60}")
    print("="*60)
    print(f"Total Trades:      {len(trades)}")
    print(f"Win Rate:          {wr*100:.1f}%")
    print(f"Total PnL (Gross): {GREEN if total_pnl_cum > 0 else RED}{total_pnl_cum:.2f}%{RESET} (Sum of margin %)")
    print("-" * 60)
    print(f"Average Daily PnL: {avg_daily:.2f}%")
    print(f"Best Day:          {max(daily_pnl.values(), default=0):.2f}%")
    print(f"Worst Day:         {min(daily_pnl.values(), default=0):.2f}%")
    print("="*60)
    print(f"Full details saved to: {csv_path}")

if __name__ == "__main__":
    main()
