#!/usr/bin/env python3
"""
Detailed Backtest Script - Uses SAME signal source as optimize_combined_leverage_filtered.py

Critical: This script uses fetch_signals() from pump_analysis_lib to get signals
from fas_v2.scoring_history with proper filters, then maps them to web.signal_analysis
IDs to fetch 1-second bars from web.agg_trades_1s.
"""
import csv
import sys
import json
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime, timezone

# Add parent directory to path to allow imports
sys.path.append(str(Path(__file__).parent.parent))

from scripts_v2.db_batch_utils import fetch_bars_batch
from scripts_v2.pump_analysis_lib import (
    get_db_connection,
    fetch_signals,
    SCORE_THRESHOLD,
    SCORE_THRESHOLD_MAX,
    INDICATOR_FILTERS,
)
from scripts_v2.optimize_combined_leverage import (
    run_strategy, 
    COMMISSION_PCT,
    BASE_ACTIVATION,
    BASE_CALLBACK,
    BASE_REENTRY_DROP,
    BASE_COOLDOWN,
)

# ---------------------------------------------------------------------------
# Composite Strategy Rules (Loaded from composite_strategy.json)
# ---------------------------------------------------------------------------
def load_rules_from_json(json_path: str = None) -> list:
    """Load rules from composite_strategy.json"""
    paths_to_try = [
        json_path,
        "composite_strategy.json",
        "../composite_strategy.json",
        Path(__file__).parent.parent / "composite_strategy.json",
    ]
    
    for path in paths_to_try:
        if path and Path(path).exists():
            print(f"Loading rules from {path}...")
            with open(path, "r") as f:
                data = json.load(f)
            # Convert from JSON format to backtest format
            rules = []
            for r in data.get("rules", []):
                f = r["filter"]
                s = r["strategy"]
                rules.append({
                    "range": (f["score_min"], f["score_max"]),
                    "filters": {"rsi": f["rsi_min"], "vol": f["vol_min"], "oi": f["oi_min"]},
                    "strategy": {
                        "leverage": s["leverage"], 
                        "sl": s["sl_pct"], 
                        "window": s["delta_window"], 
                        "threshold": s["threshold_mult"],
                        # Load params or default to constants if missing (backwards compat)
                        "activation": s.get("base_activation", BASE_ACTIVATION),
                        "callback": s.get("base_callback", BASE_CALLBACK),
                        "cooldown": s.get("base_cooldown", BASE_COOLDOWN),
                        "reentry_drop": s.get("base_reentry_drop", BASE_REENTRY_DROP),
                    }
                })
            return rules
    
    print("WARNING: composite_strategy.json not found, using defaults")
    return []

# Load rules at module level
RULES = load_rules_from_json()

# Track why signals are skipped (for diagnostics)
SKIP_REASONS = {
    "score_out_of_range": [],
    "rsi_too_low": [],
    "vol_too_low": [],
    "oi_too_low": [],
}

def get_matching_rule(score, rsi, vol, oi, verbose=False):
    """Find the correct rule for a signal based on Score and Filters.
    
    Returns (strategy_dict, matched_rule_info) or (None, skip_reason)
    """
    for rule in RULES:
        s_min, s_max = rule["range"]
        f = rule["filters"]
        
        # Check Score Range
        if s_min <= score < s_max:
            # Check Filters - with detailed reason tracking
            if rsi < f["rsi"]:
                reason = f"score={score} in [{s_min},{s_max}), but rsi={rsi:.1f} < {f['rsi']}"
                SKIP_REASONS["rsi_too_low"].append(reason)
                if verbose:
                    print(f"  SKIP: {reason}")
                return None, f"rsi<{f['rsi']}"
            if vol < f["vol"]:
                reason = f"score={score} in [{s_min},{s_max}), but vol={vol:.1f} < {f['vol']}"
                SKIP_REASONS["vol_too_low"].append(reason)
                if verbose:
                    print(f"  SKIP: {reason}")
                return None, f"vol<{f['vol']}"
            if oi < f["oi"]:
                reason = f"score={score} in [{s_min},{s_max}), but oi={oi:.1f} < {f['oi']}"
                SKIP_REASONS["oi_too_low"].append(reason)
                if verbose:
                    print(f"  SKIP: {reason}")
                return None, f"oi<{f['oi']}"
            
            # All filters pass!
            return rule["strategy"], None
    
    # Score not in any range
    reason = f"score={score} not in any range (min=100, max=500)"
    SKIP_REASONS["score_out_of_range"].append(reason)
    return None, "score_out_of_range"


def run_simulation_detailed(bars, strategy_params):
    """Run a single signal simulation and return detailed trade information.

    CRITICAL: This now matches run_strategy_fast EXACTLY:
    - Uses INDEX-BASED rolling delta (not time-based get_rolling_delta)
    - Uses continue (not break) to allow re-entry
    - Accumulates total_pnl across multiple trades
    - Uses precomputed cumsum arrays like optimizer
    """
    if not bars:
        return None

    n = len(bars)
    if n < 100:
        return None

    # Strategy parameters
    sl_pct = strategy_params["sl"]
    delta_window = strategy_params["window"]
    threshold_mult = strategy_params["threshold"]
    leverage = strategy_params["leverage"]
    
    # Dynamic parameters (with defaults)
    base_activation = strategy_params.get("activation", BASE_ACTIVATION)
    base_callback = strategy_params.get("callback", BASE_CALLBACK)
    base_cooldown = strategy_params.get("cooldown", BASE_COOLDOWN)
    base_reentry_drop = strategy_params.get("reentry_drop", BASE_REENTRY_DROP)

    # Precompute cumsum arrays EXACTLY like optimizer (precompute_bars)
    cumsum_delta = [0.0] * (n + 1)
    cumsum_abs_delta = [0.0] * (n + 1)
    for i in range(n):
        cumsum_delta[i + 1] = cumsum_delta[i] + bars[i][2]
        cumsum_abs_delta[i + 1] = cumsum_abs_delta[i] + abs(bars[i][2])
    
    # Precompute avg_delta for lookback=100 EXACTLY like optimizer
    lookback = 100
    avg_delta_arr = [0.0] * n
    for i in range(n):
        lb = min(i, lookback)
        if lb > 0:
            avg_delta_arr[i] = (cumsum_abs_delta[i] - cumsum_abs_delta[i - lb]) / lb

    # Initial state
    entry_price = bars[0][1]
    first_entry_ts = bars[0][0]
    max_price = entry_price
    in_position = True
    last_exit_ts = 0
    
    # Accumulated results (like optimizer)
    total_pnl = 0.0
    trade_count = 0
    last_exit_reason = "Timeout"
    last_exit_price = bars[-1][1]
    last_exit_timestamp = bars[-1][0]
    
    comm_cost = COMMISSION_PCT * 2 * leverage

    for idx in range(n):
        bar = bars[idx]
        ts = bar[0]
        price = bar[1]
        delta = bar[2]
        large_buy = bar[4]
        large_sell = bar[5]

        if in_position:
            # Update max price while in position
            if price > max_price:
                max_price = price
                
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100

            # Stop-loss
            if pnl_from_entry <= -sl_pct:
                total_pnl += (pnl_from_entry * leverage - comm_cost)
                trade_count += 1
                in_position = False
                last_exit_ts = ts
                last_exit_reason = "SL"
                last_exit_price = price
                last_exit_timestamp = ts
                continue

            # Trailing / momentum exit
            if pnl_from_entry >= base_activation and drawdown_from_max >= base_callback:
                # INDEX-BASED calculation - EXACTLY like optimizer
                window_start_idx = max(0, idx - delta_window)
                actual_window_size = idx - window_start_idx
                rolling_delta = cumsum_delta[idx + 1] - cumsum_delta[window_start_idx]
                
                avg_delta = avg_delta_arr[idx]
                threshold = avg_delta * threshold_mult
                
                # Proportional scaling when insufficient data
                if actual_window_size < delta_window and delta_window > 0:
                    data_ratio = actual_window_size / delta_window
                    threshold = threshold * data_ratio
                
                if not (rolling_delta > threshold) and not (rolling_delta >= 0):
                    total_pnl += (pnl_from_entry * leverage - comm_cost)
                    trade_count += 1
                    in_position = False
                    last_exit_ts = ts
                    max_price = price
                    last_exit_reason = "Trailing/Momentum"
                    last_exit_price = price
                    last_exit_timestamp = ts
                    continue
        else:
            # Re-entry logic
            if ts - last_exit_ts >= base_cooldown:
                if price < max_price:
                    drop_pct = (max_price - price) / max_price * 100
                    if drop_pct >= base_reentry_drop:
                        if delta > 0 and large_buy > large_sell:
                            # Re-enter position
                            in_position = True
                            entry_price = price
                            max_price = price
                            last_exit_ts = 0
                else:
                    # Update max_price when price goes above while OUT of position
                    max_price = price

    # If still in position after loop, close at last bar
    if in_position:
        final_price = bars[-1][1]
        pnl = (final_price - entry_price) / entry_price * 100
        total_pnl += (pnl * leverage - comm_cost)
        trade_count += 1
        last_exit_reason = "Timeout"
        last_exit_price = final_price
        last_exit_timestamp = bars[-1][0]

    return {
        "entry_price": bars[0][1],
        "exit_price": last_exit_price,
        "duration": last_exit_timestamp - first_entry_ts,
        "exit_reason": last_exit_reason,
        "pnl": total_pnl,
        "trade_count": trade_count,
    }



def get_all_signals_like_optimizer(conn) -> List[Tuple[int, str, datetime, int, float, float, float]]:
    """
    Load ALL signals using SAME logic as optimize_unified.py
    
    This uses the SAME query as preload_all_signals() in optimize_unified.py:
    - Join web.signal_analysis with fas_v2.indicators
    - score >= 100, score < 950
    - NO additional vol/oi/rsi filters (those are applied via get_matching_rule)
    
    Returns list of (signal_id, symbol, timestamp, score, rsi, vol_zscore, oi_delta) tuples
    """
    print("   Loading signals using same logic as optimize_unified.py...")
    
    signals = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sa.id, sa.pair_symbol, sa.signal_timestamp, sa.total_score,
                   i.rsi, i.volume_zscore, i.oi_delta_pct
            FROM web.signal_analysis AS sa
            JOIN fas_v2.indicators AS i ON (
                i.trading_pair_id = sa.trading_pair_id 
                AND i.timestamp = sa.signal_timestamp
                AND i.timeframe = '15m'
            )
            WHERE sa.total_score >= 100 AND sa.total_score < 950
        """)
        
        for row in cur.fetchall():
            sid, sym, ts, score, rsi, vol, oi = row
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            signals.append((
                sid, sym, ts, score,
                rsi or 0, vol or 0, oi or 0
            ))
    
    print(f"   Loaded {len(signals)} signals (same as optimizer)")
    return signals


# Default strategy params (fallback if no composite_strategy.json)
DEFAULT_STRATEGY = {
    "leverage": 10,
    "sl": 4,
    "window": 20,
    "threshold": 1.0,
    "activation": 10.0,
    "callback": 4.0,
    "cooldown": 300,
    "reentry_drop": 5.0,
}


def main():
    conn = get_db_connection()
    
    print("=" * 60)
    print("DETAILED BACKTEST (Same Source as Optimizer)")
    print("=" * 60)
    print("Loading signals...")
    
    # Get signals using SAME logic as optimize_unified.py (no pre-filtering)
    signals = get_all_signals_like_optimizer(conn)
    
    if not signals:
        print("❌ No signals found.")
        conn.close()
        return
    
    print(f"\nTotal signals loaded: {len(signals)}")
    print("-" * 60)
    
    trades = []
    batch_size = 50
    
    # Track active positions to prevent overlapping trades on same symbol
    active_positions = {}  # {symbol: exit_timestamp_epoch}
    skipped_due_to_position = 0
    skipped_no_matching_rule = 0
    
    # Process in batches
    for i in range(0, len(signals), batch_size):
        batch = signals[i : i+batch_size]
        sids = [b[0] for b in batch]
        
        # Fetch bars for this batch
        bars_dict = fetch_bars_batch(conn, sids)
        
        for item in batch:
            sid, symbol, ts, score, rsi, vol_zscore, oi_delta = item
            
            # Convert timestamp to epoch for comparison
            entry_epoch = ts.timestamp() if hasattr(ts, 'timestamp') else ts
            
            # Check if position is occupied
            if symbol in active_positions:
                if entry_epoch < active_positions[symbol]:
                    skipped_due_to_position += 1
                    continue  # Skip: position still open
            
            bars = bars_dict.get(sid, [])
            
            if not bars or len(bars) < 100:
                continue
            
            # Use get_matching_rule to find strategy based on score/rsi/vol/oi
            strat, skip_reason = get_matching_rule(score, rsi, vol_zscore, oi_delta)
            if strat is None:
                # No matching rule - skip this signal
                skipped_no_matching_rule += 1
                continue
            
            res = run_simulation_detailed(bars, strat)
            
            if res:
                # Calculate exit timestamp and update active_positions
                exit_epoch = entry_epoch + res["duration"]
                active_positions[symbol] = exit_epoch
                
                # Build strategy string with activation/callback info
                act = strat.get('activation', 10.0)
                cb = strat.get('callback', 4.0)
                strat_str = f"{strat['leverage']}x/SL{strat['sl']}/A{act}/C{cb}"
                
                trades.append({
                    "Date": ts,
                    "Symbol": symbol,
                    "Score": score,
                    "RSI": round(rsi, 1),
                    "Vol": round(vol_zscore, 1),
                    "OI": round(oi_delta, 1),
                    "Strategy": strat_str,
                    "Activation": act,
                    "Callback": cb,
                    "Entry": res["entry_price"],
                    "Exit": res["exit_price"],
                    "Duration(s)": res["duration"],
                    "Reason": res["exit_reason"],
                    "PnL%": round(res["pnl"], 2)
                })
        
        # Progress indicator
        processed = min(i + batch_size, len(signals))
        print(f"   Processed {processed}/{len(signals)} signals...", end='\r')
    
    print()  # Newline after progress

    conn.close()
    
    # Sort trades by date
    trades.sort(key=lambda x: x["Date"])
    
    # -----------------------------------------------------------------------
    # SKIP STATISTICS (Detailed Diagnostics)
    # -----------------------------------------------------------------------
    print("\n" + "="*80)
    print(f"{'FILTER STATISTICS (Why signals were skipped)':^80}")
    print("="*80)
    print(f"Skipped due to occupied position: {skipped_due_to_position}")
    print(f"Skipped due to no matching rule: {skipped_no_matching_rule}")
    print(f"  - RSI too low: {len(SKIP_REASONS['rsi_too_low'])}")
    print(f"  - Vol too low: {len(SKIP_REASONS['vol_too_low'])}")
    print(f"  - OI too low: {len(SKIP_REASONS['oi_too_low'])}")
    print(f"  - Score out of range: {len(SKIP_REASONS['score_out_of_range'])}")
    print(f"Actually traded: {len(trades)}")
    print("="*80)

    # Console Output with Colors
    print("\n" + "="*140)
    print(f"{'DETAILED TRADE LOG':^140}")
    print("="*140)
    print(f"{'DATE':<20} | {'SYMBOL':<12} | {'SCORE':<5} | {'RSI':<5} | {'VOL':<5} | {'OI':<5} | {'ACT%':<5} | {'CB%':<4} | {'ENTRY':<10} | {'EXIT':<10} | {'DUR':<5} | {'PNL%':<8} | {'REASON'}")
    print("-" * 140)

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
        
        print(f"{date_str:<20} | {t['Symbol']:<12} | {t['Score']:<5} | {t['RSI']:<5} | {t['Vol']:<5} | {t['OI']:<5} | {t['Activation']:<5} | {t['Callback']:<4} | {t['Entry']:<10.5f} | {t['Exit']:<10.5f} | {t['Duration(s)']:<5} | {color}{pnl_str:<8}{RESET} | {t['Reason']}")

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
    keys = ["Date", "Symbol", "Score", "RSI", "Vol", "OI", "Strategy", "Activation", "Callback", "Entry", "Exit", "Duration(s)", "Reason", "PnL%"]
    
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
