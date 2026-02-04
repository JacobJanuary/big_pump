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

from scripts_v2.db_batch_utils import fetch_bars_batch_extended
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
# Dollar-based P&L Tracking
# ---------------------------------------------------------------------------
POSITION_SIZE_USD = 100.0  # $100 per trade

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
                        # Time limits (hours -> seconds for simulation)
                        "max_reentry_seconds": s.get("max_reentry_hours", 48) * 3600,
                        "max_position_seconds": s.get("max_position_hours", 24) * 3600,
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


def run_simulation_detailed(bars, strategy_params, entry_ts=0):
    """Run simulation with re-entry support.
    
    Logic:
    1. Enter at first bar >= entry_ts (or bars[0] if entry_ts=0)
    2. Exit on SL (capped at sl_pct) or Trailing
    3. After exit, can re-enter if conditions met
    4. Returns total_pnl (sum of all trades) and trade_count
    
    IMPORTANT: Each trade's SL is capped at -sl_pct, not actual price drop!
    
    Args:
        entry_ts: Unix timestamp of entry time. Trading starts from first bar >= entry_ts.
                  If 0, assume first bar is entry (backwards compatibility).
    """
    if not bars:
        return None

    n = len(bars)
    if n < 10:
        return None

    # Find entry_idx - first bar where ts >= entry_ts
    entry_idx = 0
    if entry_ts > 0:
        for i, bar in enumerate(bars):
            if bar[0] >= entry_ts:
                entry_idx = i
                break
    
    # Skip if no trading bars after entry point
    if entry_idx >= n:
        return None

    # Strategy parameters
    sl_pct = strategy_params["sl"]
    delta_window = strategy_params["window"]
    threshold_mult = strategy_params["threshold"]
    leverage = strategy_params["leverage"]
    
    # Dynamic parameters
    base_activation = strategy_params.get("activation", BASE_ACTIVATION)
    base_callback = strategy_params.get("callback", BASE_CALLBACK)
    base_cooldown = strategy_params.get("cooldown", BASE_COOLDOWN)
    base_reentry_drop = strategy_params.get("reentry_drop", BASE_REENTRY_DROP)
    
    # Time limits (from composite_strategy.json)
    max_reentry_seconds = strategy_params.get("max_reentry_seconds", 0)
    max_position_seconds = strategy_params.get("max_position_seconds", 0)

    # Precompute cumsum arrays (include ALL bars for lookback delta calculation)
    cumsum_delta = [0.0] * (n + 1)
    cumsum_abs_delta = [0.0] * (n + 1)
    for i in range(n):
        cumsum_delta[i + 1] = cumsum_delta[i] + bars[i][2]
        cumsum_abs_delta[i + 1] = cumsum_abs_delta[i] + abs(bars[i][2])
    
    lookback = 100
    avg_delta_arr = [0.0] * n
    for i in range(n):
        lb = min(i, lookback)
        if lb > 0:
            avg_delta_arr[i] = (cumsum_abs_delta[i] - cumsum_abs_delta[i - lb]) / lb

    # State - trading starts from entry_idx (lookback bars [0:entry_idx] are for delta only)
    entry_price = bars[entry_idx][1]
    first_entry_ts = bars[entry_idx][0]
    position_entry_ts = first_entry_ts  # Track current position entry time
    max_price = entry_price
    in_position = True
    last_exit_ts = 0
    
    total_pnl = 0.0
    trade_count = 0
    last_exit_reason = "Timeout"
    last_exit_price = bars[-1][1]
    
    comm_cost = COMMISSION_PCT * 2 * leverage

    for idx in range(entry_idx, n):  # Start from entry_idx
        bar = bars[idx]
        ts = bar[0]
        price = bar[1]
        delta = bar[2]
        large_buy = bar[4]
        large_sell = bar[5]
        buy_volume = bar[6] if len(bar) > 6 else 0
        sell_volume = bar[7] if len(bar) > 7 else 0

        if in_position:
            if price > max_price:
                max_price = price
                
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100 if max_price > 0 else 0

            # Position timeout check (max_position_seconds)
            if max_position_seconds > 0 and (ts - position_entry_ts) >= max_position_seconds:
                # Check liquidation first
                liquidation_threshold = 100.0 / leverage
                if pnl_from_entry <= -liquidation_threshold:
                    total_pnl += -100.0
                else:
                    realized_pnl = max(pnl_from_entry * leverage, -100.0)
                    total_pnl += (realized_pnl - comm_cost)
                trade_count += 1
                in_position = False
                last_exit_ts = ts
                last_exit_reason = "Timeout"
                last_exit_price = price
                continue

            # LIQUIDATION CHECK: position wiped out at 100/leverage % price drop
            liquidation_threshold = 100.0 / leverage  # e.g. 10% for lev=10
            if pnl_from_entry <= -liquidation_threshold:
                total_pnl += -100.0  # Liquidated = 100% loss
                trade_count += 1
                in_position = False
                last_exit_ts = ts
                last_exit_reason = "LIQUIDATED"
                last_exit_price = price
                continue

            # Stop-loss (only triggers if not liquidated first)
            if pnl_from_entry <= -sl_pct:
                realized_pnl = max(pnl_from_entry * leverage, -100.0)  # Cap at -100% (use actual PnL, not sl_pct)
                total_pnl += (realized_pnl - comm_cost)
                trade_count += 1
                in_position = False
                last_exit_ts = ts
                last_exit_reason = "SL"
                last_exit_price = price
                continue

            # Trailing exit
            if pnl_from_entry >= base_activation and drawdown_from_max >= base_callback:
                window_start_idx = max(0, idx - delta_window)
                actual_window_size = idx - window_start_idx
                rolling_delta = cumsum_delta[idx + 1] - cumsum_delta[window_start_idx]
                
                avg_delta = avg_delta_arr[idx]
                threshold = avg_delta * threshold_mult
                
                # Proportional scaling when insufficient data (match optimization logic)
                if actual_window_size < delta_window and delta_window > 0:
                    data_ratio = actual_window_size / delta_window
                    threshold = threshold * data_ratio
                
                if not (rolling_delta > threshold) and not (rolling_delta >= 0):
                    realized_pnl = max(pnl_from_entry * leverage, -100.0)  # Cap at -100%
                    total_pnl += (realized_pnl - comm_cost)
                    trade_count += 1
                    in_position = False
                    last_exit_ts = ts
                    max_price = price
                    last_exit_reason = "Trailing"
                    last_exit_price = price
                    continue
        else:
            # Re-entry logic
            # Check max_reentry_seconds limit (0 = no limit)
            if max_reentry_seconds > 0 and (ts - first_entry_ts) > max_reentry_seconds:
                continue  # Past the reentry window, skip
            
            if ts - last_exit_ts >= base_cooldown:
                if price < max_price:
                    drop_pct = (max_price - price) / max_price * 100
                    if drop_pct >= base_reentry_drop:
                        # Re-entry condition: delta > 0 AND large_buy > large_sell (parity with optimize)
                        if delta > 0 and large_buy > large_sell:
                            in_position = True
                            entry_price = price
                            max_price = price
                            position_entry_ts = ts  # Track new position entry time
                            last_exit_ts = 0
                else:
                    max_price = price

    # Close if still in position
    if in_position:
        final_price = bars[-1][1]
        pnl_pct = (final_price - entry_price) / entry_price * 100
        # Check for liquidation during hold period
        liquidation_threshold = 100.0 / leverage
        if pnl_pct <= -liquidation_threshold:
            total_pnl += -100.0
        else:
            realized_pnl = max(pnl_pct * leverage, -100.0)  # Cap at -100%
            total_pnl += (realized_pnl - comm_cost)
        trade_count += 1
        last_exit_reason = "Timeout"
        last_exit_price = final_price
        last_exit_ts = bars[-1][0]  # Set exit time to last bar

    # Calculate average PnL per trade
    avg_pnl_per_trade = total_pnl / trade_count if trade_count > 0 else 0

    return {
        "entry_price": bars[0][1],
        "exit_price": last_exit_price,
        "duration": bars[-1][0] - first_entry_ts,  # For display only
        "last_exit_ts": last_exit_ts,  # Actual exit timestamp for position tracking
        "exit_reason": last_exit_reason,
        "pnl": total_pnl,  # Total accumulated PnL
        "avg_pnl": avg_pnl_per_trade,  # Average per trade
        "trade_count": trade_count,
    }



def get_all_signals_like_optimizer(conn) -> List[Tuple[int, str, datetime, datetime, int, float, float, float]]:
    """
    Load ALL signals using SAME logic as optimize_unified.py
    
    This uses the SAME query as preload_all_signals() in optimize_unified.py:
    - Join web.signal_analysis with fas_v2.indicators
    - score >= 100, score < 950
    - NO additional vol/oi/rsi filters (those are applied via get_matching_rule)
    
    Returns list of (signal_id, symbol, timestamp, entry_time, score, rsi, vol_zscore, oi_delta) tuples
    """
    print("   Loading signals using same logic as optimize_unified.py...")
    
    signals = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sa.id, sa.pair_symbol, sa.signal_timestamp, sa.entry_time, sa.total_score,
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
            sid, sym, ts, entry_t, score, rsi, vol, oi = row
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if entry_t.tzinfo is None:
                entry_t = entry_t.replace(tzinfo=timezone.utc)
            signals.append((
                sid, sym, ts, entry_t, score,
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
        # Optimizer's preload_all_bars has NO time limit - loads ALL bars for signal
        # To match, use max_seconds large enough to get all available data
        bars_dict = fetch_bars_batch_extended(conn, sids, max_seconds=999999)
        
        for item in batch:
            sid, symbol, ts, entry_time, score, rsi, vol_zscore, oi_delta = item
            
            # Convert entry_time to epoch for comparison and entry_idx calculation
            entry_epoch = entry_time.timestamp() if hasattr(entry_time, 'timestamp') else entry_time
            entry_ts = int(entry_epoch)
            
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
            
            res = run_simulation_detailed(bars, strat, entry_ts=entry_ts)
            
            if res:
                # Use actual exit timestamp for position tracking (not duration!)
                active_positions[symbol] = res["last_exit_ts"]
                
                # Build strategy string with activation/callback info
                act = strat.get('activation', 10.0)
                cb = strat.get('callback', 4.0)
                strat_str = f"{strat['leverage']}x/SL{strat['sl']}/A{act}/C{cb}"
                
                dw = strat.get('window', 300)
                leverage = strat.get('leverage', 10)
                
                trade_count = res.get("trade_count", 1)
                total_pnl_pct = res["pnl"]  # Total accumulated across all re-entries
                avg_pnl_pct = res.get("avg_pnl", total_pnl_pct)  # Average per trade
                
                # Dollar P&L: each trade uses $100
                # total_pnl is sum of individual trade P&L%, each applied to $100
                pnl_usd = POSITION_SIZE_USD * (total_pnl_pct / 100)
                
                # Capital used = $100 per trade
                capital_used = POSITION_SIZE_USD * trade_count
                
                trades.append({
                    "Date": ts,
                    "Symbol": symbol,
                    "Score": score,
                    "RSI": round(rsi, 1),
                    "Vol": round(vol_zscore, 1),
                    "OI": round(oi_delta, 1),
                    "Strategy": strat_str,
                    "DeltaWindow": dw,
                    "Activation": act,
                    "Callback": cb,
                    "Entry": res["entry_price"],
                    "Exit": res["exit_price"],
                    "Duration(s)": res["duration"],
                    "Reason": res["exit_reason"],
                    "PnL%": round(avg_pnl_pct, 2),  # Show AVERAGE per trade
                    "TotalPnL%": round(total_pnl_pct, 2),  # Total accumulated
                    "PnL_USD": round(pnl_usd, 2),
                    "Capital_USD": round(capital_used, 2),
                    "TradeCount": trade_count
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
    print("\n" + "="*160)
    print(f"{'DETAILED TRADE LOG (with Dollar P&L)':^160}")
    print("="*160)
    print(f"{'DATE':<20} | {'SYMBOL':<12} | {'SCORE':<5} | {'DW':<5} | {'ENTRY':<10} | {'EXIT':<10} | {'DUR':<5} | {'TRADES':<6} | {'PNL%':<9} | {'PNL_$':<10} | {'BALANCE_$':<12} | {'REASON'}")
    print("-" * 170)

    daily_pnl = {}  # "YYYY-MM-DD": pnl%
    daily_pnl_usd = {}  # "YYYY-MM-DD": $pnl
    
    total_pnl_cum = 0.0
    
    # -------------------------------------------------------------------------
    # PROPER CAPITAL TRACKING:
    # Each trade requires $100 at OPEN, returns ($100 + pnl) at CLOSE
    # We need to track concurrent open positions to find max capital needed
    # -------------------------------------------------------------------------
    
    # Build timeline of events: (timestamp, event_type, amount)
    # event_type: 'open' = -$100, 'close' = +($100 + pnl)
    events = []
    for t in trades:
        entry_ts = t["Date"].timestamp() if hasattr(t["Date"], 'timestamp') else t["Date"]
        exit_ts = entry_ts + t["Duration(s)"]
        pnl_usd = t["PnL_USD"]
        
        events.append((entry_ts, 'open', -POSITION_SIZE_USD))  # Commit $100 at open
        events.append((exit_ts, 'close', POSITION_SIZE_USD + pnl_usd))  # Get back $100 + pnl
    
    # Sort events by timestamp
    events.sort(key=lambda x: (x[0], x[1] == 'open'))  # closes before opens at same timestamp
    
    # Calculate running balance and find minimum
    sim_balance = 0.0
    min_sim_balance = 0.0
    max_concurrent_positions = 0
    current_positions = 0
    
    for ts, event_type, amount in events:
        sim_balance += amount
        if event_type == 'open':
            current_positions += 1
            max_concurrent_positions = max(max_concurrent_positions, current_positions)
        else:
            current_positions -= 1
        
        if sim_balance < min_sim_balance:
            min_sim_balance = sim_balance
    
    # Running balance for display (simplified - just show cumulative P&L)
    running_balance = 0.0
    
    # ANSI Colors
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    RESET = '\033[0m'

    for t in trades:
        date_str = str(t["Date"])[:19]
        day = str(t["Date"])[:10]
        pnl = t["PnL%"]
        pnl_usd = t["PnL_USD"]
        
        # Running balance = cumulative P&L (after trade closes, we got our $100 back + profit/loss)
        running_balance += pnl_usd
        
        # Accumulate Daily
        daily_pnl[day] = daily_pnl.get(day, 0.0) + pnl
        daily_pnl_usd[day] = daily_pnl_usd.get(day, 0.0) + pnl_usd
        total_pnl_cum += pnl
        
        # Colorize PnL
        pnl_str = f"{pnl:+.2f}%"
        pnl_usd_str = f"${pnl_usd:+.2f}"
        bal_str = f"${running_balance:+.2f}"
        
        color = GREEN if pnl > 0 else RED
        bal_color = GREEN if running_balance > 0 else RED
        
        print(f"{date_str:<20} | {t['Symbol']:<12} | {t['Score']:<5} | {t['DeltaWindow']:<5} | {t['Entry']:<10.5f} | {t['Exit']:<10.5f} | {t['Duration(s)']:<5} | {t['TradeCount']:<6} | {color}{pnl_str:<9}{RESET} | {color}{pnl_usd_str:<10}{RESET} | {bal_color}{bal_str:<12}{RESET} | {t['Reason']}")

    # -----------------------------------------------------------------------
    # 2. DAILY PERFORMANCE SUMMARY TABLE (with $ amounts)
    # -----------------------------------------------------------------------
    print("\n" + "="*100)
    print(f"{'DAILY PERFORMANCE SUMMARY (with Dollar P&L)':^100}")
    print("="*100)
    print(f"{'DATE':<12} | {'TRADES':<6} | {'WINS':<4} | {'LOSS':<4} | {'WR%':<6} | {'DAILY %':<12} | {'DAILY $':<12} | {'CUM $'}")
    print("-" * 100)
    
    # Sort dates
    sorted_days = sorted(daily_pnl.keys())
    running_total_pct = 0.0
    running_total_usd = 0.0
    
    for day in sorted_days:
        day_trades = [t for t in trades if str(t["Date"]).startswith(day)]
        # Count total re-entries (TradeCount), not just signals
        total_reentries = sum(t.get("TradeCount", 1) for t in day_trades)
        signals_count = len(day_trades)
        wins = len([t for t in day_trades if t["PnL%"] > 0])
        losses = signals_count - wins
        wr = (wins / signals_count * 100) if signals_count > 0 else 0
        
        d_pnl = daily_pnl[day]
        d_pnl_usd = daily_pnl_usd[day]
        running_total_pct += d_pnl
        running_total_usd += d_pnl_usd
        
        d_pnl_str = f"{d_pnl:+.2f}%"
        d_pnl_usd_str = f"${d_pnl_usd:+.2f}"
        cum_str = f"${running_total_usd:+.2f}"
        
        d_color = GREEN if d_pnl > 0 else RED
        c_color = GREEN if running_total_usd > 0 else RED
        
        print(f"{day:<12} | {total_reentries:<6} | {wins:<4} | {losses:<4} | {wr:5.1f}% | {d_color}{d_pnl_str:<12}{RESET} | {d_color}{d_pnl_usd_str:<12}{RESET} | {c_color}{cum_str}{RESET}")
    print("="*100)

    # Save CSV - include all fields from trades dict
    csv_path = "backtest_trades.csv"
    keys = ["Date", "Symbol", "Score", "RSI", "Vol", "OI", "DeltaWindow", "Activation", "Callback", "Strategy", "Entry", "Exit", "Duration(s)", "TradeCount", "Reason", "PnL%", "PnL_USD"]
    
    with open(csv_path, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(trades)
        
    # Summary with dollar calculations
    total_days = len(daily_pnl)
    avg_daily = sum(daily_pnl.values()) / total_days if total_days > 0 else 0
    avg_daily_usd = sum(daily_pnl_usd.values()) / total_days if total_days > 0 else 0
    wins = [t for t in trades if t["PnL%"] > 0]
    wr = len(wins) / len(trades) if trades else 0
    
    # Capital calculations (using proper timeline simulation)
    initial_capital_needed = abs(min_sim_balance) if min_sim_balance < 0 else POSITION_SIZE_USD
    # Minimum is at least $100 for the first trade
    initial_capital_needed = max(initial_capital_needed, POSITION_SIZE_USD)
    
    final_balance = running_balance
    total_pnl_usd = sum(t["PnL_USD"] for t in trades)
    
    # Best/worst day in dollars
    best_day_usd = max(daily_pnl_usd.values(), default=0)
    worst_day_usd = min(daily_pnl_usd.values(), default=0)
    
    # ROI calculation: profit relative to capital needed
    roi = (total_pnl_usd / initial_capital_needed * 100) if initial_capital_needed > 0 else 0
    
    print("\n" + "="*70)
    print(f"{'💰 PORTFOLIO SUMMARY (Dollar-Based)':^70}")
    print("="*70)
    print(f"Position Size:            ${POSITION_SIZE_USD:.0f} per trade")
    print(f"Total Trades:             {len(trades)}")
    print(f"Win Rate:                 {wr*100:.1f}%")
    print(f"Max Concurrent Positions: {max_concurrent_positions}")
    print("-" * 70)
    print(f"{'📊 P&L TRACKING':^70}")
    print("-" * 70)
    print(f"Total PnL (%):            {GREEN if total_pnl_cum > 0 else RED}{total_pnl_cum:+.2f}%{RESET}")
    print(f"Total PnL ($):            {GREEN if total_pnl_usd > 0 else RED}${total_pnl_usd:+.2f}{RESET}")
    print(f"Average Daily (%):        {avg_daily:+.2f}%")
    print(f"Average Daily ($):        ${avg_daily_usd:+.2f}")
    print("-" * 70)
    print(f"{'💵 CAPITAL REQUIREMENTS':^70}")
    print("-" * 70)
    print(f"Max Drawdown (lowest):    {RED}${min_sim_balance:.2f}{RESET}")
    print(f"Initial Capital Needed:   {YELLOW}${initial_capital_needed:.2f}{RESET}")
    print(f"Final Balance:            {GREEN if final_balance > 0 else RED}${final_balance:+.2f}{RESET}")
    print(f"ROI:                      {GREEN if roi > 0 else RED}{roi:+.1f}%{RESET}")
    print("-" * 70)
    print(f"Best Day ($):             {GREEN}${best_day_usd:+.2f}{RESET}")
    print(f"Worst Day ($):            {RED}${worst_day_usd:+.2f}{RESET}")
    print("="*70)
    print(f"Full details saved to: {csv_path}")

if __name__ == "__main__":
    main()
