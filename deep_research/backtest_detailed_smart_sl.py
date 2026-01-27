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


# ---------------------------------------------------------------------------
# SMART EXIT DETECTION FUNCTIONS (Institutional-Level Patterns)
# ---------------------------------------------------------------------------

# Smart Exit Parameters (can be moved to JSON later)
SMART_EXIT_CONFIG = {
    "enabled": True,
    "fast_fail_time": 30,           # seconds
    "fast_fail_loss": -2.0,         # % loss threshold
    "sell_dominance_mult": 1.5,     # sell/buy ratio
    "sell_dominance_bars": 3,       # consecutive bars
    "delta_velocity_window": 10,    # bars for velocity
    "delta_velocity_threshold": -50.0,  # negative delta/bar threshold
    "price_drop_window": 15,        # bars
    "price_drop_threshold": -1.5,   # % drop
    "volume_spike_mult": 2.0,       # vs avg volume
}


def detect_fast_fail(entry_price: float, price: float, 
                     entry_ts: int, ts: int,
                     time_limit: int = 30, 
                     loss_threshold: float = -2.0) -> bool:
    """
    Quick exit if losing fast in first N seconds.
    Rationale: If losing quickly, likely to keep losing.
    """
    pnl = (price - entry_price) / entry_price * 100
    elapsed = ts - entry_ts
    return elapsed < time_limit and pnl < loss_threshold


def detect_sell_pressure(bars: list, idx: int, 
                         window: int = 3, 
                         mult: float = 1.5) -> bool:
    """
    Check if large sells dominate large buys for N consecutive bars.
    Rationale: When large players are net selling, cascade follows.
    """
    if idx < window:
        return False
    for i in range(idx - window + 1, idx + 1):
        large_buy = bars[i][4]
        large_sell = bars[i][5]
        if large_sell <= large_buy * mult:  # Not enough sell pressure
            return False
    return True


def detect_delta_velocity(cumsum_delta: list, idx: int,
                          window: int = 10,
                          threshold: float = -50.0) -> bool:
    """
    Check if delta is accelerating negatively (getting more negative).
    Rationale: Accelerating sell pressure = panic/capitulation incoming.
    """
    if idx < window:
        return False
    # Delta change per bar over the window
    delta_change = cumsum_delta[idx + 1] - cumsum_delta[idx - window + 1]
    velocity = delta_change / window
    return velocity < threshold


def detect_momentum_reversal(bars: list, idx: int,
                             window: int = 15,
                             drop_pct: float = -1.5,
                             rolling_delta: float = 0) -> bool:
    """
    Check if price dropped significantly + delta is negative.
    Rationale: Price falling + negative momentum = trend confirmed bearish.
    """
    if idx < window:
        return False
    price_change = (bars[idx][1] - bars[idx - window][1]) / bars[idx - window][1] * 100
    return price_change < drop_pct and rolling_delta < 0


def detect_volume_spike_distribution(bars: list, idx: int,
                                     avg_volume: float,
                                     spike_mult: float = 2.0) -> bool:
    """
    High volume on price decline = smart money exiting.
    Rationale: Distribution phase - institutions are selling into retail buying.
    """
    if idx < 1 or avg_volume <= 0:
        return False
    
    # Calculate current bar volume (approximate from buy + sell)
    large_buy = bars[idx][4]
    large_sell = bars[idx][5]
    current_volume = large_buy + large_sell
    
    # Check for volume spike
    if current_volume < avg_volume * spike_mult:
        return False
    
    # Check if price is declining
    if idx < 5:
        return False
    price_change = (bars[idx][1] - bars[idx - 5][1]) / bars[idx - 5][1] * 100
    
    return price_change < 0 and bars[idx][2] < 0  # price down + negative delta


def run_simulation_detailed(bars, strategy_params):
    """Run a single signal simulation with SMART EXIT logic.

    SMART EXIT VERSION:
    - Checks 5 institutional-level exit signals BEFORE SL hits
    - FastFail: -2% loss within 30 seconds
    - SellPressure: Large sells dominate buys for 3+ bars
    - DeltaAccel: Delta accelerating negative
    - MomentumRev: Price dropping + negative delta
    - VolSpike: Volume spike on price decline
    
    Also includes original optimizer logic:
    - Uses INDEX-BASED rolling delta
    - Uses continue (not break) for re-entry
    - Accumulates total_pnl across multiple trades
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
    
    # Precompute avg_volume for volume spike detection
    total_volume = 0.0
    for i in range(min(100, n)):
        total_volume += bars[i][4] + bars[i][5]  # large_buy + large_sell
    avg_volume = total_volume / min(100, n) if n > 0 else 0
    
    # Smart exit statistics
    smart_exit_stats = {
        "FastFail": 0,
        "SellPressure": 0,
        "DeltaAccel": 0,
        "MomentumRev": 0,
        "VolSpike": 0,
    }

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
            
            # ===================================================================
            # SMART EXIT LOGIC (Before SL) - Only when losing
            # ===================================================================
            if SMART_EXIT_CONFIG["enabled"] and pnl_from_entry < 0:
                smart_exit_triggered = False
                smart_exit_reason = None
                
                # Check 1: Fast Fail (losing > 2% in first 30 seconds)
                if detect_fast_fail(
                    entry_price, price, first_entry_ts, ts,
                    time_limit=SMART_EXIT_CONFIG["fast_fail_time"],
                    loss_threshold=SMART_EXIT_CONFIG["fast_fail_loss"]
                ):
                    smart_exit_triggered = True
                    smart_exit_reason = "FastFail"
                
                # Check 2: Sell Pressure Dominance (large_sell > large_buy * 1.5 for 3 bars)
                elif detect_sell_pressure(
                    bars, idx,
                    window=SMART_EXIT_CONFIG["sell_dominance_bars"],
                    mult=SMART_EXIT_CONFIG["sell_dominance_mult"]
                ):
                    smart_exit_triggered = True
                    smart_exit_reason = "SellPressure"
                
                # Check 3: Delta Velocity (accelerating negative delta)
                elif detect_delta_velocity(
                    cumsum_delta, idx,
                    window=SMART_EXIT_CONFIG["delta_velocity_window"],
                    threshold=SMART_EXIT_CONFIG["delta_velocity_threshold"]
                ):
                    smart_exit_triggered = True
                    smart_exit_reason = "DeltaAccel"
                
                # Check 4: Momentum Reversal (price drop + negative delta)
                elif idx >= SMART_EXIT_CONFIG["price_drop_window"]:
                    window_start_idx = max(0, idx - delta_window)
                    rolling_delta = cumsum_delta[idx + 1] - cumsum_delta[window_start_idx]
                    if detect_momentum_reversal(
                        bars, idx,
                        window=SMART_EXIT_CONFIG["price_drop_window"],
                        drop_pct=SMART_EXIT_CONFIG["price_drop_threshold"],
                        rolling_delta=rolling_delta
                    ):
                        smart_exit_triggered = True
                        smart_exit_reason = "MomentumRev"
                
                # Check 5: Volume Spike on Down Move
                elif detect_volume_spike_distribution(
                    bars, idx, avg_volume,
                    spike_mult=SMART_EXIT_CONFIG["volume_spike_mult"]
                ):
                    smart_exit_triggered = True
                    smart_exit_reason = "VolSpike"
                
                # Execute smart exit
                if smart_exit_triggered:
                    total_pnl += (pnl_from_entry * leverage - comm_cost)
                    trade_count += 1
                    smart_exit_stats[smart_exit_reason] += 1
                    in_position = False
                    last_exit_ts = ts
                    last_exit_reason = f"Smart:{smart_exit_reason}"
                    last_exit_price = price
                    last_exit_timestamp = ts
                    continue

            # Stop-loss (fallback if smart exits didn't trigger)

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
        "smart_exit_stats": smart_exit_stats,
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
    
    # Global smart exit statistics
    global_smart_exit_stats = {
        "FastFail": 0,
        "SellPressure": 0,
        "DeltaAccel": 0,
        "MomentumRev": 0,
        "VolSpike": 0,
        "SL": 0,
        "Trailing": 0,
        "Timeout": 0,
    }
    
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
                
                dw = strat.get('window', 300)
                exit_reason = res["exit_reason"]
                
                # Track exit reason in global stats
                if exit_reason.startswith("Smart:"):
                    smart_type = exit_reason.split(":")[1]
                    if smart_type in global_smart_exit_stats:
                        global_smart_exit_stats[smart_type] += 1
                elif exit_reason == "SL":
                    global_smart_exit_stats["SL"] += 1
                elif "Trailing" in exit_reason:
                    global_smart_exit_stats["Trailing"] += 1
                elif exit_reason == "Timeout":
                    global_smart_exit_stats["Timeout"] += 1
                
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
                    "Reason": exit_reason,
                    "PnL%": round(res["pnl"], 2),
                    "TradeCount": res.get("trade_count", 1)
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
    
    # -----------------------------------------------------------------------
    # SMART EXIT STATISTICS
    # -----------------------------------------------------------------------
    print("\n" + "="*80)
    print(f"{'SMART EXIT STATISTICS':^80}")
    print("="*80)
    
    smart_exits_total = sum([
        global_smart_exit_stats["FastFail"],
        global_smart_exit_stats["SellPressure"],
        global_smart_exit_stats["DeltaAccel"],
        global_smart_exit_stats["MomentumRev"],
        global_smart_exit_stats["VolSpike"],
    ])
    traditional_exits = global_smart_exit_stats["SL"] + global_smart_exit_stats["Trailing"] + global_smart_exit_stats["Timeout"]
    
    print("SMART EXITS (Early Exit Before SL):")
    print(f"  - FastFail (quick loss):    {global_smart_exit_stats['FastFail']}")
    print(f"  - SellPressure (dominance): {global_smart_exit_stats['SellPressure']}")  
    print(f"  - DeltaAccel (momentum):    {global_smart_exit_stats['DeltaAccel']}")
    print(f"  - MomentumRev (reversal):   {global_smart_exit_stats['MomentumRev']}")
    print(f"  - VolSpike (distribution):  {global_smart_exit_stats['VolSpike']}")
    print(f"  TOTAL SMART EXITS:          {smart_exits_total}")
    print()
    print("TRADITIONAL EXITS:")
    print(f"  - Stop-Loss (SL):           {global_smart_exit_stats['SL']}")
    print(f"  - Trailing/Momentum:        {global_smart_exit_stats['Trailing']}")
    print(f"  - Timeout (end of window):  {global_smart_exit_stats['Timeout']}")
    print(f"  TOTAL TRADITIONAL:          {traditional_exits}")
    print()
    if smart_exits_total > 0 and global_smart_exit_stats["SL"] > 0:
        sl_avoided_pct = smart_exits_total / (smart_exits_total + global_smart_exit_stats["SL"]) * 100
        print(f"🎯 SL HITS AVOIDED BY SMART EXITS: ~{sl_avoided_pct:.1f}%")
    print("="*80)

    # Console Output with Colors
    print("\n" + "="*140)
    print(f"{'DETAILED TRADE LOG':^140}")
    print("="*140)
    print(f"{'DATE':<20} | {'SYMBOL':<12} | {'SCORE':<5} | {'DW':<5} | {'ACT%':<5} | {'CB%':<4} | {'ENTRY':<10} | {'EXIT':<10} | {'DUR':<5} | {'#TR':<3} | {'PNL%':<9} | {'REASON'}")
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
        
        print(f"{date_str:<20} | {t['Symbol']:<12} | {t['Score']:<5} | {t['DeltaWindow']:<5} | {t['Activation']:<5} | {t['Callback']:<4} | {t['Entry']:<10.5f} | {t['Exit']:<10.5f} | {t['Duration(s)']:<5} | {t['TradeCount']:<3} | {color}{pnl_str:<9}{RESET} | {t['Reason']}")

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

    # Save CSV - include all fields from trades dict
    csv_path = "backtest_trades.csv"
    keys = ["Date", "Symbol", "Score", "RSI", "Vol", "OI", "DeltaWindow", "Activation", "Callback", "Strategy", "Entry", "Exit", "Duration(s)", "TradeCount", "Reason", "PnL%"]
    
    with open(csv_path, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
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
