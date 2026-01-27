#!/usr/bin/env python3
"""
Timeout Optimization Script

Iterates through timeouts from 2 hours to 21 hours (step 1h).
Fetches 21 hours of data once, then slices for each simulation.
Outputs comparison table and full details for the best timeout.
"""
import sys
import json
import csv
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime, timezone

# Add parent directory to path
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
    COMMISSION_PCT,
    BASE_ACTIVATION,
    BASE_CALLBACK,
    BASE_REENTRY_DROP,
    BASE_COOLDOWN,
)

# Constants
POSITION_SIZE_USD = 100.0

# ---------------------------------------------------------------------------
# STRATEGY LOADING
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
                        "activation": s.get("base_activation", BASE_ACTIVATION),
                        "callback": s.get("base_callback", BASE_CALLBACK),
                        "cooldown": s.get("base_cooldown", BASE_COOLDOWN),
                        "reentry_drop": s.get("base_reentry_drop", BASE_REENTRY_DROP),
                    }
                })
            return rules
    return []

RULES = load_rules_from_json()

def get_matching_rule(score, rsi, vol, oi):
    """Find the correct rule for a signal"""
    for rule in RULES:
        s_min, s_max = rule["range"]
        f = rule["filters"]
        if s_min <= score < s_max:
            if rsi < f["rsi"]: return None, "rsi"
            if vol < f["vol"]: return None, "vol"
            if oi < f["oi"]: return None, "oi"
            return rule["strategy"], None
    return None, "score"

# ---------------------------------------------------------------------------
# SIMULATION ENGINE (1-signal = 1-trade logic + re-entry)
# ---------------------------------------------------------------------------
def run_simulation(bars, strategy_params):
    """Run simulation with re-entry support."""
    if not bars: return None
    n = len(bars)
    if n < 10: return None

    # Params
    sl_pct = strategy_params["sl"]
    delta_window = strategy_params["window"]
    threshold_mult = strategy_params["threshold"]
    leverage = strategy_params["leverage"]
    
    base_activation = strategy_params.get("activation", BASE_ACTIVATION)
    base_callback = strategy_params.get("callback", BASE_CALLBACK)
    base_cooldown = strategy_params.get("cooldown", BASE_COOLDOWN)
    base_reentry_drop = strategy_params.get("reentry_drop", BASE_REENTRY_DROP)

    # Precompute
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

    # State
    entry_price = bars[0][1]
    first_entry_ts = bars[0][0]
    max_price = entry_price
    in_position = True
    last_exit_ts = 0
    
    total_pnl = 0.0
    trade_count = 0
    last_exit_reason = "Timeout"
    last_exit_price = bars[-1][1]
    
    comm_cost = COMMISSION_PCT * 2 * leverage

    for idx in range(n):
        bar = bars[idx]
        ts = bar[0]
        price = bar[1]
        delta = bar[2]
        large_buy = bar[4]
        large_sell = bar[5]

        if in_position:
            if price > max_price: max_price = price
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100 if max_price > 0 else 0

            # SL
            if pnl_from_entry <= -sl_pct:
                total_pnl += (-sl_pct * leverage - comm_cost)
                trade_count += 1
                in_position = False
                last_exit_ts = ts
                last_exit_reason = "SL"
                last_exit_price = price
                continue

            # Trailing
            if pnl_from_entry >= base_activation and drawdown_from_max >= base_callback:
                window_start_idx = max(0, idx - delta_window)
                rolling_delta = cumsum_delta[idx + 1] - cumsum_delta[window_start_idx]
                avg_delta = avg_delta_arr[idx]
                threshold = avg_delta * threshold_mult
                
                if rolling_delta < threshold and rolling_delta < 0:
                    total_pnl += (pnl_from_entry * leverage - comm_cost)
                    trade_count += 1
                    in_position = False
                    last_exit_ts = ts
                    max_price = price
                    last_exit_reason = "Trailing"
                    last_exit_price = price
                    continue
        else:
            # Re-entry
            if ts - last_exit_ts >= base_cooldown:
                if price < max_price:
                    drop_pct = (max_price - price) / max_price * 100
                    if drop_pct >= base_reentry_drop:
                        if delta > 0 and large_buy > large_sell:
                            in_position = True
                            entry_price = price
                            max_price = price
                            last_exit_ts = 0
                else:
                    max_price = price

    # Close at end (Timeout)
    if in_position:
        final_price = bars[-1][1]
        pnl_pct = (final_price - entry_price) / entry_price * 100
        if pnl_pct < -sl_pct: pnl_pct = -sl_pct
        total_pnl += (pnl_pct * leverage - comm_cost)
        trade_count += 1
        last_exit_reason = "Timeout"
        last_exit_price = final_price

    avg_pnl = total_pnl / trade_count if trade_count > 0 else 0
    return {
        "entry_price": bars[0][1],
        "exit_price": last_exit_price,
        "duration": bars[-1][0] - first_entry_ts,
        "exit_reason": last_exit_reason,
        "pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "trade_count": trade_count,
    }

def get_signals_for_backtest(conn) -> List[Tuple[int, str, datetime, int, float, float, float]]:
    """
    Load signals using SAME logic as optimize_unified.py / backtest_detailed.py
    Returns: list of (signal_id, symbol, timestamp, score, rsi, vol_zscore, oi_delta) tuples
    """
    print("Loading signals from DB...")
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
            WHERE sa.total_score >= 100 
              AND sa.total_score < 950
              AND sa.signal_timestamp > NOW() - INTERVAL '30 days'
            ORDER BY sa.signal_timestamp ASC
        """)
        rows = cur.fetchall()
    return rows

def fetch_bars_batch_extended(conn, signal_ids: List[int], max_seconds: int = 75600):
    """Fetch bars with configurable time window (default 21 hours).
    INLINED to avoid dependency on scripts_v2 updates.
    """
    if not signal_ids:
        return {}

    result: Dict[int, List[Tuple[int, float, float, float, int, int]]] = {}
    chunk_size = 5
    
    print(f"Fetching bars for {len(signal_ids)} signals in batches of {chunk_size}...")
    
    sql = """
        SELECT t.signal_analysis_id, t.second_ts, t.close_price, t.delta,
               t.large_buy_count, t.large_sell_count
        FROM web.agg_trades_1s t
        JOIN web.signal_analysis s ON s.id = t.signal_analysis_id
        WHERE t.signal_analysis_id = ANY(%s)
          AND t.second_ts >= EXTRACT(EPOCH FROM s.entry_time)::bigint
          AND t.second_ts <= (EXTRACT(EPOCH FROM s.entry_time)::bigint + %s)
        ORDER BY t.signal_analysis_id, t.second_ts
    """
    
    with conn.cursor() as cur:
        for i in range(0, len(signal_ids), chunk_size):
            chunk = signal_ids[i : i + chunk_size]
            print(f"   Fetching chunk {i+1}-{min(i+chunk_size, len(signal_ids))}...", end='\r')
            
            cur.execute(sql, (chunk, max_seconds))
            rows = cur.fetchall()
            
            for sid, ts, price, delta, buy_cnt, sell_cnt in rows:
                result.setdefault(sid, []).append((ts, float(price), float(delta), 0.0, int(buy_cnt), int(sell_cnt)))
                
    print(f"\n   Data fetch complete. {len(result)} signals have data.")
    return result

# ---------------------------------------------------------------------------
# MAIN OPTIMIZATION LOOP
# ---------------------------------------------------------------------------
def main():
    print("="*60)
    print("TIMEOUT OPTIMIZATION (2h - 21h)")
    print("="*60)
    
    conn = get_db_connection()
    
    # 1. Fetch Signals
    print("Fetching signals...")
    signals = get_signals_for_backtest(conn)
    print(f"Found {len(signals)} signals matching basic filters.")
    
    if not signals:
        return

    # 2. Filter valid signals (Score + Indicators)
    valid_signals = []
    for sig in signals:
        sid, sym, entry_time, score, vol_z, rsi, oi_delta = sig
        rule, reason = get_matching_rule(score, rsi, vol_z, oi_delta)
        if rule:
            valid_signals.append((sig, rule))
    
    print(f"Signals after strategy filtering: {len(valid_signals)}")
    
    # 3. Fetch ALL bars (Max 21h = 75600s)
    print("Fetching 21h of 1s bars for all signals (this may take a moment)...")
    sids = [s[0][0] for s in valid_signals]
    MAX_TIMEOUT_SECONDS = 21 * 3600  # 75600
    bars_cache = fetch_bars_batch_extended(conn, sids, max_seconds=MAX_TIMEOUT_SECONDS)
    conn.close()
    
    print(f"Fetch complete. Got bars for {len(bars_cache)} signals.")
    
    # 4. Optimization Loop
    results = []
    
    print("\n" + "-"*85)
    print(f"{'TIMEOUT':<8} | {'TRADES':<6} | {'WIN RATE':<8} | {'TOTAL PNL $':<12} | {'ROI':<8} | {'DRAWDOWN':<12}")
    print("-"*85)
    
    best_pnl = -float('inf')
    best_result = None
    
    # Loop 2h to 21h
    for hours in range(2, 22):
        timeout_sec = hours * 3600
        
        current_trades = []
        
        for sig_tuple, strat in valid_signals:
            sid, sym, entry_time, score, vol_z, rsi, oi_delta = sig_tuple
            
            full_bars = bars_cache.get(sid, [])
            if not full_bars: continue
            
            # SLICE BARS according to timeout
            entry_ts = full_bars[0][0]
            cutoff = entry_ts + timeout_sec
            
            # Binary search or specific slice? Linear is fine for simplicity here
            # Optimized slice: find index where ts > cutoff
            # Since bars are sorted by time, we can just iterate or bisect
            # For simplicity and correctness:
            sliced_bars = []
            for b in full_bars:
                if b[0] <= cutoff:
                    sliced_bars.append(b)
                else:
                    break
            
            if not sliced_bars: continue
            
            res = run_simulation(sliced_bars, strat)
            if res:
                trade_count = res["trade_count"]
                total_pnl = res["pnl"]
                pnl_usd = POSITION_SIZE_USD * (total_pnl / 100)  # Total PnL across all re-entries
                
                current_trades.append({
                    "PnL_USD": pnl_usd,
                    "PnLpct": total_pnl,
                    "Date": entry_ts,
                    "Duration": res["duration"],
                    "ExitReason": res["exit_reason"],
                    "TradeCount": trade_count
                })

        # Calculate metrics for this timeout
        total_pnl_usd = sum(t["PnL_USD"] for t in current_trades)
        wins = len([t for t in current_trades if t["PnLpct"] > 0])
        count = len(current_trades)
        wr = (wins / count * 100) if count > 0 else 0
        
        # Calculate Drawdown / Capital Needed
        # Reconstruct timeline
        events = []
        for t in current_trades:
            ts = t["Date"]
            dur = t["Duration"]
            pnl = t["PnL_USD"]
            # Open: commit $100
            events.append((ts, 'open', -POSITION_SIZE_USD))
            # Close: return $100 + pnl
            events.append((ts + dur, 'close', POSITION_SIZE_USD + pnl))
        
        events.sort(key=lambda x: (x[0], x[1] == 'open'))
        
        balance = 0.0
        min_balance = 0.0
        for _, etype, flow in events:
            balance += flow
            if balance < min_balance: min_balance = balance
            
        initial_capital = max(abs(min_balance), POSITION_SIZE_USD) if count > 0 else 0
        roi = (total_pnl_usd / initial_capital * 100) if initial_capital > 0 else 0
        
        # Color output
        GREEN = '\033[92m'
        RED = '\033[91m'
        RESET = '\033[0m'
        pnl_color = GREEN if total_pnl_usd > 0 else RED
        
        print(f"{hours}h      | {count:<6} | {wr:5.1f}%   | {pnl_color}${total_pnl_usd:<11.2f}{RESET} | {roi:5.1f}%   | ${min_balance:.2f}")
        
        # Store result
        res_obj = {
            "hours": hours,
            "trades": current_trades,
            "pnl": total_pnl_usd,
            "roi": roi,
            "min_balance": min_balance,
            "capital": initial_capital,
            "wr": wr,
            "count": count
        }
        results.append(res_obj)
        
        if total_pnl_usd > best_pnl:
            best_pnl = total_pnl_usd
            best_result = res_obj
            
    # 5. Report Best
    if best_result:
        best_hours = best_result['hours']
        print("\n" + "="*60)
        print(f"🏆 BEST TIMEOUT: {best_hours} HOURS")
        print("="*60)
        
        # Rerun detailed simulation for best timeout to get full logs
        print(f"Generating full detailed report for {best_hours}h timeout...")
        run_detailed_report(best_hours, bars_cache, valid_signals)

def run_detailed_report(timeout_hours, bars_cache, valid_signals):
    timeout_sec = timeout_hours * 3600
    
    trades = []
    daily_pnl = {}
    daily_pnl_usd = {}
    total_pnl_cum = 0.0
    running_balance = 0.0
    min_balance = 0.0
    
    # Sim loop
    for sig_tuple, strat in valid_signals:
        sid, sym, entry_time, score, vol_z, rsi, oi_delta = sig_tuple
        full_bars = bars_cache.get(sid, [])
        if not full_bars: continue
        
        entry_ts = full_bars[0][0]
        cutoff = entry_ts + timeout_sec
        sliced_bars = [b for b in full_bars if b[0] <= cutoff]
        if not sliced_bars: continue
        
        res = run_simulation(sliced_bars, strat)
        if res:
            # Stats
            trade_count = res["trade_count"]
            total_pnl_pct = res["pnl"]
            avg_pnl_pct = res["avg_pnl"]
            pnl_usd = POSITION_SIZE_USD * (total_pnl_pct / 100)
            capital_used = POSITION_SIZE_USD * trade_count
            
            # Formate strategy string
            act = strat.get('activation', 10.0)
            cb = strat.get('callback', 4.0)
            strat_str = f"{strat['leverage']}x/SL{strat['sl']}/A{act}/C{cb}"
            
            trades.append({
                "Date": entry_ts,
                "Symbol": sym,
                "Score": score,
                "Entry": res["entry_price"],
                "Exit": res["exit_price"],
                "Duration": res["duration"],
                "Reason": res["exit_reason"],
                "PnL%": avg_pnl_pct,
                "TotalPnL%": total_pnl_pct,
                "PnL_USD": pnl_usd,
                "Capital_USD": capital_used,
                "TradeCount": trade_count,
                 # Extra for display matches
                "Strat": strat_str,
                "DW": strat.get('window', 300)
            })

    trades.sort(key=lambda x: x["Date"])

    # PRINT DETAILED LOG
    print("\n" + "="*160)
    print(f"{'DETAILED TRADE LOG (Best Timeout: ' + str(timeout_hours) + 'h)':^160}")
    print("="*160)
    print(f"{'DATE':<20} | {'SYMBOL':<10} | {'SCORE':<5} | {'ENTRY':<10} | {'EXIT':<10} | {'DUR':<5} | {'PNL%':<9} | {'PNL_$':<10} | {'BALANCE_$':<12} | {'REASON'}")
    print("-" * 160)
    
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    RESET = '\033[0m'
    
    # Timeline for max drawdown
    timeline_events = []
    
    for t in trades:
        # Balance tracking
        running_balance += t["PnL_USD"]
        if running_balance < min_balance: min_balance = running_balance
        
        # Daily
        date_str = datetime.fromtimestamp(t["Date"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        day = date_str[:10]
        
        daily_pnl[day] = daily_pnl.get(day, 0.0) + t["TotalPnL%"]
        daily_pnl_usd[day] = daily_pnl_usd.get(day, 0.0) + t["PnL_USD"]
        total_pnl_cum += t["TotalPnL%"]
        
        # Colors
        pnl_val = t["PnL_USD"]
        color = GREEN if pnl_val > 0 else RED
        bal_color = GREEN if running_balance > 0 else RED
        
        print(f"{date_str:<20} | {t['Symbol']:<10} | {t['Score']:<5} | {t['Entry']:<10.5f} | {t['Exit']:<10.5f} | {t['Duration']:<5} | {color}{t['PnL%']:<9.2f}{RESET} | {color}{pnl_val:<10.2f}{RESET} | {bal_color}${running_balance:<11.2f}{RESET} | {t['Reason']}")
        
        # Timeline events
        timeline_events.append((t["Date"], 'open', -POSITION_SIZE_USD))
        timeline_events.append((t["Date"] + t["Duration"], 'close', POSITION_SIZE_USD + t["PnL_USD"]))

    # Daily Summary
    print("\n" + "="*100)
    print(f"{'DAILY PERFORMANCE SUMMARY':^100}")
    print("="*100)
    print(f"{'DATE':<12} | {'TRADES':<6} | {'WINS':<4} | {'LOSS':<4} | {'WR%':<6} | {'DAILY $':<12} | {'CUM $'}")
    print("-" * 100)
    
    sorted_days = sorted(daily_pnl.keys())
    running_total_usd = 0.0
    
    for day in sorted_days:
        # Find trades for this day
        # Note: simplistic string match, fine for report
        day_trades = [tr for tr in trades if datetime.fromtimestamp(tr["Date"], tz=timezone.utc).strftime('%Y-%m-%d') == day]
        count = len(day_trades)
        wins = len([tr for tr in day_trades if tr["PnL_USD"] > 0])
        losses = count - wins
        wr = (wins/count*100) if count else 0
        
        d_usd = daily_pnl_usd[day]
        running_total_usd += d_usd
        
        d_color = GREEN if d_usd > 0 else RED
        c_color = GREEN if running_total_usd > 0 else RED
        
        print(f"{day:<12} | {count:<6} | {wins:<4} | {losses:<4} | {wr:5.1f}% | {d_color}${d_usd:<11.2f}{RESET} | {c_color}${running_total_usd:.2f}{RESET}")

    # Correct Capital Calculation
    timeline_events.sort(key=lambda x: (x[0], x[1] == 'open'))
    sim_balance = 0.0
    abs_min_balance = 0.0
    for _, _, amount in timeline_events:
        sim_balance += amount
        if sim_balance < abs_min_balance: abs_min_balance = sim_balance
        
    initial_capital = max(abs(abs_min_balance), POSITION_SIZE_USD)
    total_pnl_usd = sum(t["PnL_USD"] for t in trades)
    roi = (total_pnl_usd / initial_capital * 100) if initial_capital > 0 else 0
    final_balance_val = running_balance

    print("\n" + "="*70)
    print(f"{'💰 PORTFOLIO SUMMARY (Dollar-Based) : Timeout {timeout_hours}h':^70}")
    print("="*70)
    print(f"Max Drawdown:             {RED}${abs_min_balance:.2f}{RESET}")
    print(f"Initial Capital Needed:   {YELLOW}${initial_capital:.2f}{RESET}")
    print(f"Final Balance:            {GREEN if final_balance_val > 0 else RED}${final_balance_val:.2f}{RESET}")
    print(f"ROI:                      {GREEN if roi > 0 else RED}{roi:+.1f}%{RESET}")
    print("="*70)

if __name__ == "__main__":
    main()
