# optimize_unified.py – Unified optimizer with global position tracking

"""
Unified optimizer that searches for the best combination of signal filter parameters
and strategy parameters, while ensuring that a position is not opened for the same
trading pair if a previous position is still active (global position tracking).

Features:
* Exhaustive filter grid (score, RSI, volume_zscore, oi_delta)
* Strategy grid (leverage, SL, delta_window, threshold_mult)
* Per‑pair sequential processing → global position tracking
* Parallel execution across pairs (multiprocessing Pool)
* tqdm progress bar for filter‑grid iteration
"""

import os
import itertools
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, NamedTuple
import multiprocessing as mp

# Ensure scripts_v2 is on sys.path for imports
import sys
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir.parent / "scripts_v2"))
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection
from optimize_combined_leverage_filtered import (
    run_strategy,  # returns (pnl, last_bar_ts)
    load_bars_for_signal,
)

# ---------------------------------------------------------------------------
# Configuration (can be overridden via env vars)
# ---------------------------------------------------------------------------
SCORE_RANGE = range(100, 901, 50)  # 100‑900 step 50 → 17 values
MIN_SIGNALS_FOR_EVAL = int(os.getenv("MIN_SIGNALS_FOR_EVAL", "0"))  # 0 = disabled
MIN_WIN_RATE = float(os.getenv("MIN_WIN_RATE", "0.0"))  # disabled by default
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "12"))

# ---------------------------------------------------------------------------
# Helper data structures
# ---------------------------------------------------------------------------
class SignalInfo(NamedTuple):
    """Metadata needed for global position tracking"""
    signal_id: int
    pair: str
    timestamp: datetime

# ---------------------------------------------------------------------------
# Filter grid generation
# ---------------------------------------------------------------------------
def generate_filter_grid() -> List[Dict]:
    """Create exhaustive filter combinations.

    Each dict contains the minimum thresholds for:
    * total_score (score_min, score_max)
    * rsi (rsi_min)
    * volume_zscore (vol_min)
    * oi_delta_pct (oi_min)
    """
    grid = []
    for score, rsi, vol, oi in itertools.product(
        SCORE_RANGE,               # total_score min
        range(0, 81, 5),           # RSI 0‑80 step 5
        range(0, 16, 2),           # volume_zscore 0-14 step 2
        range(0, 41, 3),           # oi_delta 0-39 step 3
    ):
        grid.append({
            "score_min": score,
            "score_max": score + 50,  # window matches step size
            "rsi_min": rsi,
            "vol_min": vol,
            "oi_min": oi,
        })
    return grid

# ---------------------------------------------------------------------------
# Strategy grid generation (same as in optimize_combined_leverage)
# ---------------------------------------------------------------------------
def generate_strategy_grid() -> List[Dict]:
    leverage_opts = [5, 10]
    delta_window_opts = [10, 30, 60, 120, 300, 600, 1800, 3600, 7200]
    threshold_opts = [1.0, 1.5, 2.0, 2.5, 3.0]
    sl_by_leverage = {
        5: [3, 4, 5, 7, 10, 15],
        10: [3, 4, 5, 7, 10, 15],
    }
    grid = []
    for lev in leverage_opts:
        for sl in sl_by_leverage[lev]:
            for win in delta_window_opts:
                for thresh in threshold_opts:
                    grid.append({
                        "leverage": lev,
                        "sl_pct": sl,
                        "delta_window": win,
                        "threshold_mult": thresh,
                    })
    return grid

# ---------------------------------------------------------------------------
# Fetch signals that satisfy a filter configuration
# ---------------------------------------------------------------------------
def fetch_filtered_signals(filter_cfg: Dict) -> List[SignalInfo]:
    """Return a list of SignalInfo objects that satisfy the filter configuration.

    Uses web.signal_analysis directly (same as scripts_v2/optimize_unified.py)
    joined with fas_v2.indicators for filter thresholds.
    """
    try:
        with get_db_connection() as conn:
            query = """
                SELECT sa.id, sa.pair_symbol, sa.signal_timestamp
                FROM web.signal_analysis AS sa
                JOIN fas_v2.indicators AS i ON (
                    i.trading_pair_id = sa.trading_pair_id 
                    AND i.timestamp = sa.signal_timestamp
                    AND i.timeframe = '15m'
                )
                WHERE sa.total_score >= %(score_min)s
                  AND sa.total_score < %(score_max)s
                  AND i.rsi >= %(rsi_min)s
                  AND i.volume_zscore >= %(vol_min)s
                  AND i.oi_delta_pct >= %(oi_min)s
            """
            params = {
                "score_min": filter_cfg["score_min"],
                "score_max": filter_cfg["score_max"],
                "rsi_min": filter_cfg["rsi_min"],
                "vol_min": filter_cfg["vol_min"],
                "oi_min": filter_cfg["oi_min"],
            }
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            matched: List[SignalInfo] = []
            for signal_id, sym, ts in rows:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                matched.append(SignalInfo(signal_id=signal_id, pair=sym, timestamp=ts))
            return matched
    except Exception as e:
        print(f"Error fetching filtered signals: {e}")
        return []

# ---------------------------------------------------------------------------
# Per‑pair sequential processing with global position tracking
# ---------------------------------------------------------------------------
def process_pair(pair: str, signals: List[SignalInfo], strategy_grid: List[Dict]) -> Dict[int, float]:
    """Process all signals for a single pair sequentially.

    Returns a dict `strategy_id -> aggregated_pnl`.
    """
    aggregated: Dict[int, float] = {i: 0.0 for i in range(len(strategy_grid))}
    position_tracker_ts = 0  # timestamp of last exit for this pair
    for info in sorted(signals, key=lambda x: x.timestamp):
        # Skip if a position is still open (timestamp earlier than last exit)
        signal_ts = int(info.timestamp.timestamp())
        if signal_ts < position_tracker_ts:
            continue
        bars = load_bars_for_signal(info.signal_id)
        if len(bars) < 100:
            continue
        # Track the latest exit timestamp across all strategies for this signal
        max_last_ts = position_tracker_ts
        for sp_idx, sp in enumerate(strategy_grid):
            pnl, last_ts = run_strategy(
                bars,
                sp["sl_pct"],
                sp["delta_window"],
                sp["threshold_mult"],
                sp["leverage"],
            )
            aggregated[sp_idx] += pnl
            if last_ts > max_last_ts:
                max_last_ts = last_ts
        # Update the global tracker after evaluating all strategies for this signal
        position_tracker_ts = max_last_ts
    return aggregated

# ---------------------------------------------------------------------------
# Evaluate a single filter configuration
# ---------------------------------------------------------------------------
def evaluate_filter(filter_cfg: Dict, strategy_grid: List[Dict]) -> Tuple[Dict, Dict[int, float]]:
    """Run optimisation for one filter config.

    Returns the filter config and a dict `strategy_id -> total_pnl` aggregated over
    all pairs.
    """
    signals = fetch_filtered_signals(filter_cfg)
    if len(signals) < MIN_SIGNALS_FOR_EVAL:
        return filter_cfg, {}
    # Group by pair
    by_pair: Dict[str, List[SignalInfo]] = {}
    for s in signals:
        by_pair.setdefault(s.pair, []).append(s)
    # Parallel processing per pair
    aggregated: Dict[int, float] = {i: 0.0 for i in range(len(strategy_grid))}
    if MAX_WORKERS == 1:
        for pair, pair_signals in by_pair.items():
            pair_agg = process_pair(pair, pair_signals, strategy_grid)
            for sid, val in pair_agg.items():
                aggregated[sid] += val
    else:
        with mp.Pool(processes=MAX_WORKERS) as pool:
            tasks = [(pair, sigs, strategy_grid) for pair, sigs in by_pair.items()]
            results = pool.starmap(process_pair, tasks)
            for pair_agg in results:
                for sid, val in pair_agg.items():
                    aggregated[sid] += val
    return filter_cfg, aggregated

# ---------------------------------------------------------------------------
# Main optimisation loop
# ---------------------------------------------------------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Unified optimizer with global position tracking")
    parser.add_argument("--workers", type=int, default=12, help="Number of parallel workers (default 12)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of filter configs (for testing)")
    parser.add_argument("--min-signals", type=int, default=50, help="Minimum signals required per filter (default 50)")
    args = parser.parse_args()

    global MAX_WORKERS, MIN_SIGNALS_FOR_EVAL
    MAX_WORKERS = args.workers
    MIN_SIGNALS_FOR_EVAL = args.min_signals

    filter_grid = generate_filter_grid()
    if args.limit > 0:
        filter_grid = filter_grid[:args.limit]
        print(f"Limiting filter grid to first {args.limit} configs for testing.")

    strategy_grid = generate_strategy_grid()

    all_results = []
    try:
        from tqdm import tqdm
        iterator = tqdm(filter_grid, desc="Optimizing filters")
    except ImportError:
        iterator = filter_grid
        print(f"Optimizing {len(filter_grid)} filter configurations...")

    for filter_cfg in iterator:
        cfg, agg = evaluate_filter(filter_cfg, strategy_grid)
        if not agg:
            continue
        # Find best strategy for this filter
        best_sid = max(agg, key=lambda k: agg[k])
        best_params = strategy_grid[best_sid]
        result_entry = {
            "filter": cfg,
            "strategy": best_params,
            "metrics": {
                "total_pnl": agg[best_sid],
                "strategy_id": best_sid,
            },
        }
        all_results.append(result_entry)
        # Write intermediate result (append mode) – safe for long runs
        try:
            with open("intermediate_results.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(result_entry) + "\n")
        except Exception as e:
            print(f"Warning: could not write intermediate result: {e}")

    # Sort final results by total PnL descending
    all_results.sort(key=lambda x: x["metrics"]["total_pnl"], reverse=True)

    output_path = Path(__file__).with_name("filter_strategy_optimization.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nSaved aggregated results to {output_path}")
    if all_results:
        best = all_results[0]
        print("\nBest configuration:")
        print(f"Filter: {best['filter']}")
        print(f"Strategy: {best['strategy']}")
        print(f"Total PnL: {best['metrics']['total_pnl']:.2f}%")
    else:
        print("No viable configurations found.")

if __name__ == "__main__":
    main()
