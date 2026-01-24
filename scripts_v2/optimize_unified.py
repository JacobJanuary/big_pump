# optimize_unified.py – Unified optimizer for signal filters and strategy parameters

"""Unified optimizer that searches for the best combination of signal filter
parameters and strategy parameters.

Key features:
* Expanded SCORE_RANGE (100‑900 step 10).
* Batched DB reads via :pymod:`db_batch_utils.fetch_bars_batch`.
* Multiprocessing pool (default 12 workers) – each worker gets its own DB
  connection.
* Pruning: discard filter configs with too few signals or low win‑rate.
* Result aggregation and CSV report generation.
"""

import os
import itertools
import multiprocessing as mp
from typing import List, Dict, Tuple

from pump_analysis_lib import get_db_connection, fetch_signals
from db_batch_utils import fetch_bars_batch, batch_execute, get_connection
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration (can be overridden via env vars)
# ---------------------------------------------------------------------------
SCORE_RANGE = range(100, 901, 10)  # 100‑900 inclusive, step 10
MIN_SIGNALS_FOR_EVAL = int(os.getenv("MIN_SIGNALS_FOR_EVAL", "50"))
MIN_WIN_RATE = float(os.getenv("MIN_WIN_RATE", "0.4"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "12"))

# ---------------------------------------------------------------------------
def generate_filter_grid() -> List[Dict]:
    """Generate the full filter grid.

    Returns a list of dictionaries with thresholds for:
    * total_score (min, max)
    * rsi (min)
    * volume_zscore (min)
    * oi_delta_pct (min)
    """
    from itertools import product
    grid = []
    for score, rsi, vol, oi in product(
        SCORE_RANGE,               # total_score min
        range(0, 81, 1),          # RSI 0‑80
        range(0, 16, 1),          # Volume z‑score 0‑15
        range(0, 41, 1)           # OI delta % 0‑40
    ):
        grid.append({
            "score_min": score,
            "score_max": score + 10,
            "rsi_min": rsi,
            "vol_min": vol,
            "oi_min": oi,
        })
    return grid
# Core optimizer logic
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
def process_signal(signal_id: int, strategy_grid: List[Dict]) -> List[Tuple[int, bool, float, Dict]]:
    """Execute ALL trading strategies for a single signal.
    
    This function:
    1. Fetches bars for the signal ONCE (expensive DB op).
    2. Iterates through the strategy_grid (cheap CPU op).
    3. Returns a list of results for each strategy config.
    """
    # Import the core strategy function
    from optimize_combined_leverage import run_strategy

    # Fetch bars for this signal using batched utility (single-id batch)
    conn = get_connection()
    bars_dict = fetch_bars_batch(conn, [signal_id])
    conn.close()
    
    bars = bars_dict.get(signal_id, [])
    results = []
    
    if not bars:
        # No data – treat as loss with zero PnL for all strategies
        # Or simply return empty? Returning loss is safer for data consistency.
        for sp in strategy_grid:
            results.append((signal_id, False, 0.0, sp))
        return results

    # Run all strategies on the cached bars
    for sp in strategy_grid:
        # Map strategy_params to expected arguments
        sl_pct = sp.get("sl") or sp.get("sl_pct")
        delta_window = sp.get("window") or sp.get("delta_window")
        threshold_mult = sp.get("threshold") or sp.get("threshold_mult")
        leverage = sp.get("leverage", 1)

        pnl = run_strategy(bars, sl_pct, delta_window, threshold_mult, leverage)
        is_win = pnl > 0
        results.append((signal_id, is_win, pnl, sp))
        
    return results

def evaluate_filter(filter_cfg: Dict) -> Tuple[Dict, List[Tuple[int, bool, float, Dict]]]:
    """Evaluate a single filter configuration.

    * Load matching signal IDs.
    * Apply pruning based on ``MIN_SIGNALS_FOR_EVAL`` and ``MIN_WIN_RATE``.
    * Run strategy optimisation for the surviving signals (parallel).
    * Return the filter config and a list of strategy results.
    """
    conn = get_connection()
    # Query joining indicators table for RSI, volume_zscore, oi_delta_pct
    query = """
        SELECT sa.id, sa.is_win
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
        signal_data = cur.fetchall()
    conn.close()

    # signal_data = [(id, is_win), ...]
    signal_ids = [row[0] for row in signal_data]

    # Early pruning based on count (check BEFORE slicing for test limit)
    if len(signal_ids) < MIN_SIGNALS_FOR_EVAL:
        return filter_cfg, []

    # Limit for quick testing if requested
    test_limit = int(os.getenv("TEST_LIMIT", "0"))
    if test_limit > 0:
        signal_ids = signal_ids[:5]
    # Early win-rate pruning
    # Disabled because is_win can be NULL for new signals
    # if signal_data:
    #     win_rate = sum(1 for _, is_win in signal_data if is_win) / len(signal_data)
    #     if win_rate < MIN_WIN_RATE:
    #         return filter_cfg, []
    # else:
    #     return filter_cfg, []

    # Strategy parameters grid (based on optimize_combined_leverage.py)
    # Define ranges
    leverage_opts = [1, 5, 10]
    delta_window_opts = [10, 20, 30, 60, 120]
    threshold_opts = [1.0, 1.5, 2.0, 2.5, 3.0]
    
    # SL options depend on leverage
    sl_by_leverage = {
        1: [5, 7, 10, 15, 20],
        5: [3, 4, 5, 7, 10, 15],
        10: [2, 3, 4, 5, 7, 8],
    }

    strategy_params_grid = []
    for lev in leverage_opts:
        for sl in sl_by_leverage[lev]:
            for win in delta_window_opts:
                for thresh in threshold_opts:
                    strategy_params_grid.append({
                        "leverage": lev,
                        "sl": sl,
                        "window": win,
                        "threshold": thresh
                    })

    tasks = []
    # Each task is (signal_id, strategy_grid_list)
    # Note: passing the full list per task pickles it each time. 
    # Since the list is constant, this is acceptable for multiprocessing in Python 3.8+ (copy-on-write on fork)
    # but on spawn (Win/Mac default for 3.8+), it pickles.
    # The grid is ~375 dicts, small enough.
    for sid in signal_ids:
        tasks.append((sid, strategy_params_grid))

    # Parallel vs Sequential execution
    if MAX_WORKERS == 1:
        # Avoid multiprocessing overhead/errors for single worker
        # results will be a list of lists: [[(sid, win, pnl, params), ...], ...]
        result_batches = [process_signal(sid, grid) for sid, grid in tasks]
    else:
        # Use starmap to avoid pickling issues with local functions
        pool = mp.Pool(processes=MAX_WORKERS)
        result_batches = pool.starmap(process_signal, tasks)
        pool.close()
        pool.join()
        
    # Flatten the list of lists
    results = [item for batch in result_batches for item in batch]
    return filter_cfg, results

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
import argparse

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Unified Optimizer for Strategy Parameters")
    parser.add_argument("--workers", type=int, default=12, help="Number of parallel workers (default: 12)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of filter configurations (for testing)")
    parser.add_argument("--min-signals", type=int, default=50, help="Minimum signals required to evaluate a filter (default: 50)")
    
    args = parser.parse_args()
    
    # Update global constants based on args
    global MAX_WORKERS, MIN_SIGNALS_FOR_EVAL
    MAX_WORKERS = args.workers
    MIN_SIGNALS_FOR_EVAL = args.min_signals

    filter_grid = generate_filter_grid()
    
    # Apply limit
    if args.limit > 0:
        print(f"Limiting filter grid to first {args.limit} configurations for testing.")
        filter_grid = filter_grid[:args.limit]
        
    all_results = []
    
    # Use tqdm for progress bar if available
    try:
        from tqdm import tqdm
        iterator = tqdm(filter_grid, desc="Optimizing filters")
    except ImportError:
        iterator = filter_grid
        print(f"Starting optimization of {len(filter_grid)} configurations...")

    for filter_cfg in iterator:
        cfg, results = evaluate_filter(filter_cfg)
        if results:
            # Aggregate results by strategy parameters
            grouped_stats = {}
            for sid, is_win, pnl, sp in results:
                # Group by strategy params (hashable key)
                sp_key = tuple(sorted(sp.items()))
                if sp_key not in grouped_stats:
                    grouped_stats[sp_key] = {"pnl": 0.0, "wins": 0, "total": 0, "params": sp}
                
                stats = grouped_stats[sp_key]
                stats["pnl"] += pnl
                stats["total"] += 1
                if is_win:
                    stats["wins"] += 1

            # Find best strategy for this filter
            best_strat_pnl = -float('inf')
            best_strat_stats = None

            for sp_key, stats in grouped_stats.items():
                if stats["pnl"] > best_strat_pnl:
                    best_strat_pnl = stats["pnl"]
                    best_strat_stats = stats

            if best_strat_stats:
                result_entry = {
                    "filter": filter_cfg,
                    "strategy": best_strat_stats["params"],
                    "metrics": {
                        "total_pnl": best_strat_stats["pnl"],
                        "win_rate": best_strat_stats["wins"] / best_strat_stats["total"],
                        "total_signals": best_strat_stats["total"]
                    }
                }
                all_results.append(result_entry)
                
                # Save intermediate result immediately (append to JSONL)
                try:
                    with open("intermediate_results.jsonl", "a", encoding="utf-8") as f:
                        f.write(json.dumps(result_entry) + "\n")
                except Exception as e:
                    print(f"Warning: could not write intermediate result: {e}")

    # Sort by total PnL
    all_results.sort(key=lambda x: x["metrics"]["total_pnl"], reverse=True)
    
    # Save results
    output_path = Path(__file__).with_name("filter_strategy_optimization.json")
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2)
    
    print(f"\nAggregated results for {len(all_results)} filter configs.")
    if all_results:
        best = all_results[0]
        print(f"Best PnL: {best['metrics']['total_pnl']:.2f}")
        print(f"Best Config: Filter={best['filter']}, Strategy={best['strategy']}")
        
        # Save best config separately
        with open(Path(__file__).with_name("best_config.json"), "w") as f:
            json.dump(best, f, indent=2)
    else:
        print("Best PnL: N/A")

    print(f"Evaluated {len(filter_grid)} filter configurations.")

if __name__ == "__main__":
    main()
