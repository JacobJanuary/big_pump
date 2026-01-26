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

# Ensure deep_research is FIRST in sys.path for imports (to use optimized run_strategy)
import sys
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))  # deep_research FIRST
sys.path.append(str(current_dir.parent / "scripts_v2"))

from pump_analysis_lib import get_db_connection
from optimize_combined_leverage_filtered import (
    precompute_bars,      # Precompute cumsum arrays ONCE per signal
    run_strategy_fast,    # Use precomputed data for 540x faster execution
    run_strategy,         # Legacy wrapper (for compatibility)
)

# ---------------------------------------------------------------------------
# Configuration (can be overridden via env vars)
# ---------------------------------------------------------------------------
SCORE_RANGE = range(100, 901, 50)  # 100‑900 step 50 → 17 values
MIN_SIGNALS_FOR_EVAL = int(os.getenv("MIN_SIGNALS_FOR_EVAL", "0"))  # 0 = disabled
MIN_WIN_RATE = float(os.getenv("MIN_WIN_RATE", "0.0"))  # disabled by default
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))  # reduced to prevent DB connection exhaustion

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
        SCORE_RANGE,               # total_score min (100-900 step 50)
        range(0, 81, 5),           # RSI 0-80 step 5
        range(0, 16, 1),           # volume_zscore 0-15 step 1
        range(0, 41, 1),           # oi_delta 0-40 step 1
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
# PRELOAD ALL DATA ONCE (eliminates SQL in optimization loop)
# ---------------------------------------------------------------------------
class SignalData(NamedTuple):
    """Extended signal info with indicator values for in-memory filtering"""
    signal_id: int
    pair: str
    timestamp: datetime
    score: int
    rsi: float
    vol_zscore: float
    oi_delta: float

def preload_all_signals() -> List[SignalData]:
    """Load ALL signals with their indicators in ONE query.
    
    Returns list of SignalData for in-memory filtering.
    """
    print("[PRELOAD] Loading all signals from database...")
    signals = []
    try:
        with get_db_connection() as conn:
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
                    signals.append(SignalData(
                        signal_id=sid, pair=sym, timestamp=ts,
                        score=score, rsi=rsi or 0, vol_zscore=vol or 0, oi_delta=oi or 0
                    ))
        print(f"[PRELOAD] Loaded {len(signals)} signals")
    except Exception as e:
        print(f"Error preloading signals: {e}")
    return signals

def preload_all_bars(signal_ids: List[int]) -> Dict[int, List[tuple]]:
    """Load ALL bars for all signals in chunks.
    
    Returns dict: {signal_id: [(ts, price, delta, 0, buy, sell), ...]}
    """
    print(f"[PRELOAD] Loading bars for {len(signal_ids)} signals...")
    bars_map: Dict[int, List[tuple]] = {}
    chunk_size = 10  # Smaller chunks for better progress visibility
    total_chunks = (len(signal_ids) + chunk_size - 1) // chunk_size
    
    for chunk_idx, i in enumerate(range(0, len(signal_ids), chunk_size)):
        chunk = signal_ids[i:i + chunk_size]
        print(f"[PRELOAD] Loading chunk {chunk_idx + 1}/{total_chunks} (signals {i+1}-{min(i+chunk_size, len(signal_ids))})...", end=" ", flush=True)
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT signal_analysis_id, second_ts, close_price, delta,
                               large_buy_count, large_sell_count
                        FROM web.agg_trades_1s
                        WHERE signal_analysis_id = ANY(%s)
                        ORDER BY signal_analysis_id, second_ts
                    """, (chunk,))
                    rows = cur.fetchall()
                    for row in rows:
                        sid, ts, price, delta, buy, sell = row
                        bars_map.setdefault(sid, []).append(
                            (ts, float(price), float(delta), 0.0, buy, sell)
                        )
                    print(f"{len(rows):,} rows", flush=True)
        except Exception as e:
            print(f"ERROR: {e}")
    
    print(f"[PRELOAD] Bars loaded for {len(bars_map)} signals")
    return bars_map

# ---------------------------------------------------------------------------
# Filter signals in memory (no SQL)
# ---------------------------------------------------------------------------
def filter_signals_in_memory(all_signals: List[SignalData], filter_cfg: Dict) -> List[SignalInfo]:
    """Filter preloaded signals by filter config - pure Python, no SQL."""
    matched = []
    score_min = filter_cfg["score_min"]
    score_max = filter_cfg["score_max"]
    rsi_min = filter_cfg["rsi_min"]
    vol_min = filter_cfg["vol_min"]
    oi_min = filter_cfg["oi_min"]
    
    for s in all_signals:
        if (score_min <= s.score < score_max and
            s.rsi >= rsi_min and
            s.vol_zscore >= vol_min and
            s.oi_delta >= oi_min):
            matched.append(SignalInfo(signal_id=s.signal_id, pair=s.pair, timestamp=s.timestamp))
    return matched

# ---------------------------------------------------------------------------
# Legacy fetch functions (kept for reference but not used in main loop)
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
# Batch load bars for multiple signals in ONE query (with chunking)
# ---------------------------------------------------------------------------
MAX_SIGNALS_PER_BATCH = 5  # Limit to prevent huge queries

def fetch_bars_batch(signal_ids: List[int]) -> Dict[int, List[tuple]]:
    """Load all bars for multiple signals, chunked to prevent memory issues.
    
    Returns dict: {signal_id: [(second_ts, close_price, delta, 0.0, large_buy, large_sell), ...]}
    """
    if not signal_ids:
        return {}
    
    bars_map: Dict[int, List[tuple]] = {}
    
    # Process in chunks to avoid huge queries
    for i in range(0, len(signal_ids), MAX_SIGNALS_PER_BATCH):
        chunk = signal_ids[i:i + MAX_SIGNALS_PER_BATCH]
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT signal_analysis_id, second_ts, close_price, delta,
                               large_buy_count, large_sell_count
                        FROM web.agg_trades_1s
                        WHERE signal_analysis_id = ANY(%s)
                        ORDER BY signal_analysis_id, second_ts
                        """,
                        (chunk,)
                    )
                    for row in cur.fetchall():
                        sid, ts, price, delta, buy, sell = row
                        bars_map.setdefault(sid, []).append(
                            (ts, float(price), float(delta), 0.0, buy, sell)
                        )
        except Exception as e:
            print(f"Error batch loading bars (chunk {i}): {e}")
    return bars_map

# ---------------------------------------------------------------------------
# Per‑pair sequential processing with global position tracking
# ---------------------------------------------------------------------------
def process_pair(pair: str, signals: List[SignalInfo], strategy_grid: List[Dict],
                 bars_cache: Dict[int, List[tuple]]) -> Dict[int, float]:
    """Process all signals for a single pair sequentially.

    Args:
        bars_cache: Pre-loaded bars {signal_id: List[tuple]}
    
    Returns a dict `strategy_id -> aggregated_pnl`.
    
    OPTIMIZED: Uses precompute_bars() ONCE per signal, then run_strategy_fast() for each strategy.
    This avoids recalculating cumsum 540 times for the same bars.
    """
    aggregated: Dict[int, float] = {i: 0.0 for i in range(len(strategy_grid))}
    position_tracker_ts = 0  # timestamp of last exit for this pair
    
    for info in sorted(signals, key=lambda x: x.timestamp):
        # Skip if a position is still open (timestamp earlier than last exit)
        signal_ts = int(info.timestamp.timestamp())
        if signal_ts < position_tracker_ts:
            continue
        bars = bars_cache.get(info.signal_id, [])
        if len(bars) < 100:
            continue
        
        # OPTIMIZATION: Precompute cumsum arrays ONCE for this signal
        precomputed = precompute_bars(bars)
        if precomputed is None:
            continue
        
        # Track the latest exit timestamp across all strategies for this signal
        max_last_ts = position_tracker_ts
        for sp_idx, sp in enumerate(strategy_grid):
            pnl, last_ts = run_strategy_fast(
                precomputed,
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
# Evaluate a single filter configuration (PRELOADED VERSION - no SQL)
# ---------------------------------------------------------------------------
# Global cache - set by main() before parallel processing
PRELOADED_SIGNALS: List[SignalData] = []
PRELOADED_BARS: Dict[int, List[tuple]] = {}
# (MAX_SIGNALS_PER_FILTER removed - run_strategy is now O(n) instead of O(n²))

def evaluate_filter_preloaded(filter_cfg: Dict, strategy_grid: List[Dict]) -> Tuple[Dict, Dict[int, float]]:
    """Run optimisation for one filter config using PRELOADED data.

    Returns the filter config and a dict `strategy_id -> total_pnl` aggregated over
    all pairs.
    
    NOTE: Uses in-memory filtering, NO SQL queries executed here.
    """
    # Filter signals in memory (no SQL!)
    signals = filter_signals_in_memory(PRELOADED_SIGNALS, filter_cfg)
    
    # Skip filters with too few signals
    if len(signals) < MIN_SIGNALS_FOR_EVAL:
        return filter_cfg, {}
    
    # Group by pair
    by_pair: Dict[str, List[SignalInfo]] = {}
    for s in signals:
        by_pair.setdefault(s.pair, []).append(s)
    
    # Use preloaded bars cache (no SQL!)
    # Sequential processing per pair (required for global position tracking)
    aggregated: Dict[int, float] = {i: 0.0 for i in range(len(strategy_grid))}
    for pair, pair_signals in by_pair.items():
        pair_agg = process_pair(pair, pair_signals, strategy_grid, PRELOADED_BARS)
        for sid, val in pair_agg.items():
            aggregated[sid] += val
    return filter_cfg, aggregated

# ---------------------------------------------------------------------------
# Main optimisation loop
# ---------------------------------------------------------------------------
DEBUG_LOGGING = False  # Set to True for verbose logging

def _evaluate_filter_wrapper(args):
    """Wrapper for multiprocessing - unpacks args tuple."""
    filter_cfg, strategy_grid = args
    if DEBUG_LOGGING:
        import os
        pid = os.getpid()
        print(f"[PID {pid}] Starting filter: score={filter_cfg.get('score_min')}, rsi={filter_cfg.get('rsi_min')}, vol={filter_cfg.get('vol_min')}, oi={filter_cfg.get('oi_min')}", flush=True)
    result = evaluate_filter_preloaded(filter_cfg, strategy_grid)
    if DEBUG_LOGGING:
        print(f"[PID {pid}] Finished filter: score={filter_cfg.get('score_min')}", flush=True)
    return result

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
    print(f"Filter grid: {len(filter_grid)} combinations, Strategy grid: {len(strategy_grid)} combinations")
    print(f"Using {MAX_WORKERS} workers for parallel filter processing")

    # =========================================================================
    # PRELOAD ALL DATA ONCE (eliminates all SQL queries during optimization)
    # =========================================================================
    global PRELOADED_SIGNALS, PRELOADED_BARS
    
    print("\n" + "="*70)
    print("PHASE 1: Preloading all data into memory (one-time DB access)")
    print("="*70)
    
    PRELOADED_SIGNALS = preload_all_signals()
    if not PRELOADED_SIGNALS:
        print("ERROR: No signals loaded. Exiting.")
        return
    
    all_signal_ids = [s.signal_id for s in PRELOADED_SIGNALS]
    PRELOADED_BARS = preload_all_bars(all_signal_ids)
    
    # Filter out signals without bars
    signals_with_bars = [s for s in PRELOADED_SIGNALS if s.signal_id in PRELOADED_BARS and len(PRELOADED_BARS[s.signal_id]) >= 100]
    PRELOADED_SIGNALS = signals_with_bars
    print(f"[PRELOAD] Signals with sufficient bars (>=100): {len(PRELOADED_SIGNALS)}")
    
    print("="*70)
    print("PHASE 2: Running optimization (NO SQL queries from here)")
    print("="*70 + "\n")

    all_results = []
    
    # Parallel processing of filter combinations
    tasks = [(cfg, strategy_grid) for cfg in filter_grid]
    
    if MAX_WORKERS == 1:
        # Sequential for debugging
        try:
            from tqdm import tqdm
            iterator = tqdm(tasks, desc="Optimizing filters")
        except ImportError:
            iterator = tasks
            print(f"Optimizing {len(filter_grid)} filter configurations...")
        
        for task in iterator:
            cfg, agg = _evaluate_filter_wrapper(task)
            if agg:
                best_sid = max(agg, key=lambda k: agg[k])
                best_params = strategy_grid[best_sid]
                result_entry = {
                    "filter": cfg,
                    "strategy": best_params,
                    "metrics": {"total_pnl": agg[best_sid], "strategy_id": best_sid},
                }
                all_results.append(result_entry)
                try:
                    with open("intermediate_results.jsonl", "a", encoding="utf-8") as f:
                        f.write(json.dumps(result_entry) + "\n")
                except Exception:
                    pass
    else:
        # Parallel processing with imap_unordered for progress tracking
        try:
            from tqdm import tqdm
            with mp.Pool(processes=MAX_WORKERS, maxtasksperchild=100) as pool:
                for cfg, agg in tqdm(pool.imap_unordered(_evaluate_filter_wrapper, tasks), 
                                      total=len(tasks), desc="Optimizing filters"):
                    if agg:
                        best_sid = max(agg, key=lambda k: agg[k])
                        best_params = strategy_grid[best_sid]
                        result_entry = {
                            "filter": cfg,
                            "strategy": best_params,
                            "metrics": {"total_pnl": agg[best_sid], "strategy_id": best_sid},
                        }
                        all_results.append(result_entry)
                        try:
                            with open("intermediate_results.jsonl", "a", encoding="utf-8") as f:
                                f.write(json.dumps(result_entry) + "\n")
                        except Exception:
                            pass
        except ImportError:
            print(f"Optimizing {len(filter_grid)} filter configurations...")
            with mp.Pool(processes=MAX_WORKERS) as pool:
                results = pool.map(_evaluate_filter_wrapper, tasks)
            for cfg, agg in results:
                if agg:
                    best_sid = max(agg, key=lambda k: agg[k])
                    best_params = strategy_grid[best_sid]
                    all_results.append({
                        "filter": cfg,
                        "strategy": best_params,
                        "metrics": {"total_pnl": agg[best_sid], "strategy_id": best_sid},
                    })

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
