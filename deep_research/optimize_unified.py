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
# ---------------------------------------------------------------------------
# LOOKUP TABLE ARCHITECTURE
# ---------------------------------------------------------------------------
# Global vars for WORKERS
WORKER_SIGNALS: List[SignalData] = []
WORKER_RESULTS: Dict[int, Dict[int, Tuple[float, int]]] = {}

def init_worker_lookup(signals: List[SignalData], results: Dict[int, Dict[int, Tuple[float, int]]]):
    """Initialize worker with read-only data (Copy-on-Write optimization)."""
    global WORKER_SIGNALS, WORKER_RESULTS
    WORKER_SIGNALS = signals
    WORKER_RESULTS = results

def precompute_all_strategies(
    signals: List[SignalData],
    bars_cache: Dict[int, List[tuple]],
    strategy_grid: List[Dict]
) -> Dict[int, Dict[int, Tuple[float, int]]]:
    """Phase 1: Run ALL strategies for ALL signals.
    
    Returns: {signal_id: {strategy_idx: (pnl, last_exit_ts)}}
    """
    print(f"[PRECOMPUTE] Running {len(strategy_grid)} strategies for {len(signals)} signals...")
    results_map = {}
    
    # We can use parallelization here too if needed, but sequential is fine for 300k items
    # (~1ms per item = 300s. Parallel 12 workers = 25s)
    
    # Let's use a pool for this precomputation to make it super fast
    # Map (signal, bars) -> {strat_idx: res}
    
    start_time = datetime.now()
    processed_count = 0
    
    for sig in signals:
        sid = sig.signal_id
        bars = bars_cache.get(sid, [])
        if len(bars) < 100:
            continue
            
        # Precompute cumsum once
        precomputed = precompute_bars(bars)
        if not precomputed:
            continue
            
        sig_results = {}
        for s_idx, sp in enumerate(strategy_grid):
            # Run strategy
            pnl, last_ts = run_strategy_fast(
                precomputed,
                sp["sl_pct"],
                sp["delta_window"],
                sp["threshold_mult"],
                sp["leverage"]
            )
            sig_results[s_idx] = (pnl, last_ts)
        
        results_map[sid] = sig_results
        processed_count += 1
        
        if processed_count % 100 == 0:
            print(f"[PRECOMPUTE] PROGESS: {processed_count}/{len(signals)} signals processed...", end='\r')

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n[PRECOMPUTE] Done in {elapsed:.1f}s. Computed {len(results_map)} signals.")
    return results_map

def evaluate_filter_lookup(filter_cfg: Dict, strategy_grid: List[Dict]) -> Tuple[Dict, Dict[int, float]]:
    """Phase 2: Evaluate filter using LOOKUP TABLE (No calculation)."""
    # 1. Filter signals (InMemory)
    matched_signals = filter_signals_in_memory(WORKER_SIGNALS, filter_cfg)
    if len(matched_signals) < MIN_SIGNALS_FOR_EVAL:
        return filter_cfg, {}

    # 2. Group by pair
    by_pair: Dict[str, List[SignalInfo]] = {}
    for s in matched_signals:
        by_pair.setdefault(s.pair, []).append(SignalInfo(s.signal_id, s.pair, s.timestamp))

    # 3. Sum results from LOOKUP TABLE
    aggregated: Dict[int, float] = {i: 0.0 for i in range(len(strategy_grid))}
    
    for pair, pair_signals in by_pair.items():
        # Sequential check for pair position tracking
        position_tracker_ts = 0
        sorted_signals = sorted(pair_signals, key=lambda x: x.timestamp)
        
        for sig in sorted_signals:
            if int(sig.timestamp.timestamp()) < position_tracker_ts:
                continue
                
            # Lookup results
            sig_res = WORKER_RESULTS.get(sig.signal_id)
            if not sig_res:
                continue
                
            # Add PnL for all strategies
            max_last_ts = position_tracker_ts
            for s_idx, (pnl, last_ts) in sig_res.items():
                aggregated[s_idx] += pnl
                if last_ts > max_last_ts:
                    max_last_ts = last_ts
                    
            position_tracker_ts = max_last_ts

    return filter_cfg, aggregated

# ---------------------------------------------------------------------------
# Evaluate a single filter configuration (PRELOADED VERSION - no SQL)
# ---------------------------------------------------------------------------
# Global cache - set by main() before parallel processing
PRELOADED_SIGNALS: List[SignalData] = []
PRELOADED_BARS: Dict[int, List[tuple]] = {}
# (MAX_SIGNALS_PER_FILTER removed - run_strategy is now O(n) instead of O(n²))

# (evaluate_filter_preloaded removed - using Lookup Table architecture)
# ---------------------------------------------------------------------------
# Evaluate a single filter configuration (LOOKUP TABLE WRAPPER)
# ---------------------------------------------------------------------------
def _evaluate_filter_wrapper_lookup(args):
    """Wrapper for multiprocessing with Lookup Table."""
    filter_cfg, strategy_grid = args
    return evaluate_filter_lookup(filter_cfg, strategy_grid)

# ---------------------------------------------------------------------------
# Main optimisation loop
# ---------------------------------------------------------------------------
DEBUG_LOGGING = False

def _evaluate_filter_wrapper_lookup(args):
    """Wrapper for multiprocessing with Lookup Table."""
    filter_cfg, strategy_grid = args
    return evaluate_filter_lookup(filter_cfg, strategy_grid)

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
    # PHASE 1: PRELOAD & PRECOMPUTE (The Heavy Lifting)
    # =========================================================================
    global PRELOADED_SIGNALS, PRELOADED_BARS
    
    print("\n" + "="*70)
    print("PHASE 1: Preloading & Precomputing Strategy Results")
    print("="*70)
    
    # 1. Load Signals
    all_signals = preload_all_signals()
    if not all_signals:
        print("ERROR: No signals loaded. Exiting.")
        return
    
    # 2. Load Bars
    all_signal_ids = [s.signal_id for s in all_signals]
    bars_cache = preload_all_bars(all_signal_ids)
    
    # 3. Filter signals with sufficient bars
    signals_with_bars = [s for s in all_signals if s.signal_id in bars_cache and len(bars_cache[s.signal_id]) >= 100]
    print(f"[PRELOAD] Signals with sufficient bars (>=100): {len(signals_with_bars)}")
    
    # Log score distribution to understand data
    score_dist = {}
    for s in signals_with_bars:
        bucket = (s.score // 50) * 50
        score_dist[bucket] = score_dist.get(bucket, 0) + 1
    print("[PRELOAD] Score distribution:")
    for score in sorted(score_dist.keys()):
        print(f"  {score}-{score+49}: {score_dist[score]} signals")
    
    if not signals_with_bars:
        print("No signals with sufficient data.")
        return

    # 4. PRECOMPUTE ALL STRATEGIES (Lookup Table Generation)
    # This runs 540 strategies for every signal once.
    lookup_table = precompute_all_strategies(signals_with_bars, bars_cache, strategy_grid)
    
    # Clean up bars_cache to free memory (we only need the lookup table now!)
    del bars_cache
    import gc
    gc.collect()
    print("[MEMORY] Cleared bars cache. Ready for optimization.")

    print("="*70)
    print("PHASE 2: Running optimization (Lookup Table Mode - Ultra Fast)")
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
        
        # Initialize global worker state for sequential run
        init_worker_lookup(signals_with_bars, lookup_table)
        
        for task in iterator:
            cfg, agg = _evaluate_filter_wrapper_lookup(task)
            if agg:
                best_sid = max(agg, key=lambda k: agg[k])
                best_params = strategy_grid[best_sid]
                result_entry = {
                    "filter": cfg,
                    "strategy": best_params,
                    "metrics": {"total_pnl": agg[best_sid], "strategy_id": best_sid},
                }
                all_results.append(result_entry)
                # Save immediately for seq mode
                try:
                    with open("intermediate_results.jsonl", "a", encoding="utf-8") as f:
                        f.write(json.dumps(result_entry) + "\n")
                except Exception:
                    pass
    else:
        # Multiprocessing with Initializer
        with mp.Pool(processes=MAX_WORKERS, initializer=init_worker_lookup, initargs=(signals_with_bars, lookup_table)) as pool:
            try:
                from tqdm import tqdm
                iterator = tqdm(pool.imap_unordered(_evaluate_filter_wrapper_lookup, tasks, chunksize=1000), 
                              total=len(tasks), desc="Optimizing filters")
            except ImportError:
                iterator = pool.imap_unordered(_evaluate_filter_wrapper_lookup, tasks, chunksize=1000)
                print(f"Optimizing {len(filter_grid)} filter configurations...")

            for cfg, agg in iterator:
                if agg:
                    best_sid = max(agg, key=lambda k: agg[k])
                    best_params = strategy_grid[best_sid]
                    result_entry = {
                        "filter": cfg,
                        "strategy": best_params,
                        "metrics": {"total_pnl": agg[best_sid], "strategy_id": best_sid},
                    }
                    all_results.append(result_entry)
                    # Save occasionally
                    if len(all_results) % 500 == 0:
                        try:
                            with open("intermediate_results.jsonl", "a", encoding="utf-8") as f:
                                f.write(json.dumps(result_entry) + "\n")
                        except Exception:
                            pass

    # -----------------------------------------------------------------------
    # Save results
    # -----------------------------------------------------------------------
    print("\n" + "="*70)
    print(f"Optimization complete. Found {len(all_results)} valid configurations.")
    
    if not all_results:
        print("No valid results found.")
        return

    # Sort by Total PnL
    all_results.sort(key=lambda x: x["metrics"]["total_pnl"], reverse=True)
    
    # Print Top 10
    print("\n🏆 TOP 10 CONFIGURATIONS:")
    print(f"{'#':<3} {'Score':<12} {'RSI':<5} {'Vol':<5} {'OI':<5} | {'Lev':<4} {'SL%':<5} {'Win':<4} {'Mult':<5} | {'Total PnL %':<12}")
    print("-" * 85)
    
    for i, res in enumerate(all_results[:10], 1):
        f = res["filter"]
        s = res["strategy"]
        m = res["metrics"]
        print(f"{i:<3} {f['score_min']}-{f['score_max']:<5} {f['rsi_min']:<5} {f['vol_min']:<5} {f['oi_min']:<5} | "
              f"{s['leverage']:<4} {s['sl_pct']:<5} {s['delta_window']:<4} {s['threshold_mult']:<5} | {m['total_pnl']:<12.2f}")
    
    # Save JSON
    output_file = "optimization_results_unified.json"
    try:
        with open(output_file, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\nSaved full results to {output_file}")
    except Exception as e:
        print(f"Error saving results: {e}")

if __name__ == "__main__":
    mp.freeze_support()
    main()
