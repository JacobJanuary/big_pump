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
import gc
import itertools
import json
import pickle
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, NamedTuple, Optional
import multiprocessing as mp
import numpy as np

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

# Checkpoint configuration
CHECKPOINT_DIR = Path("checkpoints")
CHECKPOINT_PHASE1_FILE = CHECKPOINT_DIR / "phase1_lookup.pkl"
CHECKPOINT_PROGRESS_FILE = CHECKPOINT_DIR / "phase1_progress.json"
CHECKPOINT_INTERVAL = 25  # Save checkpoint every N signals
OUTPUT_JSONL_FILE = Path("optimization_results.jsonl")
CRASH_LOG_FILE = Path("optimize_crash.log")

def crash_log(msg: str):
    """Write message to crash log file with immediate flush. Survives OOM kills."""
    import resource
    mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB to MB on Linux
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(CRASH_LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] [MEM:{mem_mb:.0f}MB] {msg}\n")
        f.flush()
        os.fsync(f.fileno())  # Force write to disk

# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------
def save_phase1_checkpoint(
    lookup_table: Dict[int, Dict[int, Tuple[float, int]]],
    processed_signal_ids: List[int],
    progress_info: Dict
):
    """Save Phase 1 checkpoint to disk (ATOMIC - crash-safe)."""
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    
    # Atomic write: temp file + rename (survives OOM kills)
    temp_pkl = CHECKPOINT_PHASE1_FILE.with_suffix('.tmp')
    temp_json = CHECKPOINT_PROGRESS_FILE.with_suffix('.tmp')
    
    try:
        # Save lookup table (binary pickle for speed)
        with open(temp_pkl, "wb") as f:
            pickle.dump({
                "lookup_table": lookup_table,
                "processed_signal_ids": processed_signal_ids,
            }, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        # Atomic rename (POSIX guarantees atomicity)
        temp_pkl.rename(CHECKPOINT_PHASE1_FILE)
        
        # Save progress info (JSON for readability)
        with open(temp_json, "w") as f:
            json.dump(progress_info, f, indent=2)
        temp_json.rename(CHECKPOINT_PROGRESS_FILE)
        
        print(f"[CHECKPOINT] Saved: {len(lookup_table)} signals processed")
    except Exception as e:
        print(f"[CHECKPOINT] Error saving: {e}")
        # Clean up temp files if they exist
        if temp_pkl.exists():
            temp_pkl.unlink()
        if temp_json.exists():
            temp_json.unlink()

def load_phase1_checkpoint() -> Optional[Tuple[Dict, List[int], Dict]]:
    """Load Phase 1 checkpoint if exists."""
    if not CHECKPOINT_PHASE1_FILE.exists() or not CHECKPOINT_PROGRESS_FILE.exists():
        return None
    
    try:
        with open(CHECKPOINT_PHASE1_FILE, "rb") as f:
            data = pickle.load(f)
        
        with open(CHECKPOINT_PROGRESS_FILE, "r") as f:
            progress_info = json.load(f)
        
        print(f"[CHECKPOINT] Loaded: {len(data['lookup_table'])} signals from checkpoint")
        return data["lookup_table"], data["processed_signal_ids"], progress_info
    except Exception as e:
        print(f"[CHECKPOINT] Error loading checkpoint: {e}")
        return None

def clear_phase1_checkpoint():
    """Remove Phase 1 checkpoint files including lookup table."""
    if CHECKPOINT_PHASE1_FILE.exists():
        CHECKPOINT_PHASE1_FILE.unlink()
    if CHECKPOINT_PROGRESS_FILE.exists():
        CHECKPOINT_PROGRESS_FILE.unlink()
    lookup_file = CHECKPOINT_DIR / "phase1_lookup_table.pkl"
    if lookup_file.exists():
        lookup_file.unlink()
    print("[CHECKPOINT] Cleared Phase 1 checkpoint files")

def load_lookup_table() -> Optional[Dict[int, Dict[int, Tuple[float, int]]]]:
    """Load saved lookup table for Phase 2 crash recovery."""
    lookup_file = CHECKPOINT_DIR / "phase1_lookup_table.pkl"
    if not lookup_file.exists():
        return None
    try:
        with open(lookup_file, "rb") as f:
            lookup_table = pickle.load(f)
        print(f"[RECOVERY] Loaded lookup table: {len(lookup_table)} signals")
        return lookup_table
    except Exception as e:
        print(f"[RECOVERY] Error loading lookup table: {e}")
        return None


# ---------------------------------------------------------------------------
# Precomputed bars cache (save/load for rapid grid iteration)
# ---------------------------------------------------------------------------

def save_precomputed_bars(
    signals: List,
    bars_cache: Dict[int, List[tuple]],
    output_path: Path,
    preload_workers: int = 8,
    batch_size: int = 200
):
    """Save precomputed bar data to disk for reuse across grid runs.
    
    MEMORY-SAFE: Saves each batch to a separate pickle file in a directory,
    clearing memory after each batch. No accumulation in RAM.
    
    Output: directory with batch_001.pkl, batch_002.pkl, ...
    Each file: {signal_id: precomputed_dict}
    """
    # Use output_path as directory (e.g. bars_cache/)
    cache_dir = output_path
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*70}")
    print(f"SAVE-BARS MODE: Caching precomputed bars to {cache_dir}/")
    print(f"{'='*70}")
    print(f"  Signals: {len(signals)}")
    print(f"  Each batch saved to separate file (no RAM accumulation)")
    print(f"  After this, use --load-bars {cache_dir} to skip DB loading!")
    print(f"{'='*70}\n")
    
    total_cached = 0
    skipped = 0
    start_time = datetime.now()
    num_batches = (len(signals) + batch_size - 1) // batch_size
    
    for batch_idx in range(num_batches):
        batch_start = batch_idx * batch_size
        batch_signals = signals[batch_start:batch_start + batch_size]
        batch_sids = [s.signal_id for s in batch_signals]
        batch_file = cache_dir / f"batch_{batch_idx+1:03d}.pkl"
        
        # Skip already saved batches (resume support)
        if batch_file.exists():
            # Quick check: load and count
            with open(batch_file, "rb") as f:
                existing = pickle.load(f)
            total_cached += len(existing)
            del existing
            print(f"[BATCH {batch_idx+1}/{num_batches}] ✓ Already saved, skipping ({total_cached} total)", flush=True)
            continue
        
        print(f"\n[BATCH {batch_idx+1}/{num_batches}] Loading {len(batch_signals)} signals from DB...", flush=True)
        batch_bars = preload_all_bars(batch_sids, preload_workers=preload_workers)
        print(f"[BATCH {batch_idx+1}] Bars loaded. Precomputing {len(batch_signals)} signals...", flush=True)
        
        batch_cache: Dict[int, Dict] = {}
        for sig_i, sig in enumerate(batch_signals):
            bars = batch_bars.get(sig.signal_id, [])
            if len(bars) < 100:
                skipped += 1
                continue
            
            entry_ts = int(sig.entry_time.timestamp())
            pc = precompute_bars(bars, entry_ts)
            if pc:
                batch_cache[sig.signal_id] = pc
            else:
                skipped += 1
            
            if (sig_i + 1) % 10 == 0:
                total_done = total_cached + len(batch_cache) + skipped
                elapsed = (datetime.now() - start_time).total_seconds()
                speed = total_done / elapsed if elapsed > 0 else 0
                remaining = len(signals) - total_done
                eta = remaining / speed / 60 if speed > 0 else 0
                print(f"  [{sig_i+1}/{len(batch_signals)}] "
                      f"batch={len(batch_cache)} | total={total_done}/{len(signals)} | "
                      f"{speed:.1f} sig/s | ETA: {eta:.1f}min", flush=True)
        
        # Save this batch to its own file
        temp_path = batch_file.with_suffix('.tmp')
        with open(temp_path, "wb") as f:
            pickle.dump(batch_cache, f, protocol=pickle.HIGHEST_PROTOCOL)
        temp_path.rename(batch_file)
        size_mb = batch_file.stat().st_size / 1024 / 1024
        total_cached += len(batch_cache)
        
        print(f"[BATCH {batch_idx+1}] ✓ Saved {len(batch_cache)} signals ({size_mb:.0f}MB) to {batch_file.name}", flush=True)
        print(f"[PROGRESS] Total cached: {total_cached}, Skipped: {skipped}", flush=True)
        
        # Free ALL batch memory immediately
        del batch_bars, batch_cache
        gc.collect()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*70}")
    print(f"✅ SAVE-BARS COMPLETE!")
    print(f"  Cached: {total_cached} signals in {num_batches} batch files")
    print(f"  Skipped: {skipped}")
    print(f"  Directory: {cache_dir}/")
    print(f"  Time: {elapsed/60:.1f} min")
    print(f"  Now run with --load-bars {cache_dir} to skip DB loading!")
    print(f"{'='*70}\n")


def load_precomputed_bars(input_path: Path) -> Optional[List[Path]]:
    """Find precomputed bar batch files.
    
    Returns: sorted list of batch pickle file paths, or None if not found.
    """
    cache_dir = input_path
    if not cache_dir.exists() or not cache_dir.is_dir():
        print(f"[ERROR] Bars cache directory not found: {cache_dir}")
        return None
    
    batch_files = sorted(cache_dir.glob("batch_*.pkl"))
    if not batch_files:
        print(f"[ERROR] No batch files found in {cache_dir}")
        return None
    
    total_size = sum(f.stat().st_size for f in batch_files) / 1024 / 1024
    print(f"[LOAD-BARS] ✅ Found {len(batch_files)} batch files ({total_size:.0f}MB total)")
    return batch_files

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
# Strategy grid generation (EXPANDED with BASE_* parameterization)
# ---------------------------------------------------------------------------
def generate_strategy_grid() -> List[Dict]:
    """Generate strategy grid with R:R >= 1:2 constraint for pump trading.
    
    Key constraints (enforced mathematically):
    1. activation >= 2 × sl_pct  (R:R >= 1:2)
    2. callback <= activation / 2  (lock in at least half the move)
    
    Parameters:
    - sl_pct:     [3, 5, 7, 10]
    - activation: [6, 8, 10, 15, 20, 25, 30, 40, 50] %
    - callback:   [2, 3, 5, 7, 10, 15] %
    - delta_window: 8 options
    - cooldown: 3 options
    - reentry: 4 options  
    - position: 5 options
    
    Constraint logic per combo:
      act >= 2*sl  AND  cb <= act/2
    
    ~134 valid SL×ACT×CB triplets × 480 other combos = ~64,320 strategies
    """
    leverage_opts = [10]
    sl_opts = [3, 5, 7, 10]
    activation_opts = [6, 8, 10, 15, 20, 25, 30, 40, 50]
    callback_opts = [2, 3, 5, 7, 10, 15]
    delta_window_opts = [5, 30, 60, 120, 300, 600, 1800, 3600]
    threshold_opts = [1.0]
    base_reentry_drop = 5.0
    base_cooldown_opts = [60, 300, 600]
    max_reentry_hours_opts = [4, 8, 12, 24]
    max_position_hours_opts = [2, 4, 6, 12, 24]
    
    # Build valid SL×ACT×CB triplets with R:R >= 1:2
    sl_act_cb_triplets = []
    for sl in sl_opts:
        for act in activation_opts:
            if act < 2 * sl:  # R:R < 1:2 → skip
                continue
            for cb in callback_opts:
                if cb > act / 2:  # callback eats more than half the move → skip
                    continue
                sl_act_cb_triplets.append((sl, float(act), float(cb)))
    
    grid = []
    for lev in leverage_opts:
        for (sl, act, cb) in sl_act_cb_triplets:
            for win in delta_window_opts:
                for thresh in threshold_opts:
                    for cool in base_cooldown_opts:
                        for reentry_h in max_reentry_hours_opts:
                            for pos_h in max_position_hours_opts:
                                grid.append({
                                    "leverage": lev,
                                    "sl_pct": sl,
                                    "delta_window": win,
                                    "threshold_mult": thresh,
                                    "base_activation": act,
                                    "base_callback": cb,
                                    "base_reentry_drop": base_reentry_drop,
                                    "base_cooldown": cool,
                                    "max_reentry_hours": reentry_h,
                                    "max_position_hours": pos_h,
                                })
    return grid

def _grid_to_tuples(grid: List[Dict]) -> List[tuple]:
    """Convert strategy grid dicts to tuples for 2-3x faster inner loop access."""
    return [
        (sp["sl_pct"], sp["delta_window"], sp["threshold_mult"], sp["leverage"],
         sp["base_activation"], sp["base_callback"], sp["base_reentry_drop"],
         sp["base_cooldown"], sp["max_reentry_hours"] * 3600, sp["max_position_hours"] * 3600)
        for sp in grid
    ]

# ---------------------------------------------------------------------------
# PRELOAD ALL DATA ONCE (eliminates SQL in optimization loop)
# ---------------------------------------------------------------------------
class SignalData(NamedTuple):
    """Extended signal info with indicator values for in-memory filtering"""
    signal_id: int
    pair: str
    timestamp: datetime  # signal_timestamp (when pattern was detected)
    entry_time: datetime  # entry_time = signal_timestamp + 17 min (when position opens)
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
                    signals.append(SignalData(
                        signal_id=sid, pair=sym, timestamp=ts, entry_time=entry_t,
                        score=score, rsi=rsi or 0, vol_zscore=vol or 0, oi_delta=oi or 0
                    ))
        print(f"[PRELOAD] Loaded {len(signals)} signals")
    except Exception as e:
        print(f"Error preloading signals: {e}")
    return signals

def preload_all_bars(signal_ids: List[int], preload_workers: int = 8) -> Dict[int, List[tuple]]:
    """Load ALL bars for all signals in chunks (PARALLEL - uses ThreadPoolExecutor).
    
    Args:
        signal_ids: List of signal IDs to load bars for.
        preload_workers: Number of parallel DB connections for loading (default 8).
    
    Returns dict: {signal_id: [(ts, price, delta, 0, buy, sell), ...]}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    
    print(f"[PRELOAD] Loading bars for {len(signal_ids)} signals using {preload_workers} workers...")
    bars_map: Dict[int, List[tuple]] = {}
    bars_lock = threading.Lock()
    chunk_size = 10  # Smaller chunks for better progress visibility
    total_chunks = (len(signal_ids) + chunk_size - 1) // chunk_size
    
    # Split into chunks
    chunks = []
    for i in range(0, len(signal_ids), chunk_size):
        chunks.append((i // chunk_size, signal_ids[i:i + chunk_size]))
    
    completed = [0]  # Mutable counter for progress
    
    def load_chunk(args):
        chunk_idx, chunk = args
        local_bars = {}
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
                        local_bars.setdefault(sid, []).append(
                            (ts, float(price), float(delta), 0.0, buy, sell)
                        )
            return (chunk_idx, local_bars, len(rows))
        except Exception as e:
            print(f"[PRELOAD] Chunk {chunk_idx} error: {e}")
            return (chunk_idx, {}, 0)
    
    # Parallel execution
    with ThreadPoolExecutor(max_workers=preload_workers) as executor:
        futures = [executor.submit(load_chunk, chunk) for chunk in chunks]
        
        for future in as_completed(futures):
            chunk_idx, local_bars, row_count = future.result()
            with bars_lock:
                for sid, bars in local_bars.items():
                    bars_map.setdefault(sid, []).extend(bars)
                completed[0] += 1
            
            # Progress every 10 chunks or at completion
            if completed[0] % 10 == 0 or completed[0] == total_chunks:
                print(f"[PRELOAD] Progress: {completed[0]}/{total_chunks} chunks ({completed[0]*100//total_chunks}%)", flush=True)
    
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

# ---------------------------------------------------------------------------
# LOAD-BARS parallel worker infrastructure
# ---------------------------------------------------------------------------
LOADBARS_GRID: List[tuple] = []  # tuples for fast inner loop
LOADBARS_PC: Dict[int, Dict] = {}

def loadbars_process_signal(sid: int) -> Tuple[int, Optional[np.ndarray]]:
    """Worker: run all strategies on one precomputed signal, return numpy array."""
    pc = LOADBARS_PC.get(sid)
    if pc is None:
        return (sid, None)
    grid = LOADBARS_GRID
    n = len(grid)
    results = np.zeros((n, 6), dtype=np.float32)
    for i in range(n):
        sl, dw, tm, lev, act, cb, rd, cool, re_s, pos_s = grid[i]
        results[i] = run_strategy_fast(pc, sl, dw, tm, lev, act, cb, rd, cool, re_s, pos_s)
    return (sid, results)


# ---------------------------------------------------------------------------
# Shared data for Phase 1 workers
PHASE1_BARS_CACHE: Dict[int, List[tuple]] = {}
PHASE1_STRATEGY_GRID: List[Dict] = []

def init_phase1_worker(bars_cache: Dict[int, List[tuple]], strategy_grid: List[Dict]):
    """Initialize worker with shared read-only data."""
    global PHASE1_BARS_CACHE, PHASE1_STRATEGY_GRID
    PHASE1_BARS_CACHE = bars_cache
    PHASE1_STRATEGY_GRID = strategy_grid

def process_single_signal(sig: SignalData) -> Tuple[int, Dict[int, Tuple[float, int]]]:
    """Worker function: process one signal through all strategies.
    
    Returns: (signal_id, {strategy_idx: (pnl, last_exit_ts)}) or (signal_id, None) if skipped
    """
    sid = sig.signal_id
    bars = PHASE1_BARS_CACHE.get(sid, [])
    
    if len(bars) < 100:
        return (sid, None)
    
    # Use entry_time (signal + 17 min), matches db_batch_utils fetch
    entry_ts = int(sig.entry_time.timestamp())
    precomputed = precompute_bars(bars, entry_ts)
    if not precomputed:
        return (sid, None)
    
    grid = PHASE1_STRATEGY_GRID
    n = len(grid)
    sig_results = np.zeros((n, 6), dtype=np.float32)
    for i in range(n):
        sl, dw, tm, lev, act, cb, rd, cool, re_s, pos_s = grid[i]
        sig_results[i] = run_strategy_fast(precomputed, sl, dw, tm, lev, act, cb, rd, cool, re_s, pos_s)
    
    return (sid, sig_results)

def precompute_all_strategies(
    signals: List[SignalData],
    bars_cache: Dict[int, List[tuple]],
    strategy_grid: List[Dict],
    num_workers: int = 1,
    resume: bool = True
) -> Dict[int, Dict[int, Tuple[float, int]]]:
    """Phase 1: Run ALL strategies for ALL signals (PARALLEL).
    
    Args:
        num_workers: Number of parallel workers (default 1 = sequential)
        resume: If True, attempt to resume from checkpoint
    
    Returns: {signal_id: {strategy_idx: (pnl, last_exit_ts)}}
    """
    total_signals = len(signals)
    total_strategies = len(strategy_grid)
    total_iterations = total_signals * total_strategies
    
    print(f"\n{'='*70}")
    print(f"[PRECOMPUTE] Phase 1: Strategy Pre-computation")
    print(f"{'='*70}")
    print(f"  Signals to process: {total_signals:,}")
    print(f"  Strategies per signal: {total_strategies:,}")
    print(f"  Total iterations: {total_iterations:,}")
    print(f"  Workers: {num_workers}")
    print(f"  Checkpoint interval: every {CHECKPOINT_INTERVAL} signals")
    print(f"{'='*70}\n")
    
    results_map = {}
    processed_signal_ids = []
    start_time = datetime.now()
    processed_count = 0
    skipped_count = 0
    
    # Try to resume from checkpoint
    if resume:
        checkpoint_data = load_phase1_checkpoint()
        if checkpoint_data:
            results_map, processed_signal_ids, progress_info = checkpoint_data
            processed_count = progress_info.get("processed_count", len(results_map))
            skipped_count = progress_info.get("skipped_count", 0)
            print(f"[RESUME] Resuming from checkpoint: {processed_count} signals already processed")
    
    # Filter out already processed signals
    if processed_signal_ids:
        processed_set = set(processed_signal_ids)
        signals_to_process = [s for s in signals if s.signal_id not in processed_set]
        print(f"[RESUME] {len(signals_to_process)} signals remaining")
    else:
        signals_to_process = signals
    
    if num_workers <= 1:
        # Sequential mode (original logic)
        for i, sig in enumerate(signals_to_process):
            sid, sig_results = process_single_signal_sequential(sig, bars_cache, strategy_grid)
            if sig_results is None:
                skipped_count += 1
            else:
                results_map[sid] = sig_results
                processed_signal_ids.append(sid)
                processed_count += 1
            
            if processed_count % 10 == 0 and processed_count > 0:
                _print_phase1_progress(processed_count, total_signals, skipped_count, start_time, total_strategies)
            
            # Checkpoint every CHECKPOINT_INTERVAL signals
            if processed_count % CHECKPOINT_INTERVAL == 0 and processed_count > 0:
                save_phase1_checkpoint(results_map, processed_signal_ids, {
                    "processed_count": processed_count,
                    "skipped_count": skipped_count,
                    "timestamp": datetime.now().isoformat(),
                    "total_signals": total_signals,
                })
    else:
        # Parallel mode with fork-inherited globals (no pickle copy!)
        # Set globals BEFORE creating Pool - workers inherit via fork COW
        global PHASE1_BARS_CACHE, PHASE1_STRATEGY_GRID
        PHASE1_BARS_CACHE = bars_cache
        PHASE1_STRATEGY_GRID = strategy_grid
        
        crash_log(f"PHASE 1 START: {len(signals_to_process)} signals, {num_workers} workers")
        print(f"[PRECOMPUTE] Starting {num_workers} parallel workers...")
        print(f"[MEMORY] Set globals for fork inheritance ({len(bars_cache)} signals, {len(strategy_grid)} strategies)\n")
        
        # Create Pool WITHOUT initargs - workers inherit globals via fork
        with mp.Pool(processes=num_workers) as pool:
            # Use imap for ordered results with progress tracking
            for sid, sig_results in pool.imap(process_single_signal, signals_to_process, chunksize=5):
                if sig_results is None:
                    skipped_count += 1
                else:
                    results_map[sid] = sig_results
                    processed_signal_ids.append(sid)
                    processed_count += 1
                
                # Progress every 10 processed signals
                if processed_count % 10 == 0 and processed_count > 0:
                    _print_phase1_progress(processed_count, total_signals, skipped_count, start_time, total_strategies)
                
                # Checkpoint every CHECKPOINT_INTERVAL signals
                if processed_count % CHECKPOINT_INTERVAL == 0 and processed_count > 0:
                    crash_log(f"Phase 1 progress: {processed_count}/{total_signals} signals")
                    save_phase1_checkpoint(results_map, processed_signal_ids, {
                        "processed_count": processed_count,
                        "skipped_count": skipped_count,
                        "timestamp": datetime.now().isoformat(),
                        "total_signals": total_signals,
                    })

    elapsed = (datetime.now() - start_time).total_seconds()
    final_iters = processed_count * total_strategies
    final_speed = final_iters / elapsed if elapsed > 0 else 0
    
    print(f"\n{'='*70}")
    print(f"[PRECOMPUTE] ✅ Phase 1 Complete!")
    print(f"  Processed: {processed_count:,} signals")
    print(f"  Skipped: {skipped_count:,} (insufficient bars)")
    print(f"  Total time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"  Final speed: {final_speed/1000:,.1f}k iterations/sec")
    if num_workers > 1:
        print(f"  Speedup vs single-thread: ~{num_workers}x (theoretical)")
    print(f"{'='*70}\n")
    
    # CRITICAL: Save lookup table to separate file for crash recovery
    # DO NOT delete checkpoints yet - wait until Phase 2 writes first result!
    lookup_file = CHECKPOINT_DIR / "phase1_lookup_table.pkl"
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    with open(lookup_file, "wb") as f:
        pickle.dump(results_map, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"[SAFETY] Saved lookup table to {lookup_file} ({len(results_map)} signals)")
    print("[SAFETY] Checkpoints will be cleared AFTER Phase 2 writes first result")
    
    return results_map

def precompute_all_strategies_batched(
    signals: List[SignalData],
    strategy_grid: List[Dict],
    num_workers: int = 8,
    batch_size: int = 200,
    preload_workers: int = 8,
    resume: bool = True
) -> Dict[int, Dict[int, Tuple[float, int]]]:
    """Phase 1 BATCHED: Process signals in memory-efficient batches.
    
    Instead of loading ALL bars upfront (56GB), loads bars for batch_size signals at a time,
    processes them with num_workers, then clears memory before next batch.
    
    Args:
        signals: All signals to process
        strategy_grid: All strategy combinations
        num_workers: Workers for strategy computation (8 recommended)
        batch_size: Signals per batch (200 = ~6GB memory)
        preload_workers: DB connections for parallel bar loading
        resume: Resume from checkpoint if available
    
    Returns: {signal_id: {strategy_idx: (pnl, last_exit_ts)}}
    """
    
    total_signals = len(signals)
    total_strategies = len(strategy_grid)
    total_iterations = total_signals * total_strategies
    num_batches = (total_signals + batch_size - 1) // batch_size
    
    print(f"\n{'='*70}")
    print(f"[BATCHED] Phase 1: Strategy Pre-computation (Memory-Efficient Mode)")
    print(f"{'='*70}")
    print(f"  Signals: {total_signals:,} in {num_batches} batches of {batch_size}")
    print(f"  Strategies per signal: {total_strategies:,}")
    print(f"  Total iterations: {total_iterations:,}")
    print(f"  Compute workers: {num_workers}")
    print(f"  Preload workers: {preload_workers}")
    print(f"  Expected memory: ~{batch_size * 30 // 1000}GB per batch (vs 56GB all at once)")
    print(f"{'='*70}\n")
    
    results_map: Dict[int, Dict[int, Tuple[float, int]]] = {}
    processed_signal_ids: List[int] = []
    start_time = datetime.now()
    total_processed = 0
    total_skipped = 0
    
    # Try to resume from checkpoint
    if resume:
        checkpoint_data = load_phase1_checkpoint()
        if checkpoint_data:
            results_map, processed_signal_ids, progress_info = checkpoint_data
            total_processed = progress_info.get("processed_count", len(results_map))
            total_skipped = progress_info.get("skipped_count", 0)
            print(f"[RESUME] Loaded checkpoint: {total_processed} signals already processed")
    
    # Filter out already processed signals
    processed_set = set(processed_signal_ids)
    signals_to_process = [s for s in signals if s.signal_id not in processed_set]
    print(f"[BATCHED] {len(signals_to_process)} signals remaining to process\n")
    
    if not signals_to_process:
        print("[BATCHED] All signals already processed!")
        return results_map
    
    # Split into batches
    batches = []
    for i in range(0, len(signals_to_process), batch_size):
        batches.append(signals_to_process[i:i + batch_size])
    
    global PHASE1_BARS_CACHE, PHASE1_STRATEGY_GRID
    PHASE1_STRATEGY_GRID = strategy_grid
    
    for batch_idx, batch_signals in enumerate(batches):
        batch_start = datetime.now()
        batch_sids = [s.signal_id for s in batch_signals]
        
        print(f"\n{'='*50}")
        print(f"[BATCH {batch_idx + 1}/{len(batches)}] Processing {len(batch_signals)} signals")
        print(f"{'='*50}")
        
        # 1. Load bars for this batch only
        print(f"[BATCH {batch_idx + 1}] Loading bars...")
        batch_bars = preload_all_bars(batch_sids, preload_workers=preload_workers)
        PHASE1_BARS_CACHE = batch_bars
        
        batch_processed = 0
        batch_skipped = 0
        
        if num_workers <= 1:
            # Sequential processing
            for sig in batch_signals:
                sid, sig_results = process_single_signal_sequential(sig, batch_bars, strategy_grid)
                if sig_results is None:
                    batch_skipped += 1
                else:
                    results_map[sid] = sig_results
                    processed_signal_ids.append(sid)
                    batch_processed += 1
        else:
            # Parallel processing
            print(f"[BATCH {batch_idx + 1}] Starting {num_workers} workers...")
            batch_start_compute = datetime.now()
            with mp.Pool(processes=num_workers) as pool:
                for sid, sig_results in pool.imap(process_single_signal, batch_signals, chunksize=5):
                    if sig_results is None:
                        batch_skipped += 1
                    else:
                        results_map[sid] = sig_results
                        processed_signal_ids.append(sid)
                        batch_processed += 1
                    
                    # Progress every 10 signals within batch
                    batch_total = batch_processed + batch_skipped
                    if batch_total % 10 == 0 and batch_total > 0:
                        elapsed = (datetime.now() - batch_start_compute).total_seconds()
                        speed = batch_total / elapsed if elapsed > 0 else 0
                        remaining = len(batch_signals) - batch_total
                        eta = remaining / speed if speed > 0 else 0
                        print(f"[BATCH {batch_idx + 1}] {batch_total}/{len(batch_signals)} signals | {speed:.2f} sig/s | ETA: {eta:.0f}s", flush=True)
        
        total_processed += batch_processed
        total_skipped += batch_skipped
        
        # 3. Clear batch memory
        del batch_bars
        PHASE1_BARS_CACHE = {}
        gc.collect()
        
        batch_time = (datetime.now() - batch_start).total_seconds()
        total_elapsed = (datetime.now() - start_time).total_seconds()
        remaining_batches = len(batches) - batch_idx - 1
        eta_seconds = (total_elapsed / (batch_idx + 1)) * remaining_batches if batch_idx > 0 else 0
        
        print(f"[BATCH {batch_idx + 1}] ✓ Done: {batch_processed:,} processed, {batch_skipped} skipped in {batch_time:.1f}s")
        print(f"[PROGRESS] Total: {total_processed:,}/{total_signals:,} ({100*total_processed//total_signals}%) | ETA: {eta_seconds/60:.1f}m")
        
        # Save checkpoint after each batch
        save_phase1_checkpoint(results_map, processed_signal_ids, {
            "processed_count": total_processed,
            "skipped_count": total_skipped,
            "timestamp": datetime.now().isoformat(),
            "total_signals": total_signals,
            "batch_idx": batch_idx + 1,
            "total_batches": len(batches),
        })
    
    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    final_iters = total_processed * total_strategies
    final_speed = final_iters / elapsed if elapsed > 0 else 0
    
    print(f"\n{'='*70}")
    print(f"[BATCHED] ✅ Phase 1 Complete!")
    print(f"  Processed: {total_processed:,} signals in {len(batches)} batches")
    print(f"  Skipped: {total_skipped:,} (insufficient bars)")
    print(f"  Total time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"  Final speed: {final_speed/1000:,.1f}k iterations/sec")
    print(f"{'='*70}\n")
    
    # Save final lookup table
    lookup_file = CHECKPOINT_DIR / "phase1_lookup_table.pkl"
    CHECKPOINT_DIR.mkdir(exist_ok=True)
    with open(lookup_file, "wb") as f:
        pickle.dump(results_map, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"[SAFETY] Saved lookup table to {lookup_file} ({len(results_map)} signals)")
    
    return results_map

def process_single_signal_sequential(
    sig: SignalData, 
    bars_cache: Dict[int, List[tuple]], 
    strategy_grid: List[Dict]
) -> Tuple[int, Dict[int, Tuple[float, int]]]:
    """Sequential version for single-worker mode."""
    sid = sig.signal_id
    bars = bars_cache.get(sid, [])
    
    if len(bars) < 100:
        return (sid, None)
    
    # Use entry_time (signal + 17 min), matches db_batch_utils fetch
    entry_ts = int(sig.entry_time.timestamp())
    precomputed = precompute_bars(bars, entry_ts)
    if not precomputed:
        return (sid, None)
    
    n = len(strategy_grid)
    sig_results = np.zeros((n, 6), dtype=np.float32)
    for i in range(n):
        sp = strategy_grid[i]
        sl, dw, tm, lev, act, cb, rd, cool, re_s, pos_s = sp
        sig_results[i] = run_strategy_fast(precomputed, sl, dw, tm, lev, act, cb, rd, cool, re_s, pos_s)
    
    return (sid, sig_results)

def _print_phase1_progress(processed: int, total: int, skipped: int, start_time, total_strategies: int):
    """Helper to print progress."""
    elapsed = (datetime.now() - start_time).total_seconds()
    speed = processed / elapsed if elapsed > 0 else 0
    remaining = total - processed - skipped
    eta_sec = remaining / speed if speed > 0 else 0
    eta_min = eta_sec / 60
    
    iters_done = processed * total_strategies
    iter_speed = iters_done / elapsed if elapsed > 0 else 0
    
    print(f"[PRECOMPUTE] {processed:>4}/{total} signals | "
          f"{elapsed:>5.1f}s elapsed | "
          f"{speed:>5.1f} sig/s | "
          f"{iter_speed/1000:>6.1f}k iter/s | "
          f"ETA: {eta_min:>4.1f}m", flush=True)

def evaluate_filter_lookup(filter_cfg: Dict, strategy_grid: List) -> Tuple[Dict, Dict, int]:
    """Phase 2: Evaluate filter using LOOKUP TABLE. Returns only BEST strategy."""
    matched_signals = filter_signals_in_memory(WORKER_SIGNALS, filter_cfg)
    if len(matched_signals) < MIN_SIGNALS_FOR_EVAL:
        return filter_cfg, {}, 0

    by_pair: Dict[str, List[SignalInfo]] = {}
    for s in matched_signals:
        by_pair.setdefault(s.pair, []).append(SignalInfo(s.signal_id, s.pair, s.timestamp))

    n_strategies = len(strategy_grid)
    agg_pnl = np.zeros(n_strategies, dtype=np.float64)
    agg_trades = np.zeros(n_strategies, dtype=np.int32)
    agg_ts_wins = np.zeros(n_strategies, dtype=np.int32)
    agg_sl = np.zeros(n_strategies, dtype=np.int32)
    agg_to = np.zeros(n_strategies, dtype=np.int32)
    
    total_processed = 0
    
    for pair, pair_signals in by_pair.items():
        position_tracker_ts = 0
        sorted_signals = sorted(pair_signals, key=lambda x: x.timestamp)
        
        for sig in sorted_signals:
            if int(sig.timestamp.timestamp()) < position_tracker_ts:
                continue
            sig_res = WORKER_RESULTS.get(sig.signal_id)
            if sig_res is None:
                continue
            total_processed += 1
            agg_pnl += sig_res[:, 0]
            agg_trades += sig_res[:, 2].astype(np.int32)
            agg_ts_wins += sig_res[:, 3].astype(np.int32)
            agg_sl += sig_res[:, 4].astype(np.int32)
            agg_to += sig_res[:, 5].astype(np.int32)
            # Use median last_ts for position tracking (not max across all strategies)
            position_tracker_ts = max(position_tracker_ts, int(np.median(sig_res[:, 1])))
    
    if total_processed == 0:
        return filter_cfg, {}, 0
    
    # Return ONLY best strategy (not 64K dict)
    best_idx = int(agg_pnl.argmax())
    trades = int(agg_trades[best_idx])
    ts_wins = int(agg_ts_wins[best_idx])
    best_result = {
        best_idx: {
            "total_pnl": float(agg_pnl[best_idx]),
            "win_rate": (ts_wins / trades) if trades > 0 else 0.0,
            "total_trades": trades,
            "ts_wins": ts_wins,
            "sl_exits": int(agg_sl[best_idx]),
            "timeout_exits": int(agg_to[best_idx]),
        }
    }
    return filter_cfg, best_result, total_processed

# ---------------------------------------------------------------------------
# Evaluate a single filter configuration (PRELOADED VERSION - no SQL)
# ---------------------------------------------------------------------------
# Global cache - set by main() before parallel processing
PRELOADED_SIGNALS: List[SignalData] = []
PRELOADED_BARS: Dict[int, List[tuple]] = {}
# (MAX_SIGNALS_PER_FILTER removed - run_strategy is now O(n) instead of O(n²))

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
    parser.add_argument("--min-signals", type=int, default=5, help="Minimum signals required per filter (default 5)")
    parser.add_argument("--preload-workers", type=int, default=8, help="Number of DB connections for preload (default 8)")
    parser.add_argument("--batched", action="store_true", help="Use memory-efficient batch processing (recommended)")
    parser.add_argument("--batch-size", type=int, default=200, help="Signals per batch when --batched (default 200)")
    parser.add_argument("--save-bars", type=str, default="", help="Save precomputed bars to pickle file (run once, ~18h). Example: --save-bars bars_cache.pkl")
    parser.add_argument("--load-bars", type=str, default="", help="Load precomputed bars from pickle (skip DB, ~30min). Example: --load-bars bars_cache.pkl")
    args = parser.parse_args()

    global MAX_WORKERS, MIN_SIGNALS_FOR_EVAL
    MAX_WORKERS = args.workers
    MIN_SIGNALS_FOR_EVAL = args.min_signals

    filter_grid = generate_filter_grid()
    if args.limit > 0:
        filter_grid = filter_grid[:args.limit]
        print(f"Limiting filter grid to first {args.limit} configs for testing.")

    strategy_grid_dicts = generate_strategy_grid()  # keep dicts for JSON output
    strategy_grid = _grid_to_tuples(strategy_grid_dicts)  # tuples for fast inner loops
    print(f"Filter grid: {len(filter_grid)} combinations, Strategy grid: {len(strategy_grid)} combinations")
    print(f"Using {MAX_WORKERS} workers for parallel filter processing")

    # =========================================================================
    # PHASE 1: PRELOAD & PRECOMPUTE (The Heavy Lifting)
    # =========================================================================
    global PRELOADED_SIGNALS, PRELOADED_BARS
    
    print("\n" + "="*70)
    print("PHASE 1: Preloading & Precomputing Strategy Results")
    print("="*70)
    
    # 1. Load Signals (always needed — small, fast)
    all_signals = preload_all_signals()
    if not all_signals:
        print("ERROR: No signals loaded. Exiting.")
        return
    
    # ---- SAVE-BARS MODE: Cache precomputed bars to disk, then exit ----
    if args.save_bars:
        save_path = Path(args.save_bars)
        save_precomputed_bars(
            all_signals,
            bars_cache={},  # Not used — save_precomputed_bars loads its own
            output_path=save_path,
            preload_workers=args.preload_workers,
            batch_size=args.batch_size,
        )
        print(f"\n[DONE] Bars cached to {save_path}. Now run:")
        print(f"  python3 {sys.argv[0]} --load-bars {save_path} --batched --workers {MAX_WORKERS}")
        return
    
    # ---- LOAD-BARS MODE: Skip DB, run strategies from cached bars ----
    if args.load_bars:
        load_path = Path(args.load_bars)
        batch_files = load_precomputed_bars(load_path)
        if not batch_files:
            print("ERROR: Could not load bars cache. Exiting.")
            return
        
        n_strategies = len(strategy_grid)
        num_workers = min(MAX_WORKERS, mp.cpu_count())
        lookup_table: Dict[int, np.ndarray] = {}
        start_time = datetime.now()
        total_signals_done = 0
        
        print(f"\n[LOAD-BARS] Processing with {num_workers} workers, {n_strategies:,} strategies per signal")
        
        for bf_idx, batch_file in enumerate(batch_files):
            print(f"\n[LOAD-BARS] Loading {batch_file.name}...", flush=True)
            with open(batch_file, "rb") as f:
                batch_cache = pickle.load(f)
            batch_sids = list(batch_cache.keys())
            print(f"[LOAD-BARS] {len(batch_sids)} signals. Parallelizing across {num_workers} workers...", flush=True)
            
            # FIX: Set globals BEFORE fork — workers inherit via COW (no pickle copy!)
            global LOADBARS_GRID, LOADBARS_PC
            LOADBARS_GRID = strategy_grid
            LOADBARS_PC = batch_cache
            with mp.Pool(processes=num_workers) as pool:
                for i, (sid, result) in enumerate(pool.imap_unordered(loadbars_process_signal, batch_sids)):
                    if result is not None:
                        lookup_table[sid] = result
                    total_signals_done += 1
                    
                    if (i + 1) % 5 == 0 or (i + 1) == len(batch_sids):
                        elapsed = (datetime.now() - start_time).total_seconds()
                        speed = total_signals_done / elapsed if elapsed > 0 else 0
                        remaining = 1962 - total_signals_done  # approximate
                        eta = remaining / speed / 60 if speed > 0 else 0
                        mem_gb = total_signals_done * n_strategies * 6 * 4 / 1024**3
                        print(f"  [batch {bf_idx+1}/{len(batch_files)} sig {i+1}/{len(batch_sids)}] "
                              f"{speed:.1f} sig/s | {total_signals_done} done | "
                              f"~{mem_gb:.1f}GB RAM | ETA: {eta:.0f}min", flush=True)
            
            # Free batch bars immediately
            del batch_cache
            gc.collect()
        
        # Match signals
        signals_with_bars = [s for s in all_signals if s.signal_id in lookup_table]
        
        elapsed = (datetime.now() - start_time).total_seconds()
        total_iters = len(lookup_table) * n_strategies
        mem_gb = len(lookup_table) * n_strategies * 6 * 4 / 1024**3
        print(f"\n[LOAD-BARS] ✅ Phase 1 complete!")
        print(f"  {len(lookup_table)} signals × {n_strategies:,} strategies")
        print(f"  {total_iters/1e6:.1f}M iterations in {elapsed/60:.1f}min ({total_iters/elapsed:.0f} iter/s)")
        print(f"  Lookup table: ~{mem_gb:.1f}GB RAM (numpy float32)")
    
    # ---- STANDARD MODES (no --save-bars / --load-bars) ----
    else:
        # Check for saved lookup_table first (Phase 2 crash recovery)
        lookup_table = load_lookup_table()
        if lookup_table:
            print(f"[RECOVERY] Using saved lookup table - skipping Phase 1!")
            print(f"[RECOVERY] This saves ~10 hours of computation!")
            signals_with_bars = [s for s in all_signals if s.signal_id in lookup_table]
        elif args.batched:
            # BATCHED MODE: Memory-efficient, loads bars per batch
            print(f"\n[MODE] Using BATCHED processing (--batched)")
            print(f"[MODE] Batch size: {args.batch_size}, Workers: {MAX_WORKERS}\n")
            
            lookup_table = precompute_all_strategies_batched(
                all_signals,
                strategy_grid,
                num_workers=MAX_WORKERS,
                batch_size=args.batch_size,
                preload_workers=args.preload_workers,
                resume=True
            )
            signals_with_bars = [s for s in all_signals if s.signal_id in lookup_table]
        else:
            # ORIGINAL MODE: Load all bars upfront (requires lots of RAM)
            print(f"\n[MODE] Using ORIGINAL processing (all bars in memory)")
            print(f"[MODE] Consider --batched if running out of memory\n")
            
            all_signal_ids = [s.signal_id for s in all_signals]
            bars_cache = preload_all_bars(all_signal_ids, preload_workers=args.preload_workers)
            
            signals_with_bars = [s for s in all_signals if s.signal_id in bars_cache and len(bars_cache[s.signal_id]) >= 100]
            print(f"[PRELOAD] Signals with sufficient bars (>=100): {len(signals_with_bars)}")
            
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

            lookup_table = precompute_all_strategies(signals_with_bars, bars_cache, strategy_grid, num_workers=MAX_WORKERS)
        
            del bars_cache
            gc.collect()
            print("[MEMORY] Cleared bars cache. Ready for optimization.")
    
    crash_log("Phase 1 complete.")

    print("="*70)
    print("PHASE 2: Running optimization (Lookup Table Mode - Ultra Fast)")
    print("="*70 + "\n")
    crash_log("PHASE 2 START")

    # Prepare output file (streaming JSONL - write immediately, no memory accumulation)
    output_jsonl = OUTPUT_JSONL_FILE
    backup_jsonl = Path(str(output_jsonl) + ".backup")
    
    # Backup old results instead of deleting (safety in case of crash)
    if output_jsonl.exists():
        if backup_jsonl.exists():
            backup_jsonl.unlink()
        output_jsonl.rename(backup_jsonl)
        print(f"[SAFETY] Backed up previous results to {backup_jsonl}")
    
    results_count = 0
    best_results = []  # Keep only top 10 for final display
    first_write_done = False  # Track if we've written anything
    
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
            cfg, agg, count = _evaluate_filter_wrapper_lookup(task)
            if agg:

                # agg is now Dict[int, Dict[str, float]]
                # Find best strategy by PnL (or should valid winrate be the key? sticking to PnL for now but displaying stats)
                # Actually, user wants to filter by winrate, but this script just Outputs them.
                # Let's still sort by PnL but include WinRate in metrics.
                best_sid = max(agg, key=lambda k: agg[k]["total_pnl"])
                best_stats = agg[best_sid]
                best_params = strategy_grid_dicts[best_sid]
                
                result_entry = {
                    "filter": cfg,
                    "strategy": best_params,
                    "metrics": {
                        "total_pnl": best_stats["total_pnl"],
                        "win_rate": best_stats["win_rate"], # NEW METRIC
                        "total_trades": best_stats["total_trades"],
                        "ts_wins": best_stats["ts_wins"],
                        "strategy_id": best_sid,
                        "signal_count": count
                    },
                }
                
                # Stream to JSONL immediately (no memory accumulation)
                with open(output_jsonl, "a", encoding="utf-8") as f:
                    f.write(json.dumps(result_entry) + "\n")
                
                results_count += 1
                
                # Track first write (checkpoints now kept permanently)
                if not first_write_done:
                    first_write_done = True
                    # clear_phase1_checkpoint()  # DISABLED - keep checkpoints
                    print("[SAFETY] First result written - checkpoints KEPT for safety")
                
                # Track top 10 for display
                best_results.append(result_entry)
                if len(best_results) > 100:
                    best_results.sort(key=lambda x: x["metrics"]["total_pnl"], reverse=True)
                    best_results = best_results[:10]
    else:
        # Multiprocessing with fork-inherited globals (no pickle copy!)
        # Set globals BEFORE creating Pool - workers inherit via fork COW
        global WORKER_SIGNALS, WORKER_RESULTS
        WORKER_SIGNALS = signals_with_bars
        WORKER_RESULTS = lookup_table
        
        crash_log(f"Set globals: signals={len(signals_with_bars)}, lookup={len(lookup_table)}")
        print(f"[MEMORY] Set global lookup table ({len(lookup_table)} signals, {len(strategy_grid)} strategies)")
        
        # Phase 2 reads lookup_table via fork COW — safe to use all cores
        phase2_workers = MAX_WORKERS
        
        try:
            # Create Pool WITHOUT initargs - workers inherit globals via fork
            crash_log(f"BEFORE Pool creation (workers={phase2_workers})")
            print("[PHASE2] Creating worker pool...")
            with mp.Pool(processes=phase2_workers) as pool:
                crash_log("Pool CREATED successfully")
                print(f"[PHASE2] Pool created with {phase2_workers} workers (capped at 4)")
                print("[PHASE2] Starting filter optimization...")
                
                try:
                    from tqdm import tqdm
                    iterator = tqdm(pool.imap_unordered(_evaluate_filter_wrapper_lookup, tasks, chunksize=1000), 
                                  total=len(tasks), desc="Optimizing filters")
                except ImportError:
                    iterator = pool.imap_unordered(_evaluate_filter_wrapper_lookup, tasks, chunksize=1000)
                    print(f"Optimizing {len(filter_grid)} filter configurations...")

                for cfg, agg, count in iterator:
                    if agg:
                        # agg is Dict[int, Dict[str, float]] with keys: total_pnl, win_rate, total_trades, ts_wins
                        best_sid = max(agg, key=lambda k: agg[k]["total_pnl"])
                        best_stats = agg[best_sid]
                        best_params = strategy_grid_dicts[best_sid]
                        result_entry = {
                            "filter": cfg,
                            "strategy": best_params,
                            "metrics": {
                                "total_pnl": best_stats["total_pnl"],
                                "win_rate": best_stats["win_rate"],
                                "total_trades": best_stats["total_trades"],
                                "ts_wins": best_stats["ts_wins"],
                                "strategy_id": best_sid,
                                "signal_count": count
                            },
                        }
                        
                        # Stream to JSONL immediately (no memory accumulation)
                        with open(output_jsonl, "a", encoding="utf-8") as f:
                            f.write(json.dumps(result_entry) + "\n")
                        
                        results_count += 1
                        
                        # Log progress every 1000 results
                        if results_count % 1000 == 0:
                            crash_log(f"Phase 2 progress: {results_count} results written")
                        
                        # Track first write (checkpoints now kept permanently)
                        if not first_write_done:
                            first_write_done = True
                            # clear_phase1_checkpoint()  # DISABLED - keep checkpoints
                            print("[SAFETY] First result written - checkpoints KEPT for safety")
                        
                        # Track top 10 for display
                        best_results.append(result_entry)
                        if len(best_results) > 100:
                            best_results.sort(key=lambda x: x["metrics"]["total_pnl"], reverse=True)
                            best_results = best_results[:10]
                        
                        # Periodic memory cleanup
                        if results_count % 5000 == 0:
                            gc.collect()
        except Exception as e:
            import traceback
            print("\n" + "="*70)
            print("[PHASE2 CRASH] An error occurred in Phase 2!")
            print(f"[PHASE2 CRASH] Error: {e}")
            print("[PHASE2 CRASH] Traceback:")
            traceback.print_exc()
            print("="*70)
            print("[PHASE2 CRASH] Lookup table is saved - you can re-run to resume!")
            print(f"[PHASE2 CRASH] Results written so far: {results_count}")
            raise  # Re-raise to exit

    # -----------------------------------------------------------------------
    # Final results
    # -----------------------------------------------------------------------
    print("\n" + "="*70)
    print(f"Optimization complete. Found {results_count} valid configurations.")
    print(f"Results saved to: {output_jsonl}")
    
    if results_count == 0:
        print("No valid results found.")
        return

    # Sort top results for display
    best_results.sort(key=lambda x: x["metrics"]["total_pnl"], reverse=True)
    
    # Print Top 10
    print("\n🏆 TOP 10 CONFIGURATIONS:")
    print(f"{'#':<3} {'Score':<12} {'RSI':<5} {'Vol':<5} {'OI':<5} | {'Lev':<4} {'SL%':<5} {'Win':<4} {'Mult':<5} | {'Total PnL %':<12}")
    print("-" * 85)
    
    for i, res in enumerate(best_results[:10], 1):
        f = res["filter"]
        s = res["strategy"]
        m = res["metrics"]
        print(f"{i:<3} {f['score_min']}-{f['score_max']:<5} {f['rsi_min']:<5} {f['vol_min']:<5} {f['oi_min']:<5} | "
              f"{s['leverage']:<4} {s['sl_pct']:<5} {s['delta_window']:<4} {s['threshold_mult']:<5} | {m['total_pnl']:<12.2f}")
    
    # Also save as JSON for backwards compatibility (load from JSONL)
    print("\nConverting JSONL to JSON for backwards compatibility...")
    try:
        # Use external sort to avoid loading all into memory
        import subprocess
        
        # Count lines first
        line_count = sum(1 for _ in open(output_jsonl, "r", encoding="utf-8"))
        print(f"  Sorting {line_count} results...")
        
        # Sort by extracting total_pnl, sort numerically, then take top results
        # This avoids loading entire file into Python memory
        output_json = "optimization_results_unified.json"
        
        if line_count <= 50000:
            # Safe to load into memory for smaller files
            all_results = []
            with open(output_jsonl, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        all_results.append(json.loads(line))
            
            all_results.sort(key=lambda x: x["metrics"]["total_pnl"], reverse=True)
            
            with open(output_json, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"  Saved {len(all_results)} sorted results to {output_json}")
        else:
            # For very large files, just copy without sorting
            print(f"  Large file ({line_count} lines), saving without in-memory sort...")
            with open(output_json, "w") as f_out:
                f_out.write("[\n")
                first = True
                with open(output_jsonl, "r", encoding="utf-8") as f_in:
                    for line in f_in:
                        if line.strip():
                            if not first:
                                f_out.write(",\n")
                            f_out.write("  " + line.strip())
                            first = False
                f_out.write("\n]")
            print(f"  Saved to {output_json} (unsorted, use analyze_results.py to sort)")
            
    except Exception as e:
        print(f"Warning: Could not convert to JSON: {e}")

if __name__ == "__main__":
    mp.freeze_support()
    main()
