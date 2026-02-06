#!/usr/bin/env python3
"""
enrich_results.py — Post-processing script to add win_rate metrics
to an existing optimization_results.jsonl WITHOUT re-running the full 24h Phase 1.

Strategy:
1. Read existing JSONL → extract unique (filter_cfg, strategy_id) combos
2. Load ALL signals from DB (same as optimize_unified.py)
3. Load bars for ALL signals (same preload_all_bars)
4. For each unique combo: filter signals → simulate → compute win_rate
5. Write enriched JSONL with win_rate, total_trades, ts_wins

This is O(unique_combos * matched_signals) instead of O(9600 * all_signals).
With 524 unique strategies and ~2000 signals, this runs in ~1-2 hours, not 24.
"""

import json
import sys
import os
import gc
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, NamedTuple
from collections import defaultdict

# Setup paths
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))
sys.path.append(str(current_dir.parent / "scripts_v2"))

from pump_analysis_lib import get_db_connection
from optimize_combined_leverage_filtered import precompute_bars, run_strategy_fast
from optimize_unified import (
    preload_all_signals, 
    preload_all_bars, 
    SignalData, 
    SignalInfo,
    generate_strategy_grid,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_JSONL = Path("optimization_results.jsonl")
OUTPUT_JSONL = Path("optimization_results_enriched.jsonl")
PRELOAD_WORKERS = 8


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enrich optimization results with win_rate metric")
    parser.add_argument("--input", type=str, default=str(INPUT_JSONL), help="Input JSONL file")
    parser.add_argument("--output", type=str, default=str(OUTPUT_JSONL), help="Output enriched JSONL file")
    parser.add_argument("--preload-workers", type=int, default=PRELOAD_WORKERS, help="DB workers for bar loading")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of lines to process (0=all)")
    args = parser.parse_args()

    input_file = Path(args.input)
    output_file = Path(args.output)

    # =========================================================================
    # STEP 1: Read existing JSONL and extract unique combos
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"[STEP 1] Reading existing results from {input_file}")
    print(f"{'='*70}")

    results = []
    with open(input_file, "r") as f:
        for i, line in enumerate(f):
            if args.limit > 0 and i >= args.limit:
                break
            results.append(json.loads(line.strip()))

    print(f"  Loaded {len(results):,} result entries")

    # Extract unique strategy IDs and their params
    strategy_grid = generate_strategy_grid()
    unique_strategy_ids = set()
    for r in results:
        unique_strategy_ids.add(r["metrics"]["strategy_id"])

    print(f"  Unique strategy IDs: {len(unique_strategy_ids)}")

    # =========================================================================
    # STEP 2: Load signals and bars
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"[STEP 2] Loading signals and bars from database")
    print(f"{'='*70}")

    all_signals = preload_all_signals()
    if not all_signals:
        print("ERROR: No signals loaded. Exiting.")
        return

    # Build signal lookup by ID
    signal_by_id: Dict[int, SignalData] = {s.signal_id: s for s in all_signals}

    # Load bars for ALL signals
    all_sids = [s.signal_id for s in all_signals]
    print(f"\n  Loading bars for {len(all_sids)} signals...")
    bars_cache = preload_all_bars(all_sids, preload_workers=args.preload_workers)
    print(f"  Bars loaded for {len(bars_cache)} signals")

    # =========================================================================
    # STEP 3: Precompute bars for all signals (once)
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"[STEP 3] Precomputing bar data for all signals")
    print(f"{'='*70}")

    precomputed_cache: Dict[int, dict] = {}
    skipped = 0
    for sig in all_signals:
        bars = bars_cache.get(sig.signal_id, [])
        if len(bars) < 100:
            skipped += 1
            continue
        entry_ts = int(sig.entry_time.timestamp())
        pc = precompute_bars(bars, entry_ts)
        if pc:
            precomputed_cache[sig.signal_id] = pc

    print(f"  Precomputed: {len(precomputed_cache)} signals ({skipped} skipped)")

    # Free bars cache — we only need precomputed from now
    del bars_cache
    gc.collect()

    # =========================================================================
    # STEP 4: For each result line, simulate the strategy and compute win_rate
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"[STEP 4] Enriching {len(results):,} results with win_rate")
    print(f"{'='*70}")

    # Pre-build signal index by (pair, score_range, filters) for fast filtering
    # Actually, we'll just filter in-memory each time (same as optimize_unified)

    enriched_count = 0
    start_time = datetime.now()

    with open(output_file, "w", encoding="utf-8") as out_f:
        for idx, result in enumerate(results):
            filter_cfg = result["filter"]
            strategy_id = result["metrics"]["strategy_id"]
            sp = strategy_grid[strategy_id]

            # Filter signals matching this filter config
            score_min = filter_cfg["score_min"]
            score_max = filter_cfg["score_max"]
            rsi_min = filter_cfg["rsi_min"]
            vol_min = filter_cfg["vol_min"]
            oi_min = filter_cfg["oi_min"]

            matched_signals = []
            for s in all_signals:
                if (score_min <= s.score < score_max and
                    s.rsi >= rsi_min and
                    s.vol_zscore >= vol_min and
                    s.oi_delta >= oi_min):
                    matched_signals.append(s)

            # Group by pair for sequential position tracking
            by_pair: Dict[str, List[SignalData]] = defaultdict(list)
            for s in matched_signals:
                by_pair[s.pair].append(s)

            total_trades = 0
            ts_wins = 0
            sl_exits_total = 0
            timeout_exits_total = 0

            for pair, pair_signals in by_pair.items():
                position_tracker_ts = 0
                sorted_sigs = sorted(pair_signals, key=lambda x: x.timestamp)

                for sig in sorted_sigs:
                    sig_ts = int(sig.timestamp.timestamp())
                    if sig_ts < position_tracker_ts:
                        continue

                    pc = precomputed_cache.get(sig.signal_id)
                    if not pc:
                        continue

                    pnl, last_ts, t_cnt, ts_w, sl_ex, to_ex = run_strategy_fast(
                        pc,
                        sp["sl_pct"],
                        sp["delta_window"],
                        sp["threshold_mult"],
                        sp["leverage"],
                        sp.get("base_activation", 10.0),
                        sp.get("base_callback", 4.0),
                        sp.get("base_reentry_drop", 5.0),
                        sp.get("base_cooldown", 300),
                        sp.get("max_reentry_hours", 0) * 3600,
                        sp.get("max_position_hours", 0) * 3600,
                    )

                    total_trades += t_cnt
                    ts_wins += ts_w
                    sl_exits_total += sl_ex
                    timeout_exits_total += to_ex

                    if last_ts > position_tracker_ts:
                        position_tracker_ts = last_ts

            win_rate = (ts_wins / total_trades) if total_trades > 0 else 0.0

            # Enrich the result
            result["metrics"]["win_rate"] = round(win_rate, 4)
            result["metrics"]["total_trades"] = total_trades
            result["metrics"]["ts_wins"] = ts_wins
            result["metrics"]["sl_exits"] = sl_exits_total
            result["metrics"]["timeout_exits"] = timeout_exits_total

            out_f.write(json.dumps(result) + "\n")
            enriched_count += 1

            # Progress
            if (idx + 1) % 500 == 0 or (idx + 1) == len(results):
                elapsed = (datetime.now() - start_time).total_seconds()
                speed = (idx + 1) / elapsed if elapsed > 0 else 0
                remaining = len(results) - idx - 1
                eta = remaining / speed if speed > 0 else 0
                print(f"  [{idx+1:>6}/{len(results)}] "
                      f"{speed:.1f} results/s | "
                      f"ETA: {eta/60:.1f}m | "
                      f"Last win_rate: {win_rate:.2%}", flush=True)

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*70}")
    print(f"[DONE] Enriched {enriched_count:,} results in {elapsed:.1f}s ({elapsed/60:.1f}m)")
    print(f"  Output: {output_file}")
    print(f"{'='*70}")

    # Quick stats
    print(f"\n[STATS] Top 10 by win_rate (with PnL > 100):")
    top_results = sorted(
        [r for r in results if r["metrics"]["total_pnl"] > 100],
        key=lambda x: x["metrics"]["win_rate"],
        reverse=True
    )[:10]
    for i, r in enumerate(top_results):
        m = r["metrics"]
        s = r["strategy"]
        print(f"  {i+1}. WR={m['win_rate']:.1%} | PnL={m['total_pnl']:.0f}% | "
              f"Trades={m['total_trades']} | TS_wins={m['ts_wins']} | "
              f"Act={s['base_activation']} | CB={s['base_callback']} | "
              f"SL={s['sl_pct']} | PosH={s['max_position_hours']}")


if __name__ == "__main__":
    main()
