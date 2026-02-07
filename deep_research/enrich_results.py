#!/usr/bin/env python3
"""
enrich_results.py — Memory-efficient post-processor to add win_rate to existing JSONL.

Architecture (low-memory):
  Phase A: Read JSONL → extract 524 unique strategy IDs
  Phase B: Load signals metadata from DB (small: ~2000 rows)
  Phase C: For EACH signal (one at a time):
           - Load bars from DB (single query)
           - Precompute
           - Run ONLY the 524 needed strategies
           - Store compact results: {signal_id: {strategy_id: (pnl, last_ts, trades, ts_wins, sl, to)}}
           - Free bars immediately
  Phase D: For each JSONL line:
           - Filter signals matching the filter config
           - Aggregate stats from the lookup table
           - Write enriched line

Peak memory: ~10MB (one signal's bars) + ~50MB (lookup table) = ~60MB total.
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
    SignalData,
    SignalInfo,
    generate_strategy_grid,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_JSONL = Path("optimization_results.jsonl")
OUTPUT_JSONL = Path("optimization_results_enriched.jsonl")


def load_bars_for_signal(signal_id: int) -> List[tuple]:
    """Load bars for a SINGLE signal from DB. Minimal memory footprint."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ts, price, delta
                    FROM web.signal_bars
                    WHERE signal_id = %s
                    ORDER BY ts
                """, (signal_id,))
                return cur.fetchall()
    except Exception as e:
        print(f"  [WARN] Failed to load bars for signal {signal_id}: {e}")
        return []


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enrich optimization results with win_rate (memory-efficient)")
    parser.add_argument("--input", type=str, default=str(INPUT_JSONL), help="Input JSONL file")
    parser.add_argument("--output", type=str, default=str(OUTPUT_JSONL), help="Output enriched JSONL file")
    parser.add_argument("--limit", type=int, default=0, help="Limit JSONL lines to process (0=all)")
    args = parser.parse_args()

    input_file = Path(args.input)
    output_file = Path(args.output)

    # =========================================================================
    # PHASE A: Read JSONL → extract unique strategy IDs
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"PHASE A: Reading {input_file}")
    print(f"{'='*70}")

    results = []
    with open(input_file, "r") as f:
        for i, line in enumerate(f):
            if args.limit > 0 and i >= args.limit:
                break
            results.append(json.loads(line.strip()))

    strategy_grid = generate_strategy_grid()
    unique_sids = set(r["metrics"]["strategy_id"] for r in results)
    print(f"  {len(results):,} result entries, {len(unique_sids)} unique strategies")

    # Build strategy params lookup for the needed strategies only
    strategy_params = {sid: strategy_grid[sid] for sid in unique_sids}

    # =========================================================================
    # PHASE B: Load signal metadata (small — just IDs, scores, timestamps)
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"PHASE B: Loading signal metadata from DB")
    print(f"{'='*70}")

    all_signals = preload_all_signals()
    if not all_signals:
        print("ERROR: No signals loaded. Exiting.")
        return

    print(f"  {len(all_signals)} signals loaded (metadata only, no bars yet)")

    # =========================================================================
    # PHASE C: Process signals ONE AT A TIME → build lookup table
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"PHASE C: Running {len(unique_sids)} strategies across {len(all_signals)} signals")
    print(f"  Memory mode: ONE signal's bars in memory at a time")
    print(f"{'='*70}")

    # lookup[signal_id][strategy_id] = (pnl, last_ts, trades, ts_wins, sl, to)
    lookup: Dict[int, Dict[int, Tuple]] = {}
    skipped = 0
    start_time = datetime.now()

    for sig_idx, sig in enumerate(all_signals):
        # 1. Load bars for THIS signal only
        bars = load_bars_for_signal(sig.signal_id)
        if len(bars) < 100:
            skipped += 1
            continue

        # 2. Precompute
        entry_ts = int(sig.entry_time.timestamp())
        pc = precompute_bars(bars, entry_ts)
        if not pc:
            skipped += 1
            del bars
            continue

        # 3. Run ONLY the needed strategies (524, not 9600)
        sig_results = {}
        for sid, sp in strategy_params.items():
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
            sig_results[sid] = (pnl, last_ts, t_cnt, ts_w, sl_ex, to_ex)

        lookup[sig.signal_id] = sig_results

        # 4. Free bars immediately
        del bars, pc
        if (sig_idx + 1) % 50 == 0:
            gc.collect()

        # Progress
        if (sig_idx + 1) % 100 == 0 or (sig_idx + 1) == len(all_signals):
            elapsed = (datetime.now() - start_time).total_seconds()
            speed = (sig_idx + 1) / elapsed if elapsed > 0 else 0
            eta = (len(all_signals) - sig_idx - 1) / speed if speed > 0 else 0
            print(f"  [{sig_idx+1:>5}/{len(all_signals)}] "
                  f"{speed:.1f} sig/s | ETA: {eta/60:.1f}m | "
                  f"lookup: {len(lookup)} entries | skipped: {skipped}", flush=True)

    elapsed_c = (datetime.now() - start_time).total_seconds()
    print(f"\n  Phase C done in {elapsed_c:.0f}s ({elapsed_c/60:.1f}m)")
    print(f"  Lookup: {len(lookup)} signals × {len(unique_sids)} strategies")
    print(f"  Skipped: {skipped}")

    # =========================================================================
    # PHASE D: Enrich JSONL using lookup table
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"PHASE D: Enriching {len(results):,} JSONL lines")
    print(f"{'='*70}")

    start_d = datetime.now()

    with open(output_file, "w", encoding="utf-8") as out_f:
        for idx, result in enumerate(results):
            filter_cfg = result["filter"]
            strategy_id = result["metrics"]["strategy_id"]

            score_min = filter_cfg["score_min"]
            score_max = filter_cfg["score_max"]
            rsi_min = filter_cfg["rsi_min"]
            vol_min = filter_cfg["vol_min"]
            oi_min = filter_cfg["oi_min"]

            # Filter signals
            matched = [s for s in all_signals
                       if score_min <= s.score < score_max
                       and s.rsi >= rsi_min
                       and s.vol_zscore >= vol_min
                       and s.oi_delta >= oi_min]

            # Group by pair for position tracking
            by_pair: Dict[str, List[SignalData]] = defaultdict(list)
            for s in matched:
                by_pair[s.pair].append(s)

            total_trades = 0
            ts_wins = 0
            sl_exits_total = 0
            timeout_exits_total = 0

            for pair, pair_signals in by_pair.items():
                position_tracker_ts = 0
                for sig in sorted(pair_signals, key=lambda x: x.timestamp):
                    sig_ts = int(sig.timestamp.timestamp())
                    if sig_ts < position_tracker_ts:
                        continue

                    sig_res = lookup.get(sig.signal_id)
                    if not sig_res or strategy_id not in sig_res:
                        continue

                    pnl, last_ts, t_cnt, ts_w, sl_ex, to_ex = sig_res[strategy_id]
                    total_trades += t_cnt
                    ts_wins += ts_w
                    sl_exits_total += sl_ex
                    timeout_exits_total += to_ex

                    if last_ts > position_tracker_ts:
                        position_tracker_ts = last_ts

            win_rate = (ts_wins / total_trades) if total_trades > 0 else 0.0

            result["metrics"]["win_rate"] = round(win_rate, 4)
            result["metrics"]["total_trades"] = total_trades
            result["metrics"]["ts_wins"] = ts_wins
            result["metrics"]["sl_exits"] = sl_exits_total
            result["metrics"]["timeout_exits"] = timeout_exits_total

            out_f.write(json.dumps(result) + "\n")

            if (idx + 1) % 5000 == 0:
                elapsed = (datetime.now() - start_d).total_seconds()
                print(f"  [{idx+1:>6}/{len(results)}] {(idx+1)/elapsed:.0f} lines/s", flush=True)

    elapsed_d = (datetime.now() - start_d).total_seconds()
    print(f"\n  Phase D done in {elapsed_d:.1f}s")

    # =========================================================================
    # Summary
    # =========================================================================
    total_time = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*70}")
    print(f"DONE in {total_time/60:.1f}m total")
    print(f"  Output: {output_file}")
    print(f"{'='*70}")

    # Top results
    print(f"\nTop 10 by win_rate (PnL > 100):")
    top = sorted(
        [r for r in results if r["metrics"]["total_pnl"] > 100],
        key=lambda x: x["metrics"]["win_rate"], reverse=True
    )[:10]
    for i, r in enumerate(top):
        m = r["metrics"]
        s = r["strategy"]
        print(f"  {i+1}. WR={m['win_rate']:.1%} PnL={m['total_pnl']:.0f}% "
              f"Trades={m['total_trades']} TS={m['ts_wins']} "
              f"Act={s['base_activation']} SL={s['sl_pct']} PosH={s['max_position_hours']}")


if __name__ == "__main__":
    main()
