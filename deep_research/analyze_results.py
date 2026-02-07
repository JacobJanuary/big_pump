#!/usr/bin/env python3
"""
Smart strategy analyzer with Quality Score ranking.

Ranking formula:  quality_score = WR^2 × √PnL × timeout_penalty × trades_penalty

This eliminates "casino" strategies (high PnL, low WR) and favors
strategies with consistent trailing-stop exits AND decent profit.

Usage:
    python3 analyze_results.py
    python3 analyze_results.py --file optimization_results_enriched.jsonl
    python3 analyze_results.py --min-trades 10 --min-wr 0.2
"""
import json
import math
import argparse
from pathlib import Path
from collections import defaultdict
from typing import List, Dict


# ---------------------------------------------------------------------------
# Quality Score
# ---------------------------------------------------------------------------
def quality_score(metrics: Dict, min_trades: int = 5) -> float:
    """
    Compute quality score for a strategy result.
    
    Formula: WR^2 × √PnL × timeout_penalty × trades_penalty
    
    - WR^2: win rate dominates (0.87^2=0.76 vs 0.04^2=0.002)
    - √PnL: profit matters but sublinearly (√300=17 vs √2300=48)
    - timeout_penalty: 1.0 if TO<30%, scales down to 0.1 if TO>80%
    - trades_penalty: 1.0 if trades>=min_trades, else trades/min_trades
    """
    wr = metrics.get("win_rate", 0)
    pnl = max(metrics.get("total_pnl", 0), 0.01)  # avoid negative/zero
    trades = metrics.get("total_trades", 0)
    ts_wins = metrics.get("ts_wins", 0)
    sl_exits = metrics.get("sl_exits", 0)
    to_exits = metrics.get("timeout_exits", 0)

    if trades == 0:
        return 0.0

    # Timeout penalty: penalizes strategies that mostly timeout
    to_ratio = to_exits / trades if trades > 0 else 0
    if to_ratio <= 0.3:
        to_penalty = 1.0
    elif to_ratio <= 0.5:
        to_penalty = 0.7
    elif to_ratio <= 0.8:
        to_penalty = 0.3
    else:
        to_penalty = 0.1

    # Trades penalty: not enough data = unreliable
    tr_penalty = min(1.0, trades / min_trades) if min_trades > 0 else 1.0

    return (wr ** 2) * math.sqrt(pnl) * to_penalty * tr_penalty


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------
def load_results(file_path: str = None) -> List[Dict]:
    """Load results from JSONL or JSON file."""
    paths = [
        file_path,
        "optimization_results_enriched.jsonl",
        "optimization_results.jsonl",
        "optimization_results_unified.json",
    ]
    for p in paths:
        if p and Path(p).exists():
            print(f"Loading from {p}...")
            with open(p) as f:
                if p.endswith(".jsonl"):
                    return [json.loads(line) for line in f if line.strip()]
                else:
                    return json.load(f)
    print("ERROR: No results file found!")
    return []


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
def find_best_per_range(results: List[Dict], top_n: int = 1,
                        min_trades: int = 5, min_wr: float = 0.0) -> Dict[str, List[Dict]]:
    """Group by score range → rank by quality_score."""
    buckets = defaultdict(list)
    for r in results:
        key = f"{r['filter']['score_min']}-{r['filter']['score_max']}"
        buckets[key].append(r)

    best = {}
    for sr, configs in buckets.items():
        # Filter by minimum requirements
        valid = [c for c in configs
                 if c["metrics"].get("total_trades", 0) >= min_trades
                 and c["metrics"].get("win_rate", 0) >= min_wr
                 and c["metrics"].get("total_pnl", 0) > 0]
        if not valid:
            valid = [c for c in configs if c["metrics"].get("total_pnl", 0) > 0]

        scored = sorted(valid, key=lambda x: quality_score(x["metrics"], min_trades), reverse=True)
        best[sr] = scored[:top_n]
    return best


def create_composite_strategy(best_per_range: Dict[str, List[Dict]]) -> List[Dict]:
    """Create composite strategy rules from best-per-range."""
    rules = []
    for sr in sorted(best_per_range.keys(), key=lambda x: int(x.split("-")[0])):
        configs = best_per_range[sr]
        if not configs:
            continue
        b = configs[0]
        f, s, m = b["filter"], b["strategy"], b["metrics"]
        qs = quality_score(m)

        rules.append({
            "priority": len(rules) + 1,
            "score_range": sr,
            "quality_score": round(qs, 2),
            "filter": {
                "score_min": f["score_min"],
                "score_max": f["score_max"],
                "rsi_min": f["rsi_min"],
                "vol_min": f["vol_min"],
                "oi_min": f["oi_min"],
            },
            "strategy": {
                "leverage": s["leverage"],
                "sl_pct": s["sl_pct"],
                "delta_window": s["delta_window"],
                "threshold_mult": s["threshold_mult"],
                "base_activation": s.get("base_activation", 10.0),
                "base_callback": s.get("base_callback", 4.0),
                "base_reentry_drop": s.get("base_reentry_drop", 5.0),
                "base_cooldown": s.get("base_cooldown", 300),
                "max_reentry_hours": s.get("max_reentry_hours", 48),
                "max_position_hours": s.get("max_position_hours", 24),
            },
            "metrics": {
                "total_pnl": round(m.get("total_pnl", 0), 1),
                "win_rate": round(m.get("win_rate", 0), 4),
                "total_trades": m.get("total_trades", 0),
                "ts_wins": m.get("ts_wins", 0),
                "sl_exits": m.get("sl_exits", 0),
                "timeout_exits": m.get("timeout_exits", 0),
            },
        })
    return rules


def print_report(rules: List[Dict]):
    """Print composite strategy report."""
    print(f"\n{'='*110}")
    print(f"{'🎯 SMART COMPOSITE STRATEGY (Quality Score Ranking)':^110}")
    print(f"{'='*110}")

    hdr = (f"{'#':<3} {'Score':<10} {'QS':<7} | "
           f"{'WR':<7} {'PnL':<8} {'Trades':<7} {'TS':<5} {'SL':<5} {'TO':<5} | "
           f"{'Act':<5} {'CB':<4} {'SL%':<4} {'PosH':<5} {'DW':<6}")
    print(hdr)
    print("-" * 110)

    total_pnl = 0
    for r in rules:
        m, s = r["metrics"], r["strategy"]
        total_pnl += m["total_pnl"]
        to_flag = " ⚠️" if m["timeout_exits"] > m["total_trades"] * 0.3 else ""
        print(f"{r['priority']:<3} {r['score_range']:<10} {r['quality_score']:<7.1f} | "
              f"{m['win_rate']:<7.1%} {m['total_pnl']:<8.0f} {m['total_trades']:<7} "
              f"{m['ts_wins']:<5} {m['sl_exits']:<5} {m['timeout_exits']:<5} | "
              f"{s['base_activation']:<5} {s['base_callback']:<4} {s['sl_pct']:<4} "
              f"{s['max_position_hours']:<5} {s['delta_window']:<6}{to_flag}")

    print("-" * 110)
    avg_wr = sum(r["metrics"]["win_rate"] for r in rules) / len(rules) if rules else 0
    print(f"    {'TOTAL':<10} {'':7} | {avg_wr:<7.1%} {total_pnl:<8.0f}")
    print(f"{'='*110}")
    print("\n⚠️  Combined PnL assumes independent ranges. Real PnL is ~3-5x lower (cross-blocking).")


def save_composite_strategy(rules: List[Dict], output_file: str):
    """Save strategy to JSON."""
    output = {
        "version": "2.0",
        "description": "Quality-ranked composite strategy (WR²×√PnL scoring)",
        "rules": rules,
        "total_expected_pnl": sum(r["metrics"]["total_pnl"] for r in rules),
        "avg_win_rate": round(
            sum(r["metrics"]["win_rate"] for r in rules) / len(rules), 4
        ) if rules else 0,
    }
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n💾 Saved to {output_file}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Smart strategy analyzer (Quality Score)")
    parser.add_argument("--file", type=str, help="Path to results file")
    parser.add_argument("--top", type=int, default=1, help="Top N per score range")
    parser.add_argument("--output", type=str, default="composite_strategy.json")
    parser.add_argument("--min-trades", type=int, default=10, help="Min trades for full confidence")
    parser.add_argument("--min-wr", type=float, default=0.0, help="Min win rate filter (0.0-1.0)")
    parser.add_argument("--min-pnl", type=float, default=0, help="Min PnL (skip negative ranges)")
    args = parser.parse_args()

    results = load_results(args.file)
    if not results:
        return

    print(f"Loaded {len(results):,} entries")

    best = find_best_per_range(results, args.top, args.min_trades, args.min_wr)
    rules = create_composite_strategy(best)

    # Filter by min PnL
    if args.min_pnl > 0:
        before = len(rules)
        rules = [r for r in rules if r["metrics"]["total_pnl"] >= args.min_pnl]
        # Re-number priorities
        for i, r in enumerate(rules):
            r["priority"] = i + 1
        print(f"Filtered: {before} → {len(rules)} rules (PnL >= {args.min_pnl})")

    print_report(rules)
    save_composite_strategy(rules, args.output)


if __name__ == "__main__":
    main()
