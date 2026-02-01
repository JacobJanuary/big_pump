#!/usr/bin/env python3
"""
Analyze optimization results and create a composite trading strategy.

This script:
1. Loads results from optimization_results_unified.json
2. Groups by Score Range
3. Finds the best filter+strategy for each range
4. Creates a composite strategy that covers all score ranges

Usage:
    python3 analyze_results.py
    python3 analyze_results.py --file path/to/results.json
    python3 analyze_results.py --top 5  # Show top 5 per range
"""
import json
import argparse
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Any


def load_results(file_path: str = None) -> List[Dict]:
    """Load results from JSON file."""
    paths_to_try = [
        file_path,
        "optimization_results_unified.json",
        "deep_research/optimization_results_unified.json",
        "filter_strategy_optimization.json",
    ]
    
    for path in paths_to_try:
        if path and Path(path).exists():
            print(f"Loading results from {path}...")
            with open(path, "r") as f:
                return json.load(f)
    
    print("ERROR: No results file found!")
    print("Tried: ", [p for p in paths_to_try if p])
    return []


def aggregate_by_score_range(results: List[Dict]) -> Dict[str, List[Dict]]:
    """Group results by score range."""
    buckets = defaultdict(list)
    for r in results:
        key = f"{r['filter']['score_min']}-{r['filter']['score_max']}"
        buckets[key].append(r)
    return buckets


def find_best_per_range(buckets: Dict[str, List[Dict]], top_n: int = 1) -> Dict[str, List[Dict]]:
    """Find best configurations per score range."""
    best_per_range = {}
    for score_range, configs in buckets.items():
        # Sort by PnL descending
        sorted_configs = sorted(configs, key=lambda x: x["metrics"]["total_pnl"], reverse=True)
        best_per_range[score_range] = sorted_configs[:top_n]
    return best_per_range


def create_composite_strategy(best_per_range: Dict[str, List[Dict]], min_pnl: float = 0) -> List[Dict]:
    """Create composite strategy rules.
    
    Args:
        min_pnl: Minimum PnL threshold. Ranges with lower PnL are skipped.
    """
    rules = []
    skipped = []
    
    for score_range in sorted(best_per_range.keys(), key=lambda x: int(x.split("-")[0])):
        configs = best_per_range[score_range]
        if not configs:
            continue
            
        best = configs[0]
        f = best["filter"]
        s = best["strategy"]
        m = best["metrics"]
        
        # Skip unprofitable ranges
        if m["total_pnl"] < min_pnl:
            skipped.append((score_range, m["total_pnl"]))
            continue
        
        rule = {
            "priority": len(rules) + 1,
            "score_range": score_range,
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
                # Trailing exit parameters
                "base_activation": s.get("base_activation", 10.0),
                "base_callback": s.get("base_callback", 4.0),
                "base_reentry_drop": s.get("base_reentry_drop", 5.0),
                "base_cooldown": s.get("base_cooldown", 300),
                # Time limits
                "max_reentry_hours": s.get("max_reentry_hours", 48),
                "max_position_hours": s.get("max_position_hours", 24),
            },
            "expected_pnl": m["total_pnl"],
        }
        rules.append(rule)
    
    if skipped:
        print(f"\n⚠️  Skipping {len(skipped)} unprofitable ranges (PnL < {min_pnl}%):")
        for sr, pnl in skipped:
            print(f"   - {sr}: {pnl:.2f}%")
    
    return rules


def print_composite_report(rules: List[Dict], best_per_range: Dict[str, List[Dict]]):
    """Print human-readable composite strategy report."""
    print("\n" + "="*90)
    print(f"{'🎯 COMPOSITE TRADING STRATEGY':^90}")
    print("="*90)
    print("\nThis strategy combines the best configurations for each score range.\n")
    
    total_expected_pnl = sum(r["expected_pnl"] for r in rules)
    
    print(f"{'#':<3} {'Score':<12} {'RSI>=':<6} {'Vol>=':<6} {'OI>=':<6} | {'Lev':<5} {'SL%':<5} {'Win':<6} {'Mult':<5} | {'PnL %':<12}")
    print("-"*90)
    
    for rule in rules:
        f = rule["filter"]
        s = rule["strategy"]
        print(f"{rule['priority']:<3} {rule['score_range']:<12} {f['rsi_min']:<6} {f['vol_min']:<6} {f['oi_min']:<6} | "
              f"{s['leverage']:<5} {s['sl_pct']:<5} {s['delta_window']:<6} {s['threshold_mult']:<5} | {rule['expected_pnl']:<12.2f}")
    
    print("-"*90)
    print(f"{'COMBINED EXPECTED PnL:':<69} | {total_expected_pnl:<12.2f}")
    print("="*90)
    print("\n⚠️  WARNING: Above sum assumes score ranges are INDEPENDENT.")
    print("   In real trading, positions block overlapping signals across ALL ranges.")
    print("   Realistic expected PnL is ~3-5x lower due to cross-range blocking.")
    
    print("\n📋 STRATEGY EXECUTION LOGIC:")
    print("─"*60)
    for rule in rules:
        f = rule["filter"]
        s = rule["strategy"]
        print(f"""
Rule {rule['priority']}: Score {rule['score_range']}
  IF total_score >= {f['score_min']} AND total_score < {f['score_max']}
     AND rsi >= {f['rsi_min']}
     AND volume_zscore >= {f['vol_min']}
     AND oi_delta_pct >= {f['oi_min']}
  THEN:
     Open LONG with leverage = {s['leverage']}x
     Stop Loss = {s['sl_pct']}%
     Exit Window = {s['delta_window']}s
     Threshold Mult = {s['threshold_mult']}
     Trailing: activation={s.get('base_activation', 10.0)}%, callback={s.get('base_callback', 4.0)}%
     Cooldown = {s.get('base_cooldown', 300)}s
     Max Reentry Window = {s.get('max_reentry_hours', 48)}h
     Max Position Time = {s.get('max_position_hours', 24)}h
""")
    
    print("="*90)


def save_composite_strategy(rules: List[Dict], output_file: str = "composite_strategy.json"):
    """Save composite strategy to JSON."""
    output = {
        "version": "1.0",
        "description": "Composite trading strategy combining best configs per score range",
        "rules": rules,
        "total_expected_pnl": sum(r["expected_pnl"] for r in rules),
    }
    
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n💾 Saved composite strategy to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Analyze optimization results")
    parser.add_argument("--file", type=str, help="Path to results JSON file")
    parser.add_argument("--top", type=int, default=1, help="Top N configs per range to show")
    parser.add_argument("--output", type=str, default="composite_strategy.json", help="Output file")
    parser.add_argument("--min-pnl", type=float, default=0, help="Minimum PnL to include range (default: 0, skip negative)")
    args = parser.parse_args()
    
    results = load_results(args.file)
    if not results:
        return
    
    print(f"Loaded {len(results):,} configurations")
    
    # Group by score range
    buckets = aggregate_by_score_range(results)
    print(f"Found {len(buckets)} score ranges")
    
    # Find best per range
    best_per_range = find_best_per_range(buckets, args.top)
    
    # Create composite strategy (skip ranges below min_pnl)
    rules = create_composite_strategy(best_per_range, min_pnl=args.min_pnl)
    
    # Print report
    print_composite_report(rules, best_per_range)
    
    # Save to file
    save_composite_strategy(rules, args.output)


if __name__ == "__main__":
    main()
