#!/usr/bin/env python3
import json
import os
from collections import defaultdict
import glob
from pathlib import Path

def load_results():
    """Load results from either the main JSON output or the intermediate JSONL."""
    json_path = Path("scripts_v2/filter_strategy_optimization.json")
    jsonl_path = Path("intermediate_results.jsonl")
    
    # Try local paths if script is run from project root
    if not json_path.exists():
        json_path = Path("filter_strategy_optimization.json")
    
    data = []
    
    # Priority 1: Full JSON file
    if json_path.exists():
        print(f"Loading full results from {json_path}...")
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"Error loading JSON: {e}")
            
    # Priority 2: Intermediate JSONL
    # Find the newest jsonl file
    files = glob.glob("**/intermediate_results.jsonl", recursive=True)
    if not files:
        print("No result files found! Please ensure 'filter_strategy_optimization.json' or 'intermediate_results.jsonl' exists.")
        return []
        
    target_file = sorted(files, key=os.path.getmtime)[-1]
    print(f"Loading intermediate results from {target_file}...")
    
    with open(target_file, "r") as f:
        for line in f:
            if line.strip():
                try:
                    data.append(json.loads(line))
                except:
                    pass
    return data

def aggregate_rules(results):
    """
    Cluster results by Score bucket and find the dominant strategy for each cluster.
    We assume the optimizer used Step 50 for scores.
    """
    # Bucketize by Score Range (e.g., "100-150")
    score_buckets = defaultdict(list)
    
    print(f"Analyzing {len(results)} successful configurations...")
    
    for res in results:
        # Extract metadata
        f = res["filter"]
        score_key = f"{f['score_min']}-{f['score_max']}"
        
        # We only care about configs that actually made money
        if res["metrics"]["total_pnl"] > 0:
            score_buckets[score_key].append(res)
            
    # For each bucket, find the "Best Logic"
    # We want to find a strategy that is stable across different secondary filters (RSI/Vol/OI)
    final_rules = []
    
    sorted_keys = sorted(score_buckets.keys(), key=lambda x: int(x.split('-')[0]))
    
    for score_key in sorted_keys:
        configs = score_buckets[score_key]
        if not configs:
            continue
            
        # 1. Find the absolute best PnL in this bucket
        best_cfg = max(configs, key=lambda x: x["metrics"]["total_pnl"])
        
        # 2. Check stability: Average WinRate in top 10 configs for this bucket
        top_10 = sorted(configs, key=lambda x: x["metrics"]["total_pnl"], reverse=True)[:10]
        avg_wr = sum(c["metrics"]["win_rate"] for c in top_10) / len(top_10)
        max_pnl = best_cfg["metrics"]["total_pnl"]
        
        # 3. Extract the winning strategy parameters
        strat = best_cfg["strategy"]
        filt = best_cfg["filter"]
        
        rule = {
            "score_range": score_key,
            "condition": f"RSI > {filt.get('rsi_min', 0)} AND Vol > {filt.get('vol_min', 0)} AND OI > {filt.get('oi_min', 0)}",
            "action": {
                "leverage": strat.get('leverage'),
                "stop_loss": strat.get('sl'),
                "take_profit_window": strat.get('window'), # seconds
                "threshold": strat.get('threshold')
            },
            "stats": {
                "max_pnl": round(max_pnl, 2),
                "avg_win_rate": f"{avg_wr*100:.1f}%",
                "signals": best_cfg["metrics"]["total_signals"]
            }
        }
        final_rules.append(rule)
        
    return final_rules

def print_report(rules):
    print("\n" + "="*80)
    print(f"{'COMPOSITE TRADING STRATEGY REPORT':^80}")
    print("="*80 + "\n")
    
    print("Based on historical data analysis, here are the recommended rules per Score Range:\n")
    
    for rule in rules:
        r = rule["score_range"]
        stats = rule["stats"]
        act = rule["action"]
        
        print(f"🔷 SCORE RANGE: {r}")
        print(f"   ├─ 🔍 Filters:      {rule['condition']}")
        print(f"   ├─ 📈 Performance:  PnL: ${stats['max_pnl']} | Avg WR: {stats['avg_win_rate']} | Signals: {stats['signals']}")
        print(f"   └─ ⚙️  STRATEGY:     Lev: {act['leverage']}x | SL: {act['stop_loss']}% | Exit: {act['take_profit_window']}s | Thresh: {act['threshold']}")
        print("-" * 60)
        
    print("\n💡 INTERPRETATION:")
    print("1. Configure your bot to check 'Total Score' first.")
    print("2. Select the matching rule from above.")
    print("3. Apply the specific Filters (RSI/Vol/OI) for that score range.")
    print("4. Use the recommended Leverage and SL settings.")
    print("="*80)

def main():
    data = load_results()
    if not data:
        return
        
    rules = aggregate_rules(data)
    print_report(rules)

if __name__ == "__main__":
    main()
