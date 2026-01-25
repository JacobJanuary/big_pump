#!/usr/bin/env python3
import json
import sys
import os
from pathlib import Path

def main():
    # Define search paths: current dir, script dir, parent of script dir
    search_dirs = [
        Path.cwd(),
        Path(__file__).resolve().parent,
        Path(__file__).resolve().parent.parent
    ]
    
    # Find all candidates
    candidates = []
    for d in search_dirs:
        p = d / "intermediate_results.jsonl"
        if p.exists():
            candidates.append(p)
    
    # Remove duplicates
    candidates = list(set(candidates))
    
    if not candidates:
        print("Error: Could not find 'intermediate_results.jsonl'")
        print(f"Searched in: {[str(d) for d in search_dirs]}")
        sys.exit(1)
        
    # Sort by modification time (newest first)
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    
    target_file = candidates[0]
    print(f"Reading from: {target_file} (Last modified: {target_file.stat().st_mtime})")
    
    results = []
    try:
        with open(target_file, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    results.append(data)
                except json.JSONDecodeError:
                    # It's common for the last line to be partial if writing is active
                    pass
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

    if not results:
        print("File is empty or contains no valid data.")
        sys.exit(0)

    # Sort by Total PnL descending
    results.sort(key=lambda x: x["metrics"]["total_pnl"], reverse=True)
    
    top_n = 10
    print(f"\nTop {top_n} Configurations out of {len(results)} evaluated:\n")
    
    # Header
    header = f"{'Rank':<5} | {'PnL':<10} | {'Win%':<6} | {'Sig#':<5} | {'Filter Config':<60} | {'Strategy Config':<40}"
    print("-" * len(header))
    print(header)
    print("-" * len(header))
    
    for rank, res in enumerate(results[:top_n], 1):
        metrics = res["metrics"]
        filt = res["filter"]
        strat = res["strategy"]
        
        # Format metrics
        pnl = f"{metrics['total_pnl']:.2f}"
        win_rate = metrics['win_rate'] * 100
        win_str = f"{win_rate:.1f}%"
        signals = metrics['total_signals']
        
        # Format Configs compactly
        # Filter: Score:100-110, RSI>20, Vol>5, OI>10
        filt_str = (f"Score:{filt.get('score_min')}-{filt.get('score_max')} "
                    f"RSI>{filt.get('rsi_min')} Vol>{filt.get('vol_min')} OI>{filt.get('oi_min')}")
        
        # Strategy: Lev:5x SL:5% Win:60s Th:2.0
        strat_str = (f"Lev:{strat.get('leverage')}x SL:{strat.get('sl')}% "
                     f"Win:{strat.get('window')}s Th:{strat.get('threshold')}")
        
        print(f"{rank:<5} | {pnl:<10} | {win_str:<6} | {signals:<5} | {filt_str:<60} | {strat_str:<40}")

if __name__ == "__main__":
    main()
