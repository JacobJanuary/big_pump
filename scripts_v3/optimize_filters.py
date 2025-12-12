"""
Optimize filter parameters to find the best signals (Growth > 15%)
and minimize weak signals (Growth < 10%).

Algorithm:
1. Load all signals from web.big_pump_signals.
2. Define parameter grid (Score, RSI, Vol Z-Score, OI Delta).
3. For each combination:
    a. Filter signals based on parameters.
    b. Deduplicate: For overlap windows (12h) per pair, keep ONLY the signal with highest max_grow_pr.
       (This models the "ideal selection" the user requested).
    c. Count:
       - Best: max_grow_pr > 15
       - Weak: max_grow_pr < 10
       - Total selected
    d. Calculate Score = Best - (Weak * 0.5) to penalize weak signals? 
       Or user said "as few as possible < 10%".
       Let's use: Score = Best - Weak.
4. Output top results.
"""

import sys
import os
from pathlib import Path
from datetime import timedelta

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# Parameter Grid
# Adjust ranges based on typical values
SCORES = range(100, 301, 20)      # 100 to 300, step 20
RSI_THRESHOLDS = range(50, 81, 5) # 50 to 80, step 5
VOL_ZSCORES = range(0, 21, 2)     # 0 to 20, step 2
OI_DELTAS = range(0, 51, 5)       # 0 to 50, step 5

def optimize_filters():
    print("Loading signals from web.big_pump_signals...")
    
    signals = []
    try:
        with get_db_connection() as conn:
            # Fetch necessary columns
            query = """
                SELECT 
                    id, pair_symbol, signal_timestamp, 
                    total_score, rsi_threshold, volume_zscore, oi_delta,
                    max_grow_pr
                FROM web.big_pump_signals
                ORDER BY signal_timestamp ASC
            """
            with conn.cursor() as cur:
                cur.execute(query)
                # Convert to list of dicts for faster processing
                cols = ['id', 'pair_symbol', 'timestamp', 'score', 'rsi', 'vol_z', 'oi_delta', 'max_grow']
                for row in cur.fetchall():
                    signals.append(dict(zip(cols, row)))
                    
    except Exception as e:
        print(f"Error loading signals: {e}")
        return

    if not signals:
        print("No signals found.")
        return

    print(f"Loaded {len(signals)} signals. Starting optimization...")
    print(f"Grid size: {len(SCORES)} x {len(RSI_THRESHOLDS)} x {len(VOL_ZSCORES)} x {len(OI_DELTAS)} = {len(SCORES)*len(RSI_THRESHOLDS)*len(VOL_ZSCORES)*len(OI_DELTAS)} combinations")
    
    results = []
    
    import time
    start_time = time.time()
    count = 0
    
    # Cache processed results? No, direct loop is fast enough for ~5k combinations if signals < 1000.
    # If signals >> 1000, might need optimization. Assuming < 10k signals.
    
    for score in SCORES:
        for rsi in RSI_THRESHOLDS:
            for vol in VOL_ZSCORES:
                for oi in OI_DELTAS:
                    count += 1
                    if count % 100 == 0:
                        print(f"Processed {count} combinations...", end='\r')
                    
                    # 1. Filter
                    # List comprehension is fast
                    filtered = [
                        s for s in signals 
                        if s['score'] >= score and
                           s['rsi'] >= rsi and
                           s['vol_z'] >= vol and
                           s['oi_delta'] >= oi
                    ]
                    
                    if not filtered:
                        continue
                        
                    # 2. Deduplicate / Select Best in 12h Window
                    # Group by pair first
                    by_pair = {}
                    for s in filtered:
                        p = s['pair_symbol']
                        if p not in by_pair: by_pair[p] = []
                        by_pair[p].append(s)
                    
                    final_selection = []
                    
                    for pair, pair_signals in by_pair.items():
                        # Sort by time
                        # pair_signals.sort(key=lambda x: x['timestamp']) # Already sorted from DB
                        
                        # Logic: Select locally optimal signals in 12h windows
                        # This is tricky iteratively.
                        # Simple greedy approach:
                        # Find global max for pair? No, 12h window.
                        # Sliding window is complex. 
                        # User wants: "if... > 1 signal in 12h, select best".
                        # Let's cluster them. If S1 and S2 are within 12h, they are in same cluster.
                        # Then pick max from cluster.
                        
                        # Clustering logic:
                        if not pair_signals: continue
                        
                        clusters = []
                        current_cluster = [pair_signals[0]]
                        
                        for i in range(1, len(pair_signals)):
                            s = pair_signals[i]
                            prev = current_cluster[-1]
                            
                            # Diff hours
                            diff = (s['timestamp'] - prev['timestamp']).total_seconds() / 3600
                            
                            if diff <= 12:
                                current_cluster.append(s)
                            else:
                                # Close cluster, pick best
                                best = max(current_cluster, key=lambda x: x['max_grow'])
                                final_selection.append(best)
                                # Start new
                                clusters.append(current_cluster)
                                current_cluster = [s]
                                
                        # Last cluster
                        best = max(current_cluster, key=lambda x: x['max_grow'])
                        final_selection.append(best)
                        
                    # 3. Calculate Stats
                    best_signals = 0 # > 15
                    weak_signals = 0 # < 10
                    mid_signals = 0  # 10 <= x <= 15
                    
                    for s in final_selection:
                        g = s['max_grow']
                        if g > 15:
                            best_signals += 1
                        elif g < 10:
                            weak_signals += 1
                        else:
                            mid_signals += 1
                            
                    total = len(final_selection)
                    
                    # 4. Score
                    # Objective: Maximize Best, Minimize Weak
                    # optimization_metric = best_signals - (weak_signals * 1.5) # Penalty
                    # Or just best_signals desc, weak_signals asc
                    
                    # Let's store raw stats and sort later
                    results.append({
                        'params': (score, rsi, vol, oi),
                        'total': total,
                        'best': best_signals,
                        'weak': weak_signals,
                        'mid': mid_signals,
                        'win_rate_15': (best_signals/total*100) if total else 0,
                        'bad_rate_10': (weak_signals/total*100) if total else 0
                    })

    print(f"\nOptimization complete. Processed {count} combinations.")
    
    # Sorting Strategy:
    # User wants: Maximize Best (>15%) AND Minimize Weak (<10%).
    # Previous metric (Best * (1 - BadRate)) favored high volume too much.
    # New Metric: Profitability Score = Best_Signals - Weak_Signals
    # This directly penalizes every weak signal.
    # If a config gives 100 Best and 100 Weak, Score = 0.
    # If a config gives 50 Best and 10 Weak, Score = 40 (Better).
    
    def rank_score(r):
        if r['total'] < 5: return -999999 # Require minimum sample
        return r['best'] - r['weak']
        
    results.sort(key=rank_score, reverse=True)
    
    print("\nTop 20 Filter Configurations (Sorted by Best - Weak):")
    print(f"{'Score':<6} | {'RSI':<4} | {'VolZ':<4} | {'OI%':<4} || {'Total':<5} | {'Best (>15%)':<12} | {'Weak (<10%)':<12} | {'Bad Rate %':<10} | {'Score':<8}")
    print("-" * 105)
    
    for r in results[:20]:
        p = r['params']
        print(f"{p[0]:<6} | {p[1]:<4} | {p[2]:<4} | {p[3]:<4} || {r['total']:<5} | {r['best']:<12} | {r['weak']:<12} | {r['bad_rate_10']:<10.1f} | {rank_score(r):<8}")

if __name__ == "__main__":
    optimize_filters()
