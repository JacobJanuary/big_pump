"""
Diagnostic script to debug optimization logic.
Focuses on a specific parameter combination and compares deduplication methods:
1. "Chaining" (Old, aggressive)
2. "Global Greedy" (New, correct, matches SQL)

Usage: python3 scripts_v3/debug_optimization.py
"""
import sys
import os
from pathlib import Path
from datetime import timedelta

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# DEBUG PARAMETERS
DEBUG_SCORE = 100
DEBUG_RSI = 50
DEBUG_VOL = 0
DEBUG_OI = 0

def debug_optimization():
    print(f"--- DEBUGGING OPTIMIZATION LOGIC ---")
    print(f"Parameters: Score={DEBUG_SCORE}, RSI={DEBUG_RSI}, Vol={DEBUG_VOL}, OI={DEBUG_OI}")
    
    signals = []
    try:
        with get_db_connection() as conn:
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
                cols = ['id', 'pair_symbol', 'timestamp', 'score', 'rsi', 'vol_z', 'oi_delta', 'max_grow']
                for row in cur.fetchall():
                    signals.append(dict(zip(cols, row)))
    except Exception as e:
        print(f"Error loading: {e}")
        return

    # 1. Filtering
    filtered = [
        s for s in signals 
        if s['score'] >= DEBUG_SCORE and
           s['rsi'] >= DEBUG_RSI and
           s['vol_z'] >= DEBUG_VOL and
           s['oi_delta'] >= DEBUG_OI
    ]
    
    print(f"Signals passing values filter: {len(filtered)}")
    
    # Group by pair
    by_pair = {}
    for s in filtered:
        p = s['pair_symbol']
        if p not in by_pair: by_pair[p] = []
        by_pair[p].append(s)

    # ---------------------------------------------------------
    # METHOD 1: CHAINING (OLD)
    # ---------------------------------------------------------
    count_chaining = 0
    for pair, pair_signals in by_pair.items():
        if not pair_signals: continue
        # pair_signals sorted by time ASC
        clusters = []
        current_cluster = [pair_signals[0]]
        for i in range(1, len(pair_signals)):
            s = pair_signals[i]
            prev = current_cluster[-1]
            diff = (s['timestamp'] - prev['timestamp']).total_seconds() / 3600
            if diff <= 12:
                current_cluster.append(s)
            else:
                count_chaining += 1 # Pick 1 best
                current_cluster = [s]
        count_chaining += 1 # Last cluster

    # ---------------------------------------------------------
    # METHOD 2: GLOBAL GREEDY (NEW)
    # ---------------------------------------------------------
    kept_greedy = []
    for pair, pair_signals in by_pair.items():
        if not pair_signals: continue
        
        # Sort by Growth DESC (Best first)
        # Tie-breaker: timestamp?
        candidates = sorted(pair_signals, key=lambda x: x['max_grow'], reverse=True)
        
        pair_kept = []
        consumed_indices = set()
        
        for i in range(len(candidates)):
            if i in consumed_indices:
                continue
            
            # Pick best
            best = candidates[i]
            pair_kept.append(best)
            
            # Mask neighbors
            for j in range(i+1, len(candidates)):
                if j in consumed_indices: continue
                
                other = candidates[j]
                # Check time diff
                diff_hours = abs((best['timestamp'] - other['timestamp']).total_seconds()) / 3600.0
                
                if diff_hours <= 12.0:
                    consumed_indices.add(j)
        
        kept_greedy.extend(pair_kept)

    print(f"\n--- COMPARISON ---")
    print(f"Method 1 (Old Chaining) Count: {count_chaining}")
    print(f"Method 2 (New Greedy) Count:   {len(kept_greedy)}")
    
    # Stats for Greedy
    best_greedy = len([s for s in kept_greedy if s['max_grow'] > 15])
    weak_greedy = len([s for s in kept_greedy if s['max_grow'] < 10])
    
    print(f"\n--- STATS (New Method) ---")
    print(f"Total: {len(kept_greedy)}")
    print(f"Best (>15%): {best_greedy}")
    print(f"Weak (<10%): {weak_greedy}")
    
    if len(kept_greedy) > 0:
        print(f"Win Rate (>15%): {best_greedy/len(kept_greedy)*100:.1f}%")

if __name__ == "__main__":
    debug_optimization()
