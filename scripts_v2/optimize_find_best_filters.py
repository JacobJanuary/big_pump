#!/usr/bin/env python3
"""
Step 2: Optimize filters using precalculated is_win data

IN-MEMORY OPTIMIZED VERSION
Loads all signals once and performs 850k+ checks in memory.
Speed: ~1-2 seconds for full grid search.

Dependencies: pandas (optional but recommended for speed)
"""

import sys
from pathlib import Path
import json
from datetime import datetime

# Add current directory to path
sys.path.append(str(Path(__file__).resolve().parent))
from pump_analysis_lib import get_db_connection

# Full Parameter Ranges (as requested)
SCORE_RANGE = range(100, 260, 10)       # 16 steps
RSI_RANGE = range(0, 81, 1)             # 81 steps
VOLUME_ZSCORE_RANGE = range(0, 16, 1)   # 16 steps
OI_DELTA_RANGE = range(0, 41, 1)        # 41 steps

def load_all_signals(conn):
    """Load relevant signal data"""
    print("Loading all signals into memory...")
    query = """
        SELECT 
            total_score as score,
            rsi,
            volume_zscore,
            oi_delta_pct,
            is_win
        FROM web.signal_analysis
    """
    
    # Try importing pandas for speed
    try:
        import pandas as pd
        df = pd.read_sql(query, conn)
        df['rsi'] = df['rsi'].fillna(0)
        df['volume_zscore'] = df['volume_zscore'].fillna(0)
        df['oi_delta_pct'] = df['oi_delta_pct'].fillna(0)
        print(f"Loaded {len(df)} signals (using pandas).")
        return df, True
    except ImportError:
        print("Pandas not found, using standard SQL fetch (slower but works without dependencies).")
        with conn.cursor() as cur:
            cur.execute(query)
            # Fetch as list of dicts or tuples
            data = []
            for row in cur.fetchall():
                data.append({
                    'score': row[0],
                    'rsi': row[1] or 0,
                    'volume_zscore': row[2] or 0,
                    'oi_delta_pct': row[3] or 0,
                    'is_win': row[4]
                })
        print(f"Loaded {len(data)} signals (list mode).")
        return data, False

def optimize_filters():
    """Test all parameter combinations in memory"""
    conn = get_db_connection()
    data, use_pandas = load_all_signals(conn)
    conn.close()
    
    if len(data) == 0:
        print("No signals found in web.signal_analysis. Please run optimize_calculate_is_win.py first.")
        return

    print("="*100)
    print("FILTER OPTIMIZATION - IN-MEMORY GRID SEARCH")
    print("="*100)
    
    total_combinations = len(SCORE_RANGE) * len(RSI_RANGE) * len(VOLUME_ZSCORE_RANGE) * len(OI_DELTA_RANGE)
    print(f"Testing {total_combinations:,} combinations...")
    
    results = []
    
    start_time = datetime.now()
    
    if use_pandas:
        # PANDAS VECTORIZED APPROACH (FAST)
        df = data
        scores = df['score'].values
        rsis = df['rsi'].values
        vols = df['volume_zscore'].values
        ois = df['oi_delta_pct'].values
        wins_col = df['is_win'].map({True: 1, False: 0, None: -1}).values # 1=Win, 0=Loss, -1=Timeout
        
        tested = 0
        for score_thresh in SCORE_RANGE:
            mask_score = scores > score_thresh
            
            for rsi_thresh in RSI_RANGE:
                mask_rsi = (rsis > rsi_thresh) if rsi_thresh > 0 else (rsis > -999)
                mask_s_r = mask_score & mask_rsi
                
                for vol_thresh in VOLUME_ZSCORE_RANGE:
                    mask_vol = (vols > vol_thresh) if vol_thresh > 0 else (vols > -999)
                    mask_s_r_v = mask_s_r & mask_vol
                    
                    for oi_thresh in OI_DELTA_RANGE:
                        tested += 1
                        mask_oi = (ois > oi_thresh) if oi_thresh > 0 else (ois > -999)
                        
                        final_mask = mask_s_r_v & mask_oi
                        filtered_wins = wins_col[final_mask]
                        
                        total_signals = len(filtered_wins)
                        if total_signals < 10:
                            continue
                            
                        n_wins = (filtered_wins == 1).sum()
                        n_losses = (filtered_wins == 0).sum()
                        n_timeouts = (filtered_wins == -1).sum()
                        
                        trades = n_wins + n_losses
                        win_rate = (n_wins / trades * 100) if trades > 0 else 0.0
                        
                        if trades >= 20 and win_rate > 50:
                             results.append({
                                'score': score_thresh,
                                'rsi': rsi_thresh,
                                'volume_zscore': vol_thresh,
                                'oi_delta': oi_thresh,
                                'win_rate': float(win_rate),
                                'wins': int(n_wins),
                                'losses': int(n_losses),
                                'timeouts': int(n_timeouts),
                                'total_signals': int(total_signals),
                                'trades': int(trades)
                            })
                        
                        if tested % 50000 == 0:
                            print(f"Processed {tested:,} combinations...", end='\r')
    else:
        # PURE PYTHON APPROACH (SLOWER BUT NO DEPENDENCIES)
        tested = 0
        
        # Pre-sort/filter data for efficiency? Maybe too complex for now.
        # Just iterate. It will be slower but will finish in minutes, not hours.
        
        for score_thresh in SCORE_RANGE:
            # Pre-filter by score
            score_filtered = [x for x in data if x['score'] > score_thresh]
            
            for rsi_thresh in RSI_RANGE:
                rsi_filtered = [x for x in score_filtered if (x['rsi'] > rsi_thresh if rsi_thresh > 0 else True)]
                
                for vol_thresh in VOLUME_ZSCORE_RANGE:
                    vol_filtered = [x for x in rsi_filtered if (x['volume_zscore'] > vol_thresh if vol_thresh > 0 else True)]
                    
                    for oi_thresh in OI_DELTA_RANGE:
                        tested += 1
                        
                        # Only loop over 800k times is fast, main cost is here:
                        final_stats = {True: 0, False: 0, None: 0} # Win, Loss, Timeout
                        
                        # Use list comprehension or explicit loop
                        count_total = 0
                        for x in vol_filtered:
                            if (x['oi_delta_pct'] > oi_thresh if oi_thresh > 0 else True):
                                final_stats[x['is_win']] += 1
                                count_total += 1
                        
                        if count_total < 10:
                            continue
                            
                        n_wins = final_stats[True]
                        n_losses = final_stats[False]
                        n_timeouts = final_stats[None]
                        
                        trades = n_wins + n_losses
                        win_rate = (n_wins / trades * 100) if trades > 0 else 0.0
                        
                        if trades >= 20 and win_rate > 50:
                            results.append({
                                'score': score_thresh,
                                'rsi': rsi_thresh,
                                'volume_zscore': vol_thresh,
                                'oi_delta': oi_thresh,
                                'win_rate': win_rate,
                                'wins': n_wins,
                                'losses': n_losses,
                                'timeouts': n_timeouts,
                                'total_signals': count_total,
                                'trades': trades
                            })
                            
                        if tested % 5000 == 0:
                            print(f"Processed {tested:,} combinations...", end='\r')

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nOptimization complete in {elapsed:.2f} seconds.")
    
    # Sort results
    results.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Save top results
    output_file = 'filter_optimization_results.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
        
    print(f"Found {len(results):,} valid combinations. Saved to {output_file}")

    print(f"\n{'='*100}")
    print("TOP 20 FILTER COMBINATIONS")
    print("="*100)
    print(f"\n{'Rank':<6} {'Score':<8} {'RSI':<6} {'Vol Z':<8} {'OI Δ':<8} {'Win%':<8} {'Wins':<6} {'Loss':<6} {'Trades':<8} {'Signals':<8}")
    print("-"*100)
    
    for i, result in enumerate(results[:20], 1):
        print(f"{i:<6} "
              f">{result['score']:<7} "
              f">{result['rsi']:<5} "
              f">{result['volume_zscore']:<7} "
              f">{result['oi_delta']:<7} "
              f"{result['win_rate']:<7.2f}% "
              f"{result['wins']:<6} "
              f"{result['losses']:<6} "
              f"{result['trades']:<8} "
              f"{result['total_signals']:<8}")

if __name__ == '__main__':
    optimize_filters()
