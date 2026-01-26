#!/usr/bin/env python3
"""Diagnostic script to analyze freeze point data volume"""

import sys
sys.path.append('/Users/evgeniyyanvarskiy/PycharmProjects/big_pump/scripts_v2')

from pump_analysis_lib import get_db_connection

# Filter at freeze point: score=150
filter_cfg = {
    'score_min': 150,
    'score_max': 200,
    'rsi_min': 0,
    'vol_min': 15,
    'oi_min': 18,
}

print(f"Analyzing filter: {filter_cfg}")
print("="*60)

with get_db_connection() as conn:
    # 1. Count signals matching this filter
    query1 = '''
        SELECT COUNT(*) 
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
    '''
    with conn.cursor() as cur:
        cur.execute(query1, filter_cfg)
        signal_count = cur.fetchone()[0]
        print(f"1. Signals matching filter: {signal_count}")
    
    # 2. Get signal IDs
    query2 = '''
        SELECT sa.id
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
    '''
    with conn.cursor() as cur:
        cur.execute(query2, filter_cfg)
        signal_ids = [r[0] for r in cur.fetchall()]
    
    if signal_ids:
        # 3. Count total bars for these signals
        query3 = '''
            SELECT COUNT(*) 
            FROM web.agg_trades_1s
            WHERE signal_analysis_id = ANY(%s)
        '''
        with conn.cursor() as cur:
            cur.execute(query3, (signal_ids,))
            bar_count = cur.fetchone()[0]
            print(f"2. Total bars for these signals: {bar_count:,}")
            print(f"3. Average bars per signal: {bar_count / len(signal_ids):,.0f}")
    else:
        print("2. No signals found for this filter")

# Also check the general distribution
print("\n" + "="*60)
print("General statistics for all score ranges:")
with get_db_connection() as conn:
    for score_min in [100, 150, 200, 250, 300]:
        score_max = score_min + 50
        query = '''
            SELECT COUNT(*)
            FROM web.signal_analysis
            WHERE total_score >= %s AND total_score < %s
        '''
        with conn.cursor() as cur:
            cur.execute(query, (score_min, score_max))
            cnt = cur.fetchone()[0]
            print(f"  score [{score_min}, {score_max}): {cnt} signals")
