#!/usr/bin/env python3
"""Test script to diagnose bar loading performance.

Compares different query approaches and chunk sizes.
"""
import sys
import time
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent / "scripts_v2"))

from pump_analysis_lib import get_db_connection


def test_query_direct(conn, signal_ids, chunk_size=10):
    """Test using EXACT query from optimize_unified.py"""
    print(f"\n[TEST] Direct query (optimize_unified.py style), chunk_size={chunk_size}")
    
    sql = """
        SELECT signal_analysis_id, second_ts, close_price, delta,
               large_buy_count, large_sell_count
        FROM web.agg_trades_1s
        WHERE signal_analysis_id = ANY(%s)
        ORDER BY signal_analysis_id, second_ts
    """
    
    total_rows = 0
    start = time.time()
    
    with conn.cursor() as cur:
        for i in range(0, len(signal_ids), chunk_size):
            chunk = signal_ids[i:i + chunk_size]
            chunk_start = time.time()
            
            cur.execute(sql, (chunk,))
            rows = cur.fetchall()
            
            chunk_time = time.time() - chunk_start
            total_rows += len(rows)
            print(f"   Chunk {i//chunk_size + 1}: {len(rows):,} rows in {chunk_time:.2f}s")
    
    elapsed = time.time() - start
    print(f"[RESULT] Total: {total_rows:,} rows in {elapsed:.2f}s")
    return total_rows, elapsed


def test_single_signal(conn, signal_id):
    """Test fetching bars for a single signal to measure data volume"""
    print(f"\n[TEST] Single signal ID={signal_id}")
    
    sql = """
        SELECT COUNT(*), MIN(second_ts), MAX(second_ts)
        FROM web.agg_trades_1s
        WHERE signal_analysis_id = %s
    """
    
    start = time.time()
    with conn.cursor() as cur:
        cur.execute(sql, (signal_id,))
        count, min_ts, max_ts = cur.fetchone()
    elapsed = time.time() - start
    
    duration_hours = (max_ts - min_ts) / 3600 if min_ts and max_ts else 0
    print(f"   Rows: {count:,}, Duration: {duration_hours:.1f}h, Query time: {elapsed:.2f}s")
    return count


def get_sample_signals(conn, limit=10):
    """Get sample signal IDs for testing"""
    query = """
        SELECT id FROM web.signal_analysis 
        WHERE total_score >= 100 AND total_score < 950
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        return [row[0] for row in cur.fetchall()]


def main():
    print("=" * 60)
    print("BAR LOADING PERFORMANCE TEST")
    print("=" * 60)
    
    conn = get_db_connection()
    
    # Get sample signals
    print("\n[SETUP] Fetching sample signal IDs...")
    signal_ids = get_sample_signals(conn, limit=50)
    print(f"[SETUP] Got {len(signal_ids)} signal IDs: {signal_ids[:5]}...")
    
    # Test single signal to understand data volume
    if signal_ids:
        test_single_signal(conn, signal_ids[0])
    
    # Test with different chunk sizes
    for chunk_size in [1, 5, 10]:
        test_query_direct(conn, signal_ids[:20], chunk_size=chunk_size)
    
    conn.close()
    print("\n[DONE] Test complete")


if __name__ == "__main__":
    main()
