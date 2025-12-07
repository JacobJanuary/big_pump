"""
Backtest: Populate signal analysis with custom score threshold
Modified version of populate_signal_analysis.py
"""
import sys
import os
from pathlib import Path
import json

# Add parent scripts directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
scripts_dir = parent_dir / 'scripts_v2'
sys.path.append(str(scripts_dir))

from pump_analysis_lib import (
    get_db_connection,
    deduplicate_signals,
    get_entry_price_and_candles,
    EXCHANGE_FILTER,
    EXCHANGE_IDS,
    TARGET_PATTERNS,
    INDICATOR_FILTERS
)

def fetch_signals_custom_score(conn, days=30, limit=None, min_score=150):
    """
    Fetch signals with custom minimum score threshold
    Modified version that accepts min_score parameter
    """
    placeholders = ','.join([f"'{p}'" for p in TARGET_PATTERNS])
    
    # Exchange filter
    exchange_filter_clause = ""
    if EXCHANGE_FILTER == 'BINANCE':
        exchange_filter_clause = f"AND tp.exchange_id = {EXCHANGE_IDS['BINANCE']}"
    elif EXCHANGE_FILTER == 'BYBIT':
        exchange_filter_clause = f"AND tp.exchange_id = {EXCHANGE_IDS['BYBIT']}"
    
    # Indicator filters
    indicator_conditions = []
    if INDICATOR_FILTERS['rsi_threshold'] > 0:
        indicator_conditions.append(f"AND i.rsi > {INDICATOR_FILTERS['rsi_threshold']}")
    if INDICATOR_FILTERS['volume_zscore_threshold'] > 0:
        indicator_conditions.append(f"AND i.volume_zscore > {INDICATOR_FILTERS['volume_zscore_threshold']}")
    if INDICATOR_FILTERS['oi_delta_threshold'] > 0:
        indicator_conditions.append(f"AND i.oi_delta_pct > {INDICATOR_FILTERS['oi_delta_threshold']}")
    
    indicator_filter_clause = "\n    ".join(indicator_conditions)
    
    query = f"""
        SELECT DISTINCT ON (sh.id)
            sh.id,
            sh.trading_pair_id,
            tp.pair_symbol,
            sh.total_score,
            sh.timestamp,
            sp.pattern_type,
            sp.timeframe
        FROM fas_v2.scoring_history sh
        JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
        JOIN fas_v2.sh_patterns shp ON shp.scoring_history_id = sh.id
        JOIN fas_v2.signal_patterns sp ON shp.signal_patterns_id = sp.id
            AND sp.pattern_type IN ({placeholders})
            AND sp.timeframe IN ('15m', '1h', '4h')
        JOIN fas_v2.sh_indicators shi ON shi.scoring_history_id = sh.id
        JOIN fas_v2.indicators i ON (
            i.trading_pair_id = shi.indicators_trading_pair_id 
            AND i.timestamp = shi.indicators_timestamp 
            AND i.timeframe = shi.indicators_timeframe
        )
        WHERE sh.total_score > {min_score}
            AND tp.contract_type_id = 1
            AND tp.is_active = TRUE
            AND sh.is_active = TRUE
            AND shi.indicators_timeframe = sp.timeframe
            {indicator_filter_clause}
            {exchange_filter_clause}
            AND sh.timestamp >= NOW() - INTERVAL '{days} days'
        ORDER BY sh.id, sh.total_score DESC, sh.timestamp DESC
        {"LIMIT " + str(limit) if limit else ""}
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    
    signals = []
    for row in rows:
        signals.append({
            'id': row[0],
            'trading_pair_id': row[1],
            'pair_symbol': row[2],
            'total_score': row[3],
            'timestamp': row[4],
            'pattern_type': row[5],
            'timeframe': row[6]
        })
    
    return signals

def populate_signal_analysis(days=30, limit=None, force_refresh=False, min_score=150):
    """
    Fetch signals with custom score threshold and store in web.signal_analysis table
    
    IMPORTANT: This version does NOT deduplicate signals!
    We need ALL signals to test different score thresholds.
    Deduplication will happen during optimization phase based on score.
    
    Args:
        days: Number of days to look back
        limit: Limit number of signals
        force_refresh: If True, TRUNCATE and repopulate
        min_score: Minimum total_score threshold (default: 150)
    """
    print(f"Populating signal analysis table for the last {days} days...")
    print(f"Minimum score threshold: {min_score}")
    print(f"⚠️  NO DEDUPLICATION - Loading ALL signals for score testing")
    
    try:
        with get_db_connection() as conn:
            if force_refresh:
                print("Force refresh: Clearing existing data from web.signal_analysis...")
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE web.signal_analysis CASCADE")
                conn.commit()
            else:
                print("Incremental mode: Only adding new signals...")
            
            # Fetch signals with custom score threshold
            signals = fetch_signals_custom_score(conn, days=days, limit=limit, min_score=min_score)
            
            if not signals:
                print("No signals found.")
                return []
            
            print(f"Found {len(signals)} signals from database.")
            
            # NO DEDUPLICATION - we need all signals for testing different thresholds
            unique_signals = signals
            print(f"Loading all {len(unique_signals)} signals (no deduplication).")
            
            # Get existing signal timestamps to skip
            if not force_refresh:
                existing_query = """
                    SELECT signal_timestamp, pair_symbol, total_score
                    FROM web.signal_analysis
                """
                with conn.cursor() as cur:
                    cur.execute(existing_query)
                    # Use (timestamp, symbol, score) as unique key to allow multiple scores for same pair
                    existing = set((row[0], row[1], row[2]) for row in cur.fetchall())
                print(f"Found {len(existing)} existing signals in database.")
            else:
                existing = set()

            
            # Process each signal
            inserted = 0
            skipped = 0
            inserted_signals = []
            
            for i, signal in enumerate(unique_signals, 1):
                if i % 10 == 0:
                    print(f"Processing {i}/{len(unique_signals)}... (inserted: {inserted}, skipped: {skipped})", end='\r')
                
                # Skip if already exists (same timestamp, symbol, AND score)
                if (signal['timestamp'], signal['pair_symbol'], signal['total_score']) in existing:
                    skipped += 1
                    continue
                
                # Get entry price and candles
                entry_price, candles, entry_time_dt = get_entry_price_and_candles(
                    conn, signal,
                    analysis_hours=24,
                    entry_offset_minutes=17
                )
                
                if entry_price is None or not candles:
                    skipped += 1
                    continue
                
                # Calculate metrics
                entry_time_ms = candles[0]['open_time']
                max_price = entry_price
                max_price_time_ms = entry_time_ms
                min_price = entry_price
                
                for candle in candles:
                    high = float(candle['high_price'])
                    low = float(candle['low_price'])
                    
                    if high > max_price:
                        max_price = high
                        max_price_time_ms = candle['open_time']
                    
                    if low < min_price:
                        min_price = low
                
                max_growth_pct = ((max_price - entry_price) / entry_price) * 100
                max_drawdown_pct = ((min_price - entry_price) / entry_price) * 100
                time_to_peak_seconds = int((max_price_time_ms - entry_time_ms) / 1000)
                
                from datetime import datetime, timezone
                max_price_time_dt = datetime.fromtimestamp(max_price_time_ms / 1000, tz=timezone.utc)
                
                # Convert candles to JSON
                candles_json = json.dumps([{
                    'time': c['open_time'],
                    'o': float(c['open_price']),
                    'h': float(c['high_price']),
                    'l': float(c['low_price']),
                    'c': float(c['close_price'])
                } for c in candles])
                
                # Insert into database
                insert_query = """
                    INSERT INTO web.signal_analysis (
                        signal_timestamp, pair_symbol, trading_pair_id, total_score,
                        entry_time, entry_price,
                        max_price, max_price_time, min_price,
                        max_growth_pct, max_drawdown_pct,
                        time_to_peak_seconds,
                        candles_data, analysis_window_hours
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s,
                        %s::jsonb, %s
                    )
                """
                
                with conn.cursor() as cur:
                    cur.execute(insert_query, (
                        signal['timestamp'], signal['pair_symbol'], signal['trading_pair_id'], signal['total_score'],
                        entry_time_dt, entry_price,
                        max_price, max_price_time_dt, min_price,
                        max_growth_pct, max_drawdown_pct,
                        time_to_peak_seconds,
                        candles_json, 24
                    ))
                
                inserted += 1
                inserted_signals.append(signal)
                
                # Commit every 50 signals
                if inserted % 50 == 0:
                    conn.commit()
            
            # Final commit
            conn.commit()
            
            print(f"\n\nSuccessfully populated {inserted} new signals into web.signal_analysis")
            if skipped > 0:
                print(f"Skipped {skipped} existing signals")
                
            return inserted_signals

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Populate signal analysis with custom score threshold (NO deduplication).')
    parser.add_argument('--days', type=int, default=30, help='Days to look back')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of signals')
    parser.add_argument('--force-refresh', action='store_true', help='Force full refresh')
    parser.add_argument('--min-score', type=int, default=150, help='Minimum total_score threshold')
    args = parser.parse_args()
    
    populate_signal_analysis(
        days=args.days,
        limit=args.limit,
        force_refresh=args.force_refresh,
        min_score=args.min_score
    )

