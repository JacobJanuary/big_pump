#!/usr/bin/env python3
"""
Quick test: Compare data sources
Check if minute_candles table has different data than candles_data JSON
"""
import sys
from pathlib import Path
import json

current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
scripts_dir = parent_dir / 'scripts_v2'
sys.path.append(str(scripts_dir))

from pump_analysis_lib import get_db_connection

def compare_data_sources():
    """Compare minute_candles vs candles_data for same signal"""
    
    conn = get_db_connection()
    
    # Get one signal with both data sources
    query = """
        SELECT 
            sa.id,
            sa.pair_symbol,
            sa.candles_data,
            (SELECT COUNT(*) FROM web.minute_candles mc WHERE mc.signal_analysis_id = sa.id) as mc_count
        FROM web.signal_analysis sa
        WHERE sa.candles_data IS NOT NULL
        AND EXISTS (SELECT 1 FROM web.minute_candles mc WHERE mc.signal_analysis_id = sa.id)
        LIMIT 1
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    
    if not row:
        print("No signals with both data sources found")
        return
    
    signal_id, symbol, candles_json, mc_count = row
    
    print(f"Signal: {symbol} (ID: {signal_id})")
    print(f"Minute candles count: {mc_count}")
    
    # Parse JSON candles
    if isinstance(candles_json, str):
        json_candles = json.loads(candles_json)
    else:
        json_candles = candles_json
    
    print(f"JSON candles count: {len(json_candles)}")
    
    # Get minute candles from table
    mc_query = """
        SELECT open_time, open_price, high_price, low_price, close_price
        FROM web.minute_candles
        WHERE signal_analysis_id = %s
        ORDER BY open_time ASC
        LIMIT 5
    """
    
    with conn.cursor() as cur:
        cur.execute(mc_query, (signal_id,))
        mc_rows = cur.fetchall()
    
    print(f"\nFirst 5 candles from minute_candles table:")
    for row in mc_rows:
        print(f"  {row}")
    
    print(f"\nFirst 5 candles from JSON:")
    for c in json_candles[:5]:
        print(f"  time={c['time']}, o={c['o']}, h={c['h']}, l={c['l']}, c={c['c']}")
    
    # Check if they match
    if len(json_candles) != mc_count:
        print(f"\n⚠️  COUNT MISMATCH: JSON has {len(json_candles)}, table has {mc_count}")
    else:
        print(f"\n✅ Counts match: {len(json_candles)}")
    
    # Compare first candle
    if mc_rows and json_candles:
        mc_first = mc_rows[0]
        json_first = json_candles[0]
        
        print(f"\nFirst candle comparison:")
        print(f"  Table: time={mc_first[0]}, o={mc_first[1]}, h={mc_first[2]}, l={mc_first[3]}, c={mc_first[4]}")
        print(f"  JSON:  time={json_first['time']}, o={json_first['o']}, h={json_first['h']}, l={json_first['l']}, c={json_first['c']}")
        
        if (mc_first[0] == json_first['time'] and 
            float(mc_first[1]) == float(json_first['o']) and
            float(mc_first[2]) == float(json_first['h'])):
            print(f"  ✅ Data matches!")
        else:
            print(f"  ❌ Data DIFFERS!")
    
    conn.close()

if __name__ == "__main__":
    compare_data_sources()
