"""
Inspect a specific signal (ICNTUSDT) to verify its values against filters.
User claims it matches all criteria. we need to verify DB values.
"""
import sys
from pathlib import Path
import asyncio
import asyncpg
from datetime import datetime, timezone

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, EXCHANGE_IDS

# Target
TARGET_SYMBOL = 'ICNTUSDT'
TARGET_TIME_STR = '2025-12-12 11:15:00+00'

async def inspect_signal():
    print(f"--- INSPECTING {TARGET_SYMBOL} around {TARGET_TIME_STR} ---")

    conn = await asyncpg.connect(
        user='elcrypto_readonly',
        password='LohNeMamont@)11',
        database='fox_crypto_new',
        host='localhost',
        port=5433
    )

    try:
        # 1. Find the specific scoring history details + Patterns
        print("\n--- FETCHING DETAILS + PATTERNS ---")
        
        # Get basic info + contract type
        query_info = f"""
            SELECT 
                sh.id,
                sh.total_score,
                tp.contract_type_id,
                tp.exchange_id
            FROM fas_v2.scoring_history sh
            JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
            WHERE tp.pair_symbol = '{TARGET_SYMBOL}'
              AND sh.timestamp = '{TARGET_TIME_STR}'
        """
        info = await conn.fetchrow(query_info)
        
        if not info:
            print("❌ Root record not found.")
            return

        sid = info['id']
        print(f"Signal ID: {sid}")
        print(f"Contract Type: {info['contract_type_id']} (1=Perp, 2=Spot)")
        print(f"Exchange ID: {info['exchange_id']}")
        
        # Get Patterns
        query_patterns = f"""
            SELECT sp.pattern_type, sp.timeframe
            FROM fas_v2.sh_patterns shp
            JOIN fas_v2.signal_patterns sp ON shp.signal_patterns_id = sp.id
            WHERE shp.scoring_history_id = {sid}
        """
        
        patterns = await conn.fetch(query_patterns)
        print(f"Patterns Found ({len(patterns)}):")
        has_4h_target = False
        for p in patterns:
            pt = p['pattern_type']
            tf = p['timeframe']
            print(f"  - {pt} on {tf}")
            
            if tf == '4h' and pt in ['SQUEEZE_IGNITION', 'OI_EXPLOSION']:
                has_4h_target = True
        
        if has_4h_target:
            print("✅ HAS valid 4h pattern! (Should match 4h indicators)")
        else:
            print("❌ NO valid 4h pattern (Cannot use 4h indicators)")

        # Re-check 4h indicators specifically
        query_4h_ind = f"""
            SELECT i.volume_zscore, i.oi_delta_pct
            FROM fas_v2.sh_indicators shi
            JOIN fas_v2.indicators i ON (
                i.trading_pair_id = shi.indicators_trading_pair_id 
                AND i.timestamp = shi.indicators_timestamp 
                AND i.timeframe = shi.indicators_timeframe
            )
            WHERE shi.scoring_history_id = {sid}
              AND shi.indicators_timeframe = '4h'
        """
        ind4h = await conn.fetchrow(query_4h_ind)
        if ind4h:
            v = ind4h['volume_zscore']
            o = ind4h['oi_delta_pct']
            print(f"4h Indicators: Vol={v}, OI={o}")
            if v > 12 and o > 40:
                print("✅ 4h Indicators PASS filters")
            else:
                print("❌ 4h Indicators FAIL filters")
        else:
            print("⚠️ No 4h indicators found linked.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(inspect_signal())
