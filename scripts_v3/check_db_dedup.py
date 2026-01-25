"""
Check if IOUSDT exists in web.signal_analysis in the 12h window before 17:45.
To verify if the 'blocker' was a real processed signal.
"""
import sys
from pathlib import Path
import asyncio
import asyncpg
from datetime import datetime, timedelta

TARGET_SYMBOL = 'IOUSDT'
TARGET_TIME_STR = '2025-12-12 17:45:00+00'
LOOKBACK_HOURS = 12

async def check_db_history():
    print(f"--- CHECKING WEB.SIGNAL_ANALYSIS HISTORY ---")
    print(f"Target: {TARGET_SYMBOL} at {TARGET_TIME_STR}")
    print(f"Lookback: {LOOKBACK_HOURS} hours") # 05:45 to 17:45

    conn = await asyncpg.connect(
        user='elcrypto_readonly',
        password='LohNeMamont@)11',
        database='fox_crypto_new',
        host='localhost',
        port=5433
    )

    try:
        target_ts = datetime.fromisoformat(TARGET_TIME_STR)
        start_ts = target_ts - timedelta(hours=LOOKBACK_HOURS)
        
        query = f"""
            SELECT *
            FROM web.signal_analysis
            WHERE pair_symbol = '{TARGET_SYMBOL}'
              AND signal_timestamp >= '{start_ts.isoformat()}'
              AND signal_timestamp < '{target_ts.isoformat()}' -- Strict less than target
            ORDER BY signal_timestamp DESC
        """
        
        rows = await conn.fetch(query)
        
        print(f"\nFound {len(rows)} entries in web.signal_analysis:")
        for r in rows:
            print(f"  - {r['signal_timestamp']} (Score: {r['total_score']})")
            
        if not rows:
            print("\n✅ CONFIRMED: No 'real' signals in DB history.")
            print("   The Server's in-memory deduplication BLOCKED 17:45 based on a ghost signal.")
            print("   Recommendation: Switch Server to use DB-based deduplication.")
        else:
            print("\n❌ FOUND entries. The deduplication was theoretically correct based on DB history.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_db_history())
