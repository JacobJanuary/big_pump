"""
Deep Investigation: EPICUSDT Signal Detection
Check why the WebSocket server missed this signal.
"""
import sys
from pathlib import Path
import asyncio
import asyncpg
from datetime import datetime, timedelta

TARGET_SYMBOL = 'EPICUSDT'

async def investigate_signal():
    print("=" * 70)
    print(f"DEEP INVESTIGATION: {TARGET_SYMBOL}")
    print("=" * 70)

    conn = await asyncpg.connect(
        user='elcrypto_readonly',
        password='LohNeMamont@)11',
        database='fox_crypto_new',
        host='localhost',
        port=5433
    )

    try:
        # Step 1: Find all recent signals for this symbol
        print("\n[STEP 1] Finding all recent signals in fas_v2.scoring_history...")
        query1 = f"""
            SELECT sh.id, sh.timestamp, sh.total_score, sh.is_active,
                   tp.exchange_id, tp.contract_type_id, tp.is_active as tp_active
            FROM fas_v2.scoring_history sh
            JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
            WHERE tp.pair_symbol = '{TARGET_SYMBOL}'
              AND sh.timestamp >= NOW() - INTERVAL '24 hours'
            ORDER BY sh.timestamp DESC
            LIMIT 10
        """
        rows = await conn.fetch(query1)
        print(f"Found {len(rows)} signals in last 24h:")
        for r in rows:
            print(f"  ID={r['id']} | {r['timestamp']} | Score={r['total_score']:.2f} | "
                  f"sh.is_active={r['is_active']} | tp.is_active={r['tp_active']} | "
                  f"exchange={r['exchange_id']} | contract={r['contract_type_id']}")
        
        if not rows:
            print("❌ No signals found in scoring_history!")
            return
        
        # Step 2: Check patterns for each signal
        print("\n[STEP 2] Checking patterns (SQUEEZE_IGNITION, OI_EXPLOSION)...")
        for row in rows[:3]:  # Check top 3
            sh_id = row['id']
            query2 = f"""
                SELECT sp.pattern_type, sp.timeframe
                FROM fas_v2.sh_patterns shp
                JOIN fas_v2.signal_patterns sp ON shp.signal_patterns_id = sp.id
                WHERE shp.scoring_history_id = {sh_id}
                  AND sp.pattern_type IN ('SQUEEZE_IGNITION', 'OI_EXPLOSION')
                  AND sp.timeframe IN ('15m', '1h', '4h')
            """
            patterns = await conn.fetch(query2)
            if patterns:
                print(f"  ID={sh_id}: ✅ Has patterns: {[(p['pattern_type'], p['timeframe']) for p in patterns]}")
            else:
                print(f"  ID={sh_id}: ❌ NO matching patterns!")
        
        # Step 3: Check indicators for each signal
        print("\n[STEP 3] Checking indicators (RSI>1, Vol>12, OI>40)...")
        for row in rows[:3]:
            sh_id = row['id']
            query3 = f"""
                SELECT i.rsi, i.volume_zscore, i.oi_delta_pct, shi.indicators_timeframe
                FROM fas_v2.sh_indicators shi
                JOIN fas_v2.indicators i ON (
                    i.trading_pair_id = shi.indicators_trading_pair_id 
                    AND i.timestamp = shi.indicators_timestamp 
                    AND i.timeframe = shi.indicators_timeframe
                )
                WHERE shi.scoring_history_id = {sh_id}
                  AND shi.indicators_timeframe IN ('15m', '1h', '4h')
            """
            indicators = await conn.fetch(query3)
            for ind in indicators:
                rsi_ok = ind['rsi'] and ind['rsi'] > 1
                vol_ok = ind['volume_zscore'] and ind['volume_zscore'] > 12
                oi_ok = ind['oi_delta_pct'] and ind['oi_delta_pct'] > 40
                status = "✅" if (rsi_ok and vol_ok and oi_ok) else "❌"
                print(f"  ID={sh_id} [{ind['indicators_timeframe']}]: RSI={ind['rsi']:.2f} "
                      f"Vol={ind['volume_zscore']:.2f} OI={ind['oi_delta_pct']:.2f} {status}")
        
        # Step 4: Check if pattern timeframe matches indicator timeframe
        print("\n[STEP 4] Checking pattern/indicator timeframe alignment...")
        for row in rows[:3]:
            sh_id = row['id']
            query4 = f"""
                SELECT sp.pattern_type, sp.timeframe as pattern_tf, 
                       shi.indicators_timeframe as indicator_tf,
                       i.rsi, i.volume_zscore, i.oi_delta_pct
                FROM fas_v2.sh_patterns shp
                JOIN fas_v2.signal_patterns sp ON shp.signal_patterns_id = sp.id
                JOIN fas_v2.sh_indicators shi ON shi.scoring_history_id = shp.scoring_history_id
                JOIN fas_v2.indicators i ON (
                    i.trading_pair_id = shi.indicators_trading_pair_id 
                    AND i.timestamp = shi.indicators_timestamp 
                    AND i.timeframe = shi.indicators_timeframe
                )
                WHERE shp.scoring_history_id = {sh_id}
                  AND sp.pattern_type IN ('SQUEEZE_IGNITION', 'OI_EXPLOSION')
                  AND sp.timeframe IN ('15m', '1h', '4h')
                  AND shi.indicators_timeframe = sp.timeframe
                  AND i.rsi > 1
                  AND i.volume_zscore > 12
                  AND i.oi_delta_pct > 40
            """
            matches = await conn.fetch(query4)
            if matches:
                print(f"  ID={sh_id}: ✅ HAS aligned pattern+indicators: {len(matches)} match(es)")
                for m in matches:
                    print(f"    Pattern={m['pattern_type']}@{m['pattern_tf']} | "
                          f"RSI={m['rsi']:.1f} Vol={m['volume_zscore']:.1f} OI={m['oi_delta_pct']:.1f}")
            else:
                print(f"  ID={sh_id}: ❌ NO aligned pattern+indicator pair!")
        
        # Step 5: Check web.signal_analysis for deduplication
        print("\n[STEP 5] Checking web.signal_analysis (deduplication)...")
        query5 = f"""
            SELECT id, signal_timestamp, entry_time, total_score
            FROM web.signal_analysis
            WHERE pair_symbol = '{TARGET_SYMBOL}'
              AND entry_time > NOW() - INTERVAL '24 hours'
            ORDER BY entry_time DESC
        """
        analysis = await conn.fetch(query5)
        if analysis:
            print(f"Found {len(analysis)} entries in web.signal_analysis:")
            for a in analysis:
                print(f"  ID={a['id']} | Signal={a['signal_timestamp']} | Entry={a['entry_time']} | Score={a['total_score']:.2f}")
        else:
            print("  ❌ No entries in web.signal_analysis (not blocked by dedup)")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(investigate_signal())
