"""
Comprehensive Audit Test #3: TRADOORUSDT Specific Investigation
Deep dive into why TRADOORUSDT was broadcasted by WebSocket but not added to web.signal_analysis
"""
import asyncio
import asyncpg
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

async def investigate_tradoor():
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME
    )
    
    print("="*80)
    print("AUDIT TEST #3: TRADOORUSDT Investigation")
    print("="*80)
    
    # Find recent TRADOORUSDT signals
    print("\n1️⃣ Recent TRADOORUSDT signals in fas_v2.scoring_history")
    print("-"*80)
    
    query1 = """
        SELECT sh.id, sh.timestamp, sh.created_at, sh.total_score, tp.exchange_id
        FROM fas_v2.scoring_history sh
        JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
        WHERE tp.pair_symbol = 'TRADOORUSDT'
          AND sh.timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY sh.timestamp DESC
        LIMIT 5
    """
    
    signals = await conn.fetch(query1)
    
    if signals:
        print(f"Found {len(signals)} signals in last 24h:")
        for s in signals:
            print(f"  ID:{s['id']} | TS:{s['timestamp'].strftime('%H:%M')} | Created:{s['created_at'].strftime('%H:%M:%S')} | Score:{s['total_score']:.0f} | Exchange:{s['exchange_id']}")
        
        target_signal_id = signals[0]['id']
        target_ts = signals[0]['timestamp']
        
        # Check patterns
        print(f"\n2️⃣ Patterns for signal ID {target_signal_id}")
        print("-"*80)
        
        query2 = """
            SELECT sp.pattern_type, sp.timeframe, sp.timestamp as pattern_ts
            FROM fas_v2.sh_patterns shp
            JOIN fas_v2.signal_patterns sp ON shp.signal_patterns_id = sp.id
            WHERE shp.scoring_history_id = $1
        """
        
        patterns = await conn.fetch(query2, target_signal_id)
        
        if patterns:
            print(f"Found {len(patterns)} patterns:")
            target_patterns = []
            for p in patterns:
                is_target = p['pattern_type'] in ['SQUEEZE_IGNITION', 'OI_EXPLOSION']
                marker = "🎯" if is_target else "  "
                print(f"  {marker} {p['pattern_type']:20} | {p['timeframe']:5} | TS:{p['pattern_ts'].strftime('%H:%M')}")
                if is_target:
                    target_patterns.append(p['pattern_type'])
            
            print(f"\nTarget patterns: {target_patterns if target_patterns else '❌ NONE'}")
        else:
            print("❌ NO patterns found")
        
        # Check if in web.signal_analysis
        print(f"\n3️⃣ Check in web.signal_analysis")
        print("-"*80)
        
        query3 = """
            SELECT signal_timestamp, created_at, total_score
            FROM web.signal_analysis
            WHERE pair_symbol = 'TRADOORUSDT'
              AND signal_timestamp = $1
        """
        
        web_signal = await conn.fetchrow(query3, target_ts)
        
        if web_signal:
            print(f"✅ FOUND in web.signal_analysis")
            print(f"   Inserted at: {web_signal['created_at'].strftime('%H:%M:%S')}")
        else:
            print(f"❌ NOT in web.signal_analysis")
        
        # Check previous signals for deduplication
        print(f"\n4️⃣ Previous TRADOORUSDT signals (deduplication check)")
        print("-"*80)
        
        query4 = """
            SELECT signal_timestamp, created_at
            FROM web.signal_analysis
            WHERE pair_symbol = 'TRADOORUSDT'
              AND signal_timestamp < $1
            ORDER BY signal_timestamp DESC
            LIMIT 3
        """
        
        prev_signals = await conn.fetch(query4, target_ts)
        
        if prev_signals:
            print(f"Found {len(prev_signals)} previous signals:")
            for ps in prev_signals:
                diff_h = (target_ts - ps['signal_timestamp']).total_seconds() / 3600
                status = "❌ < 12h" if diff_h < 12 else "✅ >= 12h"
                print(f"  {ps['signal_timestamp'].strftime('%m-%d %H:%M')} | {diff_h:.1f}h ago | {status}")
        else:
            print("No previous signals (would pass deduplication)")
        
        # Simulate Scanner query
        print(f"\n5️⃣ Would Scanner fetch_signals return it?")
        print("-"*80)
        
        scanner_query = """
            SELECT COUNT(*) as cnt
            FROM fas_v2.scoring_history sh
            JOIN fas_v2.signal_patterns sp ON sh.trading_pair_id = sp.trading_pair_id 
                AND sp.timestamp BETWEEN sh.timestamp - INTERVAL '1 hour' AND sh.timestamp + INTERVAL '1 hour'
            JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
            WHERE sh.id = $1
              AND sh.total_score > 250
              AND sp.pattern_type IN ('SQUEEZE_IGNITION', 'OI_EXPLOSION')
              AND tp.contract_type_id = 1
              AND tp.is_active = TRUE
              AND tp.exchange_id = 1
        """
        
        scanner_result = await conn.fetchrow(scanner_query, target_signal_id)
        
        if scanner_result['cnt'] > 0:
            print(f"✅ YES - Scanner query returns {scanner_result['cnt']} row(s)")
        else:
            print(f"❌ NO - Scanner query returns 0 rows")
            print(f"   Possible reasons:")
            print(f"   - No target patterns in time window")
            print(f"   - Score <= 250")
            print(f"   - contract_type_id != 1")
            print(f"   - is_active = FALSE")
            print(f"   - exchange_id != 1")
    else:
        print("❌ No TRADOORUSDT signals found in last 24h")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(investigate_tradoor())
