"""
Test psycopg timezone behavior with PostgreSQL
"""
import sys
from pathlib import Path

# Add paths
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))
sys.path.append(str(current_dir.parent / 'config'))

from pump_analysis_lib import get_db_connection
from datetime import datetime, timezone

print("="*80)
print("PSYCOPG TIMEZONE BEHAVIOR TEST")
print("="*80)

try:
    with get_db_connection() as conn:
        print("\n1. Checking signal_analysis table schema...")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_schema = 'web' 
                  AND table_name = 'signal_analysis'
                  AND column_name IN ('signal_timestamp', 'entry_time', 'max_price_time', 'created_at')
                ORDER BY column_name
            """)
            rows = cur.fetchall()
            if rows:
                print("\n   Column Name          | Data Type                    | UDT Name")
                print("   " + "-"*70)
                for row in rows:
                    print(f"   {row[0]:<20} | {row[1]:<28} | {row[2]}")
            else:
                print("   ⚠️  Table 'signal_analysis' not found or has no timestamp columns")
        
        print("\n2. Checking actual timezone behavior...")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT signal_timestamp, entry_time 
                FROM web.signal_analysis 
                LIMIT 1
            """)
            row = cur.fetchone()
            
            if row:
                signal_ts = row[0]
                entry_time = row[1]
                
                print(f"\n   signal_timestamp:")
                print(f"     Value: {signal_ts}")
                print(f"     Type: {type(signal_ts)}")
                print(f"     Timezone: {signal_ts.tzinfo}")
                print(f"     Is timezone-aware: {signal_ts.tzinfo is not None}")
                
                print(f"\n   entry_time:")
                print(f"     Value: {entry_time}")
                print(f"     Type: {type(entry_time)}")
                print(f"     Timezone: {entry_time.tzinfo}")
                print(f"     Is timezone-aware: {entry_time.tzinfo is not None}")
                
                # Test arithmetic
                print(f"\n3. Testing datetime arithmetic...")
                from datetime import timedelta
                
                new_time = signal_ts + timedelta(hours=1)
                print(f"   signal_ts + 1 hour = {new_time}")
                print(f"   Timezone preserved: {new_time.tzinfo}")
                
                # Test conversion to timestamp
                print(f"\n4. Testing Unix timestamp conversion...")
                ts_ms = int(signal_ts.timestamp() * 1000)
                print(f"   signal_ts.timestamp() * 1000 = {ts_ms}")
                
                # Convert back
                dt_back = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                print(f"   Back to datetime (UTC): {dt_back}")
                print(f"   Match original: {dt_back == signal_ts}")
                
            else:
                print("   ⚠️  No data in signal_analysis table")
        
        print("\n5. Checking minute_candles schema...")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_schema = 'web' 
                  AND table_name = 'minute_candles'
                  AND column_name = 'open_time'
            """)
            row = cur.fetchone()
            if row:
                print(f"\n   open_time: {row[1]} ({row[2]})")
            else:
                print("   ⚠️  Table 'minute_candles' not found")
        
        print("\n6. Checking fas_v2.scoring_history schema...")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_schema = 'fas_v2' 
                  AND table_name = 'scoring_history'
                  AND column_name = 'timestamp'
            """)
            row = cur.fetchone()
            if row:
                print(f"\n   timestamp: {row[1]} ({row[2]})")
                
                # Check actual value
                cur.execute("SELECT timestamp FROM fas_v2.scoring_history LIMIT 1")
                ts_row = cur.fetchone()
                if ts_row:
                    ts_val = ts_row[0]
                    print(f"   Sample value: {ts_val}")
                    print(f"   Timezone-aware: {ts_val.tzinfo is not None}")
                    print(f"   Timezone: {ts_val.tzinfo}")
            else:
                print("   ⚠️  Table 'fas_v2.scoring_history' not found")
        
        print("\n" + "="*80)
        print("✅ PSYCOPG TIMEZONE TEST COMPLETE")
        print("="*80)

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
