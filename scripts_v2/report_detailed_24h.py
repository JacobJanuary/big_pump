"""
Report Detailed Signal Analysis (24h)
Analyzes each signal's price action over 24 hours.
Focuses on drawdowns > 4% and subsequent recovery/pumps.
"""
import sys
from pathlib import Path
import argparse
import json
from datetime import datetime, timezone, timedelta

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, EXCHANGE_FILTER, EXCHANGE_IDS

def format_time_diff(start_dt, end_dt):
    """Format time difference as HH:MM"""
    diff_seconds = (end_dt - start_dt).total_seconds()
    hours = int(diff_seconds // 3600)
    minutes = int((diff_seconds % 3600) // 60)
    return f"{hours:02d}:{minutes:02d}"

def analyze_signal_candles(candles, entry_price, entry_time_dt):
    """
    Analyze candles to find max drawdown and subsequent pump
    Returns: (drawdown_pct, drawdown_time_dt, pump_pct, pump_time_dt)
    """
    min_price = entry_price
    min_price_time = entry_time_dt
    max_pump_after_dd = -999
    max_pump_time = None
    
    # 1. Find Max Drawdown
    for c in candles:
        low = float(c['l'])
        c_time = datetime.fromtimestamp(c['time'] / 1000, tz=timezone.utc)
        
        if low < min_price:
            min_price = low
            min_price_time = c_time
            
    drawdown_pct = ((min_price - entry_price) / entry_price) * 100
    
    # 2. Find Max Pump AFTER the drawdown bottom (recovery)
    # OR Max Pump from Entry (if requested "max pump")
    # The user said "затем +ХХ% ... (максимальный памп)"
    # Usually means "Max Gain relative to Entry" achieved AFTER the drawdown
    
    max_price = 0
    
    for c in candles:
        c_time = datetime.fromtimestamp(c['time'] / 1000, tz=timezone.utc)
        
        # Only look at candles AFTER the drawdown bottom
        if c_time >= min_price_time:
            high = float(c['h'])
            if high > max_price:
                max_price = high
                max_pump_time = c_time
    
    if max_price == 0:
        # Should not happen unless no candles after drawdown
        max_pump_pct = 0
        max_pump_time = min_price_time
    else:
        max_pump_pct = ((max_price - entry_price) / entry_price) * 100
        
    return drawdown_pct, min_price_time, max_pump_pct, max_pump_time

def generate_detailed_report(days=30):
    print("="*120)
    print(f"DETAILED SIGNAL ANALYSIS (LAST {days} DAYS)")
    print("="*120)
    print(f"{'Signal Time':<18} {'Symbol':<10} {'Score':<6} {'Drawdown':<25} {'Subsequent Max Pump':<25}")
    print("-" * 120)
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Fetch signals with candles data
            query = f"""
                SELECT 
                    sa.pair_symbol,
                    sa.signal_timestamp,
                    sa.total_score,
                    sa.entry_price,
                    sa.candles_data,
                    sa.entry_time
                FROM web.signal_analysis sa
                JOIN public.trading_pairs tp ON sa.trading_pair_id = tp.id
                WHERE sa.signal_timestamp >= NOW() - INTERVAL '{days} days'
                AND (
                    '{EXCHANGE_FILTER}' = 'ALL' 
                    OR ('{EXCHANGE_FILTER}' = 'BINANCE' AND tp.exchange_id = {EXCHANGE_IDS['BINANCE']})
                    OR ('{EXCHANGE_FILTER}' = 'BYBIT' AND tp.exchange_id = {EXCHANGE_IDS['BYBIT']})
                )
                ORDER BY sa.signal_timestamp DESC
            """
            cur.execute(query)
            rows = cur.fetchall()
            
            if not rows:
                print("No signals found.")
                return

            for row in rows:
                symbol = row[0]
                signal_ts = row[1]
                score = row[2]
                entry_price = float(row[3])
                candles_data = row[4]
                entry_time_dt = row[5]
                
                if not candles_data:
                    continue
                    
                # Parse candles
                if isinstance(candles_data, str):
                    candles = json.loads(candles_data)
                else:
                    candles = candles_data
                
                # Analyze
                dd_pct, dd_time, pump_pct, pump_time = analyze_signal_candles(candles, entry_price, entry_time_dt)
                
                # Format Output
                ts_str = signal_ts.strftime('%Y-%m-%d %H:%M')
                
                # Drawdown Info
                dd_time_str = format_time_diff(signal_ts, dd_time)
                dd_info = f"{dd_pct:>6.2f}% at {dd_time_str}"
                
                # Pump Info
                if pump_time:
                    pump_time_str = format_time_diff(signal_ts, pump_time)
                    pump_info = f"+{pump_pct:>6.2f}% at {pump_time_str}"
                else:
                    pump_info = "N/A"
                
                # Highlight logic
                # User asked: "по каждому сигналу на каждой просадке более 4%"
                # We show ALL, but maybe mark significant ones?
                # User said "по каждому сигналу без сокращений" -> Show ALL.
                
                # If drawdown > 4% (meaning < -4%), user specifically asked about these details.
                # We will print all, but the format matches their request.
                
                print(f"{ts_str:<18} {symbol:<10} {score:<6} {dd_info:<25} {pump_info:<25}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Detailed signal analysis report.')
    parser.add_argument('--days', type=int, default=30, help='Days to look back')
    args = parser.parse_args()
    
    generate_detailed_report(args.days)
