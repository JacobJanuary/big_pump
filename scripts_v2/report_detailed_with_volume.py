"""
Enhanced Report: Detailed Signal Analysis with Volume Data
Based on report_detailed_24h.py with added volume metrics
"""
import sys
from pathlib import Path
import argparse
import json
from datetime import datetime, timezone, timedelta
import requests
import time

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

def get_binance_klines(symbol, interval, start_time, end_time, limit=1):
    """Get klines from Binance API"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': limit
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception:
        return None

def get_volume_at_entry(symbol, entry_time):
    """
    Get 1m volume at entry time and 1h volume before entry
    Returns: (volume_1m_usdt, volume_1h_usdt) or (None, None)
    """
    entry_ts = int(entry_time.timestamp() * 1000)
    
    # 1-minute volume at entry
    kline_1m = get_binance_klines(symbol, '1m', entry_ts, entry_ts + 60000, limit=1)
    volume_1m = None
    if kline_1m and len(kline_1m) > 0:
        volume_1m = float(kline_1m[0][7])  # Quote asset volume (USDT)
    
    # 1-hour volume before entry
    hour_before = entry_ts - 3600000
    kline_1h = get_binance_klines(symbol, '1h', hour_before, entry_ts, limit=1)
    volume_1h = None
    if kline_1h and len(kline_1h) > 0:
        volume_1h = float(kline_1h[0][7])
    
    return volume_1m, volume_1h

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
    
    # 2. Find Max Pump AFTER the drawdown bottom
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
        max_pump_pct = 0
        max_pump_time = min_price_time
    else:
        max_pump_pct = ((max_price - entry_price) / entry_price) * 100
        
    return drawdown_pct, min_price_time, max_pump_pct, max_pump_time

def generate_detailed_report(days=30):
    print("="*140)
    print(f"DETAILED SIGNAL ANALYSIS WITH VOLUME (LAST {days} DAYS)")
    print("="*140)
    print(f"{'Signal Time':<18} {'Symbol':<10} {'Score':<6} {'Drawdown':<25} {'Subsequent Max Pump':<25} {'Vol1m':<12} {'Vol1h':<12}")
    print("-" * 140)
    
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

            # Rate limiting setup
            delay_between_signals = 0.7  # seconds
            
            for i, row in enumerate(rows):
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
                
                # Analyze price action
                dd_pct, dd_time, pump_pct, pump_time = analyze_signal_candles(candles, entry_price, entry_time_dt)
                
                # Get volume data
                if entry_time_dt.tzinfo is None:
                    entry_time_dt = entry_time_dt.replace(tzinfo=timezone.utc)
                
                vol_1m, vol_1h = get_volume_at_entry(symbol, entry_time_dt)
                
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
                
                # Volume Info
                vol_1m_str = f"${vol_1m/1000:.1f}K" if vol_1m else "N/A"
                vol_1h_str = f"${vol_1h/1000000:.2f}M" if vol_1h else "N/A"
                
                print(f"{ts_str:<18} {symbol:<10} {score:<6} {dd_info:<25} {pump_info:<25} {vol_1m_str:<12} {vol_1h_str:<12}")
                
                # Rate limiting
                if i < len(rows) - 1:
                    time.sleep(delay_between_signals)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enhanced detailed signal analysis report with volume.')
    parser.add_argument('--days', type=int, default=30, help='Days to look back')
    args = parser.parse_args()
    
    generate_detailed_report(args.days)
