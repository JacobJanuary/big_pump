#!/usr/bin/env python3
"""
Enhanced 24h Signal Report with Volume Data
Shows detailed signal statistics including volume at entry time
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts_v2.pump_analysis_lib import get_db_connection
from datetime import datetime, timedelta, timezone
import requests
import time

def get_binance_klines(symbol, interval, start_time, end_time, limit=1):
    """
    Get klines from Binance API
    interval: '1m', '1h', etc.
    Returns list of klines or None on error
    """
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
            print(f"  ⚠️ Binance API error for {symbol}: {response.status_code}")
            return None
    except Exception as e:
        print(f"  ⚠️ Request failed for {symbol}: {e}")
        return None

def get_volume_at_entry(symbol, entry_time):
    """
    Get 1m volume at entry time and 1h volume before entry
    entry_time: datetime object (timezone aware)
    Returns: (volume_1m_usdt, volume_1h_usdt) or (None, None)
    """
    entry_ts = int(entry_time.timestamp() * 1000)
    
    # 1-minute volume at entry
    kline_1m = get_binance_klines(symbol, '1m', entry_ts, entry_ts + 60000, limit=1)
    volume_1m = None
    if kline_1m and len(kline_1m) > 0:
        # Kline format: [open_time, open, high, low, close, volume, close_time, quote_volume, ...]
        quote_volume = float(kline_1m[0][7])  # Quote asset volume (USDT)
        volume_1m = quote_volume
    
    # 1-hour volume before entry
    hour_before = entry_ts - 3600000  # 1 hour before
    kline_1h = get_binance_klines(symbol, '1h', hour_before, entry_ts, limit=1)
    volume_1h = None
    if kline_1h and len(kline_1h) > 0:
        quote_volume = float(kline_1h[0][7])
        volume_1h = quote_volume
    
    return volume_1m, volume_1h

def main():
    print("="*80)
    print("📊 Enhanced 24h Signal Report with Volume Data")
    print("="*80)
    
    conn = get_db_connection()
    
    # Get signals from last 24h
    query = """
        SELECT 
            pair_symbol,
            signal_timestamp,
            total_score,
            entry_price,
            max_growth_pct,
            max_drawdown_pct,
            time_to_peak_seconds,
            created_at
        FROM web.signal_analysis
        WHERE signal_timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY signal_timestamp DESC
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        signals = cur.fetchall()
    
    conn.close()
    
    print(f"\n📈 Total signals in last 24h: {len(signals)}")
    
    if not signals:
        print("\n No signals found in last 24 hours")
        return
    
    # Statistics
    total_signals = len(signals)
    profitable_5 = sum(1 for s in signals if s[4] and s[4] > 5)  # max_growth_pct > 5%
    profitable_10 = sum(1 for s in signals if s[4] and s[4] > 10)  # max_growth_pct > 10%
    avg_score = sum(s[2] for s in signals) / total_signals if total_signals > 0 else 0
    avg_growth = sum(s[4] for s in signals if s[4]) / total_signals if total_signals > 0 else 0
    
    print(f"\n📊 Statistics:")
    print(f"   Win Rate (>5%): {profitable_5}/{total_signals} ({profitable_5/total_signals*100:.1f}%)")
    print(f"   Win Rate (>10%): {profitable_10}/{total_signals} ({profitable_10/total_signals*100:.1f}%)")
    print(f"   Average Score: {avg_score:.1f}")
    print(f"   Average Growth: {avg_growth:.1f}%")
    
    print(f"\n📊 Statistics:")
    print(f"   Win Rate (>5% in 15m): {profitable_5}/{total_signals} ({profitable_5/total_signals*100:.1f}%)")
    print(f"   Win Rate (>10% in 1h): {profitable_10}/{total_signals} ({profitable_10/total_signals*100:.1f}%)")
    print(f"   Average Score: {avg_score:.1f}")
    
    # Detailed signals with volume
    print(f"\n📋 Detailed Signals (with Volume):")
    print("-"*90)
    print(f"{'Symbol':<12} {'Time':<8} {'Score':<6} {'Entry':<10} {'Growth%':<8} {'Vol1m':<12} {'Vol1h':<12}")
    print("-"*90)
    
    # Rate limiting: 200 requests per minute = ~3 req/sec
    # We make 2 requests per signal (1m volume + 1h volume)
    # So max ~1.5 signals per second = delay 0.67s between signals
    delay_between_signals = 0.7  # seconds (safe margin)
    
    for i, signal in enumerate(signals, 1):
        symbol = signal[0]
        signal_ts = signal[1]
        score = signal[2]
        entry_price = signal[3]
        max_growth = signal[4] if signal[4] else 0
        
        # Entry time = signal_timestamp + 17 minutes
        if signal_ts.tzinfo is None:
            signal_ts = signal_ts.replace(tzinfo=timezone.utc)
        
        entry_time = signal_ts + timedelta(minutes=17)
        
        # Get volume data from Binance
        vol_1m, vol_1h = get_volume_at_entry(symbol, entry_time)
        
        # Format volume
        vol_1m_str = f"${vol_1m/1000:.1f}K" if vol_1m else "N/A"
        vol_1h_str = f"${vol_1h/1000000:.2f}M" if vol_1h else "N/A"
        
        # Format growth with color
        growth_str = f"+{max_growth:.1f}%" if max_growth > 0 else f"{max_growth:.1f}%"
        
        print(f"{symbol:<12} {signal_ts.strftime('%H:%M'):<8} {score:<6.0f} {entry_price:<10.6f} {growth_str:<8} {vol_1m_str:<12} {vol_1h_str:<12}")
        
        # Rate limiting
        if i < len(signals):  # Not the last signal
            time.sleep(delay_between_signals)
    
    print("-"*80)
    print(f"\n✅ Report complete. Processed {len(signals)} signals.")
    print(f"   API calls made: ~{len(signals) * 2} (within rate limit)")

if __name__ == "__main__":
    main()
