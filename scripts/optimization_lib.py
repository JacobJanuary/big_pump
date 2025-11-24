"""
Optimization library for strategy parameter testing
"""
from datetime import datetime, timedelta, timezone

def simulate_sl_with_timeout(candles, entry_price, sl_pct, timeout_hours=None):
    """
    Simulate Stop-Loss with optional timeout
    
    Args:
        candles: List of candle dicts
        entry_price: Entry price
        sl_pct: Stop-loss percentage (negative)
        timeout_hours: Hours to hold if no SL hit (None = hold till end)
    
    Returns: PnL percentage
    """
    sl_price = entry_price * (1 + sl_pct / 100)
    entry_time_ms = candles[0]['open_time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    for candle in candles:
        low = float(candle['low_price'])
        candle_time = candle['open_time']
        
        # Check SL
        if low <= sl_price:
            return sl_pct
        
        # Check timeout
        if timeout_ms and candle_time >= timeout_ms:
            close = float(candle['close_price'])
            return ((close - entry_price) / entry_price) * 100
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    return ((final_price - entry_price) / entry_price) * 100

def simulate_trailing_stop(candles, entry_price, activation_pct, callback_pct, timeout_hours=None):
    """
    Simulate Trailing Stop with optional timeout if not activated
    
    Args:
        candles: List of candle dicts
        entry_price: Entry price
        activation_pct: TS activation percentage (positive)
        callback_pct: TS callback percentage (positive)
        timeout_hours: Hours to hold if TS not activated
    
    Returns: PnL percentage
    """
    activation_price = entry_price * (1 + activation_pct / 100)
    ts_activated = False
    peak_price = entry_price
    entry_time_ms = candles[0]['open_time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    for candle in candles:
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        candle_time = candle['open_time']
        
        # Check if TS should activate
        if not ts_activated and high >= activation_price:
            ts_activated = True
            peak_price = high
        
        if ts_activated:
            # Update peak
            if high > peak_price:
                peak_price = high
            
            # Check callback from peak
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                return ((ts_exit_price - entry_price) / entry_price) * 100
        else:
            # TS not activated, check timeout
            if timeout_ms and candle_time >= timeout_ms:
                close = float(candle['close_price'])
                return ((close - entry_price) / entry_price) * 100
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    return ((final_price - entry_price) / entry_price) * 100

def simulate_combined(candles, entry_price, sl_pct, activation_pct, callback_pct, timeout_hours=None):
    """
    Simulate combined SL + TS strategy with timeout
    
    Args:
        candles: List of candle dicts
        entry_price: Entry price
        sl_pct: Stop-loss percentage (negative)
        activation_pct: TS activation percentage (positive)
        callback_pct: TS callback percentage (positive)
        timeout_hours: Hours to hold if TS not activated
    
    Returns: PnL percentage
    """
    sl_price = entry_price * (1 + sl_pct / 100)
    activation_price = entry_price * (1 + activation_pct / 100)
    ts_activated = False
    peak_price = entry_price
    entry_time_ms = candles[0]['open_time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    for candle in candles:
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        candle_time = candle['open_time']
        
        # Check SL (only before TS activation)
        if not ts_activated and low <= sl_price:
            return sl_pct
        
        # Check if TS should activate
        if not ts_activated and high >= activation_price:
            ts_activated = True
            peak_price = high
        
        if ts_activated:
            # Update peak
            if high > peak_price:
                peak_price = high
            
            # Check callback from peak
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                return ((ts_exit_price - entry_price) / entry_price) * 100
        else:
            # TS not activated, check timeout
            if timeout_ms and candle_time >= timeout_ms:
                close = float(candle['close_price'])
                return ((close - entry_price) / entry_price) * 100
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    return ((final_price - entry_price) / entry_price) * 100

def calculate_peak_time_stats(signals_data):
    """
    Calculate statistics about time to peak
    
    Args:
        signals_data: List of signal dicts with entry_price and candles
    
    Returns: dict with median, mean, percentiles
    """
    times_to_peak = []
    
    for sig in signals_data:
        entry_price = sig['entry_price']
        candles = sig['candles']
        entry_time_ms = candles[0]['open_time']
        
        # Find peak
        max_price = entry_price
        max_price_time_ms = entry_time_ms
        
        for candle in candles:
            high = float(candle['high_price'])
            if high > max_price:
                max_price = high
                max_price_time_ms = candle['open_time']
        
        # Time to peak in hours
        time_to_peak_hours = (max_price_time_ms - entry_time_ms) / (1000 * 3600)
        times_to_peak.append(time_to_peak_hours)
    
    times_to_peak.sort()
    n = len(times_to_peak)
    
    return {
        'median': times_to_peak[n // 2] if n > 0 else 0,
        'mean': sum(times_to_peak) / n if n > 0 else 0,
        'p25': times_to_peak[n // 4] if n > 0 else 0,
        'p75': times_to_peak[3 * n // 4] if n > 0 else 0,
        'p90': times_to_peak[int(0.9 * n)] if n > 0 else 0,
        'min': times_to_peak[0] if n > 0 else 0,
        'max': times_to_peak[-1] if n > 0 else 0
    }
