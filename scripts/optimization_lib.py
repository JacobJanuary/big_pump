"""
Optimization library for strategy parameter testing
UPDATED: Hybrid timeout exit strategy for loss minimization
"""
from datetime import datetime, timedelta, timezone

def get_candle_direction(candle):
    """
    Determine candle direction to understand intracandle event order
    
    Returns:
        'bullish' if close >= open (low happens first, then high)
        'bearish' if close < open (high happens first, then low)
    """
    open_price = float(candle['open_price'])
    close_price = float(candle['close_price'])
    return 'bullish' if close_price >= open_price else 'bearish'

def simulate_combined(candles, entry_price, sl_pct, activation_pct, callback_pct, timeout_hours=None):
    """
    Simulate combined SL + TS strategy with HYBRID timeout exit
    
    Hybrid Timeout Strategy:
    - If profit when timeout → close immediately
    - If small loss (-2% or less) → grace period 2h to reach breakeven
    - If big loss (> -2%) → close immediately
    
    Args:
        candles: List of candle dicts with OHLC
        entry_price: Entry price
        sl_pct: Stop-loss percentage (negative, e.g. -5)
        activation_pct: TS activation percentage (positive, e.g. 10)
        callback_pct: TS callback percentage (positive, e.g. 2)
        timeout_hours: Hours to hold if TS not activated
    
    Returns: PnL percentage
    """
    sl_price = entry_price * (1 + sl_pct / 100)
    activation_price = entry_price * (1 + activation_pct / 100)
    ts_activated = False
    peak_price = entry_price
    entry_time_ms = candles[0]['open_time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    # Hybrid timeout parameters
    grace_period_hours = 2
    grace_period_ms = grace_period_hours * 3600 * 1000
    grace_period_active = False
    best_price_in_grace = 0
    small_loss_threshold = -2.0  # If loss <= -2%, activate grace period
    
    for candle in candles:
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        close = float(candle['close_price'])
        candle_time = candle['open_time']
        candle_dir = get_candle_direction(candle)
        current_pnl = ((close - entry_price) / entry_price) * 100
        
        # Process events in correct order based on candle direction
        if candle_dir == 'bullish':
            # Bullish candle: open → low → high → close
            # Price goes down first (to low), then up (to high)
            
            # 1. Check SL first (low happens before high)
            if not ts_activated and low <= sl_price:
                return sl_pct
            
            # 2. Check TS activation (high happens after low)
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated:
                # Update peak if we're already tracking
                if high > peak_price:
                    peak_price = high
        
        else:  # bearish
            # Bearish candle: open → high → low → close
            # Price goes up first (to high), then down (to low)
            
            # 1. Check TS activation OR update peak first (high happens before low)
            if not ts_activated and high >= activation_price:
                ts_activated = True
                peak_price = high
            elif ts_activated:
                # Update peak if already activated
                if high > peak_price:
                    peak_price = high
            
            # 2. Check SL only if TS not activated (low happens after high)
            if not ts_activated and low <= sl_price:
                return sl_pct
        
        # Check TS callback (after processing high/low in correct order)
        if ts_activated:
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                # Calculate exact exit PnL
                return ((ts_exit_price - entry_price) / entry_price) * 100
        
        # HYBRID TIMEOUT LOGIC
        if not ts_activated and timeout_ms:
            # Main timeout reached
            if candle_time >= timeout_ms and not grace_period_active:
                if current_pnl >= 0:
                    # PROFIT or BREAKEVEN → close immediately
                    return current_pnl
                
                elif current_pnl >= small_loss_threshold:
                    # SMALL LOSS → activate grace period
                    grace_period_active = True
                    best_price_in_grace = close
                    continue
                
                else:
                    # BIG LOSS → cut it immediately
                    return current_pnl
            
            # Grace period active (additional 2h for recovery)
            if grace_period_active:
                # Track best price in grace period
                if close > best_price_in_grace:
                    best_price_in_grace = close
                
                # Exit if recovered to near-breakeven
                if current_pnl >= -0.5:  # Almost breakeven
                    return current_pnl
                
                # Grace period expired
                if candle_time >= timeout_ms + grace_period_ms:
                    # Exit at current price (grace period didn't help enough)
                    return current_pnl
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    return ((final_price - entry_price) / entry_price) * 100

def simulate_sl_with_timeout(candles, entry_price, sl_pct, timeout_hours=None):
    """
    Simulate Stop-Loss with optional timeout
    
    Args:
        candles: List of candle dicts
        entry_price: Entry price
        sl_pct: Stop-loss percentage (negative)
        timeout_hours: Hours to hold if no SL hit
    
    Returns: PnL percentage
    """
    sl_price = entry_price * (1 + sl_pct / 100)
    entry_time_ms = candles[0]['open_time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    for candle in candles:
        low = float(candle['low_price'])
        close = float(candle['close_price'])
        candle_time = candle['open_time']
        
        # Check SL (always checks low regardless of candle direction)
        if low <= sl_price:
            return sl_pct
        
        # Check timeout
        if timeout_ms and candle_time >= timeout_ms:
            return ((close - entry_price) / entry_price) * 100
    
    # Position still open at end
    final_price = float(candles[-1]['close_price'])
    return ((final_price - entry_price) / entry_price) * 100

def simulate_trailing_stop(candles, entry_price, activation_pct, callback_pct, timeout_hours=None):
    """
    Simulate Trailing Stop with correct intracandle order
    
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
        close = float(candle['close_price'])
        candle_time = candle['open_time']
        
        # Check activation (high is always the activation point)
        if not ts_activated and high >= activation_price:
            ts_activated = True
            peak_price = high
        elif ts_activated:
            # Update peak
            if high > peak_price:
                peak_price = high
        
        # Check callback
        if ts_activated:
            ts_exit_price = peak_price * (1 - callback_pct / 100)
            if low <= ts_exit_price:
                return ((ts_exit_price - entry_price) / entry_price) * 100
        
        # Check timeout if not activated
        if not ts_activated and timeout_ms and candle_time >= timeout_ms:
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
