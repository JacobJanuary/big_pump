"""
Optimization library for strategy parameter testing
UPDATED: Volume-based exit detection + Dynamic trailing stop callbacks
"""
from datetime import datetime, timedelta, timezone

def calculate_volume_trend(volumes):
    """
    Calculate linear regression slope for volume trend
    
    Args:
        volumes: List of volume values
    
    Returns:
        Normalized slope (positive = increasing, negative = decreasing)
    """
    n = len(volumes)
    if n < 2:
        return 0.0
    
    x = list(range(n))
    y = volumes
    
    # Linear regression slope
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    
    if y_mean == 0:
        return 0.0
    
    numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
    denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
    
    if denominator == 0:
        return 0.0
    
    slope = numerator / denominator
    return slope / y_mean  # Normalize by mean volume

def calculate_price_trend(prices):
    """
    Calculate linear regression slope for price trend
    
    Args:
        prices: List of price values
    
    Returns:
        Normalized slope (positive = increasing, negative = decreasing)
    """
    n = len(prices)
    if n < 2:
        return 0.0
    
    x = list(range(n))
    y = prices
    
    # Linear regression slope
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    
    if y_mean == 0:
        return 0.0
    
    numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
    denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
    
    if denominator == 0:
        return 0.0
    
    slope = numerator / denominator
    return slope / y_mean  # Normalize by mean price

def detect_volume_weakness(candles, entry_price, current_idx, lookback=5):
    """
    Detect volume weakness during price rise (bearish divergence)
    
    Price rising + volume falling = weakness signal
    
    Args:
        candles: List of candle dicts with OHLC + volume
        entry_price: Entry price
        current_idx: Current candle index
        lookback: Number of candles to analyze
    
    Returns:
        weakness_score: 0.0 (no weakness) to 1.0+ (strong weakness)
    """
    if current_idx < lookback:
        return 0.0
    
    # Check if price is rising
    price_now = candles[current_idx]['close_price']
    profit_pct = ((price_now - entry_price) / entry_price) * 100
    
    if profit_pct < 5:  # Only check when in significant profit
        return 0.0
    
    # Calculate volume trend (last 'lookback' candles)
    recent_volumes = [candles[i].get('volume', 0) for i in range(current_idx - lookback + 1, current_idx + 1)]
    
    # Skip if no volume data
    if not recent_volumes or all(v == 0 for v in recent_volumes):
        return 0.0
    
    volume_trend = calculate_volume_trend(recent_volumes)
    
    # Calculate price trend
    recent_prices = [candles[i]['close_price'] for i in range(current_idx - lookback + 1, current_idx + 1)]
    price_trend = calculate_price_trend(recent_prices)
    
    # Divergence: price rising but volume falling
    if price_trend > 0 and volume_trend < 0:
        # Strength of divergence (how much volume is falling relative to price rising)
        divergence_strength = abs(volume_trend) / (price_trend + 0.001)  # Avoid division by zero
        return min(divergence_strength, 2.0)  # Cap at 2.0
    
    return 0.0

def get_dynamic_callback(current_profit_pct):
    """
    Calculate dynamic callback percentage based on current profit level
    
    Tightens callback as profit increases to protect gains
    
    Args:
        current_profit_pct: Current profit percentage from entry
    
    Returns:
        callback_pct: Dynamic callback percentage
    """
    if current_profit_pct >= 48:
        return 1.0  # Very tight for exceptional profits (48%+)
    elif current_profit_pct >= 20:
        return 1.5  # Tight for good profits (20-48%)
    elif current_profit_pct >= 10:
        return 2.0  # Moderate for decent profits (10-20%)
    else:
        return 3.0  # Wide for early profits (0-10%)

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

def simulate_partial_close(candles, entry_price, sl_pct, ts_configs, timeout_hours=None):
    """
    Simulate 4-part partial position closing with independent trailing stops
    
    Each portion (25%) has its own:
    - Activation threshold
    - Callback percentage
    - Exit tracking
    
    Args:
        candles: List of 1-minute OHLC candles
        entry_price: Entry price for position
        sl_pct: Stop-loss percentage (applies to all open portions)
        ts_configs: List of 4 dicts with 'portion', 'activation_pct', 'callback_pct'
        timeout_hours: Maximum hold time
        
    Returns:
        float: Weighted average PnL percentage
    """
    # Initialize portion states
    portions = []
    for config in ts_configs:
        portions.append({
            'size': config['portion'],  # 0.25
            'activation_pct': config['activation_pct'],
            'callback_pct': config['callback_pct'],
            'activated': False,
            'closed': False,
            'peak_price': entry_price,
            'exit_price': None,
            'exit_pnl': None
        })
    
    sl_price = entry_price * (1 + sl_pct / 100)
    entry_time_ms = candles[0]['open_time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    # Process candles
    for candle in candles:
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        close = float(candle['close_price'])
        candle_time = candle['open_time']
        candle_dir = get_candle_direction(candle)
        
        # Process each portion
        for portion in portions:
            if portion['closed']:
                continue
            
            activation_price = entry_price * (1 + portion['activation_pct'] / 100)
            
            # Intracandle order matters
            if candle_dir == 'bullish':
                # Bullish: open -> low -> high -> close
                # Check SL first (low happens before high)
                if not portion['activated'] and low <= sl_price:
                    portion['closed'] = True
                    portion['exit_price'] = sl_price
                    portion['exit_pnl'] = sl_pct
                    continue
                
                # Check TS activation
                if not portion['activated'] and high >= activation_price:
                    portion['activated'] = True
                    portion['peak_price'] = high
                elif portion['activated'] and high > portion['peak_price']:
                    portion['peak_price'] = high
            
            else:  # bearish
                # Bearish: open -> high -> low -> close
                # Check TS activation first (high happens before low)
                if not portion['activated'] and high >= activation_price:
                    portion['activated'] = True
                    portion['peak_price'] = high
                elif portion['activated'] and high > portion['peak_price']:
                    portion['peak_price'] = high
                
                # Check SL after
                if not portion['activated'] and low <= sl_price:
                    portion['closed'] = True
                    portion['exit_price'] = sl_price
                    portion['exit_pnl'] = sl_pct
                    continue
            
            # Check TS callback (for both bullish and bearish)
            if portion['activated']:
                ts_exit_price = portion['peak_price'] * (1 - portion['callback_pct'] / 100)
                if low <= ts_exit_price:
                    portion['closed'] = True
                    portion['exit_price'] = ts_exit_price
                    portion['exit_pnl'] = ((ts_exit_price - entry_price) / entry_price) * 100
                    continue
        
        # Check timeout for all open portions
        if timeout_ms and candle_time >= timeout_ms:
            for portion in portions:
                if not portion['closed']:
                    portion['closed'] = True
                    portion['exit_price'] = close
                    portion['exit_pnl'] = ((close - entry_price) / entry_price) * 100
            break
    
    # Close any remaining portions at final price
    final_price = float(candles[-1]['close_price'])
    for portion in portions:
        if not portion['closed']:
            portion['exit_pnl'] = ((final_price - entry_price) / entry_price) * 100
    
    # Calculate weighted PnL
    total_pnl = sum(p['exit_pnl'] * p['size'] for p in portions)
    return total_pnl

def simulate_partial_close_advanced(
    candles, 
    entry_price, 
    sl_pct, 
    ts_configs, 
    timeout_hours=None,
    use_volume_exit=True,
    use_dynamic_callback=True,
    volume_weakness_threshold=0.15
):
    """
    Advanced simulation with volume-based exits and dynamic callbacks
    
    Features:
    1. Volume weakness detection (bearish divergence)
    2. Dynamic trailing stop callbacks based on profit level
    
    Args:
        candles: List of OHLC + volume candles
        entry_price: Entry price
        sl_pct: Stop-loss percentage
        ts_configs: TS configurations for each portion
        timeout_hours: Timeout in hours
        use_volume_exit: Enable volume weakness exit
        use_dynamic_callback: Enable dynamic callbacks
        volume_weakness_threshold: Volume weakness score threshold
    
    Returns:
        (total_pnl, portions_data): PnL and detailed portion information
    """
    # Initialize portions
    portions = []
    for config in ts_configs:
        portions.append({
            'size': config['portion'],
            'activation_pct': config['activation_pct'],
            'base_callback_pct': config['callback_pct'],
            'activated': False,
            'closed': False,
            'peak_price': entry_price,
            'exit_price': None,
            'exit_pnl': None,
            'exit_reason': None
        })
    
    sl_price = entry_price * (1 + sl_pct / 100)
    entry_time_ms = candles[0]['open_time']
    timeout_ms = entry_time_ms + (timeout_hours * 3600 * 1000) if timeout_hours else None
    
    volume_lookback = 5
    
    # Process candles
    for idx, candle in enumerate(candles):
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        close = float(candle['close_price'])
        candle_time = candle['open_time']
        candle_dir = get_candle_direction(candle)
        
        current_pnl = ((close - entry_price) / entry_price) * 100
        
        # VOLUME WEAKNESS DETECTION
        if use_volume_exit and idx >= volume_lookback:
            weakness_score = detect_volume_weakness(candles, entry_price, idx, volume_lookback)
            
            if current_pnl > 5 and weakness_score >= volume_weakness_threshold:
                # Close ALL open portions immediately
                for portion in portions:
                    if not portion['closed']:
                        portion['closed'] = True
                        portion['exit_price'] = close
                        portion['exit_pnl'] = current_pnl
                        portion['exit_reason'] = 'VOLUME_WEAKNESS'
                break
        
        # Process each portion
        for portion in portions:
            if portion['closed']:
                continue
            
            activation_price = entry_price * (1 + portion['activation_pct'] / 100)
            
            # DYNAMIC CALLBACK
            if use_dynamic_callback and portion['activated']:
                peak_profit_pct = ((portion['peak_price'] - entry_price) / entry_price) * 100
                callback_pct = get_dynamic_callback(peak_profit_pct)
            else:
                callback_pct = portion['base_callback_pct']
            
            # Intracandle order logic
            if candle_dir == 'bullish':
                # SL first
                if not portion['activated'] and low <= sl_price:
                    portion['closed'] = True
                    portion['exit_price'] = sl_price
                    portion['exit_pnl'] = sl_pct
                    portion['exit_reason'] = 'STOP_LOSS'
                    continue
                
                # TS activation
                if not portion['activated'] and high >= activation_price:
                    portion['activated'] = True
                    portion['peak_price'] = high
                elif portion['activated'] and high > portion['peak_price']:
                    portion['peak_price'] = high
            
            else:  # bearish
                # TS activation first
                if not portion['activated'] and high >= activation_price:
                    portion['activated'] = True
                    portion['peak_price'] = high
                elif portion['activated'] and high > portion['peak_price']:
                    portion['peak_price'] = high
                
                # SL after
                if not portion['activated'] and low <= sl_price:
                    portion['closed'] = True
                    portion['exit_price'] = sl_price
                    portion['exit_pnl'] = sl_pct
                    portion['exit_reason'] = 'STOP_LOSS'
                    continue
            
            # TS callback
            if portion['activated']:
                ts_exit_price = portion['peak_price'] * (1 - callback_pct / 100)
                if low <= ts_exit_price:
                    portion['closed'] = True
                    portion['exit_price'] = ts_exit_price
                    portion['exit_pnl'] = ((ts_exit_price - entry_price) / entry_price) * 100
                    portion['exit_reason'] = 'TRAILING_STOP'
                    continue
        
        # Timeout check
        if timeout_ms and candle_time >= timeout_ms:
            for portion in portions:
                if not portion['closed']:
                    portion['closed'] = True
                    portion['exit_price'] = close
                    portion['exit_pnl'] = ((close - entry_price) / entry_price) * 100
                    portion['exit_reason'] = 'TIMEOUT'
            break
    
    # Final close
    final_price = float(candles[-1]['close_price'])
    for portion in portions:
        if not portion['closed']:
            portion['exit_pnl'] = ((final_price - entry_price) / entry_price) * 100
            portion['exit_reason'] = 'END_OF_DATA'
    
    # Calculate weighted PnL
    total_pnl = sum(p['exit_pnl'] * p['size'] for p in portions)
    
    return total_pnl, portions

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
