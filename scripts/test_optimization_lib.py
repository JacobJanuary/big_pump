"""
Unit tests for optimization_lib to verify correct intracandle order processing
"""
import sys
from pathlib import Path

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from optimization_lib import simulate_combined, get_candle_direction

def test_candle_direction():
    """Test candle direction detection"""
    # Bullish candle
    bullish = {'open_price': 100, 'close_price': 105}
    assert get_candle_direction(bullish) == 'bullish'
    
    # Bearish candle
    bearish = {'open_price': 105, 'close_price': 100}
    assert get_candle_direction(bearish) == 'bearish'
    
    # Doji (equal)
    doji = {'open_price': 100, 'close_price': 100}
    assert get_candle_direction(doji) == 'bullish'  # Treated as bullish
    
    print("✓ Candle direction tests passed")

def test_bullish_candle_sl_before_activation():
    """
    Bullish candle: low happens BEFORE high
    If SL is hit before TS activation, should close at SL
    """
    entry_price = 100
    sl_pct = -5  # SL at 95
    activation_pct = 10  # Activation at 110
    callback_pct = 2
    
    # Bullish candle: low=94 (hits SL), then high=112 (would activate TS)
    candles = [{
        'open_time': 0,
        'open_price': 98,
        'high_price': 112,
        'low_price': 94,
        'close_price': 105
    }]
    
    result = simulate_combined(candles, entry_price, sl_pct, activation_pct, callback_pct)
    
    # Should exit at SL because low happens first
    assert result == -5, f"Expected -5%, got {result}%"
    print("✓ Bullish candle: SL before activation - PASS")

def test_bearish_candle_activation_before_sl():
    """
    Bearish candle: high happens BEFORE low
    If TS activates before SL is hit, should use TS logic
    """
    entry_price = 100
    sl_pct = -5  # SL at 95
    activation_pct = 10  # Activation at 110
    callback_pct = 2  # Callback 2%
    
    # Bearish candle: high=112 (activates TS), then low=94 (triggers callback)
    candles = [{
        'open_time': 0,
        'open_price': 102,
        'high_price': 112,
        'low_price': 94,
        'close_price': 96
    }]
    
    result = simulate_combined(candles, entry_price, sl_pct, activation_pct, callback_pct)
    
    # Should exit at TS callback: 112 * 0.98 = 109.76, profit = 9.76%
    expected = ((109.76 - 100) / 100) * 100
    assert abs(result - expected) < 0.01, f"Expected {expected:.2f}%, got {result}%"
    print(f"✓ Bearish candle: TS activation before SL - PASS (exit at {result:.2f}%)")

def test_bearish_candle_sl_without_activation():
    """
    Bearish candle: high NOT enough to activate TS
    Should hit SL normally
    """
    entry_price = 100
    sl_pct = -5  # SL at 95
    activation_pct = 10  # Activation at 110
    callback_pct = 2
    
    # Bearish candle: high=108 (doesn't activate TS), low=94 (hits SL)
    candles = [{
        'open_time': 0,
        'open_price': 102,
        'high_price': 108,
        'low_price': 94,
        'close_price': 96
    }]
    
    result = simulate_combined(candles, entry_price, sl_pct, activation_pct, callback_pct)
    
    # Should exit at SL
    assert result == -5, f"Expected -5%, got {result}%"
    print("✓ Bearish candle: SL without activation - PASS")

def test_multiple_candles_progression():
    """
    Test progression across multiple candles
    """
    entry_price = 100
    sl_pct = -5
    activation_pct = 10
    callback_pct = 2
    
    # Candle 1: Small gain, no activation
    # Candle 2: Activates TS at 115, peak=115
    # Candle 3: Callback triggers at 115*0.98=112.7
    candles = [
        {
            'open_time': 0,
            'open_price': 100,
            'high_price': 105,
            'low_price': 99,
            'close_price': 104
        },
        {
            'open_time': 60000,
            'open_price': 104,
            'high_price': 115,  # Activates TS, peak=115
            'low_price': 103,
            'close_price': 114
        },
        {
            'open_time': 120000,
            'open_price': 114,
            'high_price': 114.5,  # No new peak (< 115)
            'low_price': 112,     # Triggers callback at 115*0.98=112.7
            'close_price': 113
        }
    ]
    
    result = simulate_combined(candles, entry_price, sl_pct, activation_pct, callback_pct)
    
    # Should exit at 112.7, profit = 12.7%
    expected = 12.7
    assert abs(result - expected) < 0.01, f"Expected {expected:.2f}%, got {result}%"
    print(f"✓ Multiple candles progression - PASS (exit at {result:.2f}%)")

def test_timeout_without_activation():
    """
    Test timeout when TS never activates
    """
    entry_price = 100
    sl_pct = -5
    activation_pct = 20  # High activation, won't trigger
    callback_pct = 2
    timeout_hours = 2
    
    # 3 hours of candles, none reaching activation
    candles = [
        {'open_time': i * 3600000, 'open_price': 105, 'high_price': 110, 'low_price': 100, 'close_price': 108}
        for i in range(4)  # 0h, 1h, 2h, 3h
    ]
    
    result = simulate_combined(candles, entry_price, sl_pct, activation_pct, callback_pct, timeout_hours)
    
    # Should exit at timeout (2h) with close price of that candle
    expected = 8.0
    assert result == expected, f"Expected {expected}%, got {result}%"
    print(f"✓ Timeout without activation - PASS (exit at {result}%)")

def run_all_tests():
    """Run all tests"""
    print("="*60)
    print("Running optimization_lib tests...")
    print ("="*60)
    
    try:
        test_candle_direction()
        test_bullish_candle_sl_before_activation()
        test_bearish_candle_activation_before_sl()
        test_bearish_candle_sl_without_activation()
        test_multiple_candles_progression()
        test_timeout_without_activation()
        
        print("="*60)
        print("✓ ALL TESTS PASSED!")
        print("="*60)
        return True
        
    except AssertionError as e:
        print("="*60)
        print(f"✗ TEST FAILED: {e}")
        print("="*60)
        return False
    except Exception as e:
        print("="*60)
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        print("="*60)
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
