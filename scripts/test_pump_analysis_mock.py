
import unittest
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path
from datetime import datetime, timedelta
import io

# Add scripts dir to path to import the script module
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# Mock settings before importing the script to avoid import errors if settings relies on env vars
sys.modules['settings'] = MagicMock()
sys.modules['settings'].DATABASE = {
    'host': 'localhost', 'port': '5432', 'dbname': 'test', 'user': 'test', 'password': 'password'
}

import pump_analysis_30d

class TestPumpAnalysis(unittest.TestCase):
    
    @patch('pump_analysis_30d.psycopg.connect')
    def test_analyze_pumps_logic(self, mock_connect):
        # Setup Mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Test Data
        base_time = datetime(2023, 10, 27, 12, 0, 0)
        
        # 1. Mock Signals Return
        mock_signal = {
            'trading_pair_id': 1,
            'pair_symbol': 'TESTUSDT',
            'timestamp': base_time,
            'total_score': 300,
            'pattern_type': 'SQUEEZE_IGNITION'
        }
        
        # 2. Mock Candles Return
        # Entry time is base_time + 15m = 12:15
        entry_time = base_time + timedelta(minutes=15)
        
        # Candle sequence:
        # 12:15: Open 100 (Entry)
        # 12:30: Low 90 (Drawdown -10%)
        # 13:00: High 150 (Growth +50%)
        # 14:00: Close 120
        
        mock_candles = [
            {'timestamp': entry_time, 'open': 100, 'high': 105, 'low': 98, 'close': 102},
            {'timestamp': entry_time + timedelta(minutes=15), 'open': 102, 'high': 103, 'low': 90, 'close': 95}, # Low 90
            {'timestamp': entry_time + timedelta(minutes=45), 'open': 95, 'high': 150, 'low': 95, 'close': 140}, # High 150
            {'timestamp': entry_time + timedelta(hours=1), 'open': 140, 'high': 145, 'low': 135, 'close': 138},
        ]
        
        # Configure side_effect for fetchall to return signals first, then candles
        mock_cursor.fetchall.side_effect = [[mock_signal], mock_candles]
        
        # Capture Stdout
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        # Run Analysis
        pump_analysis_30d.analyze_pumps()
        
        # Restore Stdout
        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()
        
        print("\n--- Captured Output ---")
        print(output)
        print("-----------------------")
        
        # Assertions
        self.assertIn("TESTUSDT", output)
        self.assertIn("300", output) # Score
        self.assertIn("-10.00", output) # Drawdown (90 vs 100)
        self.assertIn("50.00", output) # Growth (150 vs 100)
        
        # Time to peak: 12:15 to 13:00 = 45 minutes
        self.assertIn("0:45:00", output)

if __name__ == '__main__':
    unittest.main()
