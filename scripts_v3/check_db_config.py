import sys
import os
from pathlib import Path

# Add config directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))
import pump_analysis_lib

print(f"DB Config: {pump_analysis_lib.DB_CONFIG}")
