#!/bin/bash

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment (adjust path if needed)
# Assuming standard location or user provided path
# Trying standard locations
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run the scanner
# This will:
# 1. Fetch new signals from DB
# 2. Deduplicate them
# 3. Store them in web.signal_analysis
# 4. Send Telegram alerts for NEW ones
python3 scripts_v2/pump_scanner.py

# Optional: Trigger minute candle fetch for new signals
# Since fetch_minute_candles.py now handles "mature" signals correctly, 
# we can run it here to catch any signals that just turned 24h old,
# OR to start tracking new ones if we want immediate data (though analysis needs 24h).
# For now, let's run it to keep data fresh. It skips quickly if nothing to do.
python3 scripts_v2/fetch_minute_candles.py
