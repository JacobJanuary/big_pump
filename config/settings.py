"""
Pump Detection System - Configuration Settings
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load env vars from parent directory
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / '.env')

# Database Configuration
# Note: If DB_PASSWORD is not in .env, password will be None
# This allows PostgreSQL clients to use .pgpass file
DATABASE = {
    'dbname': os.getenv('DB_NAME', 'fox_crypto_new'),
    'user': os.getenv('DB_USER', 'elcrypto'),
    'password': os.getenv('DB_PASSWORD') or None,  # None allows .pgpass usage
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5433')
}

# Individual DB parameters (for asyncpg compatibility)
DB_HOST = DATABASE['host']
DB_PORT = int(DATABASE['port'])
DB_USER = DATABASE['user']
DB_PASSWORD = DATABASE['password']  # Will be None if not in .env
DB_NAME = DATABASE['dbname']


# Detection Settings
DETECTION = {
    'interval_minutes': 5,          # Check every 5 minutes (not used in cron mode)
    'lookback_hours': 8,            # Look for anomalies in last 8 hours (2 candles)
    'min_spike_ratio': 1.5,         # Minimum spike to consider
    'extreme_spike_ratio': 5.0,     # Extreme signal threshold (research: ≥5.0x)
    'very_strong_spike_ratio': 3.0, # Very strong signal (research: ≥3.0x)
    'strong_spike_ratio': 2.0,      # Strong signal threshold (research: ≥2.0x)
    'medium_spike_ratio': 1.5,      # Medium signal threshold (research: ≥1.5x)
    'timeframe': '4h',              # Primary timeframe
    'pump_threshold_pct': 10,       # % gain to confirm pump
    'monitoring_hours': 168,        # Monitor signals for 7 days
}

# Scoring Weights (will be calibrated)
SCORING = {
    'volume_weight': 30,        # Weight for volume spike
    'oi_weight': 25,           # Weight for OI increase
    'spot_weight': 20,         # Weight for spot sync
    'confirmation_weight': 15,  # Weight for confirmations
    'timing_weight': 10,       # Weight for signal freshness
}

# Telegram Configuration
TELEGRAM = {
    'bot_token': os.getenv('TELEGRAM_BOT_TOKEN', ''),
    'channels': {
        'extreme': os.getenv('TELEGRAM_CHANNEL_EXTREME', ''),  # 80%+ confidence
        'high': os.getenv('TELEGRAM_CHANNEL_HIGH', ''),        # 60-80% confidence
        'medium': os.getenv('TELEGRAM_CHANNEL_MEDIUM', ''),    # 40-60% confidence
        'all': os.getenv('TELEGRAM_CHANNEL_ALL') or os.getenv('TELEGRAM_CHAT_ID', '')  # Fallback to CHAT_ID
    },
    'min_confidence_for_alert': 40,  # Minimum confidence to send alert
}

# Logging Configuration
LOGGING = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'dir': BASE_DIR / 'logs',
    'max_size': '100MB',
    'backup_count': 10,
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}

# System Configuration
SYSTEM = {
    'enabled': True,
    'maintenance_mode': False,
    'debug_mode': os.getenv('DEBUG', 'False').lower() == 'true',
    'max_concurrent_pairs': 50,  # Process max 50 pairs at once
    'batch_size': 100,           # Batch size for bulk operations
}

# Web API Configuration
WEB_API = {
    'host': '0.0.0.0',  # Listen on all interfaces
    'port': 2537,       # Custom port as requested
    'cors_origins': ['*'],  # Configure for production
    'rate_limit': '100/minute',
}

# File Paths
PATHS = {
    'scripts': BASE_DIR / 'scripts',
    'logs': BASE_DIR / 'logs',
    'reports': BASE_DIR / 'reports',
    'temp': Path('/tmp/pump_detector_temp'),
}
