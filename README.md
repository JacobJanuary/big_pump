# Big Pump - Cryptocurrency Trading Signal Analysis System

A comprehensive cryptocurrency trading signal analysis and optimization platform for futures trading on Binance and Bybit exchanges.

## Overview

Big Pump is a real-time trading signal analysis system that:
- Detects high-quality trading opportunities using technical indicators and pattern recognition
- Streams signals via WebSocket to trading bots
- Provides backtesting and strategy optimization tools
- Stores and analyzes historical signal performance

## Project Structure

```
big_pump/
├── scripts_v2/          # Production - Main analysis and signal system
│   ├── sql/             # Database optimization scripts
│   └── *.py             # Core modules
├── scripts_v3/          # Experimental - Development and testing
├── config/              # Configuration files
├── reports/             # Generated analysis reports
├── migrations/          # Database migrations
└── archive/             # Archived code
```

## Core Modules (scripts_v2/)

### Signal Server
- **`high_score_signal_server.py`** - WebSocket server for real-time signal streaming
  - Hybrid NOTIFY/polling architecture
  - Token-based authentication
  - Smart deduplication (12h cooldown)
  - Signal filtering by score, patterns, and indicators

### Analysis Libraries
- **`pump_analysis_lib.py`** - Core library for signal analysis
  - Database connection management
  - Multi-exchange price fetching (Binance, Bybit)
  - Signal deduplication
  - Entry price calculation (17-min offset from signal)

- **`optimization_lib.py`** - Strategy simulation and optimization
  - Stop-loss / Trailing Stop simulation
  - Partial position closing (4-part strategy)
  - Volume-based exit detection
  - Dynamic callback calculation

### Data Population
- **`populate_signal_analysis.py`** - Populate signal analysis database
- **`populate_volume_data.py`** - Fetch and store volume data
- **`populate_indicators.py`** - Calculate technical indicators
- **`fetch_minute_candles.py`** - Fetch 5-minute candle data

### Optimization Scripts
- **`optimize_advanced.py`** - Advanced parameter optimization
- **`optimize_calculate_is_win.py`** - Win/loss calculation
- **`optimize_find_best_filters.py`** - Filter optimization

### Reports
- **`report_enhanced.py`** - Enhanced analysis reports
- **`report_enhanced_double.py`** - Double-filtered reports
- **`report_detailed_24h.py`** - 24-hour detailed analysis
- **`report_signals_30d.py`** - 30-day signal report

### Utilities
- **`backtest_portfolio_realistic.py`** - Realistic portfolio backtesting
- **`pump_scanner.py`** - Signal scanning utility
- **`deep_investigation.py`** - Deep signal investigation

### SQL Optimization (scripts_v2/sql/)
Optimized PostgreSQL functions for high-performance indicator calculations:
- **`calculate_indicators_bulk_v3_fast.sql`** - Fast batch indicator calculation
- **`indicator_scores_batch_v2.sql`** - Batch scoring with market regime
- **`*_setbased.sql`** - SET-based indicator functions (RSI, ATR, MACD, etc.)
- **`optimize_early_exit.sql`** - Early exit optimization

## Experimental Modules (scripts_v3/)

Development and testing scripts for new features:
- **`analyze_signal_outcomes.py`** - Signal outcome analysis
- **`optimize_filters.py`** - Filter optimization experiments
- **`fetch_big_pump_candles.py`** - Alternative candle fetching
- **`create_big_pump_signals_table.py`** - Table creation utilities

## Technical Stack

- **Language**: Python 3.12+
- **Database**: PostgreSQL (psycopg3)
- **WebSocket**: websockets library
- **Exchanges**: Binance Futures, Bybit Perpetual
- **Data**: 5-minute candles, technical indicators

## Signal Scoring

Signals are scored based on multiple factors:
- **Pattern Score**: SQUEEZE_IGNITION, OI_EXPLOSION patterns
- **Indicator Score**: RSI, Volume Z-Score, OI Delta, MACD, etc.
- **Market Regime**: BULL/BEAR/NEUTRAL adjustment

High-quality signals: `total_score > 250`

## Configuration

Environment variables (`.env`):
```
DB_HOST=localhost
DB_PORT=5433
DB_NAME=fox_crypto_new
DB_USER=your_user
DB_PASSWORD=your_password
```

## Usage

### Start Signal Server
```bash
cd scripts_v2
python high_score_signal_server.py
```

### Run Analysis
```bash
python populate_signal_analysis.py
python report_enhanced.py
```

### Optimize Strategy
```bash
python optimize_advanced.py
```

## Database Schema

Key tables:
- `fas_v2.scoring_history` - Signal scores
- `fas_v2.indicators` - Technical indicators
- `fas_v2.market_data_aggregated` - OHLCV data
- `web.signal_analysis` - Trade analysis results

## Performance

Optimized for high-frequency updates:
- Indicator calculation: ~40s (was ~100s)
- Signal streaming: <10ms latency
- Database: 7M+ indicator rows

## License

Private/Proprietary

## Author

Evgeniy Yanvarskiy
