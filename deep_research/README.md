# Deep Research Pipeline

Standalone copy of the signal analysis pipeline for deep research and optimization.

## Quick Start

```bash
# 1. Import signals (30 days)
python3 populate_signal_analysis.py --days 30

# 2. Download tick data
python3 fetch_agg_trades.py --workers 12

# 3. Aggregate to 1-second bars
python3 prepare_delta_data.py --workers 8 --create-table

# 4. Run optimization
python3 optimize_unified.py --workers 12

# 5. Analyze results
python3 analyze_results.py

# 6. Verify with backtest
python3 backtest_detailed.py
```

## Scripts

| Script | Purpose |
|--------|---------|
| `populate_signal_analysis.py` | Import signals from fas_v2 → web.signal_analysis |
| `fetch_agg_trades.py` | Download Binance aggTrades |
| `fetch_minute_candles.py` | Fetch 1m candles from API |
| `prepare_delta_data.py` | Aggregate to 1s OHLC+Delta |
| `optimize_unified.py` | Full grid search optimization |
| `analyze_results.py` | Parse results → Strategy Rules |
| `backtest_detailed.py` | Trade-by-trade verification |
| `backtest_portfolio_realistic.py` | Portfolio simulation with fees |
| `report_enhanced.py` | Detailed signal report |

## Libraries

| Library | Purpose |
|---------|---------|
| `pump_analysis_lib.py` | DB connection, signal fetching |
| `db_batch_utils.py` | Batch data loading |
| `optimization_lib.py` | Shared optimization helpers |
