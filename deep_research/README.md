# Deep Research Pipeline

Standalone research environment for Big Pump strategy optimization. Isolated from production to allow experimental changes without breaking live alerting.

---

## 🚀 Quick Start (Full Pipeline)

```bash
cd ~/big_pump

# 1. Cleanup (if starting fresh)
# Delete old strategy file and optionally truncate signal analysis table

# 2. Import signals (30 days, score 100-900)
python3 deep_research/populate_signal_analysis.py --days 30 --min-score 100 --max-score 900 --no-dedup

# 3. Download tick data from Binance
python3 deep_research/fetch_agg_trades.py --workers 12

# 4. Optional: Download 1m candles for broader context
python3 deep_research/fetch_minute_candles.py

# 5. Aggregate to 1-second bars with Delta
python3 deep_research/prepare_delta_data.py --workers 8 --create-table

# 6. Run optimization (Grid Search ~189k combinations)
python3 deep_research/optimize_unified.py --workers 12

# 7. Analyze results → Generate composite_strategy.json
python3 deep_research/analyze_results.py

# 8. Verify with detailed backtest
python3 deep_research/backtest_detailed.py
```

---

## 📂 Script Inventory

### 1. Data Ingestion & Preparation

| Script | Purpose | Key Parameters |
|--------|---------|----------------|
| `populate_signal_analysis.py` | Import signals from `fas_v2` → `web.signal_analysis` | `--days`, `--min-score`, `--max-score`, `--no-dedup` |
| `fetch_agg_trades.py` | Download raw 1-second aggregate trades from Binance | `--workers`, `--start-date`, `--end-date` |
| `fetch_minute_candles.py` | Fetch 1-minute OHLCV candles for macro analysis | `--workers` |
| `prepare_delta_data.py` | Aggregate trades into 1s bars with Delta, RSI, Volume Z-Score | `--workers`, `--create-table` |

### 2. Strategy Optimization

| Script | Purpose | Key Parameters |
|--------|---------|----------------|
| `optimize_unified.py` | **Core Optimizer**: O(1) grid search with Matrix Pre-computation (~400k it/sec) | `--workers`, `--score-min`, `--score-max` |
| `optimize_combined_leverage_filtered.py` | Legacy optimizer; superseded by Unified | `--workers` |
| `optimize_combined_leverage.py` | Earlier version of combined optimizer | `--workers` |
| `optimization_lib.py` | Shared math utilities and indicator functions | (Library) |

### 3. Backtesting & Reporting

| Script | Purpose | Key Parameters |
|--------|---------|----------------|
| `backtest_detailed.py` | **Primary Validator**: Timeline-aware backtest with trade logging | `--strategy-file`, `--output` |
| `backtest_portfolio_realistic.py` | Multi-asset portfolio simulation with slippage/fees | `--initial-capital`, `--max-positions` |
| `analyze_results.py` | Aggregate optimization sweeps → `composite_strategy.json` | (reads from DB results) |
| `report_enhanced.py` | Generate visual/tabular reports from trade logs | `--format`, `--output` |
| `show_intermediate_top.py` | Inspect progress of long-running optimizations | (utility) |

### 4. Libraries & Utilities

| Library | Purpose |
|---------|---------|
| `pump_analysis_lib.py` | DB connection, signal fetching, production filter configs |
| `db_batch_utils.py` | Optimized database IO for batch bar loading |
| `optimization_lib.py` | Shared mathematical utilities for optimization |
| `diagnose_freeze.py` | Debug utility for identifying multiprocessing bottlenecks |

---

## ⚠️ Pre-Execution Cleanup

Before starting a new research cycle:

```bash
# 1. Delete old strategy file
rm -f composite_strategy.json

# 2. Optional: Truncate signal analysis table (if changing date range)
python3 -c "
from pump_analysis_lib import get_db_connection
conn = get_db_connection()
cur = conn.cursor()
cur.execute('TRUNCATE web.signal_analysis CASCADE')
conn.commit()
conn.close()
print('Signal analysis table truncated')
"

# 3. Optional: Truncate 1-second bars (if data is corrupted)
python3 -c "
from db_batch_utils import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute('TRUNCATE web.agg_trades_1s')
conn.commit()
conn.close()
print('1-second bars table truncated')
"
```

---

## 🛠 Troubleshooting

### 1. ModuleNotFoundError: No module named 'pump_analysis_lib'

Scripts depend on local libraries. Set `PYTHONPATH`:

```bash
cd ~/big_pump
PYTHONPATH=./deep_research python3 deep_research/backtest_detailed.py
```

Or export permanently:
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/deep_research
```

### 2. Missing Database Driver (psycopg2)

```bash
pip install psycopg2-binary
# or
pip install psycopg
```

### 3. Zero Delta or Large Trade Counts

If backtest shows all "Timeout" or zero re-entries:

```sql
-- Check data integrity
SELECT COUNT(*) FROM web.agg_trades_1s WHERE delta != 0 OR large_buy_count > 0;
```

If `large_buy_count` is 0 but `delta` populated — truncate and rerun:
```bash
# Truncate and rebuild
PYTHONPATH=./deep_research python3 -c "from db_batch_utils import get_connection; c=get_connection(); cur=c.cursor(); cur.execute('TRUNCATE web.agg_trades_1s'); c.commit()"

python3 deep_research/prepare_delta_data.py --workers 8
```

### 4. Optimizer Freezes at ~6% (11k iterations)

This indicates a data bottleneck. Run diagnostics:
```bash
python3 deep_research/diagnose_freeze.py
```

---

## 📊 Output Files

| File | Location | Description |
|------|----------|-------------|
| `composite_strategy.json` | Project root | Generated ruleset with champion parameters per score range |
| `backtest_results.json` | (configurable) | Detailed trade log from backtest |
| `optimization_results/` | (if configured) | Intermediate optimization checkpoints |

---

## 🔬 Research Environment Notes

- **Isolation**: Changes to libs won't break live alerting scanner
- **Portability**: Folder can be moved to high-performance servers for long runs
- **Key metrics**: Synthetic Yield vs Realized Portfolio Return (gap typically ~7x)
