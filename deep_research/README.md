# Deep Research Pipeline

Standalone research environment for Big Pump strategy optimization. Isolated from production to allow experimental changes without breaking live alerting.

---

## 🚀 Quick Start (Full Pipeline)

```bash
cd ~/big_pump

# 1. Cleanup (if starting fresh)
rm -f composite_strategy.json

# 2. Import signals (30 days, score 100-900)
python3 deep_research/populate_signal_analysis.py --days 30 --min-score 100 --max-score 900 --no-dedup

# 3. Download tick data from Binance
python3 deep_research/fetch_agg_trades.py --workers 12

# 4. Aggregate to 1-second bars with Delta
python3 deep_research/prepare_delta_data.py --workers 8 --create-table

# 5. Optional: Download 1m candles for portfolio backtest
python3 deep_research/fetch_minute_candles.py

# 6. Run optimization (Grid Search ~7,200 strategies × ~190k filters)
python3 deep_research/optimize_unified.py --workers 12

# 7. Analyze results → Generate composite_strategy.json
python3 deep_research/analyze_results.py

# 8. Verify with detailed backtest
python3 deep_research/backtest_detailed.py
```

---

## 📂 Script Inventory

| Script | Purpose | Key Parameters |
|--------|---------|----------------|
| `populate_signal_analysis.py` | Import signals from `fas_v2` → `web.signal_analysis` | `--days`, `--min-score`, `--max-score`, `--no-dedup` |
| `fetch_agg_trades.py` | Download raw 1-second aggregate trades from Binance | `--workers`, `--start-date`, `--end-date` |
| `prepare_delta_data.py` | Aggregate trades into 1s bars with Delta, Volume | `--workers`, `--create-table` |
| `optimize_unified.py` | **Core Optimizer**: O(1) grid search with Matrix Pre-computation | `--workers`, `--score-min`, `--score-max` |
| `analyze_results.py` | Aggregate optimization sweeps → `composite_strategy.json` | (reads from JSON results) |
| `backtest_detailed.py` | **Primary Validator**: Timeline-aware backtest with trade logging | (auto-loads `composite_strategy.json`) |
| `backtest_portfolio_realistic.py` | Multi-asset portfolio simulation with slippage/fees | `--sl`, `--activation`, `--timeout` |

*Note: `optimize_combined_leverage_filtered.py` is legacy and superseded by `optimize_unified.py`.*

---

## 📊 Database Schema Reference

The research pipeline uses a dedicated set of tables in the `web` schema to ensure isolation and high performance.

### 1. `web.signal_analysis` (Metadata)
Primary storage for signal events and pre-calculated metrics.
- `id`: Unique signal ID.
- `trading_pair_id`: Reference to `public.trading_pairs`.
- `total_score`: Signal quality score (100-900).
- `max_growth_pct`: Maximum price growth within 24h.
- `max_drawdown_pct`: Maximum drawdown within 24h.
- `time_to_peak_seconds`: Time to reach max price.
- `candles_data`: JSON blob of 24h 5m candles (for quick previews).

### 2. `web.agg_trades` (Raw Data)
Temporary holding area for raw trades from Binance dump files.
- `signal_analysis_id`: Link to signal.
- `price`, `quantity`: Trade details.
- `is_buyer_maker`: Direction (True = Sell, False = Buy).
- `transact_time`: Millisecond timestamp.

### 3. `web.agg_trades_1s` (Optimized Bars)
High-performance 1-second bars with Delta, used by the Optimizer.
- `second_ts`: Unix timestamp (seconds).
- `close_price`: Closing price of the second.
- `delta`: `Buy Volume - Sell Volume`.
- `large_buy_count`: Number of trades > 2σ mean volume.
- `large_sell_count`: Number of large sell trades.

### 4. `web.minute_candles` (Portfolio Backtest)
Standard 1-minute OHLCV candles for macro simulations.
- `open_time`: Unix timestamp.
- `open`, `high`, `low`, `close`, `volume`: Standard OHLCV data.

---

## 🧠 Optimization Logic

The `optimize_unified.py` script uses a **Two-Phase Matrix Optimization**:

### Phase 1: Pre-computation (The Heavy Lifting)
- Loads 1s bars for all signals.
- Runs **5,400+ Strategy Combinations** against every signal.
- Caches results (PnL and Exit Time) in a lookup table.
- **Speed**: ~400,000 iterations/sec.

### Phase 2: Filter Grid Search (The Intelligence)
- Iterates ~190,000 filter combinations (Score, RSI, Vol, OI).
- Filters signals in memory.
- Looks up PnL from Phase 1.
- Applies **Global Position Tracking** (prevents overlapping trades).

### 🎯 Trailing Exit Profiles
The optimizer now tests 4 distinct trailing logic profiles simultaneously:

| Profile | Activation | Callback | Behavior |
|---------|------------|----------|----------|
| **SCALPING** | 0.4% | 0.2% | Extremely aggressive. Takes small profits quickly. Good for high-churn/low-quality signals. |
| **BALANCED** | 4.0% | 2.0% | Standard balanced approach. |
| **MODERATE** | 7.0% | 3.0% | Allows for more volatility before exit. |
| **CONSERVATIVE** | 10.0% | 4.0% | Aims for "moonshots", tolerating significant pullback. |

---

## 🛠 Troubleshooting

### 1. Data Integrity Issues
If backtest shows zero trades or strange results:
```bash
# Check if 1s bars have delta
python3 -c "from pump_analysis_lib import get_db_connection; c=get_db_connection(); print(c.execute('SELECT COUNT(*) FROM web.agg_trades_1s WHERE delta != 0').fetchone()[0])"
```

### 2. "ModuleNotFoundError"
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)
```

### 3. Optimizer Freezes
If `optimize_unified.py` hangs at start:
- Reduce `--workers`.
- Ensure DB connection limit isn't hit (`max_connections` in Postgres).

---

## 📊 Output Files

| File | Description |
|------|-------------|
| `composite_strategy.json` | The "Champion" ruleset. Contains the best strategy parameters for each Score Range. |
| `optimization_results_unified.json` | Raw dump of all profitable configurations found. |
| `backtest_trades.csv` | Detailed log of every trade simulated by `backtest_detailed.py`. |
