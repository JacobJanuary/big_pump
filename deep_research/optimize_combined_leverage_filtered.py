# """
# Оптимизация COMBINED_A_B с учётом кредитного плеча (ФИЛЬТРОВАННАЯ ВЕРСИЯ).
# Эта копия использует фильтры, определённые в pump_analysis_lib.py (SCORE_THRESHOLD, INDICATOR_FILTERS, EXCHANGE_FILTER).
# Добавлен глобальный трекинг открытых позиций по паре, частичная параллелизация по разным парам и красивый прогресс‑бар tqdm.
# """

import sys
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, NamedTuple
from multiprocessing import Pool
import itertools
import json
import time
from tqdm import tqdm

# Ensure local imports work
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, fetch_signals

# ============== КОНФИГУРАЦИЯ ==============

COMMISSION_PCT = 0.04

# Фиксированные параметры
BASE_ACTIVATION = 10.0
BASE_CALLBACK = 4.0
BASE_REENTRY_DROP = 5.0
BASE_COOLDOWN = 300  # seconds

# Параметры для оптимизации
PARAM_GRID = {
    "delta_window": [10, 20, 30, 60, 120],
    "threshold_mult": [1.0, 1.5, 2.0, 2.5, 3.0],
    "leverage": [1, 5, 10],
}

# SL варианты для каждого leverage
SL_BY_LEVERAGE = {
    1: [5, 7, 10, 15, 20],
    5: [3, 4, 5, 7, 10, 15],
    10: [2, 3, 4, 5, 7, 8],
}

# Generate all parameter combinations once with IDs
PARAM_COMBINATIONS = {}
combo_id = 0
for leverage in PARAM_GRID["leverage"]:
    for sl in SL_BY_LEVERAGE[leverage]:
        for delta_window in PARAM_GRID["delta_window"]:
            for threshold in PARAM_GRID["threshold_mult"]:
                PARAM_COMBINATIONS[combo_id] = {
                    "leverage": leverage,
                    "sl_pct": sl,
                    "delta_window": delta_window,
                    "threshold_mult": threshold,
                }
                combo_id += 1

# ---------------------------------------------------------------------------
# Helper data structures
# ---------------------------------------------------------------------------

class SignalInfo(NamedTuple):
    """Хранит метаданные сигнала, необходимые для глобального трекинга"""
    signal_id: int
    pair: str
    timestamp: datetime

# ---------------------------------------------------------------------------
# Delta helpers (unchanged)
# ---------------------------------------------------------------------------

def get_rolling_delta(bars: List[tuple], idx: int, window: int) -> float:
    """Вычислить rolling delta."""
    if idx < 1 or window <= 0:
        return 0.0
    current_ts = bars[idx][0]
    window_start = current_ts - window
    delta_sum = 0.0
    for j in range(idx, -1, -1):
        if bars[j][0] < window_start:
            break
        delta_sum += bars[j][2]
    return delta_sum


def get_avg_delta(bars: List[tuple], idx: int, lookback: int = 100) -> float:
    """Вычислить среднюю абсолютную delta."""
    if idx < lookback:
        lookback = idx
    if lookback < 1:
        return 0.0
    total_abs_delta = 0.0
    count = 0
    start = idx - lookback
    for i in range(start, idx):
        total_abs_delta += abs(bars[i][2])
        count += 1
    return total_abs_delta / count if count > 0 else 0.0

# ---------------------------------------------------------------------------
# Core strategy execution – now returns PnL и timestamp последнего бара
# ---------------------------------------------------------------------------

def precompute_bars(bars: List[tuple], entry_ts: int = 0) -> Dict:
    """Precompute cumsum arrays for bars - call ONCE per signal, reuse for all strategies.
    
    Args:
        bars: List of bar tuples, may include lookback bars BEFORE entry_ts
        entry_ts: Unix timestamp of entry time. Trading starts from first bar >= entry_ts.
                  If 0, assume first bar is entry (backwards compatibility).
    """
    if not bars:
        return None
    
    n = len(bars)
    
    # Find entry_idx - first bar where ts >= entry_ts
    entry_idx = 0
    if entry_ts > 0:
        for i, bar in enumerate(bars):
            if bar[0] >= entry_ts:
                entry_idx = i
                break
    
    # Cumulative delta sums (include ALL bars for lookback)
    cumsum_delta = [0.0] * (n + 1)
    cumsum_abs_delta = [0.0] * (n + 1)
    for i in range(n):
        cumsum_delta[i + 1] = cumsum_delta[i] + bars[i][2]
        cumsum_abs_delta[i + 1] = cumsum_abs_delta[i] + abs(bars[i][2])
    
    # Precompute avg_delta for lookback=100
    lookback = 100
    avg_delta_arr = [0.0] * n
    for i in range(n):
        lb = min(i, lookback)
        if lb > 0:
            avg_delta_arr[i] = (cumsum_abs_delta[i] - cumsum_abs_delta[i - lb]) / lb
    
    return {
        'bars': bars,
        'n': n,
        'entry_idx': entry_idx,  # NEW: trading starts from this index
        'cumsum_delta': cumsum_delta,
        'avg_delta_arr': avg_delta_arr,
    }

def run_strategy_fast(
    precomputed: Dict,
    sl_pct: float,
    delta_window: int,
    threshold_mult: float,
    leverage: int,
    # Parameterized strategy constants (use defaults for backwards compatibility)
    base_activation: float = BASE_ACTIVATION,
    base_callback: float = BASE_CALLBACK,
    base_reentry_drop: float = BASE_REENTRY_DROP,
    base_cooldown: int = BASE_COOLDOWN,
    max_reentry_seconds: int = 0,  # 0 = no limit (backwards compatibility)
    max_position_seconds: int = 0,  # 0 = no limit (backwards compatibility)
) -> Tuple[float, int]:
    """Run strategy using precomputed bar data - FAST version.
    
    New in v2: Uses proportional threshold scaling when data < delta_window.
    This prevents false exits based on insufficient data.
    
    Args:
        max_reentry_seconds: Maximum time from signal start for re-entry.
                             0 = no limit (default for backwards compatibility).
        max_position_seconds: Maximum time a position can stay open.
                              0 = no limit (default for backwards compatibility).
    """
    if precomputed is None:
        return 0.0, 0
    
    bars = precomputed['bars']
    n = precomputed['n']
    cumsum_delta = precomputed['cumsum_delta']
    avg_delta_arr = precomputed['avg_delta_arr']
    entry_idx = precomputed.get('entry_idx', 0)  # NEW: start trading from this index
    entry_ts_original = precomputed.get('entry_ts', 0) # Original entry_ts from precompute_bars
    
    # Skip if no trading bars after entry point
    if entry_idx >= n:
        return 0.0, 0, 0, 0, 0, 0
    
    # Track statistics
    trade_count = 0
    ts_exits = 0
    sl_exits = 0
    timeout_exits = 0
    total_pnl = 0.0
    
    # Initialize state
    ts = 0  # ensure variable exists
    if entry_ts_original > 0: # If an explicit entry_ts was provided, we start not in position
        in_position = False
        entry_price = 0.0
        max_price = 0.0
        position_entry_ts = 0
        last_exit_ts = 0  # 0 means "ready to enter immediately" (subject to cooldown)
    else:
        # Legacy/Testing: Assume start ON ENTRY if no explicit entry_ts was given (entry_ts=0)
        # This means we start at the first bar (entry_idx=0) already in a position.
        in_position = True
        entry_price = bars[entry_idx][1]
        max_price = entry_price
        position_entry_ts = bars[entry_idx][0]
        last_exit_ts = 0
        
        # Count the INITIAL position
        trade_count += 1

    comm_cost = COMMISSION_PCT * 2 * leverage
    signal_start_ts = bars[entry_idx][0]  # Timestamp of signal start for reentry limit
    # position_entry_ts is already set above based on initial state
    
    for idx in range(entry_idx, n):  # NEW: loop starts from entry_idx
        bar = bars[idx]
        ts = bar[0]
        price = bar[1]
        
        if in_position:
            if price > max_price:
                max_price = price
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100
            
            # Position timeout check
            if max_position_seconds > 0 and (ts - position_entry_ts) >= max_position_seconds:
                # Check liquidation first
                liquidation_threshold = 100.0 / leverage
                if pnl_from_entry <= -liquidation_threshold:
                    total_pnl += -100.0
                    sl_exits += 1  # Liquidation is a bad loss
                else:
                    realized_pnl = max(pnl_from_entry * leverage, -100.0)
                    total_pnl += (realized_pnl - comm_cost)
                    timeout_exits += 1 # Timed out
                in_position = False
                last_exit_ts = ts
                continue
            
            # LIQUIDATION CHECK: position wiped out at 100/leverage % price drop
            liquidation_threshold = 100.0 / leverage  # e.g. 10% for lev=10
            if pnl_from_entry <= -liquidation_threshold:
                total_pnl += -100.0  # Liquidated = 100% loss (no commission matters)
                in_position = False
                last_exit_ts = ts
                sl_exits += 1
                continue
            
            # Stop-loss (only triggers if not liquidated first)
            if pnl_from_entry <= -sl_pct:
                realized_pnl = max(pnl_from_entry * leverage, -100.0)  # Cap at -100%
                total_pnl += (realized_pnl - comm_cost)
                in_position = False
                last_exit_ts = ts
                sl_exits += 1
                continue
            
            # Trailing / momentum exit (using parameterized constants)
            if pnl_from_entry >= base_activation and drawdown_from_max >= base_callback:
                window_start_idx = max(0, idx - delta_window)
                actual_window_size = idx - window_start_idx
                rolling_delta = cumsum_delta[idx + 1] - cumsum_delta[window_start_idx]
                
                avg_delta = avg_delta_arr[idx]
                threshold = avg_delta * threshold_mult
                
                # Proportional scaling when insufficient data
                # If we only have 50% of requested window, require 50% of threshold
                if actual_window_size < delta_window and delta_window > 0:
                    data_ratio = actual_window_size / delta_window
                    threshold = threshold * data_ratio
                
                if not (rolling_delta > threshold) and not (rolling_delta >= 0):
                    realized_pnl = max(pnl_from_entry * leverage, -100.0)  # Cap at -100%
                    total_pnl += (realized_pnl - comm_cost)
                    in_position = False
                    last_exit_ts = ts
                    max_price = price
                    ts_exits += 1 # Trailing Momentum Exit (Target)
                    continue
        else:
            # Re-entry logic (using parameterized constants)
            # Check max_reentry_seconds limit (0 = no limit)
            if max_reentry_seconds > 0 and (ts - signal_start_ts) > max_reentry_seconds:
                continue  # Past the reentry window, skip
            
            if ts - last_exit_ts >= base_cooldown:
                if price < max_price:
                    drop_pct = (max_price - price) / max_price * 100
                    if drop_pct >= base_reentry_drop:
                        if bar[2] > 0 and bar[4] > bar[5]:
                            in_position = True
                            entry_price = price
                            max_price = price
                            position_entry_ts = ts  # Track new position entry time
                            last_exit_ts = 0
                            trade_count += 1
                else:
                    max_price = price
    
    # Если позиция всё ещё открыта – закрываем в конце
    if in_position:
        final_price = bars[-1][1]
        pnl = (final_price - entry_price) / entry_price * 100
        # Check for liquidation during hold period
        liquidation_threshold = 100.0 / leverage
        if pnl <= -liquidation_threshold:
            total_pnl += -100.0
            sl_exits += 1
        else:
            realized_pnl = max(pnl * leverage, -100.0)  # Cap at -100%
            total_pnl += (realized_pnl - comm_cost)
            timeout_exits += 1 # End of file timeout
        last_exit_ts = bars[-1][0]
    
    return total_pnl, last_exit_ts, trade_count, ts_exits, sl_exits, timeout_exits

# Backwards compatibility wrapper
def run_strategy(
    bars: List[tuple],
    sl_pct: float,
    delta_window: int,
    threshold_mult: float,
    leverage: int,
    base_activation: float = BASE_ACTIVATION,
    base_callback: float = BASE_CALLBACK,
    base_reentry_drop: float = BASE_REENTRY_DROP,
    base_cooldown: int = BASE_COOLDOWN,
) -> Tuple[float, int]:
    """Legacy wrapper - precomputes each time. Use run_strategy_fast for bulk operations."""
    precomputed = precompute_bars(bars)
    return run_strategy_fast(
        precomputed, sl_pct, delta_window, threshold_mult, leverage,
        base_activation, base_callback, base_reentry_drop, base_cooldown
    )

# ---------------------------------------------------------------------------
# Signal loading helpers
# ---------------------------------------------------------------------------

def load_bars_for_signal(signal_id: int) -> List[tuple]:
    """Загрузить 1‑секундные бары для конкретного signal_analysis_id"""
    bars: List[tuple] = []
    for attempt in range(3):
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT second_ts, close_price, delta, large_buy_count, large_sell_count
                        FROM web.agg_trades_1s
                        WHERE signal_analysis_id = %s
                        ORDER BY second_ts
                        """,
                        (signal_id,)
                    )
                    rows = cur.fetchall()
                    for r in rows:
                        bars.append((r[0], float(r[1]), float(r[2]), 0.0, r[3], r[4]))
            break
        except Exception as e:
            if attempt == 2:
                print(f"Error loading signal {signal_id}: {e}")
                return []
            time.sleep(1)
    return bars

# ---------------------------------------------------------------------------
# Signal filtering – now returns full metadata
# ---------------------------------------------------------------------------

def get_filtered_signals() -> List[SignalInfo]:
    """Возвращает список SignalInfo, прошедших фильтры из pump_analysis_lib"""
    try:
        with get_db_connection() as conn:
            raw_signals = fetch_signals(conn)
            if not raw_signals:
                return []
            # Получаем все web.signal_analysis для сопоставления
            with conn.cursor() as cur:
                cur.execute("SELECT id, pair_symbol, signal_timestamp FROM web.signal_analysis")
                web_signals = cur.fetchall()
            web_map: Dict[Tuple[str, datetime], int] = {}
            for wid, sym, ts in web_signals:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                web_map[(sym, ts)] = wid
            matched: List[SignalInfo] = []
            for s in raw_signals:
                sym = s["pair_symbol"]
                ts = s["timestamp"]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if (sym, ts) in web_map:
                    matched.append(SignalInfo(signal_id=web_map[(sym, ts)], pair=sym, timestamp=ts))
            print(f"   Filtering check: FAS Signals {len(raw_signals)} -> Web Matches {len(matched)}")
            return matched
    except Exception as e:
        print(f"Failed to fetch filtered signals: {e}")
        import traceback
        traceback.print_exc()
        return []

# ---------------------------------------------------------------------------
# Per‑pair processing (sequential) – used in parallel workers
# ---------------------------------------------------------------------------

def process_pair(pair: str, signals: List[SignalInfo]) -> Tuple[Dict[int, float], int]:
    """Обрабатывает все сигналы одной пары последовательно.
    Возвращает (aggregated_results, last_exit_ts_of_pair).
    """
    position_tracker_ts = 0  # timestamp последнего выхода по этой паре
    aggregated: Dict[int, float] = {pid: 0.0 for pid in PARAM_COMBINATIONS}
    processed = 0
    skipped = 0
    for info in sorted(signals, key=lambda x: x.timestamp):
        # Конвертируем datetime в Unix timestamp для сравнения
        signal_ts = int(info.timestamp.timestamp())
        # Пропускаем, если позиция ещё открыта
        if signal_ts < position_tracker_ts:
            skipped += 1
            continue
        bars = load_bars_for_signal(info.signal_id)
        if len(bars) < 100:
            continue
        for pid, params in PARAM_COMBINATIONS.items():
            pnl, last_ts = run_strategy(
                bars,
                params["sl_pct"],
                params["delta_window"],
                params["threshold_mult"],
                params["leverage"],
            )
            aggregated[pid] += pnl
            if last_ts > position_tracker_ts:
                position_tracker_ts = last_ts
        processed += 1
    return aggregated, position_tracker_ts

# ---------------------------------------------------------------------------
# Main optimization loop
# ---------------------------------------------------------------------------

def run_optimization(workers: int = 4):
    print("🚀 Оптимизация COMBINED_A_B (Partial Parallel, Трекинг позиций)")
    signals = get_filtered_signals()
    if not signals:
        print("❌ Нет сигналов после фильтрации.")
        return
    # Group by pair
    signals_by_pair: Dict[str, List[SignalInfo]] = {}
    for s in signals:
        signals_by_pair.setdefault(s.pair, []).append(s)
    total_pairs = len(signals_by_pair)
    aggregated_results: Dict[int, float] = {pid: 0.0 for pid in PARAM_COMBINATIONS}
    # Progress bar
    with tqdm(total=total_pairs, desc="Pairs processed", unit="pair") as pbar:
        with Pool(processes=workers) as pool:
            async_results = []
            for pair, pair_signals in signals_by_pair.items():
                async_results.append(pool.apply_async(process_pair, args=(pair, pair_signals)))
            for res in async_results:
                pair_agg, _ = res.get()
                for pid, val in pair_agg.items():
                    aggregated_results[pid] += val
                pbar.update(1)
    # Output top‑10
    final_list = []
    for pid, total_pnl in aggregated_results.items():
        params = PARAM_COMBINATIONS[pid]
        final_list.append({"params": params, "total_pnl": total_pnl})
    final_list.sort(key=lambda x: x["total_pnl"], reverse=True)
    print("\n" + "=" * 90)
    print("🏆 АБСОЛЮТНЫЙ ТОП-10 (по Total PnL)")
    print("=" * 90)
    print(f"{'#':<3} {'Lev':<5} {'SL%':<6} {'Window':<8} {'Threshold':<10} {'Total PnL %':<14}")
    print("-" * 90)
    for i, res in enumerate(final_list[:10], 1):
        p = res["params"]
        print(f"{i:<3} {p['leverage']:<5}x {p['sl_pct']:<6} {p['delta_window']:<8} {p['threshold_mult']:<10} {res['total_pnl']:+12.2f}%")
    # Save report
    report_dir = Path(__file__).parent.parent / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    output_file = report_dir / "optimization_combined_leverage_filtered.json"
    with open(output_file, "w") as f:
        json.dump(final_list, f, indent=2)
    print(f"\nSaved to {output_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (by pair)")
    args = parser.parse_args()
    run_optimization(workers=args.workers)
