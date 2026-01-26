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

def run_strategy(
    bars: List[tuple],
    sl_pct: float,
    delta_window: int,
    threshold_mult: float,
    leverage: int,
) -> Tuple[float, int]:
    """Запустить стратегию на наборе баров.
    Возвращает (total_pnl, last_bar_timestamp).
    """
    if not bars:
        return 0.0, 0
    entry_price = bars[0][1]
    max_price = entry_price
    last_exit_ts = 0
    in_position = True
    total_pnl = 0.0
    comm_cost = COMMISSION_PCT * 2 * leverage
    for idx, bar in enumerate(bars):
        ts = bar[0]
        price = bar[1]
        if in_position:
            if price > max_price:
                max_price = price
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100
            # Stop‑loss
            if pnl_from_entry <= -sl_pct:
                total_pnl += (pnl_from_entry * leverage - comm_cost)
                in_position = False
                last_exit_ts = ts
                continue
            # Trailing / momentum exit
            if (pnl_from_entry >= BASE_ACTIVATION and drawdown_from_max >= BASE_CALLBACK):
                rolling_delta = get_rolling_delta(bars, idx, delta_window)
                avg_delta = get_avg_delta(bars, idx)
                threshold = avg_delta * threshold_mult
                if not (rolling_delta > threshold) and not (rolling_delta >= 0):
                    total_pnl += (pnl_from_entry * leverage - comm_cost)
                    in_position = False
                    last_exit_ts = ts
                    max_price = price
                    continue
        else:
            # Re‑entry logic
            if ts - last_exit_ts >= BASE_COOLDOWN:
                if price < max_price:
                    drop_pct = (max_price - price) / max_price * 100
                    if drop_pct >= BASE_REENTRY_DROP:
                        if bar[2] > 0 and bar[4] > bar[5]:
                            in_position = True
                            entry_price = price
                            max_price = price
                            last_exit_ts = 0
                else:
                    max_price = price
    # Если позиция всё ещё открыта – закрываем в конце
    if in_position:
        final_price = bars[-1][1]
        pnl = (final_price - entry_price) / entry_price * 100
        total_pnl += (pnl * leverage - comm_cost)
        last_exit_ts = bars[-1][0]
    return total_pnl, last_exit_ts

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
        # Пропускаем, если позиция ещё открыта
        if info.timestamp < position_tracker_ts:
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
