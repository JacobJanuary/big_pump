# """
# Оптимизация COMBINED_A_B с учётом кредитного плеча (ФИЛЬТРОВАННАЯ ВЕРСИЯ).
# Эта копия использует фильтры, определённые в pump_analysis_lib.py (SCORE_THRESHOLD, INDICATOR_FILTERS, EXCHANGE_FILTER).
# """
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict
from multiprocessing import Pool
import itertools
import json
import statistics
import time

current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, fetch_signals

# ============== КОНФИГУРАЦИЯ ==============

COMMISSION_PCT = 0.04

# Фиксированные параметры
BASE_ACTIVATION = 10.0
BASE_CALLBACK = 4.0
BASE_REENTRY_DROP = 5.0
BASE_COOLDOWN = 300

# Параметры для оптимизации
PARAM_GRID = {
    'delta_window': [10, 20, 30, 60, 120],
    'threshold_mult': [1.0, 1.5, 2.0, 2.5, 3.0],
    'leverage': [1, 5, 10],
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
for leverage in PARAM_GRID['leverage']:
    for sl in SL_BY_LEVERAGE[leverage]:
        for delta_window in PARAM_GRID['delta_window']:
            for threshold in PARAM_GRID['threshold_mult']:
                PARAM_COMBINATIONS[combo_id] = {
                    'leverage': leverage,
                    'sl_pct': sl,
                    'delta_window': delta_window,
                    'threshold_mult': threshold,
                }
                combo_id += 1

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

def run_strategy(
    bars: List[tuple],
    sl_pct: float,
    delta_window: int,
    threshold_mult: float,
    leverage: int,
) -> float:
    """Запустить стратегию на одном наборе свечей. Возвращает PnL."""
    if not bars:
        return 0.0
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
            if pnl_from_entry <= -sl_pct:
                total_pnl += (pnl_from_entry * leverage - comm_cost)
                in_position = False
                last_exit_ts = ts
                continue
            if (pnl_from_entry >= BASE_ACTIVATION and drawdown_from_max >= BASE_CALLBACK):
                rolling_delta = get_rolling_delta(bars, idx, delta_window)
                avg_delta = get_avg_delta(bars, idx)
                threshold = avg_delta * threshold_mult
                if not (rolling_delta > threshold) and not (rolling_delta >= 0):
                    total_pnl += (pnl_from_entry * leverage - comm_cost)
                    in_position = False
                    last_exit_ts = ts
                    max_price = price
        else:
            if ts - last_exit_ts >= BASE_COOLDOWN:
                if price < max_price:
                    drop_pct = (max_price - price) / max_price * 100
                    if drop_pct >= BASE_REENTRY_DROP:
                        if bar[2] > 0 and bar[4] > bar[5]:
                            in_position = True
                            entry_price = price
                            max_price = price
                else:
                    max_price = price
    if in_position:
        final_price = bars[-1][1]
        pnl = (final_price - entry_price) / entry_price * 100
        total_pnl += (pnl * leverage - comm_cost)
    return total_pnl

def process_signal_all_params(signal_id: int):
    """Worker: load one signal, evaluate all param combos, return dict pid->pnl."""
    bars = []
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
                return None
            time.sleep(1)
    if len(bars) < 100:
        return {}
    results = {}
    for pid, params in PARAM_COMBINATIONS.items():
        pnl = run_strategy(
            bars,
            params['sl_pct'],
            params['delta_window'],
            params['threshold_mult'],
            params['leverage'],
        )
        results[pid] = pnl
    return results

def get_filtered_signal_ids() -> List[int]:
    """Return list of signal_analysis_id that satisfy filters from pump_analysis_lib."""
    try:
        with get_db_connection() as conn:
            signals = fetch_signals(conn)
            return [s['id'] for s in signals]
    except Exception as e:
        print(f"Failed to fetch filtered signals: {e}")
        return []

def run_optimization(workers: int = 6):
    print("🚀 Оптимизация COMBINED_A_B (Memory Safe, Фильтры)")
    print(f"   Воркеров: {workers}")
    print(f"   Комбинаций параметров: {len(PARAM_COMBINATIONS)}")
    signal_ids = get_filtered_signal_ids()
    if not signal_ids:
        print("❌ Нет сигналов после фильтрации.")
        return
    print(f"   Сигналов для обработки: {len(signal_ids)}")
    print("-" * 60)
    aggregated_results = {pid: 0.0 for pid in PARAM_COMBINATIONS}
    processed = 0
    start_time = datetime.now()
    with Pool(processes=workers) as pool:
        for signal_results in pool.imap_unordered(process_signal_all_params, signal_ids):
            processed += 1
            if signal_results:
                for pid, pnl in signal_results.items():
                    aggregated_results[pid] += pnl
            if processed % 10 == 0:
                print(f"   Processed {processed}/{len(signal_ids)} signals...", end='\r')
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n✅ Done in {elapsed:.1f}s")
    final_list = []
    for pid, total_pnl in aggregated_results.items():
        params = PARAM_COMBINATIONS[pid]
        final_list.append({'params': params, 'total_pnl': total_pnl})
    final_list.sort(key=lambda x: x['total_pnl'], reverse=True)
    print("\n" + "="*90)
    print("🏆 АБСОЛЮТНЫЙ ТОП-10 (по Total PnL)")
    print("="*90)
    print(f"{'#':<3} {'Lev':<5} {'SL%':<6} {'Window':<8} {'Threshold':<10} {'Total PnL %':<14}")
    print("-"*90)
    for i, res in enumerate(final_list[:10], 1):
        p = res['params']
        print(f"{i:<3} {p['leverage']:<5}x {p['sl_pct']:<6} {p['delta_window']:<8} {p['threshold_mult']:<10} {res['total_pnl']:+12.2f}%")
    report_dir = Path(__file__).parent.parent / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    output_file = report_dir / "optimization_combined_leverage_filtered.json"
    with open(output_file, 'w') as f:
        json.dump(final_list, f, indent=2)
    print(f"\nSaved to {output_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=6)
    args = parser.parse_args()
    run_optimization(workers=args.workers)
