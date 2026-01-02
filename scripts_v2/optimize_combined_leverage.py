"""
Оптимизация COMBINED_A_B с учётом кредитного плеча.

Параметры для оптимизации:
- delta_window
- threshold_multiplier
- SL (с учётом leverage)
- leverage (1x, 5x, 10x)
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Tuple
from multiprocessing import Pool
import itertools
import json
import statistics

current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# ============== КОНФИГУРАЦИЯ ==============

COMMISSION_PCT = 0.04

# Фиксированные параметры (лучшие из предыдущей оптимизации)
BASE_ACTIVATION = 10.0
BASE_CALLBACK = 4.0
BASE_REENTRY_DROP = 5.0
BASE_COOLDOWN = 300

# Параметры для оптимизации
PARAM_GRID = {
    'delta_window': [10, 20, 30, 60, 120],
    'threshold_mult': [1.0, 1.5, 2.0, 2.5, 3.0],
    'leverage': [1, 5, 10],
    # SL зависит от leverage:
    # 1x: можем использовать любой SL
    # 5x: max SL = 18% (20% = ликвидация с буфером)
    # 10x: max SL = 8% (10% = ликвидация с буфером)
}

# SL варианты для каждого leverage
SL_BY_LEVERAGE = {
    1: [5, 7, 10, 15, 20],
    5: [3, 4, 5, 7, 10, 15],  # умножать на leverage при расчёте
    10: [2, 3, 4, 5, 7, 8],    # max 8% для 10x
}

# ============== ГЛОБАЛЬНЫЕ ДАННЫЕ ==============
ALL_SIGNALS_DATA = {}

def load_all_data():
    """Загрузить все данные."""
    global ALL_SIGNALS_DATA
    
    print("📥 Загрузка данных...")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT signal_analysis_id, pair_symbol
                FROM web.agg_trades_1s
                ORDER BY signal_analysis_id
            """)
            signals = cur.fetchall()
        
        for i, (signal_id, pair_symbol) in enumerate(signals):
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT second_ts, close_price, delta, buy_volume,
                           large_buy_count, large_sell_count
                    FROM web.agg_trades_1s
                    WHERE signal_analysis_id = %s
                    ORDER BY second_ts
                """, (signal_id,))
                rows = cur.fetchall()
            
            ALL_SIGNALS_DATA[signal_id] = {
                'pair_symbol': pair_symbol,
                'bars': [
                    {
                        'ts': r[0],
                        'price': float(r[1]),
                        'delta': float(r[2]),
                        'buy_vol': float(r[3]),
                        'large_buy': r[4],
                        'large_sell': r[5]
                    }
                    for r in rows
                ]
            }
            
            if (i + 1) % 30 == 0:
                print(f"   {i + 1}/{len(signals)}", flush=True)
    
    print(f"✅ Загружено {len(ALL_SIGNALS_DATA)} сигналов")

def get_rolling_delta(bars: List[dict], idx: int, window: int) -> float:
    """Вычислить rolling delta."""
    if idx < 1 or window <= 0:
        return 0
    
    current_ts = bars[idx]['ts']
    window_start = current_ts - window
    
    delta_sum = 0
    for j in range(idx, -1, -1):
        if bars[j]['ts'] < window_start:
            break
        delta_sum += bars[j]['delta']
    
    return delta_sum

def get_avg_delta(bars: List[dict], idx: int, lookback: int = 100) -> float:
    """Вычислить среднюю абсолютную delta."""
    if idx < lookback:
        lookback = idx
    if lookback < 1:
        return 0
    
    deltas = [abs(bars[i]['delta']) for i in range(idx - lookback, idx)]
    return statistics.mean(deltas) if deltas else 0

def run_strategy(
    bars: List[dict],
    sl_pct: float,
    delta_window: int,
    threshold_mult: float,
    leverage: int
) -> Tuple[float, int, int]:
    """
    Запустить COMBINED_A_B стратегию.
    
    Returns:
        (total_pnl_with_leverage, wins, losses)
    """
    if not bars or len(bars) < 100:
        return 0.0, 0, 0
    
    trades = []
    in_position = True
    entry_price = bars[0]['price']
    max_price = entry_price
    last_exit_ts = 0
    
    for idx, bar in enumerate(bars):
        price = bar['price']
        ts = bar['ts']
        
        if in_position:
            max_price = max(max_price, price)
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100
            
            # SL (без leverage - это % движения цены)
            if pnl_from_entry <= -sl_pct:
                # PnL с учётом leverage
                leveraged_pnl = pnl_from_entry * leverage - (COMMISSION_PCT * 2 * leverage)
                trades.append({'pnl': leveraged_pnl, 'reason': 'SL'})
                in_position = False
                last_exit_ts = ts
                continue
            
            # Trailing
            trailing_triggered = (pnl_from_entry >= BASE_ACTIVATION and 
                                  drawdown_from_max >= BASE_CALLBACK)
            
            if trailing_triggered:
                should_exit = True
                
                rolling_delta = get_rolling_delta(bars, idx, delta_window)
                avg_delta = get_avg_delta(bars, idx)
                
                # Фильтр A: не выходим при сильном momentum
                threshold = avg_delta * threshold_mult
                if rolling_delta > threshold:
                    should_exit = False
                
                # Фильтр B: требуем negative delta
                if should_exit and rolling_delta >= 0:
                    should_exit = False
                
                if should_exit:
                    leveraged_pnl = pnl_from_entry * leverage - (COMMISSION_PCT * 2 * leverage)
                    trades.append({'pnl': leveraged_pnl, 'reason': 'TRAIL'})
                    in_position = False
                    last_exit_ts = ts
                    max_price = price
        
        else:
            # Перезаход
            if ts - last_exit_ts < BASE_COOLDOWN:
                continue
            
            if price < max_price:
                drop_pct = (max_price - price) / max_price * 100
                
                if drop_pct >= BASE_REENTRY_DROP:
                    if bar['delta'] > 0 and bar['large_buy'] > bar['large_sell']:
                        in_position = True
                        entry_price = price
                        max_price = price
            else:
                max_price = price
    
    # Закрытие
    if in_position and bars:
        final_price = bars[-1]['price']
        pnl = (final_price - entry_price) / entry_price * 100
        leveraged_pnl = pnl * leverage - (COMMISSION_PCT * 2 * leverage)
        trades.append({'pnl': leveraged_pnl, 'reason': 'TIMEOUT'})
    
    total_pnl = sum(t['pnl'] for t in trades)
    wins = sum(1 for t in trades if t['pnl'] > 0)
    losses = sum(1 for t in trades if t['pnl'] <= 0)
    
    return total_pnl, wins, losses

def evaluate_params(params: dict) -> dict:
    """Оценить параметры."""
    total_pnl = 0
    total_wins = 0
    total_losses = 0
    
    for signal_id, data in ALL_SIGNALS_DATA.items():
        pnl, wins, losses = run_strategy(
            bars=data['bars'],
            sl_pct=params['sl_pct'],
            delta_window=params['delta_window'],
            threshold_mult=params['threshold_mult'],
            leverage=params['leverage']
        )
        total_pnl += pnl
        total_wins += wins
        total_losses += losses
    
    total_trades = total_wins + total_losses
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    
    return {
        'params': params,
        'total_pnl': total_pnl,
        'win_rate': win_rate,
        'total_trades': total_trades
    }

def worker_evaluate(params_tuple):
    """Worker для multiprocessing."""
    params = {
        'leverage': params_tuple[0],
        'sl_pct': params_tuple[1],
        'delta_window': params_tuple[2],
        'threshold_mult': params_tuple[3]
    }
    return evaluate_params(params)

def run_optimization(workers: int = 12):
    """Запустить оптимизацию."""
    print("🚀 Оптимизация COMBINED_A_B с Leverage")
    print(f"   Воркеров: {workers}")
    print("-" * 90)
    
    load_all_data()
    
    # Генерируем комбинации
    all_combinations = []
    for leverage in PARAM_GRID['leverage']:
        for sl in SL_BY_LEVERAGE[leverage]:
            for delta_window in PARAM_GRID['delta_window']:
                for threshold in PARAM_GRID['threshold_mult']:
                    all_combinations.append((leverage, sl, delta_window, threshold))
    
    print(f"   Комбинаций: {len(all_combinations)}")
    print("-" * 90)
    
    start_time = datetime.now()
    
    results = []
    with Pool(processes=workers) as pool:
        for i, result in enumerate(pool.imap_unordered(worker_evaluate, all_combinations)):
            results.append(result)
            
            if (i + 1) % 100 == 0 or i == len(all_combinations) - 1:
                elapsed = (datetime.now() - start_time).total_seconds()
                eta = elapsed / (i + 1) * (len(all_combinations) - i - 1)
                print(f"   Прогресс: {i + 1}/{len(all_combinations)} | ETA: {int(eta)}s", flush=True)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Группируем по leverage
    for lev in [1, 5, 10]:
        lev_results = [r for r in results if r['params']['leverage'] == lev]
        lev_results.sort(key=lambda x: x['total_pnl'], reverse=True)
        
        print(f"\n{'='*90}")
        print(f"📊 LEVERAGE {lev}x - ТОП 5")
        print("="*90)
        print(f"{'#':<3} {'SL%':<6} {'Window':<8} {'Threshold':<10} {'PnL %':<14} {'WinRate':<10} {'Trades'}")
        print("-"*90)
        
        for i, res in enumerate(lev_results[:5], 1):
            p = res['params']
            print(f"{i:<3} {p['sl_pct']:<6} {p['delta_window']:<8} {p['threshold_mult']:<10} "
                  f"{res['total_pnl']:>+12.2f}% {res['win_rate']:>8.1f}% {res['total_trades']:>6}")
    
    # Общий топ
    results.sort(key=lambda x: x['total_pnl'], reverse=True)
    
    print(f"\n{'='*90}")
    print("🏆 АБСОЛЮТНЫЙ ТОП-10 (все leverage)")
    print("="*90)
    print(f"{'#':<3} {'Lev':<5} {'SL%':<6} {'Window':<8} {'Threshold':<10} {'PnL %':<14} {'WinRate':<10} {'Trades'}")
    print("-"*90)
    
    for i, res in enumerate(results[:10], 1):
        p = res['params']
        print(f"{i:<3} {p['leverage']:<5}x {p['sl_pct']:<6} {p['delta_window']:<8} {p['threshold_mult']:<10} "
              f"{res['total_pnl']:>+12.2f}% {res['win_rate']:>8.1f}% {res['total_trades']:>6}")
    
    # Сохраняем
    output_file = Path(__file__).parent.parent / "reports" / "optimization_combined_leverage.json"
    
    with open(output_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'elapsed_seconds': elapsed,
            'all_results': results
        }, f, indent=2)
    
    print(f"\n📁 Результаты: {output_file}")
    print(f"⏱️ Время: {elapsed:.1f} сек")
    
    # Лучший для каждого leverage
    print("\n" + "="*90)
    print("📌 ЛУЧШИЕ ПАРАМЕТРЫ ДЛЯ КАЖДОГО LEVERAGE:")
    print("="*90)
    
    for lev in [1, 5, 10]:
        best = max([r for r in results if r['params']['leverage'] == lev], 
                   key=lambda x: x['total_pnl'])
        p = best['params']
        print(f"\n   {lev}x LEVERAGE:")
        print(f"      SL: {p['sl_pct']}%")
        print(f"      delta_window: {p['delta_window']} сек")
        print(f"      threshold_mult: {p['threshold_mult']}")
        print(f"      PnL: {best['total_pnl']:+.2f}%")
        print(f"      Win Rate: {best['win_rate']:.1f}%")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=12)
    args = parser.parse_args()
    
    run_optimization(workers=args.workers)
