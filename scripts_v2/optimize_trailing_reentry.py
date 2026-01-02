"""
Оптимизация параметров стратегии Trailing REENTRY.

Grid search по параметрам с параллельной обработкой.
"""
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Tuple
from multiprocessing import Pool, cpu_count
import itertools
import json

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# ============== КОНФИГУРАЦИЯ ==============

# Фиксированные параметры
COMMISSION_PCT = 0.04    # Комиссия Binance (taker)

# Параметры для оптимизации
PARAM_GRID = {
    'sl_pct': [2, 3, 4, 5, 7, 10, 15],              # Stop Loss %
    'trail_activation': [3, 5, 7, 10, 15, 20],      # Активация трейла при +X%
    'trail_callback': [1, 2, 3, 4, 5, 7, 10],       # Откат для выхода
    'reentry_drop': [2, 3, 5, 7, 10],               # Ждать падения X% для перезахода
    'reentry_cooldown': [30, 60, 120, 300],         # Кулдаун между сделками (сек)
}

# ============== ГЛОБАЛЬНЫЕ ДАННЫЕ ==============
# Загружаем один раз, используем во всех процессах
ALL_SIGNALS_DATA = {}

def load_all_data():
    """Загрузить все данные в память."""
    global ALL_SIGNALS_DATA
    
    print("📥 Загрузка данных в память...")
    
    with get_db_connection() as conn:
        # Получаем список сигналов
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT signal_analysis_id, pair_symbol
                FROM web.agg_trades_1s
                ORDER BY signal_analysis_id
            """)
            signals = cur.fetchall()
        
        print(f"   Найдено сигналов: {len(signals)}")
        
        # Загружаем данные для каждого сигнала
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
            
            if (i + 1) % 20 == 0:
                print(f"   Загружено: {i + 1}/{len(signals)}", flush=True)
    
    print(f"✅ Загружено {len(ALL_SIGNALS_DATA)} сигналов")
    return len(ALL_SIGNALS_DATA)

# ============== СТРАТЕГИЯ ==============

@dataclass
class TradeResult:
    entry_price: float
    exit_price: float
    pnl_pct: float
    exit_reason: str

def run_trailing_reentry_strategy(
    bars: List[dict],
    sl_pct: float,
    trail_activation: float,
    trail_callback: float,
    reentry_drop: float,
    reentry_cooldown: int
) -> Tuple[float, int, int]:
    """
    Запустить стратегию Trailing REENTRY на одном сигнале.
    
    Returns:
        (total_pnl_pct, wins, losses)
    """
    if not bars or len(bars) < 100:
        return 0.0, 0, 0
    
    trades = []
    in_position = True
    entry_price = bars[0]['price']
    entry_ts = bars[0]['ts']
    max_price = entry_price  # Максимум с момента входа
    last_exit_ts = 0
    
    for bar in bars:
        price = bar['price']
        ts = bar['ts']
        
        if in_position:
            # Обновляем максимум
            max_price = max(max_price, price)
            
            # Считаем PnL от входа
            pnl_from_entry = (price - entry_price) / entry_price * 100
            
            # Считаем откат от максимума
            drawdown_from_max = (max_price - price) / max_price * 100
            
            # Hard Stop Loss
            if pnl_from_entry <= -sl_pct:
                trades.append(TradeResult(
                    entry_price=entry_price,
                    exit_price=price,
                    pnl_pct=pnl_from_entry - (COMMISSION_PCT * 2),
                    exit_reason="SL"
                ))
                in_position = False
                last_exit_ts = ts
                continue
            
            # Trailing Stop: если достигли activation и откатили на callback
            if pnl_from_entry >= trail_activation and drawdown_from_max >= trail_callback:
                final_pnl = pnl_from_entry - (COMMISSION_PCT * 2)
                trades.append(TradeResult(
                    entry_price=entry_price,
                    exit_price=price,
                    pnl_pct=final_pnl,
                    exit_reason="TRAIL"
                ))
                in_position = False
                last_exit_ts = ts
                max_price = price  # Сбрасываем для отслеживания падения
        
        else:
            # Вне позиции — ищем перезаход
            
            # Проверяем cooldown
            if ts - last_exit_ts < reentry_cooldown:
                continue
            
            # Отслеживаем падение от последнего максимума
            if price < max_price:
                drop_pct = (max_price - price) / max_price * 100
                
                # Если упали достаточно и видим покупки
                if drop_pct >= reentry_drop:
                    # Проверяем сигнал на вход
                    if bar['delta'] > 0 and bar['large_buy'] > bar['large_sell']:
                        in_position = True
                        entry_price = price
                        entry_ts = ts
                        max_price = price
            else:
                # Обновляем максимум даже вне позиции (для отслеживания дропа)
                max_price = price
    
    # Закрываем открытую позицию в конце
    if in_position and bars:
        final_price = bars[-1]['price']
        pnl = (final_price - entry_price) / entry_price * 100 - (COMMISSION_PCT * 2)
        trades.append(TradeResult(
            entry_price=entry_price,
            exit_price=final_price,
            pnl_pct=pnl,
            exit_reason="TIMEOUT"
        ))
    
    # Считаем итоги
    total_pnl = sum(t.pnl_pct for t in trades)
    wins = sum(1 for t in trades if t.pnl_pct > 0)
    losses = sum(1 for t in trades if t.pnl_pct <= 0)
    
    return total_pnl, wins, losses

def evaluate_params(params: dict) -> dict:
    """Оценить набор параметров на всех сигналах."""
    total_pnl = 0
    total_wins = 0
    total_losses = 0
    
    for signal_id, data in ALL_SIGNALS_DATA.items():
        pnl, wins, losses = run_trailing_reentry_strategy(
            bars=data['bars'],
            sl_pct=params['sl_pct'],
            trail_activation=params['trail_activation'],
            trail_callback=params['trail_callback'],
            reentry_drop=params['reentry_drop'],
            reentry_cooldown=params['reentry_cooldown']
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
        'total_trades': total_trades,
        'wins': total_wins,
        'losses': total_losses
    }

def worker_evaluate(params_tuple):
    """Worker для multiprocessing."""
    params = {
        'sl_pct': params_tuple[0],
        'trail_activation': params_tuple[1],
        'trail_callback': params_tuple[2],
        'reentry_drop': params_tuple[3],
        'reentry_cooldown': params_tuple[4]
    }
    return evaluate_params(params)

def run_optimization(workers: int = 12):
    """Запустить оптимизацию."""
    print("🚀 Оптимизация Trailing REENTRY")
    print(f"   Воркеров: {workers}")
    print("-" * 70)
    
    # Загружаем данные
    num_signals = load_all_data()
    
    # Генерируем все комбинации параметров
    param_combinations = list(itertools.product(
        PARAM_GRID['sl_pct'],
        PARAM_GRID['trail_activation'],
        PARAM_GRID['trail_callback'],
        PARAM_GRID['reentry_drop'],
        PARAM_GRID['reentry_cooldown']
    ))
    
    # Фильтруем невалидные комбинации (trail_callback > trail_activation)
    param_combinations = [
        p for p in param_combinations
        if p[2] < p[1]  # callback < activation
    ]
    
    print(f"   Комбинаций для теста: {len(param_combinations)}")
    print("-" * 70)
    
    start_time = datetime.now()
    
    # Параллельная обработка
    results = []
    with Pool(processes=workers) as pool:
        for i, result in enumerate(pool.imap_unordered(worker_evaluate, param_combinations)):
            results.append(result)
            
            if (i + 1) % 50 == 0 or i == len(param_combinations) - 1:
                elapsed = (datetime.now() - start_time).total_seconds()
                eta = elapsed / (i + 1) * (len(param_combinations) - i - 1)
                print(f"   Прогресс: {i + 1}/{len(param_combinations)} | ETA: {int(eta)}s", flush=True)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Сортируем по PnL
    results.sort(key=lambda x: x['total_pnl'], reverse=True)
    
    print("\n" + "=" * 80)
    print("📊 ТОП-10 КОМБИНАЦИЙ ПАРАМЕТРОВ")
    print("=" * 80)
    print(f"{'#':<3} {'SL%':<6} {'Activ':<8} {'Callback':<10} {'Drop':<8} {'Cooldown':<10} {'PnL %':<12} {'WinRate':<10} {'Trades'}")
    print("-" * 80)
    
    for i, res in enumerate(results[:10], 1):
        p = res['params']
        print(f"{i:<3} {p['sl_pct']:<6} {p['trail_activation']:<8} {p['trail_callback']:<10} {p['reentry_drop']:<8} {p['reentry_cooldown']:<10} {res['total_pnl']:>+10.2f}% {res['win_rate']:>8.1f}% {res['total_trades']:>6}")
    
    # Выводим худшие 3
    print("\n" + "-" * 80)
    print("📉 ХУДШИЕ 3:")
    for i, res in enumerate(results[-3:], 1):
        p = res['params']
        print(f"{i:<3} {p['sl_pct']:<6} {p['trail_activation']:<8} {p['trail_callback']:<10} {p['reentry_drop']:<8} {p['reentry_cooldown']:<10} {res['total_pnl']:>+10.2f}% {res['win_rate']:>8.1f}% {res['total_trades']:>6}")
    
    # Сохраняем результаты
    output_file = Path(__file__).parent.parent / "reports" / "optimization_trailing_reentry.json"
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'num_signals': num_signals,
            'num_combinations': len(param_combinations),
            'elapsed_seconds': elapsed,
            'best_params': results[0]['params'],
            'best_pnl': results[0]['total_pnl'],
            'all_results': results
        }, f, indent=2)
    
    print(f"\n📁 Результаты сохранены: {output_file}")
    print(f"⏱️ Время: {elapsed:.1f} сек")
    
    # Возвращаем лучшие параметры
    return results[0]

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Оптимизация Trailing REENTRY')
    parser.add_argument('--workers', type=int, default=12, help='Кол-во процессов')
    
    args = parser.parse_args()
    
    best = run_optimization(workers=args.workers)
    
    print("\n" + "=" * 80)
    print("🏆 ЛУЧШИЕ ПАРАМЕТРЫ:")
    print(f"   sl_pct: {best['params']['sl_pct']}%")
    print(f"   trail_activation: {best['params']['trail_activation']}%")
    print(f"   trail_callback: {best['params']['trail_callback']}%")
    print(f"   reentry_drop: {best['params']['reentry_drop']}%")
    print(f"   reentry_cooldown: {best['params']['reentry_cooldown']} сек")
    print(f"   PnL: {best['total_pnl']:+.2f}%")
    print(f"   Win Rate: {best['win_rate']:.1f}%")
