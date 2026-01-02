"""
Тестирование Delta-фильтров для стратегии.

Варианты:
A) Delta как фильтр НЕВЫХОДА - не выходим если momentum сильный
B) Комбинированный exit - выходим только если callback И delta отрицательная
"""
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Tuple
from multiprocessing import Pool
import itertools
import json
import statistics

current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# ============== КОНФИГУРАЦИЯ ==============

COMMISSION_PCT = 0.04

# Базовые параметры (лучшие из предыдущей оптимизации)
BASE_SL = 15.0
BASE_ACTIVATION = 10.0
BASE_CALLBACK = 4.0
BASE_REENTRY_DROP = 5.0
BASE_COOLDOWN = 300

# Параметры Delta-фильтров для тестирования
DELTA_STRATEGIES = {
    'BASELINE': {
        'use_delta_filter': False,
        'delta_window': 0,
        'delta_threshold': 0,
        'require_negative_delta': False
    },
    'FILTER_A_WEAK': {
        # Не выходим если rolling delta > среднего
        'use_delta_filter': True,
        'delta_window': 30,  # 30 сек
        'delta_threshold_multiplier': 1.0,  # > mean
        'require_negative_delta': False
    },
    'FILTER_A_STRONG': {
        # Не выходим если rolling delta > 2x среднего
        'use_delta_filter': True,
        'delta_window': 30,
        'delta_threshold_multiplier': 2.0,  # > 2x mean
        'require_negative_delta': False
    },
    'FILTER_A_VERY_STRONG': {
        # Не выходим если rolling delta > 3x среднего
        'use_delta_filter': True,
        'delta_window': 60,
        'delta_threshold_multiplier': 3.0,
        'require_negative_delta': False
    },
    'FILTER_B_SIMPLE': {
        # Выход только если delta < 0
        'use_delta_filter': True,
        'delta_window': 10,
        'delta_threshold_multiplier': 0,
        'require_negative_delta': True
    },
    'FILTER_B_PLUS_LARGE': {
        # Выход только если delta < 0 И large_sell > large_buy
        'use_delta_filter': True,
        'delta_window': 10,
        'delta_threshold_multiplier': 0,
        'require_negative_delta': True,
        'require_large_sell': True
    },
    'COMBINED_A_B': {
        # Не выходим если delta сильная ИЛИ требуем negative для выхода
        'use_delta_filter': True,
        'delta_window': 30,
        'delta_threshold_multiplier': 1.5,
        'require_negative_delta': True
    }
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
    return len(ALL_SIGNALS_DATA)

def get_rolling_delta(bars: List[dict], idx: int, window: int) -> float:
    """Вычислить rolling delta за последние N секунд."""
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
    """Вычислить среднюю delta за lookback баров."""
    if idx < lookback:
        lookback = idx
    if lookback < 1:
        return 0
    
    deltas = [abs(bars[i]['delta']) for i in range(idx - lookback, idx)]
    return statistics.mean(deltas) if deltas else 0

def run_strategy_with_delta_filter(
    bars: List[dict],
    strategy_config: dict
) -> Tuple[float, int, int, int]:
    """
    Запустить стратегию с delta-фильтром.
    
    Returns:
        (total_pnl, wins, losses, filtered_exits)
    """
    if not bars or len(bars) < 100:
        return 0.0, 0, 0, 0
    
    use_filter = strategy_config.get('use_delta_filter', False)
    delta_window = strategy_config.get('delta_window', 30)
    threshold_mult = strategy_config.get('delta_threshold_multiplier', 1.0)
    require_neg = strategy_config.get('require_negative_delta', False)
    require_large_sell = strategy_config.get('require_large_sell', False)
    
    trades = []
    in_position = True
    entry_price = bars[0]['price']
    entry_ts = bars[0]['ts']
    max_price = entry_price
    last_exit_ts = 0
    filtered_exits = 0  # Сколько раз фильтр предотвратил выход
    
    for idx, bar in enumerate(bars):
        price = bar['price']
        ts = bar['ts']
        
        if in_position:
            max_price = max(max_price, price)
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100
            
            # Hard SL
            if pnl_from_entry <= -BASE_SL:
                trades.append({
                    'pnl': pnl_from_entry - (COMMISSION_PCT * 2),
                    'reason': 'SL'
                })
                in_position = False
                last_exit_ts = ts
                continue
            
            # Trailing условие выполнено?
            trailing_triggered = (pnl_from_entry >= BASE_ACTIVATION and 
                                  drawdown_from_max >= BASE_CALLBACK)
            
            if trailing_triggered:
                should_exit = True
                
                if use_filter:
                    rolling_delta = get_rolling_delta(bars, idx, delta_window)
                    avg_delta = get_avg_delta(bars, idx)
                    
                    # Фильтр A: не выходим при сильном momentum
                    if threshold_mult > 0:
                        threshold = avg_delta * threshold_mult
                        if rolling_delta > threshold:
                            should_exit = False
                            filtered_exits += 1
                    
                    # Фильтр B: требуем отрицательную delta
                    if require_neg and should_exit:
                        if rolling_delta >= 0:
                            should_exit = False
                            filtered_exits += 1
                    
                    # Фильтр B+: требуем large_sell > large_buy
                    if require_large_sell and should_exit:
                        if bar['large_sell'] <= bar['large_buy']:
                            should_exit = False
                            filtered_exits += 1
                
                if should_exit:
                    trades.append({
                        'pnl': pnl_from_entry - (COMMISSION_PCT * 2),
                        'reason': 'TRAIL'
                    })
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
                        entry_ts = ts
                        max_price = price
            else:
                max_price = price
    
    # Закрываем позицию
    if in_position and bars:
        final_price = bars[-1]['price']
        pnl = (final_price - entry_price) / entry_price * 100 - (COMMISSION_PCT * 2)
        trades.append({'pnl': pnl, 'reason': 'TIMEOUT'})
    
    total_pnl = sum(t['pnl'] for t in trades)
    wins = sum(1 for t in trades if t['pnl'] > 0)
    losses = sum(1 for t in trades if t['pnl'] <= 0)
    
    return total_pnl, wins, losses, filtered_exits

def evaluate_strategy(strategy_name: str) -> dict:
    """Оценить стратегию на всех сигналах."""
    config = DELTA_STRATEGIES[strategy_name]
    
    total_pnl = 0
    total_wins = 0
    total_losses = 0
    total_filtered = 0
    
    for signal_id, data in ALL_SIGNALS_DATA.items():
        pnl, wins, losses, filtered = run_strategy_with_delta_filter(
            bars=data['bars'],
            strategy_config=config
        )
        total_pnl += pnl
        total_wins += wins
        total_losses += losses
        total_filtered += filtered
    
    total_trades = total_wins + total_losses
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    
    return {
        'strategy': strategy_name,
        'total_pnl': total_pnl,
        'win_rate': win_rate,
        'total_trades': total_trades,
        'wins': total_wins,
        'losses': total_losses,
        'filtered_exits': total_filtered
    }

def run_comparison():
    """Сравнить все стратегии."""
    print("🚀 Сравнение Delta-фильтров")
    print(f"   Базовые параметры: SL={BASE_SL}%, Act={BASE_ACTIVATION}%, "
          f"Callback={BASE_CALLBACK}%, Drop={BASE_REENTRY_DROP}%, Cooldown={BASE_COOLDOWN}s")
    print("-" * 90)
    
    load_all_data()
    
    print("\n🔄 Тестирование стратегий...")
    
    results = []
    for name in DELTA_STRATEGIES:
        print(f"   {name}...", end=' ', flush=True)
        result = evaluate_strategy(name)
        results.append(result)
        print(f"PnL: {result['total_pnl']:+.2f}%")
    
    # Сортируем по PnL
    results.sort(key=lambda x: x['total_pnl'], reverse=True)
    
    # Выводим результаты
    print("\n" + "=" * 90)
    print("📊 РЕЗУЛЬТАТЫ СРАВНЕНИЯ")
    print("=" * 90)
    print(f"{'#':<3} {'Стратегия':<20} {'PnL %':<12} {'WinRate':<10} {'Trades':<10} {'Filtered':<10} {'vs BASELINE'}")
    print("-" * 90)
    
    baseline_pnl = next(r['total_pnl'] for r in results if r['strategy'] == 'BASELINE')
    
    for i, res in enumerate(results, 1):
        diff = res['total_pnl'] - baseline_pnl
        diff_sign = "🟢" if diff > 0 else "🔴" if diff < 0 else "⚪"
        print(f"{i:<3} {res['strategy']:<20} {res['total_pnl']:>+10.2f}% {res['win_rate']:>8.1f}% "
              f"{res['total_trades']:>8} {res['filtered_exits']:>8} {diff_sign} {diff:>+8.2f}%")
    
    # Сохраняем результаты
    output_file = Path(__file__).parent.parent / "reports" / "delta_filter_comparison.json"
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'base_params': {
                'sl': BASE_SL,
                'activation': BASE_ACTIVATION,
                'callback': BASE_CALLBACK,
                'reentry_drop': BASE_REENTRY_DROP,
                'cooldown': BASE_COOLDOWN
            },
            'results': results
        }, f, indent=2)
    
    print(f"\n📁 Результаты: {output_file}")
    
    # Лучшая стратегия
    best = results[0]
    print("\n" + "=" * 90)
    print(f"🏆 ЛУЧШАЯ: {best['strategy']}")
    print(f"   PnL: {best['total_pnl']:+.2f}%")
    print(f"   Превосходство над BASELINE: {best['total_pnl'] - baseline_pnl:+.2f}%")
    
    return results

if __name__ == "__main__":
    run_comparison()
