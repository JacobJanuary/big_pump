"""
Бэктестинг Delta Reversal стратегий на 1-секундных барах.

Тестирует различные стратегии входа/выхода на основе:
- Rolling Delta
- Absorption Detection
- Large Trade Spikes
- Delta Divergence
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from enum import Enum
import statistics

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# ============== КОНФИГУРАЦИЯ ==============

# Параметры симуляции
INITIAL_CAPITAL = 10000  # USD
POSITION_SIZE_PCT = 100  # % капитала на сделку
COMMISSION_PCT = 0.04    # Комиссия Binance (taker)
SLIPPAGE_PCT = 0.05      # Проскальзывание

# Параметры стратегий
ROLLING_WINDOW_SEC = 30  # Окно для rolling delta (секунды)
ABSORPTION_THRESHOLD = 2.0  # Множитель для детекции абсорбции
LARGE_TRADE_MULTIPLIER = 3.0  # Порог крупных сделок (от среднего)
DELTA_DIVERGENCE_BARS = 60  # Баров для поиска дивергенции

# Stop Loss / Take Profit
SL_PCT = 4.0   # Stop Loss %
TP_PCT = 10.0  # Take Profit %
REENTRY_COOLDOWN_SEC = 60  # Кулдаун после выхода (секунды)

# ============== СТРУКТУРЫ ==============

class Position(Enum):
    NONE = 0
    LONG = 1

@dataclass
class Trade:
    entry_time: int
    entry_price: float
    exit_time: Optional[int] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""
    pnl_pct: float = 0.0

@dataclass
class BacktestResult:
    strategy: str
    signal_id: int
    pair_symbol: str
    trades: List[Trade] = field(default_factory=list)
    total_pnl_pct: float = 0.0
    win_count: int = 0
    loss_count: int = 0

# ============== ИНДИКАТОРЫ ==============

def calculate_rolling_delta(bars: List[dict], window_sec: int = ROLLING_WINDOW_SEC) -> List[float]:
    """Вычислить rolling delta для каждого бара."""
    rolling_delta = []
    
    for i, bar in enumerate(bars):
        current_ts = bar['second_ts']
        window_start = current_ts - window_sec
        
        # Суммируем delta за окно
        delta_sum = 0
        for j in range(i, -1, -1):
            if bars[j]['second_ts'] < window_start:
                break
            delta_sum += float(bars[j]['delta'])
        
        rolling_delta.append(delta_sum)
    
    return rolling_delta

def detect_absorption(bars: List[dict], idx: int, threshold: float = ABSORPTION_THRESHOLD) -> bool:
    """
    Детектировать абсорбцию: большой объём покупок, но цена не растёт.
    """
    if idx < 10:
        return False
    
    current = bars[idx]
    prev_bars = bars[max(0, idx-30):idx]
    
    # Средний объём за последние 30 баров
    avg_buy_vol = statistics.mean([float(b['buy_volume']) for b in prev_bars]) if prev_bars else 0
    
    # Текущий бай-объём
    current_buy = float(current['buy_volume'])
    
    # Изменение цены за последние 10 баров
    price_10_ago = float(bars[idx-10]['close_price'])
    price_now = float(current['close_price'])
    price_change_pct = (price_now - price_10_ago) / price_10_ago * 100
    
    # Абсорбция = высокий объём покупок + цена стоит или падает
    is_high_volume = current_buy > avg_buy_vol * threshold
    is_price_stalled = price_change_pct < 0.5  # Цена не выросла более 0.5%
    
    return is_high_volume and is_price_stalled

def detect_delta_divergence(bars: List[dict], idx: int, lookback: int = DELTA_DIVERGENCE_BARS) -> bool:
    """
    Детектировать дивергенцию: цена делает новый хай, delta ниже.
    """
    if idx < lookback:
        return False
    
    current = bars[idx]
    lookback_bars = bars[idx-lookback:idx]
    
    # Найти предыдущий локальный хай цены
    prev_high_idx = max(range(len(lookback_bars)), key=lambda i: float(lookback_bars[i]['high_price']))
    prev_high = float(lookback_bars[prev_high_idx]['high_price'])
    prev_high_delta = float(lookback_bars[prev_high_idx]['delta'])
    
    current_high = float(current['high_price'])
    current_delta = float(current['delta'])
    
    # Дивергенция = новый хай цены + delta ниже
    is_new_high = current_high > prev_high
    is_delta_lower = current_delta < prev_high_delta * 0.7  # Delta на 30% ниже
    
    return is_new_high and is_delta_lower

def detect_panic_sell(bars: List[dict], idx: int, multiplier: float = LARGE_TRADE_MULTIPLIER) -> bool:
    """
    Детектировать панические продажи: всплеск крупных sell.
    """
    if idx < 10:
        return False
    
    current = bars[idx]
    prev_bars = bars[max(0, idx-30):idx]
    
    # Средний large_sell_count
    avg_large_sell = statistics.mean([b['large_sell_count'] for b in prev_bars]) if prev_bars else 0
    
    # Текущий
    current_large_sell = current['large_sell_count']
    
    return current_large_sell > avg_large_sell * multiplier and current_large_sell >= 3

def detect_reentry_signal(bars: List[dict], idx: int) -> bool:
    """
    Детектировать сигнал на перезаход: цена нашла дно + всплеск покупок.
    Ужесточённая версия.
    """
    if idx < 60:
        return False
    
    current = bars[idx]
    recent = bars[idx-30:idx]  # 30 баров вместо 10
    
    # Цена перестала падать (не обновляет лои)
    min_recent = min([float(b['low_price']) for b in recent])
    current_low = float(current['low_price'])
    is_above_recent_low = current_low > min_recent * 1.005  # На 0.5% выше лоя
    
    # Всплеск buy volume (3x вместо 2x)
    avg_buy = statistics.mean([float(b['buy_volume']) for b in recent])
    current_buy = float(current['buy_volume'])
    is_buy_spike = current_buy > avg_buy * 3
    
    # Сильная положительная delta (не просто > 0)
    avg_delta = statistics.mean([float(b['delta']) for b in recent])
    current_delta = float(current['delta'])
    is_strong_delta = current_delta > avg_delta * 2 and current_delta > 0
    
    # Крупных покупок больше чем продаж
    is_large_buy_dominant = current['large_buy_count'] > current['large_sell_count']
    
    return is_above_recent_low and is_buy_spike and is_strong_delta and is_large_buy_dominant

# ============== СТРАТЕГИИ ==============

def strategy_baseline(bars: List[dict], entry_price: float) -> BacktestResult:
    """
    BASELINE: Держим 24 часа без выхода.
    """
    result = BacktestResult(strategy="BASELINE", signal_id=0, pair_symbol="")
    
    if not bars:
        return result
    
    entry_time = bars[0]['second_ts']
    exit_price = float(bars[-1]['close_price'])
    exit_time = bars[-1]['second_ts']
    
    pnl_pct = (exit_price - entry_price) / entry_price * 100 - (COMMISSION_PCT * 2)
    
    trade = Trade(
        entry_time=entry_time,
        entry_price=entry_price,
        exit_time=exit_time,
        exit_price=exit_price,
        exit_reason="TIMEOUT",
        pnl_pct=pnl_pct
    )
    
    result.trades.append(trade)
    result.total_pnl_pct = pnl_pct
    result.win_count = 1 if pnl_pct > 0 else 0
    result.loss_count = 1 if pnl_pct <= 0 else 0
    
    return result

def strategy_with_exits(bars: List[dict], entry_price: float, 
                        use_absorption: bool = False,
                        use_divergence: bool = False,
                        use_panic: bool = False,
                        use_reentry: bool = False) -> BacktestResult:
    """
    Стратегия с сигналами выхода.
    """
    result = BacktestResult(strategy="CUSTOM", signal_id=0, pair_symbol="")
    
    if not bars:
        return result
    
    position = Position.LONG
    current_entry_price = entry_price
    current_entry_time = bars[0]['second_ts']
    trades = []
    
    for idx, bar in enumerate(bars):
        price = float(bar['close_price'])
        ts = bar['second_ts']
        
        # Проверка SL/TP
        if position == Position.LONG:
            pnl_pct = (price - current_entry_price) / current_entry_price * 100
            
            # Stop Loss
            if pnl_pct <= -SL_PCT:
                trades.append(Trade(
                    entry_time=current_entry_time,
                    entry_price=current_entry_price,
                    exit_time=ts,
                    exit_price=price,
                    exit_reason="SL_HIT",
                    pnl_pct=pnl_pct - (COMMISSION_PCT * 2)
                ))
                position = Position.NONE
                continue
            
            # Take Profit
            if pnl_pct >= TP_PCT:
                trades.append(Trade(
                    entry_time=current_entry_time,
                    entry_price=current_entry_price,
                    exit_time=ts,
                    exit_price=price,
                    exit_reason="TP_HIT",
                    pnl_pct=pnl_pct - (COMMISSION_PCT * 2)
                ))
                position = Position.NONE
                continue
            
            # Сигналы выхода
            exit_reason = None
            
            if use_absorption and detect_absorption(bars, idx):
                exit_reason = "ABSORPTION"
            elif use_divergence and detect_delta_divergence(bars, idx):
                exit_reason = "DIVERGENCE"
            elif use_panic and detect_panic_sell(bars, idx):
                exit_reason = "PANIC"
            
            if exit_reason:
                trades.append(Trade(
                    entry_time=current_entry_time,
                    entry_price=current_entry_price,
                    exit_time=ts,
                    exit_price=price,
                    exit_reason=exit_reason,
                    pnl_pct=pnl_pct - (COMMISSION_PCT * 2)
                ))
                position = Position.NONE
        
        # Сигналы перезахода (с кулдауном)
        elif position == Position.NONE and use_reentry:
            # Проверка кулдауна
            if trades and (ts - trades[-1].exit_time) < REENTRY_COOLDOWN_SEC:
                continue
            
            if detect_reentry_signal(bars, idx):
                position = Position.LONG
                current_entry_price = price
                current_entry_time = ts
    
    # Закрыть открытую позицию в конце
    if position == Position.LONG:
        price = float(bars[-1]['close_price'])
        pnl_pct = (price - current_entry_price) / current_entry_price * 100
        trades.append(Trade(
            entry_time=current_entry_time,
            entry_price=current_entry_price,
            exit_time=bars[-1]['second_ts'],
            exit_price=price,
            exit_reason="TIMEOUT",
            pnl_pct=pnl_pct - (COMMISSION_PCT * 2)
        ))
    
    result.trades = trades
    result.total_pnl_pct = sum(t.pnl_pct for t in trades)
    result.win_count = sum(1 for t in trades if t.pnl_pct > 0)
    result.loss_count = sum(1 for t in trades if t.pnl_pct <= 0)
    
    return result

# ============== БЭКТЕСТ ==============

def get_bars_for_signal(conn, signal_id: int) -> List[dict]:
    """Получить 1-секундные бары для сигнала."""
    query = """
        SELECT second_ts, open_price, high_price, low_price, close_price,
               buy_volume, sell_volume, delta, large_buy_count, large_sell_count
        FROM web.agg_trades_1s
        WHERE signal_analysis_id = %s
        ORDER BY second_ts
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (signal_id,))
        rows = cur.fetchall()
    
    return [
        {
            'second_ts': r[0],
            'open_price': r[1],
            'high_price': r[2],
            'low_price': r[3],
            'close_price': r[4],
            'buy_volume': r[5],
            'sell_volume': r[6],
            'delta': r[7],
            'large_buy_count': r[8],
            'large_sell_count': r[9]
        }
        for r in rows
    ]

def get_signals_list(conn, limit=None) -> List[dict]:
    """Получить список сигналов для тестирования."""
    query = """
        SELECT DISTINCT signal_analysis_id, pair_symbol
        FROM web.agg_trades_1s
        ORDER BY signal_analysis_id
    """
    if limit:
        query += f" LIMIT {limit}"
    
    with conn.cursor() as cur:
        cur.execute(query)
        return [{'signal_analysis_id': r[0], 'pair_symbol': r[1]} for r in cur.fetchall()]

def run_backtest(limit=None):
    """
    Запуск бэктеста всех стратегий.
    """
    print("🚀 Бэктестинг Delta Reversal стратегий")
    print(f"   SL: {SL_PCT}% | TP: {TP_PCT}%")
    print(f"   Комиссия: {COMMISSION_PCT}%")
    print("-" * 70)
    
    strategies = {
        'BASELINE': lambda bars, ep: strategy_baseline(bars, ep),
        'ABSORPTION': lambda bars, ep: strategy_with_exits(bars, ep, use_absorption=True),
        'DIVERGENCE': lambda bars, ep: strategy_with_exits(bars, ep, use_divergence=True),
        'PANIC': lambda bars, ep: strategy_with_exits(bars, ep, use_panic=True),
        'REENTRY': lambda bars, ep: strategy_with_exits(bars, ep, use_reentry=True),
        'COMBINED': lambda bars, ep: strategy_with_exits(
            bars, ep, use_absorption=True, use_divergence=True, use_panic=True, use_reentry=True
        )
    }
    
    results = {name: [] for name in strategies}
    
    try:
        with get_db_connection() as conn:
            signals = get_signals_list(conn, limit=limit)
            print(f"Найдено сигналов: {len(signals)}")
            print("-" * 70)
            
            for i, sig in enumerate(signals, 1):
                signal_id = sig['signal_analysis_id']
                pair_symbol = sig['pair_symbol']
                
                print(f"[{i}/{len(signals)}] {pair_symbol:<15}", end=' ', flush=True)
                
                bars = get_bars_for_signal(conn, signal_id)
                
                if not bars:
                    print("❌ Нет данных")
                    continue
                
                entry_price = float(bars[0]['open_price'])
                
                # Тестируем все стратегии
                for name, strategy_func in strategies.items():
                    result = strategy_func(bars, entry_price)
                    result.signal_id = signal_id
                    result.pair_symbol = pair_symbol
                    results[name].append(result)
                
                # Показываем результат baseline
                baseline_pnl = results['BASELINE'][-1].total_pnl_pct
                print(f"baseline: {baseline_pnl:+.2f}%")
        
        # Итоговая статистика
        print("\n" + "=" * 70)
        print("📊 РЕЗУЛЬТАТЫ БЭКТЕСТА")
        print("=" * 70)
        print(f"{'Стратегия':<15} {'PnL %':<12} {'Win Rate':<12} {'Trades':<10} {'Max DD'}")
        print("-" * 70)
        
        summary = {}
        for name, res_list in results.items():
            total_pnl = sum(r.total_pnl_pct for r in res_list)
            wins = sum(r.win_count for r in res_list)
            losses = sum(r.loss_count for r in res_list)
            total_trades = wins + losses
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
            
            # Max Drawdown (упрощённо)
            pnls = [r.total_pnl_pct for r in res_list]
            cumulative = []
            running = 0
            for p in pnls:
                running += p
                cumulative.append(running)
            max_dd = min(cumulative) if cumulative else 0
            
            print(f"{name:<15} {total_pnl:>+10.2f}% {win_rate:>10.1f}% {total_trades:>10} {max_dd:>+8.2f}%")
            
            summary[name] = {
                'total_pnl_pct': total_pnl,
                'win_rate': win_rate,
                'total_trades': total_trades,
                'wins': wins,
                'losses': losses,
                'max_drawdown': max_dd,
                'signals': len(res_list)
            }
        
        # Сохранение в JSON
        import json
        output_file = Path(__file__).parent.parent / "reports" / "backtest_delta_results.json"
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n📁 Результаты сохранены в: {output_file}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Бэктестинг Delta Reversal стратегий')
    parser.add_argument('--limit', type=int, default=None, help='Лимит сигналов')
    
    args = parser.parse_args()
    
    run_backtest(limit=args.limit)

