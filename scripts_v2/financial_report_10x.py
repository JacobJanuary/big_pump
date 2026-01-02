"""
Детальный финансовый отчёт для стратегии 10x Leverage.

Параметры:
- 10x Leverage
- SL: 7%
- delta_window: 20 сек
- threshold_mult: 1.0
- Размер позиции: $100 реальных ($1000 с плечом)
"""
import sys
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict
from collections import defaultdict
import json
import statistics

current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# ============== ПАРАМЕТРЫ СТРАТЕГИИ ==============

LEVERAGE = 10
SL_PCT = 7.0
DELTA_WINDOW = 20
THRESHOLD_MULT = 1.0

# Базовые параметры
ACTIVATION = 10.0
CALLBACK = 4.0
REENTRY_DROP = 5.0
COOLDOWN = 300

COMMISSION_PCT = 0.04
POSITION_SIZE = 100  # $100 реальных (с 10x = $1000)

# ============== ФУНКЦИИ ==============

def get_rolling_delta(bars: List[dict], idx: int, window: int) -> float:
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
    if idx < lookback:
        lookback = idx
    if lookback < 1:
        return 0
    deltas = [abs(bars[i]['delta']) for i in range(idx - lookback, idx)]
    return statistics.mean(deltas) if deltas else 0

def run_strategy_detailed(bars: List[dict], signal_date: date) -> List[dict]:
    """
    Запустить стратегию и вернуть список сделок с деталями.
    """
    if not bars or len(bars) < 100:
        return []
    
    trades = []
    in_position = True
    entry_price = bars[0]['price']
    entry_ts = bars[0]['ts']
    max_price = entry_price
    last_exit_ts = 0
    
    for idx, bar in enumerate(bars):
        price = bar['price']
        ts = bar['ts']
        
        if in_position:
            max_price = max(max_price, price)
            pnl_pct = (price - entry_price) / entry_price * 100
            drawdown = (max_price - price) / max_price * 100
            
            # SL
            if pnl_pct <= -SL_PCT:
                leveraged_pnl_pct = pnl_pct * LEVERAGE
                pnl_usd = POSITION_SIZE * leveraged_pnl_pct / 100
                commission = POSITION_SIZE * COMMISSION_PCT / 100 * 2 * LEVERAGE
                
                trades.append({
                    'signal_date': signal_date,
                    'entry_ts': entry_ts,
                    'exit_ts': ts,
                    'entry_price': entry_price,
                    'exit_price': price,
                    'pnl_pct': leveraged_pnl_pct,
                    'pnl_usd': pnl_usd - commission,
                    'exit_reason': 'SL'
                })
                in_position = False
                last_exit_ts = ts
                continue
            
            # Trailing
            trailing_triggered = pnl_pct >= ACTIVATION and drawdown >= CALLBACK
            
            if trailing_triggered:
                should_exit = True
                
                rolling_delta = get_rolling_delta(bars, idx, DELTA_WINDOW)
                avg_delta = get_avg_delta(bars, idx)
                
                if rolling_delta > avg_delta * THRESHOLD_MULT:
                    should_exit = False
                
                if should_exit and rolling_delta >= 0:
                    should_exit = False
                
                if should_exit:
                    leveraged_pnl_pct = pnl_pct * LEVERAGE
                    pnl_usd = POSITION_SIZE * leveraged_pnl_pct / 100
                    commission = POSITION_SIZE * COMMISSION_PCT / 100 * 2 * LEVERAGE
                    
                    trades.append({
                        'signal_date': signal_date,
                        'entry_ts': entry_ts,
                        'exit_ts': ts,
                        'entry_price': entry_price,
                        'exit_price': price,
                        'pnl_pct': leveraged_pnl_pct,
                        'pnl_usd': pnl_usd - commission,
                        'exit_reason': 'TRAIL'
                    })
                    in_position = False
                    last_exit_ts = ts
                    max_price = price
        
        else:
            if ts - last_exit_ts < COOLDOWN:
                continue
            
            if price < max_price:
                drop_pct = (max_price - price) / max_price * 100
                
                if drop_pct >= REENTRY_DROP:
                    if bar['delta'] > 0 and bar['large_buy'] > bar['large_sell']:
                        in_position = True
                        entry_price = price
                        entry_ts = ts
                        max_price = price
            else:
                max_price = price
    
    # Закрытие
    if in_position and bars:
        final_price = bars[-1]['price']
        pnl_pct = (final_price - entry_price) / entry_price * 100
        leveraged_pnl_pct = pnl_pct * LEVERAGE
        pnl_usd = POSITION_SIZE * leveraged_pnl_pct / 100
        commission = POSITION_SIZE * COMMISSION_PCT / 100 * 2 * LEVERAGE
        
        trades.append({
            'signal_date': signal_date,
            'entry_ts': entry_ts,
            'exit_ts': bars[-1]['ts'],
            'entry_price': entry_price,
            'exit_price': final_price,
            'pnl_pct': leveraged_pnl_pct,
            'pnl_usd': pnl_usd - commission,
            'exit_reason': 'TIMEOUT'
        })
    
    return trades

def generate_report():
    """Генерировать детальный финансовый отчёт."""
    
    print("📊 Детальный финансовый отчёт")
    print(f"   Leverage: {LEVERAGE}x")
    print(f"   SL: {SL_PCT}%")
    print(f"   Позиция: ${POSITION_SIZE} (с плечом ${POSITION_SIZE * LEVERAGE})")
    print("-" * 100)
    
    all_trades = []
    signals_by_day = defaultdict(list)
    
    with get_db_connection() as conn:
        # Получаем сигналы с датами
        with conn.cursor() as cur:
            cur.execute("""
                SELECT at.signal_analysis_id, at.pair_symbol, 
                       DATE(to_timestamp(MIN(at.second_ts))) as signal_date
                FROM web.agg_trades_1s at
                GROUP BY at.signal_analysis_id, at.pair_symbol
                ORDER BY signal_date, at.signal_analysis_id
            """)
            signals = cur.fetchall()
        
        print(f"Сигналов: {len(signals)}")
        
        for i, (signal_id, pair_symbol, signal_date) in enumerate(signals, 1):
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT second_ts, close_price, delta, buy_volume,
                           large_buy_count, large_sell_count
                    FROM web.agg_trades_1s
                    WHERE signal_analysis_id = %s
                    ORDER BY second_ts
                """, (signal_id,))
                rows = cur.fetchall()
            
            bars = [
                {
                    'ts': r[0],
                    'price': float(r[1]),
                    'delta': float(r[2]),
                    'large_buy': r[4],
                    'large_sell': r[5]
                }
                for r in rows
            ]
            
            trades = run_strategy_detailed(bars, signal_date)
            
            for t in trades:
                t['pair_symbol'] = pair_symbol
                t['signal_id'] = signal_id
            
            all_trades.extend(trades)
            signals_by_day[signal_date].append({
                'pair_symbol': pair_symbol,
                'trades': len(trades)
            })
            
            if i % 30 == 0:
                print(f"   {i}/{len(signals)}", flush=True)
    
    # Симуляция капитала
    print("\n" + "=" * 100)
    print("💰 СИМУЛЯЦИЯ КАПИТАЛА")
    print("=" * 100)
    
    balance = 0
    min_balance = 0  # Максимальный минус (требуемый капитал)
    
    daily_stats = []
    current_date = None
    day_trades = []
    
    # Сортируем сделки по времени выхода
    all_trades.sort(key=lambda t: t['exit_ts'])
    
    for trade in all_trades:
        # Дата выхода из сделки (а не дата сигнала!)
        trade_date = date.fromtimestamp(trade['exit_ts'])
        
        # Новый день?
        if current_date != trade_date:
            if current_date is not None and day_trades:
                # Сохраняем статистику предыдущего дня
                day_profit = sum(t['pnl_usd'] for t in day_trades if t['pnl_usd'] > 0)
                day_loss = sum(t['pnl_usd'] for t in day_trades if t['pnl_usd'] < 0)
                day_net = day_profit + day_loss
                
                # Подсчёт уникальных сигналов за день
                unique_signals = len(set(t['signal_id'] for t in day_trades))
                
                daily_stats.append({
                    'date': current_date,
                    'signals': unique_signals,
                    'trades': len(day_trades),
                    'profit': day_profit,
                    'loss': day_loss,
                    'net': day_net,
                    'balance': balance
                })
            
            current_date = trade_date
            day_trades = []
        
        # Симуляция: открываем позицию = -$100
        balance -= POSITION_SIZE
        min_balance = min(min_balance, balance)
        
        # Закрываем позицию: +$100 + PnL
        balance += POSITION_SIZE + trade['pnl_usd']
        
        day_trades.append(trade)
    
    # Последний день
    if day_trades:
        day_profit = sum(t['pnl_usd'] for t in day_trades if t['pnl_usd'] > 0)
        day_loss = sum(t['pnl_usd'] for t in day_trades if t['pnl_usd'] < 0)
        day_net = day_profit + day_loss
        unique_signals = len(set(t['signal_id'] for t in day_trades))
        
        daily_stats.append({
            'date': current_date,
            'signals': unique_signals,
            'trades': len(day_trades),
            'profit': day_profit,
            'loss': day_loss,
            'net': day_net,
            'balance': balance
        })
    
    # Выводим ежедневный отчёт
    print("\n📅 ЕЖЕДНЕВНЫЙ ОТЧЁТ")
    print("-" * 100)
    print(f"{'Дата':<12} {'Сигналов':<10} {'Сделок':<8} {'Профит $':<12} {'Убыток $':<12} {'Нетто $':<12} {'Баланс $'}")
    print("-" * 100)
    
    for day in daily_stats:
        print(f"{str(day['date']):<12} {day['signals']:<10} {day['trades']:<8} "
              f"{day['profit']:>+10.2f}  {day['loss']:>+10.2f}  {day['net']:>+10.2f}  {day['balance']:>+10.2f}")
    
    # Итоги
    total_profit = sum(d['profit'] for d in daily_stats)
    total_loss = sum(d['loss'] for d in daily_stats)
    total_net = total_profit + total_loss
    
    print("\n" + "=" * 100)
    print("📈 ИТОГИ")
    print("=" * 100)
    print(f"   Всего сделок: {len(all_trades)}")
    print(f"   Прибыльных: {sum(1 for t in all_trades if t['pnl_usd'] > 0)}")
    print(f"   Убыточных: {sum(1 for t in all_trades if t['pnl_usd'] <= 0)}")
    print(f"")
    print(f"   💵 Общий профит: ${total_profit:,.2f}")
    print(f"   💸 Общий убыток: ${total_loss:,.2f}")
    print(f"   💰 Чистая прибыль: ${total_net:,.2f}")
    print(f"")
    print(f"   📊 Требуемый капитал (max просадка): ${abs(min_balance):,.2f}")
    print(f"   📈 Финальный баланс: ${balance:,.2f}")
    print(f"   🎯 ROI: {(balance / abs(min_balance) * 100) if min_balance != 0 else 0:.1f}%")
    
    # Статистика по сделкам
    print("\n" + "-" * 100)
    print("📊 СТАТИСТИКА СДЕЛОК")
    print("-" * 100)
    
    winning_trades = [t for t in all_trades if t['pnl_usd'] > 0]
    losing_trades = [t for t in all_trades if t['pnl_usd'] <= 0]
    
    if winning_trades:
        avg_win = statistics.mean([t['pnl_usd'] for t in winning_trades])
        max_win = max([t['pnl_usd'] for t in winning_trades])
        print(f"   Средний профит: ${avg_win:.2f}")
        print(f"   Макс профит: ${max_win:.2f}")
    
    if losing_trades:
        avg_loss = statistics.mean([t['pnl_usd'] for t in losing_trades])
        max_loss = min([t['pnl_usd'] for t in losing_trades])
        print(f"   Средний убыток: ${avg_loss:.2f}")
        print(f"   Макс убыток: ${max_loss:.2f}")
    
    # Распределение по причинам выхода
    print("\n   По причинам выхода:")
    exit_reasons = defaultdict(list)
    for t in all_trades:
        exit_reasons[t['exit_reason']].append(t['pnl_usd'])
    
    for reason, pnls in exit_reasons.items():
        avg_pnl = statistics.mean(pnls)
        count = len(pnls)
        total = sum(pnls)
        print(f"      {reason}: {count} сделок, среднее ${avg_pnl:.2f}, всего ${total:.2f}")
    
    # Сохраняем JSON
    output_file = Path(__file__).parent.parent / "reports" / "financial_report_10x.json"
    
    with open(output_file, 'w') as f:
        json.dump({
            'params': {
                'leverage': LEVERAGE,
                'sl_pct': SL_PCT,
                'position_size': POSITION_SIZE
            },
            'summary': {
                'total_trades': len(all_trades),
                'total_profit': total_profit,
                'total_loss': total_loss,
                'net_profit': total_net,
                'required_capital': abs(min_balance),
                'final_balance': balance,
                'roi_pct': (balance / abs(min_balance) * 100) if min_balance != 0 else 0
            },
            'daily_stats': [
                {
                    'date': str(d['date']),
                    'signals': d['signals'],
                    'trades': d['trades'],
                    'profit': d['profit'],
                    'loss': d['loss'],
                    'net': d['net'],
                    'balance': d['balance']
                }
                for d in daily_stats
            ]
        }, f, indent=2)
    
    print(f"\n📁 Отчёт сохранён: {output_file}")

if __name__ == "__main__":
    generate_report()
