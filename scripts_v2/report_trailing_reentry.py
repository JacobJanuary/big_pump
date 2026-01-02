"""
Детальный отчёт по каждому сигналу с лучшими параметрами Trailing REENTRY.
"""
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict
import json

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# ============== ЛУЧШИЕ ПАРАМЕТРЫ ==============
TRAIL_ACTIVATION = 10.0    # Активация трейла при +10%
TRAIL_CALLBACK = 4.0       # Откат 4% для выхода
REENTRY_DROP = 5.0         # Перезаход после падения 5%
REENTRY_COOLDOWN = 300     # 5 минут кулдаун

COMMISSION_PCT = 0.04
SL_PCT = 15.0

# ============== СТРУКТУРЫ ==============

@dataclass
class Trade:
    entry_ts: int
    entry_price: float
    exit_ts: int
    exit_price: float
    pnl_pct: float
    exit_reason: str
    max_price: float = 0  # Максимум за время сделки
    
    def duration_sec(self) -> int:
        return self.exit_ts - self.entry_ts

@dataclass  
class SignalReport:
    signal_id: int
    pair_symbol: str
    start_price: float
    end_price: float
    baseline_pnl: float
    strategy_pnl: float
    trades: List[Trade] = field(default_factory=list)
    
    def alpha(self) -> float:
        return self.strategy_pnl - self.baseline_pnl

def run_strategy_detailed(bars: List[dict]) -> tuple:
    """
    Запустить стратегию и вернуть детальную информацию о сделках.
    """
    if not bars or len(bars) < 100:
        return [], 0.0
    
    trades = []
    in_position = True
    entry_price = bars[0]['price']
    entry_ts = bars[0]['ts']
    max_price = entry_price
    last_exit_ts = 0
    
    for bar in bars:
        price = bar['price']
        ts = bar['ts']
        
        if in_position:
            max_price = max(max_price, price)
            pnl_from_entry = (price - entry_price) / entry_price * 100
            drawdown_from_max = (max_price - price) / max_price * 100
            
            # Hard SL
            if pnl_from_entry <= -SL_PCT:
                trades.append(Trade(
                    entry_ts=entry_ts,
                    entry_price=entry_price,
                    exit_ts=ts,
                    exit_price=price,
                    pnl_pct=pnl_from_entry - (COMMISSION_PCT * 2),
                    exit_reason="SL",
                    max_price=max_price
                ))
                in_position = False
                last_exit_ts = ts
                continue
            
            # Trailing Stop
            if pnl_from_entry >= TRAIL_ACTIVATION and drawdown_from_max >= TRAIL_CALLBACK:
                trades.append(Trade(
                    entry_ts=entry_ts,
                    entry_price=entry_price,
                    exit_ts=ts,
                    exit_price=price,
                    pnl_pct=pnl_from_entry - (COMMISSION_PCT * 2),
                    exit_reason="TRAIL",
                    max_price=max_price
                ))
                in_position = False
                last_exit_ts = ts
                max_price = price
        
        else:
            # Ищем перезаход
            if ts - last_exit_ts < REENTRY_COOLDOWN:
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
    
    # Закрываем открытую позицию
    if in_position and bars:
        final_price = bars[-1]['price']
        pnl = (final_price - entry_price) / entry_price * 100 - (COMMISSION_PCT * 2)
        trades.append(Trade(
            entry_ts=entry_ts,
            entry_price=entry_price,
            exit_ts=bars[-1]['ts'],
            exit_price=final_price,
            pnl_pct=pnl,
            exit_reason="TIMEOUT",
            max_price=max_price
        ))
    
    total_pnl = sum(t.pnl_pct for t in trades)
    return trades, total_pnl

def generate_report():
    """Сгенерировать детальный отчёт."""
    
    print("📊 Генерация детального отчёта")
    print(f"   Параметры: activation={TRAIL_ACTIVATION}%, callback={TRAIL_CALLBACK}%, drop={REENTRY_DROP}%, cooldown={REENTRY_COOLDOWN}s")
    print("-" * 80)
    
    reports = []
    
    with get_db_connection() as conn:
        # Получаем сигналы
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT signal_analysis_id, pair_symbol
                FROM web.agg_trades_1s
                ORDER BY signal_analysis_id
            """)
            signals = cur.fetchall()
        
        print(f"Сигналов: {len(signals)}\n")
        
        for i, (signal_id, pair_symbol) in enumerate(signals, 1):
            # Загружаем бары
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT second_ts, close_price, delta, buy_volume,
                           large_buy_count, large_sell_count
                    FROM web.agg_trades_1s
                    WHERE signal_analysis_id = %s
                    ORDER BY second_ts
                """, (signal_id,))
                rows = cur.fetchall()
            
            if not rows:
                continue
            
            bars = [
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
            
            # Baseline = hold 24h
            start_price = bars[0]['price']
            end_price = bars[-1]['price']
            baseline_pnl = (end_price - start_price) / start_price * 100 - (COMMISSION_PCT * 2)
            
            # Strategy
            trades, strategy_pnl = run_strategy_detailed(bars)
            
            report = SignalReport(
                signal_id=signal_id,
                pair_symbol=pair_symbol,
                start_price=start_price,
                end_price=end_price,
                baseline_pnl=baseline_pnl,
                strategy_pnl=strategy_pnl,
                trades=trades
            )
            reports.append(report)
            
            # Вывод
            alpha = report.alpha()
            alpha_sign = "🟢" if alpha >= 0 else "🔴"
            
            print(f"[{i:3}/{len(signals)}] {pair_symbol:<16} Baseline: {baseline_pnl:>+7.2f}% | Strategy: {strategy_pnl:>+7.2f}% | Alpha: {alpha_sign} {alpha:>+6.2f}% | Trades: {len(trades)}")
    
    # Итоги
    total_baseline = sum(r.baseline_pnl for r in reports)
    total_strategy = sum(r.strategy_pnl for r in reports)
    total_alpha = total_strategy - total_baseline
    
    winners = [r for r in reports if r.strategy_pnl > r.baseline_pnl]
    losers = [r for r in reports if r.strategy_pnl <= r.baseline_pnl]
    
    print("\n" + "=" * 80)
    print("📈 ИТОГИ")
    print("=" * 80)
    print(f"Total Baseline PnL:  {total_baseline:>+10.2f}%")
    print(f"Total Strategy PnL:  {total_strategy:>+10.2f}%")
    print(f"Total Alpha:         {total_alpha:>+10.2f}%")
    print(f"")
    print(f"Сигналов где стратегия лучше: {len(winners)}/{len(reports)} ({len(winners)/len(reports)*100:.1f}%)")
    print(f"Сигналов где baseline лучше:  {len(losers)}/{len(reports)} ({len(losers)/len(reports)*100:.1f}%)")
    
    # Топ-5 где стратегия дала максимальную альфу
    print("\n" + "-" * 80)
    print("🏆 ТОП-5 по АЛЬФЕ (стратегия >> baseline):")
    sorted_by_alpha = sorted(reports, key=lambda r: r.alpha(), reverse=True)
    for r in sorted_by_alpha[:5]:
        print(f"   {r.pair_symbol:<16} Alpha: {r.alpha():>+7.2f}% (Strategy: {r.strategy_pnl:>+7.2f}% vs Baseline: {r.baseline_pnl:>+7.2f}%)")
    
    # Топ-5 где стратегия проиграла
    print("\n" + "-" * 80)
    print("📉 ХУДШИЕ 5 по АЛЬФЕ (baseline >> стратегия):")
    for r in sorted_by_alpha[-5:]:
        print(f"   {r.pair_symbol:<16} Alpha: {r.alpha():>+7.2f}% (Strategy: {r.strategy_pnl:>+7.2f}% vs Baseline: {r.baseline_pnl:>+7.2f}%)")
    
    # Детальный разбор нескольких сигналов
    print("\n" + "=" * 80)
    print("🔍 ДЕТАЛЬНЫЙ РАЗБОР ЛУЧШИХ СДЕЛОК")
    print("=" * 80)
    
    for r in sorted_by_alpha[:3]:
        print(f"\n{'='*40}")
        print(f"📌 {r.pair_symbol} (Signal #{r.signal_id})")
        print(f"   Start: ${r.start_price:.6f} → End: ${r.end_price:.6f}")
        print(f"   Baseline: {r.baseline_pnl:+.2f}% | Strategy: {r.strategy_pnl:+.2f}% | Alpha: {r.alpha():+.2f}%")
        print(f"   Trades: {len(r.trades)}")
        print("-" * 40)
        
        for j, t in enumerate(r.trades, 1):
            duration_min = t.duration_sec() / 60
            max_profit = (t.max_price - t.entry_price) / t.entry_price * 100
            print(f"   [{j}] {t.exit_reason:<7} Entry: ${t.entry_price:.6f} → Exit: ${t.exit_price:.6f}")
            print(f"       PnL: {t.pnl_pct:+.2f}% | Max: {max_profit:+.2f}% | Duration: {duration_min:.1f} min")
    
    # Сохраняем JSON
    output_file = Path(__file__).parent.parent / "reports" / "detailed_signal_report.json"
    
    report_data = {
        'params': {
            'trail_activation': TRAIL_ACTIVATION,
            'trail_callback': TRAIL_CALLBACK,
            'reentry_drop': REENTRY_DROP,
            'reentry_cooldown': REENTRY_COOLDOWN
        },
        'summary': {
            'total_baseline': total_baseline,
            'total_strategy': total_strategy,
            'total_alpha': total_alpha,
            'signals_strategy_better': len(winners),
            'signals_baseline_better': len(losers)
        },
        'signals': [
            {
                'signal_id': r.signal_id,
                'pair_symbol': r.pair_symbol,
                'baseline_pnl': r.baseline_pnl,
                'strategy_pnl': r.strategy_pnl,
                'alpha': r.alpha(),
                'num_trades': len(r.trades),
                'trades': [
                    {
                        'entry_price': t.entry_price,
                        'exit_price': t.exit_price,
                        'pnl_pct': t.pnl_pct,
                        'exit_reason': t.exit_reason,
                        'duration_sec': t.duration_sec()
                    }
                    for t in r.trades
                ]
            }
            for r in reports
        ]
    }
    
    with open(output_file, 'w') as f:
        json.dump(report_data, f, indent=2)
    
    print(f"\n📁 Отчёт сохранён: {output_file}")

if __name__ == "__main__":
    generate_report()
