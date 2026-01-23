# """
# Financial Report 10x (Filtered Version)
# This script generates the detailed financial report but only for signals that satisfy the filters defined in pump_analysis_lib.py.
# """
import sys
import os
from pathlib import Path
from datetime import datetime, date
from collections import defaultdict
import json
import statistics

current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection, fetch_signals

# ============== CONFIGURATION ==============

REPORT_OUTPUT = Path(__file__).parent.parent / "reports" / "financial_report_10x_filtered.json"
OPTIMIZATION_RESULT = Path(__file__).parent.parent / "reports" / "optimization_combined_leverage_filtered.json"

def apply_best_parameters():
    """Load best parameters from optimization report and apply to strategy."""
    import financial_report_10x
    
    if not OPTIMIZATION_RESULT.exists():
        print(f"⚠️ Optimization report not found at {OPTIMIZATION_RESULT}")
        print(f"   Using default parameters: SL={financial_report_10x.SL_PCT}%, Window={financial_report_10x.DELTA_WINDOW}")
        return

    try:
        with open(OPTIMIZATION_RESULT, 'r') as f:
            results = json.load(f)
        
        if not results:
            print("⚠️ Optimization report is empty.")
            return

        # Get Top 1
        best = results[0]['params']
        
        print(f"🏆 Loaded Best Parameters from Optimization:")
        print(f"   Lev: {best['leverage']}x")
        print(f"   SL: {best['sl_pct']}%")
        print(f"   Window: {best['delta_window']}")
        print(f"   Threshold: {best['threshold_mult']}")
        print("-" * 60)
        
        # Monkey-patch the module
        financial_report_10x.LEVERAGE = best['leverage']
        financial_report_10x.SL_PCT = float(best['sl_pct'])
        financial_report_10x.DELTA_WINDOW = int(best['delta_window'])
        financial_report_10x.THRESHOLD_MULT = float(best['threshold_mult'])
        
    except Exception as e:
        print(f"❌ Failed to load optimization results: {e}")

def get_signal_ids_filtered() -> list:
    """Return list of web.signal_analysis.id that resolve from filtered signals."""
    try:
        with get_db_connection() as conn:
            # 1. FAS Filtered Signals
            raw_signals = fetch_signals(conn)
            if not raw_signals:
                return []
            
            # 2. Web Signal Analysis Map
            with conn.cursor() as cur:
                cur.execute("SELECT id, pair_symbol, signal_timestamp FROM web.signal_analysis")
                web_signals = cur.fetchall()
            
            web_map = {}
            for wid, sym, ts in web_signals:
                if ts.tzinfo is None:
                    # Assume UTC if naive, as fetch_signals usually returns aware
                    from datetime import timezone
                    ts = ts.replace(tzinfo=timezone.utc)
                web_map[(sym, ts)] = wid
            
            # 3. Match
            matched_ids = []
            for s in raw_signals:
                sym = s['pair_symbol']
                ts = s['timestamp']
                
                if ts.tzinfo is None:
                    from datetime import timezone
                    ts = ts.replace(tzinfo=timezone.utc)
                
                if (sym, ts) in web_map:
                    matched_ids.append(web_map[(sym, ts)])
                    
            print(f"✅ Filtered signals: {len(raw_signals)} (Found in Web DB: {len(matched_ids)})")
            return matched_ids

    except Exception as e:
        print(f"Failed to fetch filtered signals: {e}")
        return []

def load_bars_for_signal(conn, signal_id: int):
    """Load 1‑second bars for a given signal id."""
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
        rows = cur.fetchall()
        # Convert to dicts as expected by financial_report_10x.run_strategy_detailed
        return [
            {
                'ts': r[0],
                'price': float(r[1]),
                'delta': float(r[2]),
                'large_buy': r[3],
                'large_sell': r[4]
            }
            for r in rows
        ]

def generate_report():
    apply_best_parameters()
    
    print("📊 Detailed Financial Report (Filtered)")
    print(f"   Output: {REPORT_OUTPUT}")
    print("-" * 100)

    filtered_ids = get_signal_ids_filtered()
    if not filtered_ids:
        print("❌ No signals after filtering.")
        return
    print(f"✅ Filtered signals count: {len(filtered_ids)}")

    all_trades = []
    signals_by_day = defaultdict(list)

    with get_db_connection() as conn:
        for idx, signal_id in enumerate(filtered_ids, 1):
            # Load bars for this signal
            bars = load_bars_for_signal(conn, signal_id)
            if not bars:
                continue
            # Retrieve pair symbol and date (same as original logic)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pair_symbol, MIN(second_ts) FROM web.agg_trades_1s
                    WHERE signal_analysis_id = %s GROUP BY pair_symbol
                    """,
                    (signal_id,)
                )
                res = cur.fetchone()
                if not res:
                    continue
                pair_symbol, first_ts = res
                signal_date = date.fromtimestamp(first_ts // 1000)

            # Run the detailed strategy (reuse the original run_strategy_detailed function)
            # For brevity we import it from the original module.
            from financial_report_10x import run_strategy_detailed
            trades = run_strategy_detailed(bars, signal_date)
            for t in trades:
                t['pair_symbol'] = pair_symbol
                t['signal_id'] = signal_id
            all_trades.extend(trades)
            signals_by_day[signal_date].append({
                'pair_symbol': pair_symbol,
                'trades': len(trades)
            })
            if idx % 30 == 0:
                print(f"   {idx}/{len(filtered_ids)} processed", flush=True)

    # The rest of the report generation mirrors the original script.
    # ... (the detailed aggregation, daily stats, and summary) ...
    # For brevity, we will reuse the original implementation by calling the function from the original file.
    from financial_report_10x import generate_report as original_generate_report
    # The original function expects to load all signals itself, so we cannot directly reuse it.
    # Instead, we replicate the final sections here.

    # --- Capital Simulation & Reporting ---
    # Adapted from financial_report_10x.py
    
    print("\n" + "=" * 100)
    print("💰 СИМУЛЯЦИЯ КАПИТАЛА")
    print("=" * 100)
    
    balance = 0
    min_balance = 0  # Max drawdown (required capital)
    POSITION_SIZE = 100 # From original script default
    
    daily_stats = []
    current_date = None
    day_trades = []
    
    # Sort trades by exit time
    all_trades.sort(key=lambda t: t['exit_ts'])
    
    for trade in all_trades:
        # Exit date
        trade_date = date.fromtimestamp(trade['exit_ts'])
        
        # New Day?
        if current_date != trade_date:
            if current_date is not None:
                # Save previous day stats
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
            
            current_date = trade_date
            day_trades = []
        
        # Simulation
        balance -= POSITION_SIZE
        min_balance = min(min_balance, balance)
        
        balance += POSITION_SIZE + trade['pnl_usd']
        
        day_trades.append(trade)
    
    # Last Day
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
    
    # Print Daily Report
    print("\n📅 ЕЖЕДНЕВНЫЙ ОТЧЁТ")
    print("-" * 100)
    print(f"{'Дата':<12} {'Сигналов':<10} {'Сделок':<8} {'Профит $':<12} {'Убыток $':<12} {'Нетто $':<12} {'Баланс $'}")
    print("-" * 100)
    
    for day in daily_stats:
        print(f"{str(day['date']):<12} {day['signals']:<10} {day['trades']:<8} "
              f"{day['profit']:>+10.2f}  {day['loss']:>+10.2f}  {day['net']:>+10.2f}  {day['balance']:>+10.2f}")
    
    # Summary
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
    
    # Stats
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
    
    # Exit Reasons
    print("\n   По причинам выхода:")
    exit_reasons = defaultdict(list)
    for t in all_trades:
        exit_reasons[t['exit_reason']].append(t['pnl_usd'])
    
    for reason, pnls in exit_reasons.items():
        avg_pnl = statistics.mean(pnls)
        count = len(pnls)
        total = sum(pnls)
        print(f"      {reason}: {count} сделок, среднее ${avg_pnl:.2f}, всего ${total:.2f}")

    # JSON Output
    REPORT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_OUTPUT, 'w') as f:
        json.dump({
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
            ],
            'trades': all_trades
        }, f, indent=2)
    print(f"\n📁 Report saved to {REPORT_OUTPUT}")

if __name__ == "__main__":
    generate_report()
