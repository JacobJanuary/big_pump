"""
Подготовка Delta Data: агрегация aggTrades в 1-секундные бары.

Создаёт таблицу web.agg_trades_1s с:
- OHLC (цена)
- buy_volume, sell_volume, delta
- large_buy_count, large_sell_count
- trade_count
"""
import sys
from pathlib import Path
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# Параметры
LARGE_TRADE_SIGMA = 2.0  # Крупная сделка = mean + 2σ

def create_1s_table(conn):
    """Создать таблицу для 1-секундных баров (если не существует)."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS web.agg_trades_1s (
        id BIGSERIAL PRIMARY KEY,
        signal_analysis_id INTEGER NOT NULL REFERENCES web.signal_analysis(id) ON DELETE CASCADE,
        pair_symbol VARCHAR(20) NOT NULL,
        second_ts BIGINT NOT NULL,  -- Unix timestamp в секундах
        
        -- OHLC
        open_price NUMERIC(20, 8) NOT NULL,
        high_price NUMERIC(20, 8) NOT NULL,
        low_price NUMERIC(20, 8) NOT NULL,
        close_price NUMERIC(20, 8) NOT NULL,
        
        -- Volume
        buy_volume NUMERIC(20, 8) NOT NULL DEFAULT 0,
        sell_volume NUMERIC(20, 8) NOT NULL DEFAULT 0,
        delta NUMERIC(20, 8) NOT NULL DEFAULT 0,
        
        -- Large Trades
        large_buy_count INTEGER NOT NULL DEFAULT 0,
        large_sell_count INTEGER NOT NULL DEFAULT 0,
        
        -- Trade Count
        trade_count INTEGER NOT NULL DEFAULT 0,
        
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_agg_trades_1s_signal 
        ON web.agg_trades_1s (signal_analysis_id, second_ts);
    CREATE INDEX IF NOT EXISTS idx_agg_trades_1s_symbol 
        ON web.agg_trades_1s (pair_symbol, second_ts);
    """
    
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    print("✅ Таблица web.agg_trades_1s готова")

def get_signals_to_process(conn, limit=None):
    """Получить сигналы, которые ещё не обработаны в 1s."""
    query = """
        SELECT DISTINCT at.signal_analysis_id, at.pair_symbol, sa.signal_timestamp
        FROM web.agg_trades at
        JOIN web.signal_analysis sa ON sa.id = at.signal_analysis_id
        LEFT JOIN web.agg_trades_1s a1s ON a1s.signal_analysis_id = at.signal_analysis_id
        WHERE a1s.id IS NULL
        ORDER BY at.signal_analysis_id
    """
    if limit:
        query += f" LIMIT {limit}"
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)
        return cur.fetchall()

def aggregate_signal_to_1s(conn, signal_id: int, pair_symbol: str, large_trade_sigma: float = 2.0):
    """
    Агрегировать aggTrades для одного сигнала в 1-секундные бары.
    
    Args:
        large_trade_sigma: Сколько стандартных отклонений от среднего 
                          считать "крупной сделкой" (по умолчанию 2σ)
    """
    # Получаем все трейды для сигнала
    query = """
        SELECT 
            transact_time / 1000 as second_ts,  -- мс -> секунды
            price,
            quantity,
            is_buyer_maker
        FROM web.agg_trades
        WHERE signal_analysis_id = %s
        ORDER BY transact_time
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (signal_id,))
        trades = cur.fetchall()
    
    if not trades:
        return 0
    
    # Вычисляем динамический порог для крупных сделок
    # Используем USD-объём каждой сделки
    usd_values = [float(t[1]) * float(t[2]) for t in trades]
    
    import statistics
    if len(usd_values) > 10:
        mean_usd = statistics.mean(usd_values)
        stdev_usd = statistics.stdev(usd_values)
        large_threshold = mean_usd + (large_trade_sigma * stdev_usd)
    else:
        # Мало данных - используем простой множитель
        large_threshold = statistics.median(usd_values) * 5
    
    # Группируем по секундам
    bars = {}
    
    for trade in trades:
        second_ts = int(trade[0])
        price = float(trade[1])
        qty = float(trade[2])
        is_buyer_maker = trade[3]
        
        # USD объём
        usd_value = price * qty
        is_large = usd_value > large_threshold
        
        if second_ts not in bars:
            bars[second_ts] = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'buy_volume': 0,
                'sell_volume': 0,
                'large_buy': 0,
                'large_sell': 0,
                'count': 0
            }
        
        bar = bars[second_ts]
        bar['high'] = max(bar['high'], price)
        bar['low'] = min(bar['low'], price)
        bar['close'] = price
        bar['count'] += 1
        
        if is_buyer_maker:
            # Taker = Seller
            bar['sell_volume'] += qty
            if is_large:
                bar['large_sell'] += 1
        else:
            # Taker = Buyer
            bar['buy_volume'] += qty
            if is_large:
                bar['large_buy'] += 1
    
    # Вставляем в БД
    insert_sql = """
        INSERT INTO web.agg_trades_1s (
            signal_analysis_id, pair_symbol, second_ts,
            open_price, high_price, low_price, close_price,
            buy_volume, sell_volume, delta,
            large_buy_count, large_sell_count, trade_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    rows = []
    for second_ts, bar in bars.items():
        delta = bar['buy_volume'] - bar['sell_volume']
        rows.append((
            signal_id, pair_symbol, second_ts,
            bar['open'], bar['high'], bar['low'], bar['close'],
            bar['buy_volume'], bar['sell_volume'], delta,
            bar['large_buy'], bar['large_sell'], bar['count']
        ))
    
    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
    
    return len(rows)

def prepare_delta_data(limit=None, create_table=False):
    """
    Главная функция: агрегировать все aggTrades в 1-секундные бары.
    """
    print("🚀 Подготовка Delta Data (1-секундные бары)")
    print(f"   Порог крупной сделки: mean + {LARGE_TRADE_SIGMA}σ (динамически)")
    print("-" * 60)
    
    try:
        with get_db_connection() as conn:
            if create_table:
                create_1s_table(conn)
            
            signals = get_signals_to_process(conn, limit=limit)
            
            if not signals:
                print("✅ Все сигналы уже обработаны")
                return
            
            print(f"Найдено сигналов для обработки: {len(signals)}")
            print("-" * 60)
            
            total_bars = 0
            start_time = datetime.now()
            
            for i, sig in enumerate(signals, 1):
                signal_id = sig['signal_analysis_id']
                pair_symbol = sig['pair_symbol']
                
                # Прогресс с временем
                elapsed = (datetime.now() - start_time).total_seconds()
                avg_time = elapsed / i if i > 1 else 0
                remaining = avg_time * (len(signals) - i)
                
                print(f"[{i}/{len(signals)}] {pair_symbol:<15} (signal #{signal_id})...", end=' ', flush=True)
                
                bars_count = aggregate_signal_to_1s(conn, signal_id, pair_symbol)
                conn.commit()
                
                total_bars += bars_count
                print(f"✅ {bars_count:,} баров | ETA: {int(remaining)}s")
            
            print("\n" + "=" * 60)
            print(f"📊 Итого: {total_bars} 1-секундных баров создано")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Агрегация aggTrades в 1-секундные бары')
    parser.add_argument('--limit', type=int, default=None, help='Лимит сигналов')
    parser.add_argument('--create-table', action='store_true', help='Создать таблицу (нужны права)')
    
    args = parser.parse_args()
    
    prepare_delta_data(limit=args.limit, create_table=args.create_table)
