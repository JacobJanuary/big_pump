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
import time

# Параметры
LARGE_TRADE_SIGMA = 2.0  # Крупная сделка = mean + 2σ
INSERT_BATCH_SIZE = 5000  # Вставка пачками
PAUSE_BETWEEN_SIGNALS = 0.5  # Пауза между сигналами (сек)

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
    print("   Получаю список сигналов...", end=' ', flush=True)
    
    # Оптимизированный запрос: NOT IN вместо LEFT JOIN
    query = """
        SELECT DISTINCT signal_analysis_id, pair_symbol
        FROM web.agg_trades
        WHERE signal_analysis_id NOT IN (
            SELECT DISTINCT signal_analysis_id FROM web.agg_trades_1s
        )
        ORDER BY signal_analysis_id
    """
    if limit:
        query += f" LIMIT {limit}"
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)
        result = cur.fetchall()
    
    print(f"найдено {len(result)}", flush=True)
    return result

def aggregate_signal_to_1s(conn, signal_id: int, pair_symbol: str, large_trade_sigma: float = 2.0):
    """
    Агрегировать aggTrades для одного сигнала в 1-секундные бары.
    
    Args:
        large_trade_sigma: Сколько стандартных отклонений от среднего 
                          считать "крупной сделкой" (по умолчанию 2σ)
    """
    # Получаем все трейды для сигнала
    print("loading...", end=' ', flush=True)
    
    query = """
        SELECT 
            transact_time / 1000 as second_ts,
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
    
    print(f"{len(trades):,} trades...", end=' ', flush=True)
    
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
    
    # Батчевая вставка
    with conn.cursor() as cur:
        for i in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[i:i + INSERT_BATCH_SIZE]
            cur.executemany(insert_sql, batch)
            conn.commit()
    
    return len(rows)

def process_signal(args):
    """Обработка одного сигнала (для multiprocessing)."""
    signal_id, pair_symbol, idx, total = args
    
    try:
        with get_db_connection() as conn:
            bars_count = aggregate_signal_to_1s(conn, signal_id, pair_symbol)
            print(f"[{idx}/{total}] {pair_symbol:<15} ✅ {bars_count:,} баров", flush=True)
            return bars_count
    except Exception as e:
        print(f"[{idx}/{total}] {pair_symbol:<15} ❌ {e}", flush=True)
        return 0

def prepare_delta_data(limit=None, create_table=False, workers=8):
    """
    Главная функция: агрегировать все aggTrades в 1-секундные бары.
    """
    print("🚀 Подготовка Delta Data (1-секундные бары)")
    print(f"   Порог крупной сделки: mean + {LARGE_TRADE_SIGMA}σ (динамически)")
    print(f"   Воркеров: {workers}")
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
        
        # Подготовка аргументов для пула
        args_list = [
            (sig['signal_analysis_id'], sig['pair_symbol'], i, len(signals))
            for i, sig in enumerate(signals, 1)
        ]
        
        # Параллельная обработка
        from multiprocessing import Pool
        
        start_time = datetime.now()
        
        with Pool(processes=workers) as pool:
            results = pool.map(process_signal, args_list)
        
        total_bars = sum(results)
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print(f"📊 Итого: {total_bars:,} 1-секундных баров создано")
        print(f"⏱️ Время: {elapsed:.1f} секунд")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Агрегация aggTrades в 1-секундные бары')
    parser.add_argument('--limit', type=int, default=None, help='Лимит сигналов')
    parser.add_argument('--workers', type=int, default=8, help='Кол-во параллельных процессов (default: 8)')
    parser.add_argument('--create-table', action='store_true', help='Создать таблицу (нужны права)')
    
    args = parser.parse_args()
    
    prepare_delta_data(limit=args.limit, create_table=args.create_table, workers=args.workers)

