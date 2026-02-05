# db_batch_utils.py – вспомогательный модуль для пакетных запросов к PostgreSQL

"""Utility functions to reduce load on the PostgreSQL server.

* `fetch_bars_batch(conn, signal_ids)` – получает 1‑секундные бары для
  списка `signal_ids` одним запросом (использует ANY(%s)).
* `batch_execute(conn, sql, params_iter, batch_size=1000)` – выполняет
  INSERT/UPDATE в пакетах, используя `execute_batch` из psycopg2/psycopg3.

Эти функции предназначены для использования в многопроцессном
оптимизаторе, где каждый процесс имеет своё соединение.
"""

from typing import List, Dict, Iterable, Tuple
import itertools

# Мы будем использовать функцию `get_db_connection` из pump_analysis_lib
# чтобы не дублировать параметры подключения.
from pump_analysis_lib import get_db_connection

# ---------------------------------------------------------------------------
# 1. Пакетная загрузка баров
# ---------------------------------------------------------------------------

def fetch_bars_batch(conn, signal_ids: List[int]):
    """Возвращает словарь `{signal_id: [(ts, price, delta, placeholder, large_buy, large_sell), ...]}`.

    Запрос делается одним `SELECT ... WHERE signal_analysis_id = ANY(%s)`.  
    Результаты сортируются по `signal_analysis_id, second_ts`.
    """
    if not signal_ids:
        return {}

    # Optimized query: Join with signal_analysis to get entry_time
    # and limit fetched bars to [entry_time, entry_time + 2 hours]
    # This prevents fetching weeks of data for a single signal.
    # IMPORTANT: Use entry_time (signal + 17 min), NOT signal_timestamp!
    sql = """
        SELECT t.signal_analysis_id, t.second_ts, t.close_price, t.delta,
               t.large_buy_count, t.large_sell_count
        FROM web.agg_trades_1s t
        JOIN web.signal_analysis s ON s.id = t.signal_analysis_id
        WHERE t.signal_analysis_id = ANY(%s)
          AND t.second_ts >= EXTRACT(EPOCH FROM s.entry_time)::bigint
          AND t.second_ts <= (EXTRACT(EPOCH FROM s.entry_time)::bigint + 7200)
        ORDER BY t.signal_analysis_id, t.second_ts
    """
    with conn.cursor() as cur:
        cur.execute(sql, (signal_ids,))
        rows = cur.fetchall()

    # Группируем по signal_id
    result: Dict[int, List[Tuple[int, float, float, float, int, int]]] = {}
    for sid, ts, price, delta, buy_cnt, sell_cnt in rows:
        # Insert placeholder 0.0 for the unused fourth field expected by run_strategy
        result.setdefault(sid, []).append((ts, float(price), float(delta), 0.0, int(buy_cnt), int(sell_cnt)))
    return result


def fetch_bars_batch_extended(conn, signal_ids: List[int], max_seconds: int = 75600):
    """Fetch bars following optimize_unified.py approach.
    
    Key insight: optimize_unified.py fetches ALL bars for a signal without SQL time filters,
    as the bars in agg_trades_1s are already pre-aggregated per signal. Time filtering
    happens in Python simulation, not in SQL.
    
    This approach is MUCH faster because it uses a simple index lookup on signal_analysis_id.
    """
    if not signal_ids:
        return {}

    result: Dict[int, List[Tuple[int, float, float, float, int, int, float, float]]] = {}
    chunk_size = 50  # Match optimize_unified.py chunk size
    
    print(f"Fetching bars for {len(signal_ids)} signals...")
    
    # SIMPLIFIED QUERY matching optimize_unified.py (no time filter!)
    sql = """
        SELECT signal_analysis_id, second_ts, close_price, delta,
               large_buy_count, large_sell_count,
               COALESCE(buy_volume, 0), COALESCE(sell_volume, 0)
        FROM web.agg_trades_1s
        WHERE signal_analysis_id = ANY(%s)
        ORDER BY signal_analysis_id, second_ts
    """
    
    with conn.cursor() as cur:
        for i in range(0, len(signal_ids), chunk_size):
            chunk = signal_ids[i : i + chunk_size]
            print(f"   Fetching chunk {i+1}-{min(i+chunk_size, len(signal_ids))}...", end='\r')
            
            cur.execute(sql, (chunk,))
            rows = cur.fetchall()
            
            # Bar tuple: (ts, price, delta, 0.0, large_buy, large_sell, buy_volume, sell_volume)
            for sid, ts, price, delta, buy_cnt, sell_cnt, buy_vol, sell_vol in rows:
                result.setdefault(sid, []).append((ts, float(price), float(delta), 0.0, int(buy_cnt), int(sell_cnt), float(buy_vol), float(sell_vol)))
                
    print(f"\n   Data fetch complete. {len(result)} signals have data.")
    return result

# ---------------------------------------------------------------------------
# 2. Пакетный execute (INSERT/UPDATE)
# ---------------------------------------------------------------------------

def batch_execute(conn, sql: str, params_iter: Iterable[Tuple], batch_size: int = 1000) -> int:
    """Выполняет `sql` для каждой группы параметров из `params_iter` пакетами.

    Возвращает общее количество выполненных пакетов.
    """
    # Попытка импортировать execute_batch из psycopg2/psycopg3
    try:
        from psycopg2.extras import execute_batch  # type: ignore
    except Exception:
        # fallback – простая петля
        def execute_batch(cur, sql, args_list):
            for args in args_list:
                cur.execute(sql, args)

    total_batches = 0
    with conn.cursor() as cur:
        iterator = iter(params_iter)
        while True:
            chunk = list(itertools.islice(iterator, batch_size))
            if not chunk:
                break
            execute_batch(cur, sql, chunk)
            total_batches += 1
    conn.commit()
    return total_batches

# ---------------------------------------------------------------------------
# 3. Helper для получения соединения (каждый процесс вызывает эту функцию)
# ---------------------------------------------------------------------------

def get_connection():
    """Возвращает новое соединение к базе данных.
    Используется в воркерах, чтобы каждый процесс имел своё соединение.
    """
    return get_db_connection()

# ---------------------------------------------------------------------------
# 4. Пример использования (не исполняется при импорте)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    conn = get_connection()
    sample_ids = [1, 2, 3]
    bars = fetch_bars_batch(conn, sample_ids)
    print(f"Fetched bars for {len(bars)} signals")
    conn.close()
