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

    # Optimized query: Join with signal_analysis to get signal timestamp
    # and limit fetched bars to [signal_ts, signal_ts + 2 hours]
    # This prevents fetching weeks of data for a single signal.
    sql = """
        SELECT t.signal_analysis_id, t.second_ts, t.close_price, t.delta,
               t.large_buy_count, t.large_sell_count
        FROM web.agg_trades_1s t
        JOIN web.signal_analysis s ON s.id = t.signal_analysis_id
        WHERE t.signal_analysis_id = ANY(%s)
          AND t.second_ts >= EXTRACT(EPOCH FROM s.signal_timestamp)::bigint
          AND t.second_ts <= (EXTRACT(EPOCH FROM s.signal_timestamp)::bigint + 7200)
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
