"""
Загрузка Binance aggTrades (daily dumps) для Delta Reversal бэктестинга.

Скачивает daily ZIP файлы, фильтрует 48ч окно после сигнала, загружает в БД.
"""
import os
import sys
import zipfile
import csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import requests
import hashlib
import secrets

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# Configuration
BASE_URL = "https://data.binance.vision"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "agg_trades"
FUTURES_PATH = "data/futures/um/daily/aggTrades"

def get_signals_for_loading(conn, limit=None):
    """Получить сигналы из web.signal_analysis."""
    query = """
        SELECT 
            sa.id,
            sa.pair_symbol,
            sa.signal_timestamp,
            sa.entry_time
        FROM web.signal_analysis sa
        ORDER BY sa.signal_timestamp ASC
    """
    if limit:
        query += f" LIMIT {limit}"
    
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    
    return [{'id': r[0], 'pair_symbol': r[1], 'signal_timestamp': r[2], 'entry_time': r[3]} for r in rows]

def get_required_dates(signal_timestamp):
    """
    Определить какие дни нужно скачать для 48ч окна.
    
    Returns: list of date strings ['2025-01-01', '2025-01-02', '2025-01-03']
    """
    # Конвертируем в UTC если нужно
    if signal_timestamp.tzinfo is None:
        signal_timestamp = signal_timestamp.replace(tzinfo=timezone.utc)
    
    start_date = signal_timestamp.date()
    end_date = (signal_timestamp + timedelta(hours=48)).date()
    
    # Собираем все даты в диапазоне
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return dates

def download_daily_file(symbol: str, date: str) -> Path | None:
    """
    Скачать daily aggTrades ZIP файл.
    
    Returns: Path to downloaded file or None if failed.
    """
    filename = f"{symbol}-aggTrades-{date}.zip"
    url = f"{BASE_URL}/{FUTURES_PATH}/{symbol}/{filename}"
    
    # Создаем директорию
    symbol_dir = DATA_DIR / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    dest_path = symbol_dir / filename
    
    # Проверяем есть ли уже
    if dest_path.exists():
        print(f"    ⏭️ Уже скачан: {filename}")
        return dest_path
    
    try:
        print(f"    📥 Скачиваю: {filename}...", end=' ', flush=True)
        response = requests.get(url, stream=True, timeout=300)
        
        if response.status_code == 404:
            print(f"❌ Не найден")
            return None
        
        response.raise_for_status()
        
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        size_mb = dest_path.stat().st_size / 1024 / 1024
        print(f"✅ {size_mb:.1f} MB")
        return dest_path
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

def extract_and_filter_trades(zip_path: Path, start_ms: int, end_ms: int):
    """
    Распаковать ZIP и отфильтровать трейды по временному окну.
    
    CSV формат: agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
    
    Returns: list of trade dicts
    """
    trades = []
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # В архиве один CSV файл
            csv_filename = zf.namelist()[0]
            
            with zf.open(csv_filename) as f:
                # Читаем как текст
                import io
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.reader(text_file)
                
                # Пропускаем заголовок
                next(reader, None)
                
                for row in reader:
                    # agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
                    transact_time = int(row[5])
                    
                    # Фильтруем по времени
                    if start_ms <= transact_time <= end_ms:
                        trades.append({
                            'agg_trade_id': int(row[0]),
                            'price': float(row[1]),
                            'quantity': float(row[2]),
                            'transact_time': transact_time,
                            'is_buyer_maker': row[6].lower() == 'true'
                        })
    except Exception as e:
        print(f"    ❌ Ошибка чтения ZIP: {e}")
    
    return trades

def insert_trades(conn, signal_id: int, pair_symbol: str, trades: list):
    """Вставить трейды в web.agg_trades используя COPY (psycopg3)."""
    if not trades:
        return 0
    
    import io
    
    # Подготавливаем данные для COPY (TSV формат)
    buffer = io.StringIO()
    for t in trades:
        buffer.write(f"{signal_id}\t{pair_symbol}\t{t['agg_trade_id']}\t{t['price']}\t{t['quantity']}\t{t['transact_time']}\t{t['is_buyer_maker']}\n")
    
    buffer.seek(0)
    
    # psycopg3 COPY синтаксис
    with conn.cursor() as cur:
        with cur.copy("COPY web.agg_trades (signal_analysis_id, pair_symbol, agg_trade_id, price, quantity, transact_time, is_buyer_maker) FROM STDIN") as copy:
            while data := buffer.read(65536):
                copy.write(data)
    
    return len(trades)

def process_signal(sig):
    """
    Обработать один сигнал (для multiprocessing).
    Возвращает (signal_id, trades_list) или (signal_id, None) при ошибке.
    """
    signal_id = sig['id']
    symbol = sig['pair_symbol']
    signal_ts = sig['signal_timestamp']
    
    # Вычисляем временное окно (48ч после сигнала)
    if signal_ts.tzinfo is None:
        signal_ts = signal_ts.replace(tzinfo=timezone.utc)
    
    start_ms = int(signal_ts.timestamp() * 1000)
    end_ms = int((signal_ts + timedelta(hours=48)).timestamp() * 1000)
    
    # Определяем нужные даты
    dates = get_required_dates(signal_ts)
    
    # Создаем временную директорию
    TEMP_DIR = DATA_DIR / "temp_processing"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    
    # Генерируем уникальное имя для временного файла
    temp_filename = f"{signal_id}_{start_ms}_{secrets.token_hex(4)}.tsv.gz"
    temp_path = TEMP_DIR / temp_filename

    # Скачиваем файлы и пишем сразу в TSV (gz)
    # Используем gzip для экономии места и IO
    import gzip
    
    has_trades = False
    
    try:
        with gzip.open(temp_path, 'wt', encoding='utf-8') as tsv_out:
            for date in dates:
                zip_path = download_daily_file(symbol, date)
                if zip_path:
                    # Читаем ZIP и фильтруем без загрузки всего в RAM
                    # (Прямой стриминг из ZIP в GZ занял бы меньше памяти)
                    trades = extract_and_filter_trades(zip_path, start_ms, end_ms)
                    
                    if trades:
                        has_trades = True
                        for t in trades:
                            # signal_id, pair_symbol, agg_trade_id, price, quantity, transact_time, is_buyer_maker
                            tsv_out.write(f"{signal_id}\t{symbol}\t{t['agg_trade_id']}\t{t['price']}\t{t['quantity']}\t{t['transact_time']}\t{t['is_buyer_maker']}\n")
                            
        if not has_trades:
            if temp_path.exists():
                os.remove(temp_path)
            return (signal_id, symbol, None)
            
        return (signal_id, symbol, str(temp_path))
        
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        if temp_path.exists():
            os.remove(temp_path)
        return (signal_id, symbol, None)

def insert_trades_from_file(conn, file_path):
    """Вставить трейды из временного файла."""
    import gzip
    
    if not file_path or not os.path.exists(file_path):
        return 0
        
    inserted = 0
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            with conn.cursor() as cur:
                with cur.copy("COPY web.agg_trades (signal_analysis_id, pair_symbol, agg_trade_id, price, quantity, transact_time, is_buyer_maker) FROM STDIN") as copy:
                    while data := f.read(65536):
                        copy.write(data)
                        # Estimate count? No easy way with COPY FROM STDIN without counting lines first.
                        # We'll just trust COPY.
        
        # Удаляем файл после успешной загрузки
        os.remove(file_path)
        return 1 # Возвращаем 1 как "успех" (количество строк неизвестно без чтения)
        
    except Exception as e:
        print(f"Error inserting from {file_path}: {e}")
        return 0

def fetch_agg_trades(limit=None, dry_run=False, workers=12):
    """
    Главная функция: скачать и загрузить aggTrades для всех сигналов.
    """
    from multiprocessing import Pool
    
    # Чистим temp
    TEMP_DIR = DATA_DIR / "temp_processing"
    if TEMP_DIR.exists():
        import shutil
        shutil.rmtree(TEMP_DIR)
    
    print("🚀 Загрузка AggTrades (Daily Dumps) - 48ч окно")
    print(f"   Директория: {DATA_DIR}")
    print(f"   Воркеров: {workers}")
    print(f"   Temp Dir: {TEMP_DIR}")
    print("-" * 60)
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        with get_db_connection() as conn:
            signals = get_signals_for_loading(conn, limit=limit)
            
            if not signals:
                print("✅ Все сигналы уже обработаны")
                return
            
            # Загружаем ID сигналов, для которых уже есть aggTrades
            print("Checking existing aggTrades...")
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT signal_analysis_id FROM web.agg_trades")
                existing_ids = set(row[0] for row in cur.fetchall())
            print(f"Found {len(existing_ids)} signals with existing aggTrades")
            
            # Фильтруем — оставляем только те, что ещё не загружены
            signals_to_process = [s for s in signals if s['id'] not in existing_ids]
            skipped_count = len(signals) - len(signals_to_process)
            
            if not signals_to_process:
                print(f"✅ Все {len(signals)} сигналов уже имеют aggTrades. Ничего делать не нужно.")
                return
            
            print(f"Найдено сигналов: {len(signals)} (пропущено: {skipped_count}, к обработке: {len(signals_to_process)})")
            print("-" * 60)
            
            processed = 0
            failed = 0
            
            # Параллельная загрузка
            with Pool(processes=workers) as pool:
                # Используем imap_unordered для потоковой обработки по мере завершения
                for i, (signal_id, symbol, result_path) in enumerate(pool.imap_unordered(process_signal, signals_to_process), 1):
                    
                    if result_path is None:
                        # print(f"[{i}/{len(signals)}] {symbol} - ⚠️ Нет данных", end='\r')
                        failed += 1
                        continue
                    
                    if not dry_run:
                        # В Main Process: загружаем файл в БД
                        insert_trades_from_file(conn, result_path)
                        conn.commit()
                        print(f"[{i}/{len(signals_to_process)}] {symbol} - ✅ Загружено")
                    else:
                        print(f"[{i}/{len(signals_to_process)}] {symbol} - [DRY RUN] Файл сохранен: {result_path}")
                        # В dry-run не удаляем файл или удаляем? Удалим чтобы мусор не копить.
                        if os.path.exists(result_path):
                            os.remove(result_path)
                    
                    processed += 1
            
            print("\n" + "=" * 60)
            print(f"📊 Итого:")
            print(f"   Обработано сигналов: {processed}")
            print(f"   Пропущено: {failed}")
            
    except Exception as e:
        try:
             # Освобождаем пул если была критическая ошибка
             pool.terminate()
        except:
             pass
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Загрузка Binance aggTrades')
    parser.add_argument('--limit', type=int, default=None, help='Лимит сигналов')
    parser.add_argument('--dry-run', action='store_true', help='Только показать что будет сделано')
    parser.add_argument('--workers', type=int, default=12, help='Количество параллельных воркеров')
    
    args = parser.parse_args()
    
    fetch_agg_trades(limit=args.limit, dry_run=args.dry_run, workers=args.workers)

