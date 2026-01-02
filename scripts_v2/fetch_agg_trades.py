"""
Загрузка Binance aggTrades (daily dumps) для Delta Reversal бэктестинга.

Скачивает daily ZIP файлы, фильтрует 24ч окно после сигнала, загружает в БД.
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
    Определить какие дни нужно скачать для 24ч окна.
    
    Returns: list of date strings ['2025-01-01', '2025-01-02']
    """
    # Конвертируем в UTC если нужно
    if signal_timestamp.tzinfo is None:
        signal_timestamp = signal_timestamp.replace(tzinfo=timezone.utc)
    
    start_date = signal_timestamp.date()
    end_date = (signal_timestamp + timedelta(hours=24)).date()
    
    dates = [start_date.strftime('%Y-%m-%d')]
    if end_date != start_date:
        dates.append(end_date.strftime('%Y-%m-%d'))
    
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
    """Вставить трейды в web.agg_trades."""
    if not trades:
        return 0
    
    with conn.cursor() as cur:
        # Используем executemany для скорости
        cur.executemany("""
            INSERT INTO web.agg_trades 
                (signal_analysis_id, pair_symbol, agg_trade_id, price, quantity, transact_time, is_buyer_maker)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [
            (signal_id, pair_symbol, t['agg_trade_id'], t['price'], t['quantity'], t['transact_time'], t['is_buyer_maker'])
            for t in trades
        ])
    
    return len(trades)

def fetch_agg_trades(limit=None, dry_run=False):
    """
    Главная функция: скачать и загрузить aggTrades для всех сигналов.
    """
    print("🚀 Загрузка AggTrades (Daily Dumps)")
    print(f"   Директория: {DATA_DIR}")
    print("-" * 60)
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        with get_db_connection() as conn:
            signals = get_signals_for_loading(conn, limit=limit)
            
            if not signals:
                print("✅ Все сигналы уже обработаны")
                return
            
            print(f"Найдено сигналов для обработки: {len(signals)}")
            print("-" * 60)
            
            total_trades = 0
            processed = 0
            failed = 0
            
            for i, sig in enumerate(signals, 1):
                signal_id = sig['id']
                symbol = sig['pair_symbol']
                signal_ts = sig['signal_timestamp']
                
                print(f"\n[{i}/{len(signals)}] {symbol} @ {signal_ts}")
                
                # Вычисляем временное окно (24ч после сигнала)
                if signal_ts.tzinfo is None:
                    signal_ts = signal_ts.replace(tzinfo=timezone.utc)
                
                start_ms = int(signal_ts.timestamp() * 1000)
                end_ms = int((signal_ts + timedelta(hours=24)).timestamp() * 1000)
                
                # Определяем нужные даты
                dates = get_required_dates(signal_ts)
                print(f"    Даты для загрузки: {dates}")
                
                # Скачиваем файлы
                all_trades = []
                for date in dates:
                    zip_path = download_daily_file(symbol, date)
                    if zip_path:
                        trades = extract_and_filter_trades(zip_path, start_ms, end_ms)
                        all_trades.extend(trades)
                        print(f"    📊 {date}: {len(trades)} трейдов в окне")
                
                if not all_trades:
                    print(f"    ⚠️ Нет трейдов для этого сигнала")
                    failed += 1
                    continue
                
                # Вставляем в БД
                if not dry_run:
                    inserted = insert_trades(conn, signal_id, symbol, all_trades)
                    conn.commit()
                    total_trades += inserted
                    print(f"    ✅ Загружено: {inserted} трейдов")
                else:
                    print(f"    🔍 [DRY RUN] Было бы загружено: {len(all_trades)} трейдов")
                
                processed += 1
            
            print("\n" + "=" * 60)
            print(f"📊 Итого:")
            print(f"   Обработано сигналов: {processed}")
            print(f"   Пропущено: {failed}")
            print(f"   Загружено трейдов: {total_trades}")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Загрузка Binance aggTrades')
    parser.add_argument('--limit', type=int, default=None, help='Лимит сигналов')
    parser.add_argument('--dry-run', action='store_true', help='Только показать что будет сделано')
    
    args = parser.parse_args()
    
    fetch_agg_trades(limit=args.limit, dry_run=args.dry_run)
