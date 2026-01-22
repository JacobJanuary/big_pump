#!/usr/bin/env python3
"""
Fetch Bybit historical trades for recent token listings.
Downloads first 3 days of trading data after listing from public.bybit.com.
"""

import gzip
import csv
import io
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir / 'scripts_v3'))
sys.path.append(str(parent_dir / 'config'))

# Lazy import for DB connection (avoid import error in dry-run)
def get_db_connection():
    from pump_analysis_lib import get_db_connection as _get_db_connection
    return _get_db_connection()

# Configuration
BASE_URL = "https://public.bybit.com/spot"
DATA_DIR = current_dir / "data"
LISTINGS_FILE = current_dir / "recent_listings.json"
DAYS_TO_FETCH = 3
MAX_WORKERS = 5


def load_listings():
    """Load recent listings from JSON file."""
    with open(LISTINGS_FILE) as f:
        return json.load(f)


def get_dates_to_fetch(listing_date_str: str, days: int = DAYS_TO_FETCH):
    """Get list of dates to fetch starting from listing date."""
    listing_date = datetime.strptime(listing_date_str, "%Y-%m-%d").date()
    dates = []
    for i in range(days):
        day = listing_date + timedelta(days=i)
        # Don't fetch future dates
        if day <= datetime.now().date():
            dates.append(day.strftime("%Y-%m-%d"))
    return dates


def download_daily_file(pair: str, date: str) -> Path | None:
    """Download daily trades CSV.gz file."""
    filename = f"{pair}_{date}.csv.gz"
    url = f"{BASE_URL}/{pair}/{filename}"
    
    # Create cache directory
    pair_dir = DATA_DIR / pair
    pair_dir.mkdir(parents=True, exist_ok=True)
    dest_path = pair_dir / filename
    
    # Check cache
    if dest_path.exists():
        return dest_path
    
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        
        with open(dest_path, 'wb') as f:
            f.write(response.content)
        
        return dest_path
        
    except Exception as e:
        print(f"    ❌ Error downloading {filename}: {e}")
        return None


def parse_trades(gz_path: Path) -> list:
    """Parse trades from gzipped CSV file."""
    trades = []
    
    try:
        with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                trades.append({
                    'trade_id': int(row['id']),
                    'timestamp': int(row['timestamp']),
                    'price': float(row['price']),
                    'volume': float(row['volume']),
                    'side': row['side']
                })
    except Exception as e:
        print(f"    ❌ Error parsing {gz_path}: {e}")
    
    return trades


def insert_listing(conn, symbol: str, pair: str, listing_date: str) -> int:
    """Insert or get listing ID."""
    with conn.cursor() as cur:
        # Try to get existing
        cur.execute(
            "SELECT id FROM bybit_trade.listings WHERE symbol = %s AND pair = %s",
            (symbol, pair)
        )
        row = cur.fetchone()
        if row:
            return row[0]
        
        # Insert new
        cur.execute(
            """INSERT INTO bybit_trade.listings (symbol, pair, listing_date)
               VALUES (%s, %s, %s) RETURNING id""",
            (symbol, pair, listing_date)
        )
        conn.commit()
        return cur.fetchone()[0]


def insert_trades(conn, listing_id: int, trades: list) -> int:
    """Bulk insert trades using COPY."""
    if not trades:
        return 0
    
    # Deduplicate trades by trade_id
    # Sometimes daily files might overlap slightly or contain duplicates
    unique_trades = {t['trade_id']: t for t in trades}.values()
    
    buffer = io.StringIO()
    for t in unique_trades:
        buffer.write(f"{listing_id}\t{t['trade_id']}\t{t['timestamp']}\t{t['price']}\t{t['volume']}\t{t['side']}\n")
    
    buffer.seek(0)
    
    with conn.cursor() as cur:
        # Delete existing trades for this listing to avoid duplicates
        cur.execute("DELETE FROM bybit_trade.trades WHERE listing_id = %s", (listing_id,))
        
        with cur.copy("COPY bybit_trade.trades (listing_id, trade_id, timestamp, price, volume, side) FROM STDIN") as copy:
            while data := buffer.read(65536):
                copy.write(data)
    
    return len(unique_trades)


def aggregate_candles_1s(conn, listing_id: int) -> int:
    """Aggregate trades into 1-second candles."""
    with conn.cursor() as cur:
        # Delete existing candles
        cur.execute("DELETE FROM bybit_trade.candles_1s WHERE listing_id = %s", (listing_id,))
        
        # Aggregate from trades
        cur.execute("""
            INSERT INTO bybit_trade.candles_1s 
                (listing_id, timestamp_s, open_price, high_price, low_price, close_price, 
                 volume, buy_volume, sell_volume, trade_count)
            SELECT 
                listing_id,
                (timestamp / 1000) as timestamp_s,
                (array_agg(price ORDER BY timestamp))[1] as open_price,
                MAX(price) as high_price,
                MIN(price) as low_price,
                (array_agg(price ORDER BY timestamp DESC))[1] as close_price,
                SUM(volume) as volume,
                SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) as buy_volume,
                SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) as sell_volume,
                COUNT(*) as trade_count
            FROM bybit_trade.trades
            WHERE listing_id = %s
            GROUP BY listing_id, (timestamp / 1000)
            ORDER BY timestamp_s
        """, (listing_id,))
        
        # Get count
        cur.execute("SELECT COUNT(*) FROM bybit_trade.candles_1s WHERE listing_id = %s", (listing_id,))
        return cur.fetchone()[0]


def update_listing_stats(conn, listing_id: int, trades_count: int, candles_count: int):
    """Update listing with stats."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bybit_trade.listings 
            SET data_fetched = TRUE, trades_count = %s, candles_count = %s
            WHERE id = %s
        """, (trades_count, candles_count, listing_id))


def process_listing(listing: dict, dry_run: bool = False) -> dict:
    """Process a single listing - download, parse, return stats."""
    symbol = listing['symbol']
    pair = listing['pair']
    listing_date = listing['listing_date']
    
    dates = get_dates_to_fetch(listing_date)
    all_trades = []
    
    for date in dates:
        gz_path = download_daily_file(pair, date)
        if gz_path:
            trades = parse_trades(gz_path)
            all_trades.extend(trades)
    
    return {
        'symbol': symbol,
        'pair': pair,
        'listing_date': listing_date,
        'trades': all_trades,
        'dates_fetched': len(dates)
    }


def fetch_bybit_trades(limit: int = None, dry_run: bool = False, workers: int = MAX_WORKERS):
    """Main function: fetch and store trades for all recent listings."""
    print("🚀 Bybit Historical Trades Fetcher")
    print(f"   Data directory: {DATA_DIR}")
    print(f"   Days to fetch: {DAYS_TO_FETCH}")
    print("-" * 60)
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    listings = load_listings()
    if limit:
        listings = listings[:limit]
    
    print(f"Processing {len(listings)} listings...")
    print("-" * 60)
    
    # Process sequentially to manage memory
    total_trades = 0
    total_candles = 0
    
    try:
        # Establish DB connection once (or per listing if needed, but once is better)
        conn = None 
        if not dry_run:
            conn = get_db_connection()
            
        for i, listing in enumerate(listings, 1):
            symbol = listing['symbol']
            print(f"[{i}/{len(listings)}] Processing {symbol}...")
            
            # 1. Download & Parse for this listing
            # Not using threads here to keep it simple and memory safe
            result = process_listing(listing)
            
            trades_count = len(result['trades'])
            print(f"    Downloaded {trades_count:,} trades ({result['dates_fetched']} days)")
            
            if dry_run:
                total_trades += trades_count
                # Clear memory
                del result
                continue
            
            # 2. Insert immediately
            try:
                # Ensure connection is alive
                if conn.closed:
                    conn = get_db_connection()
                    
                listing_id = insert_listing(conn, result['symbol'], result['pair'], result['listing_date'])
                
                inserted_trades = insert_trades(conn, listing_id, result['trades'])
                candles_count = aggregate_candles_1s(conn, listing_id)
                update_listing_stats(conn, listing_id, inserted_trades, candles_count)
                
                conn.commit()
                print(f"    ✅ Inserted: {inserted_trades:,} trades → {candles_count:,} candles_1s")
                
                total_trades += inserted_trades
                total_candles += candles_count
                
            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"    ❌ DB Error for {symbol}: {e}")
            
            # 3. Clear memory
            del result
            import gc
            gc.collect()
            
        print("\n" + "=" * 60)
        print(f"📊 Total:")
        print(f"   Listings: {len(listings)}")
        print(f"   Trades: {total_trades:,}")
        if not dry_run:
            print(f"   1s Candles: {total_candles:,}")
            
    except Exception as e:
        print(f"❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn and not conn.closed:
            conn.close()

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    
    # Load env vars
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='Fetch Bybit historical trades')
    parser.add_argument('--limit', type=int, default=None, help='Limit listings to process')
    parser.add_argument('--dry-run', action='store_true', help='Only download, do not insert to DB')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help='Parallel download workers (unused in new logic)')
    
    args = parser.parse_args()
    
    fetch_bybit_trades(limit=args.limit, dry_run=args.dry_run, workers=args.workers)
