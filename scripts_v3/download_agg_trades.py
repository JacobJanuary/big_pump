"""
Download Binance aggTrades data from data.binance.vision

Downloads monthly aggTrades ZIP files for symbols found in web.signal_analysis.
"""
import os
import sys
from pathlib import Path
import requests
import hashlib
from datetime import datetime, timezone
from collections import defaultdict

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

# Configuration
BASE_URL = "https://data.binance.vision"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "agg_trades"
FUTURES_PATH = "data/futures/um/monthly/aggTrades"

def get_required_downloads(conn, days=90):
    """
    Get unique (symbol, month) pairs from web.signal_analysis.
    
    Returns: list of tuples [(symbol, YYYY-MM), ...]
    """
    query = """
        SELECT DISTINCT 
            pair_symbol,
            TO_CHAR(signal_timestamp, 'YYYY-MM') as month
        FROM web.signal_analysis
        WHERE signal_timestamp >= NOW() - INTERVAL '%s days'
        ORDER BY pair_symbol, month
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (days,))
        rows = cur.fetchall()
    
    return [(row[0], row[1]) for row in rows]

def construct_download_url(symbol: str, month: str) -> tuple[str, str]:
    """
    Construct download URL and checksum URL for a given symbol and month.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        month: Month in YYYY-MM format
    
    Returns:
        (data_url, checksum_url)
    """
    filename = f"{symbol}-aggTrades-{month}.zip"
    data_url = f"{BASE_URL}/{FUTURES_PATH}/{symbol}/{filename}"
    checksum_url = f"{data_url}.CHECKSUM"
    return data_url, checksum_url

def download_file(url: str, dest_path: Path, chunk_size: int = 8192) -> bool:
    """
    Download a file from URL to destination path.
    
    Returns: True if successful, False otherwise.
    """
    try:
        print(f"  Downloading: {url}")
        response = requests.get(url, stream=True, timeout=300)
        
        if response.status_code == 404:
            print(f"  ⚠️ File not found (404): {url}")
            return False
        
        response.raise_for_status()
        
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    pct = (downloaded / total_size) * 100
                    print(f"    Progress: {pct:.1f}%", end='\r')
        
        print(f"  ✅ Downloaded: {dest_path.name} ({downloaded / 1024 / 1024:.1f} MB)")
        return True
        
    except requests.RequestException as e:
        print(f"  ❌ Download failed: {e}")
        return False

def verify_checksum(file_path: Path, checksum_url: str) -> bool:
    """
    Verify file checksum against Binance provided checksum.
    
    Returns: True if checksum matches, False otherwise.
    """
    try:
        # Download checksum file content
        response = requests.get(checksum_url, timeout=30)
        if response.status_code != 200:
            print(f"  ⚠️ Checksum file not found, skipping verification")
            return True  # Continue without verification
        
        # Parse checksum (format: "sha256sum  filename")
        checksum_line = response.text.strip()
        expected_checksum = checksum_line.split()[0]
        
        # Calculate actual checksum
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        actual_checksum = sha256_hash.hexdigest()
        
        if actual_checksum == expected_checksum:
            print(f"  ✅ Checksum verified")
            return True
        else:
            print(f"  ❌ Checksum mismatch!")
            print(f"     Expected: {expected_checksum}")
            print(f"     Actual:   {actual_checksum}")
            return False
            
    except Exception as e:
        print(f"  ⚠️ Checksum verification error: {e}")
        return True  # Continue without verification

def download_agg_trades(days: int = 90, symbols: list = None, force: bool = False):
    """
    Main function to download aggTrades data.
    
    Args:
        days: Look back N days for signals
        symbols: Optional list of specific symbols to download
        force: If True, re-download even if file exists
    """
    print(f"🚀 AggTrades Downloader")
    print(f"   Data directory: {DATA_DIR}")
    print(f"   Looking back: {days} days")
    print("-" * 60)
    
    # Create data directory
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        with get_db_connection() as conn:
            # Get required downloads from signal_analysis
            required = get_required_downloads(conn, days=days)
            
            if symbols:
                required = [(s, m) for s, m in required if s in symbols]
            
            if not required:
                print("No symbols/months to download.")
                return
            
            print(f"Found {len(required)} symbol-month combinations to download")
            print("-" * 60)
            
            # Group by symbol for cleaner output
            by_symbol = defaultdict(list)
            for symbol, month in required:
                by_symbol[symbol].append(month)
            
            downloaded = 0
            skipped = 0
            failed = 0
            
            for symbol, months in by_symbol.items():
                print(f"\n📊 {symbol} ({len(months)} months)")
                
                symbol_dir = DATA_DIR / symbol
                symbol_dir.mkdir(parents=True, exist_ok=True)
                
                for month in months:
                    filename = f"{symbol}-aggTrades-{month}.zip"
                    dest_path = symbol_dir / filename
                    
                    # Check if already exists
                    if dest_path.exists() and not force:
                        print(f"  ⏭️ Already exists: {filename}")
                        skipped += 1
                        continue
                    
                    # Download
                    data_url, checksum_url = construct_download_url(symbol, month)
                    
                    if download_file(data_url, dest_path):
                        if verify_checksum(dest_path, checksum_url):
                            downloaded += 1
                        else:
                            # Remove corrupted file
                            dest_path.unlink()
                            failed += 1
                    else:
                        failed += 1
            
            print("\n" + "=" * 60)
            print(f"📊 Download Summary:")
            print(f"   ✅ Downloaded: {downloaded}")
            print(f"   ⏭️ Skipped (exist): {skipped}")
            print(f"   ❌ Failed: {failed}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Download Binance aggTrades data')
    parser.add_argument('--days', type=int, default=90, 
                        help='Look back N days for signals (default: 90)')
    parser.add_argument('--symbols', nargs='+', 
                        help='Specific symbols to download (default: all from signal_analysis)')
    parser.add_argument('--force', action='store_true', 
                        help='Re-download even if file exists')
    
    args = parser.parse_args()
    
    download_agg_trades(days=args.days, symbols=args.symbols, force=args.force)
