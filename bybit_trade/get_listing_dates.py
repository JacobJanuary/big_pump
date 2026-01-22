#!/usr/bin/env python3
"""
Get listing dates for Bybit Token Splash tokens.
Uses public.bybit.com to find the earliest trading date for each token.
"""

import json
import re
import requests
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKENS_FILE = Path(__file__).parent / "tokens.json"
OUTPUT_FILE = Path(__file__).parent / "tokens_with_dates.json"
RECENT_TOKENS_FILE = Path(__file__).parent / "recent_listings.json"

BASE_URL = "https://public.bybit.com/spot/"
CUTOFF_DAYS = 93  # ~3 months


def get_listing_date(symbol: str) -> dict:
    """
    Get the earliest trading date for a symbol from Bybit public data.
    Returns dict with symbol, listing_date, pair, and status.
    """
    # Try common quote currencies
    pairs_to_try = [f"{symbol}USDT", f"{symbol}USDC"]
    
    for pair in pairs_to_try:
        url = f"{BASE_URL}{pair}/"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                # Parse file list to find earliest date
                # Files are named: PAIR_YYYY-MM-DD.csv.gz
                dates = re.findall(r'(\d{4}-\d{2}-\d{2})\.csv\.gz', resp.text)
                if dates:
                    # Sort dates and get the earliest
                    dates.sort()
                    earliest_date = dates[0]
                    return {
                        "symbol": symbol,
                        "pair": pair,
                        "listing_date": earliest_date,
                        "status": "found"
                    }
        except Exception as e:
            pass
    
    return {
        "symbol": symbol,
        "pair": None,
        "listing_date": None,
        "status": "not_found"
    }


def main():
    print("[START] Fetching listing dates from Bybit public data")
    
    # Load tokens
    with open(TOKENS_FILE) as f:
        tokens = json.load(f)
    
    symbols = [t["symbol"] for t in tokens]
    print(f"[INFO] Processing {len(symbols)} tokens...")
    
    results = []
    
    # Use thread pool for parallel requests
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_listing_date, s): s for s in symbols}
        
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            
            status = "✓" if result["status"] == "found" else "✗"
            date_str = result["listing_date"] or "N/A"
            print(f"[{i}/{len(symbols)}] {status} {result['symbol']}: {date_str}")
    
    # Sort by symbol
    results.sort(key=lambda x: x["symbol"])
    
    # Save all results
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"[SAVED] All tokens with dates: {OUTPUT_FILE}")
    
    # Filter recent listings (< 93 days old)
    today = datetime.now()
    cutoff_date = today - timedelta(days=CUTOFF_DAYS)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")
    
    recent = []
    for r in results:
        if r["listing_date"]:
            if r["listing_date"] >= cutoff_str:
                recent.append(r)
    
    # Sort by listing date (newest first)
    recent.sort(key=lambda x: x["listing_date"], reverse=True)
    
    with open(RECENT_TOKENS_FILE, 'w') as f:
        json.dump(recent, f, indent=2)
    
    print(f"\n[RESULT] Found {len(recent)} tokens listed in last {CUTOFF_DAYS} days")
    print(f"[SAVED] Recent listings: {RECENT_TOKENS_FILE}")
    
    # Summary
    found = sum(1 for r in results if r["status"] == "found")
    not_found = sum(1 for r in results if r["status"] == "not_found")
    print(f"\n[SUMMARY]")
    print(f"  Total tokens: {len(symbols)}")
    print(f"  Found on Bybit: {found}")
    print(f"  Not found: {not_found}")
    print(f"  Recent listings (< {CUTOFF_DAYS} days): {len(recent)}")
    
    if recent:
        print(f"\n[RECENT TOKENS]:")
        for r in recent[:20]:  # Show first 20
            print(f"  {r['listing_date']}: {r['symbol']} ({r['pair']})")
        if len(recent) > 20:
            print(f"  ... and {len(recent) - 20} more")


if __name__ == "__main__":
    main()
