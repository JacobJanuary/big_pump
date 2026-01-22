#!/usr/bin/env python3
"""
Bybit Token Splash Scraper
Uses undetected-chromedriver to bypass Cloudflare protection.
Extracts token symbols from the last 3 months and saves to JSON.
"""

import json
import time
import re
from datetime import datetime, timedelta
from pathlib import Path

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup


URL = "https://www.bybit.com/en/trade/spot/token-splash"
OUTPUT_FILE = Path(__file__).parent / "tokens.json"
DEBUG_FILE = Path(__file__).parent / "debug_page.html"


def create_driver():
    """Create an undetected Chrome driver."""
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    
    driver = uc.Chrome(options=options, version_main=None)
    return driver


def extract_tokens_from_page(soup):
    """Extract token symbols from page soup."""
    found_symbols = set()
    
    # Pattern 1: "Deposit/Trade XXX to Earn" or "Trade XXX to Earn"
    text_content = soup.get_text()
    
    # Find all matches like "Trade TOKEN to Earn" or "Deposit/Trade TOKEN to Earn"
    patterns = [
        r'(?:Deposit/)?Trade\s+([A-Z0-9]{2,15})\s+to\s+Earn',  # Trade TOKEN to Earn
        r'Total\s+Prize\s+Pool\s+\(([A-Z0-9]{2,15})\)',        # Total Prize Pool (TOKEN)
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text_content)
        for m in matches:
            if len(m) >= 2:
                found_symbols.add(m)
    
    # Pattern 2: Look for EndProjectCard elements (specific to Bybit Token Splash)
    # The card structure contains token names in various places
    cards = soup.select('[class*="EndProjectCard"], [class*="ProjectCard"]')
    for card in cards:
        card_text = card.get_text()
        for pattern in patterns:
            matches = re.findall(pattern, card_text)
            for m in matches:
                if len(m) >= 2:
                    found_symbols.add(m)
    
    return found_symbols


def wait_for_content(driver, timeout=10):
    """Wait for project cards to load."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, '[class*="ProjectCard"]')) > 0
        )
        return True
    except:
        return False


def get_total_pages(soup):
    """Extract total number of pages from pagination."""
    # Look for pagination items like moly-pagination-item-44
    pagination_items = soup.select('[class*="moly-pagination-item-"]')
    max_page = 1
    for item in pagination_items:
        classes = item.get('class', [])
        for cls in classes:
            match = re.search(r'moly-pagination-item-(\d+)', cls)
            if match:
                page_num = int(match.group(1))
                if page_num > max_page:
                    max_page = page_num
    return max_page


def click_next_page(driver):
    """Click the 'Next' button in pagination."""
    try:
        next_btn = driver.find_element(By.CSS_SELECTOR, '.moly-pagination-next button:not([disabled])')
        if next_btn:
            # Scroll element into view
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
            time.sleep(0.5)
            
            try:
                next_btn.click()
            except:
                # Try JS click as fallback
                driver.execute_script("arguments[0].click();", next_btn)
            
            time.sleep(2)  # Wait for page to load
            return True
    except Exception as e:
        print(f"[WARN] Could not click next page: {e}")
    return False


def main():
    print(f"[START] Bybit Token Splash Scraper")
    print(f"[INFO] Target URL: {URL}")
    print(f"[INFO] Output file: {OUTPUT_FILE}")
    
    driver = None
    all_tokens = set()
    
    try:
        print("[INFO] Launching browser...")
        driver = create_driver()
        
        print("[INFO] Navigating to page...")
        driver.get(URL)
        
        # Wait for page to load (Cloudflare challenge may take a few seconds)
        print("[INFO] Waiting for page to load (Cloudflare bypass)...")
        time.sleep(10)  # Initial wait for Cloudflare
        
        # Wait for content
        wait_for_content(driver)
        time.sleep(3)
        
        # Get initial page and determine total pages
        soup = BeautifulSoup(driver.page_source, "html.parser")
        total_pages = get_total_pages(soup)
        print(f"[INFO] Found {total_pages} pages to scrape")
        
        # Save debug HTML
        with open(DEBUG_FILE, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        
        # Extract from first page
        tokens = extract_tokens_from_page(soup)
        all_tokens.update(tokens)
        print(f"[INFO] Page 1: found {len(tokens)} tokens (total: {len(all_tokens)})")
        
        # Iterate through all pages
        for page_num in range(2, total_pages + 1):
            print(f"[INFO] Navigating to page {page_num}...")
            
            if not click_next_page(driver):
                print(f"[WARN] Could not navigate to page {page_num}, stopping pagination")
                break
            
            # Wait for new content
            time.sleep(2)
            wait_for_content(driver)
            
            # Extract tokens
            soup = BeautifulSoup(driver.page_source, "html.parser")
            tokens = extract_tokens_from_page(soup)
            all_tokens.update(tokens)
            print(f"[INFO] Page {page_num}: found {len(tokens)} tokens (total: {len(all_tokens)})")
            
            # Small delay to avoid rate limiting
            time.sleep(1)
        
        # Prepare final token list
        token_list = [{"symbol": s} for s in sorted(all_tokens)]
        
        print(f"\n[RESULT] Total unique tokens found: {len(token_list)}")
        
        # Save to JSON
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(token_list, f, indent=2, ensure_ascii=False)
        
        print(f"[DONE] Saved to {OUTPUT_FILE}")
        
        # Print found tokens
        if token_list:
            print("\n[TOKENS FOUND]:")
            for t in token_list:
                print(f"  - {t['symbol']}")
        else:
            print("\n[WARN] No tokens found.")
        
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            print("[INFO] Closing browser...")
            driver.quit()


if __name__ == "__main__":
    main()
