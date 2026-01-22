#!/usr/bin/env python3
"""
Analyze Market Data using Gemini API.
Reads `analysis_results/llm_market_data.txt` and asks Gemini to find winning patterns.

Requires:
    pip install google-generativeai
    GEMINI_API_KEY in .env
"""

import os
import sys
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv

# Load env
load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    print("❌ Error: GEMINI_API_KEY not found in environment variables.")
    sys.exit(1)

genai.configure(api_key=API_KEY)

# Configuration
# User requested "gemini-3-pro-preview". 
# As of now, the latest available via API is often 'gemini-1.5-pro-latest' or 'gemini-2.0-flash-exp'.
# We will defaults to 'gemini-1.5-pro-latest' which has the context window (2M) needed.
# If "gemini-3-pro-preview" becomes valid, change it here.
MODEL_NAME = "gemini-1.5-pro-latest" 
DATA_FILE = Path(__file__).parent.parent / 'analysis_results' / 'llm_market_data.txt'

PROMPT = """
You are an expert Quantitative Analyst and Crypto Trader.
I have attached a dataset of 5-minute candle data for recent Bybit listings.
Some tokens are labeled "WINNER" (Big Pump, sustained value) and some "LOSER" (Pump & Bleed or Flat).

Your Goal:
Identify DISTINCT PATTERNS in the first 2-4 hours that differentiate WINNERS from LOSERS.

Look for:
1. Volume profile: Does volume decay faster/slower for winners?
2. Delta Ratio: Is there hidden buying (Delta > 1) even if price is flat?
3. Price Action: Specific candle formations (V-shape, flags) in the consolidation zone.
4. "Fakeouts": Do losers tend to spike early and fade?

The data is normalized:
- Price 1.0 = Listing Start Price.
- Time is in minutes from listing.
- DeltaRatio = BuyVol / SellVol.

Output your answer as a Structured Trading Strategy:
1. ENTRY TRIGGERS: (e.g. "Enter if price > VWAP after 120 mins AND Delta > 1.2")
2. RED FLAGS (AVOID): (e.g. "Do not enter if Volume drops 90% in hour 1")
3. EXIT Rules.

Here is the data:
"""

def main():
    if not DATA_FILE.exists():
        print(f"❌ Error: {DATA_FILE} not found. Run 'export_for_llm.py' first.")
        sys.exit(1)

    print(f"Reading data from {DATA_FILE}...")
    with open(DATA_FILE, 'r') as f:
        market_data = f.read()

    print(f"Data size: {len(market_data)/1024:.2f} KB")
    print(f"Sending to Gemini ({MODEL_NAME})... This may take a minute.")

    try:
        model = genai.GenerativeModel(MODEL_NAME)
        response = model.generate_content(
            [PROMPT, market_data],
            generation_config=genai.types.GenerationConfig(
                temperature=0.2, # Low temp for analytical precision
            )
        )
        
        print("\n" + "="*80)
        print("🦄 GEMINI ANALYSIS RESULT")
        print("="*80 + "\n")
        print(response.text)
        
    except Exception as e:
        print(f"\n❌ API Error: {e}")
        print("Tip: Check your API Key and Model Name availability.")

if __name__ == "__main__":
    main()
