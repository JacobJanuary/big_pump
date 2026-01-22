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

# Suppress warnings BEFORE importing google
import warnings
warnings.simplefilter('ignore', FutureWarning)

print("🚀 Starting Gemini Analysis...") # Debug print

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
# User requested Pro model
MODEL_NAME = "gemini-3-pro-preview" 

DATA_FILE = Path(__file__).parent.parent / 'analysis_results' / 'llm_market_data.txt'

PROMPT = """
ROLE: Expert Quantitative Analyst & Crypto Market Maker.
TASK: Analyze the attached 1-minute candle data to reverse-engineer the "Winning Algorithm" with a focus on EXITs and RE-ENTRIES.

CONTEXT:
- Normalized data (1.0 = Listing Open).
- "WINNER" = Massive gains (e.g. SKR +300%).
- "LOSER" = Pump & Bleed.
- Column: DeltaRatio = BuyVol / SellVol.

OBJECTIVE:
1.  **Identify Entry Triggers** (as before: Accumulation Phasing, Delta Spikes).
2.  **OPTIMIZE EXITS & FREQUENCY:** (New Focus).
    - Compare "Diamond Hands" (Buy & Hold until trend break) vs "Scalping" (Sell pumps, re-buy dips).
    - Analyze the Winner charts: Do they pump in a straight line or are there deep pullbacks offering multiple entries?

ANALYSIS GUIDELINES (Chain of Thought):

1.  **Phase 1-2 (Entry):**
    - Confirm previous findings: Does DeltaRatio > 1.0 predict the pump?
    - Define the *exact* moment of entry (Trigger).

2.  **Phase 3: EXECUTION STRATEGY (The new part):**
    - **Single vs Multi-Entry:** 
      - If we sell at +15%, do we miss the +300% move? 
      - Or does the price offer re-entry at the breakout level?
      - CALCULATION: Look at SKR and ZKP. Would a "Sell at +20%, Re-buy at -5% pullback" strategy outperform pure Holding?
    - **Trailing Stop:**
      - How deep are the "shakeouts" on the way up? 
      - If we use a tight 5% Trailing Stop, do we get shaken out before the God Candle?
      - Recommend an optimal *dynamic* Trailing Stop (e.g., based on ATR or VWAP).

3.  **Phase 4: INVOLIDATION SPEED:**
    - When a "Loser" fails, does it happen instantly (1-min candle dump) or slowly?
    - Do we need a "Panic Sell" rule if Delta drops below 0.5 instantly?

OUTPUT FORMAT:

## 1. WINNER STRUCTURE
- "Winners usually pump X% then retrace Y%."
- "The God Candle usually lasts Z minutes."

## 2. STRATEGY COMPARISON
- "Buy & Hold" Score: [1-10]
- "Active Scalping" Score: [1-10]
- Recommendation: [Choose one]

## 3. THE ALGORITHM (Updated)
Define functions for a Python bot.

- **check_entry(candle)**:
  - (Include Delta, VWAP conditions)
  
- **check_exit(current_pnl, duration)**:
  - **TAKE_PROFIT**: Should we sell half at +X%?
  - **TRAILING_STOP**: Define the formula (e.g. `High - (3 * ATR)`).
  - **PANIC_SELL**: If Delta < 0.5 AND Price < Entry...
  
- **check_re_entry(exit_price)**:
  - If we sold, when do we buy back? (e.g. "Bounce off VWAP with Delta > 1").

Be precise. Quantify the "Shakeout Tolerance" (how much drawdown must we survive to catch the 300% pump?).
Here is the data:
"""

def list_available_models():
    print("\n🔍 Listing available Gemini models:")
    try:
        for m in genai.list_models():
            if 'gemini' in m.name:
                print(f" - {m.name}")
    except Exception as e:
        print(f"Error listing models: {e}")

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
        
        # Save to file
        out_path = Path(__file__).parent.parent / 'analysis_results' / 'gemini_strategy.md'
        with open(out_path, 'w') as f:
            f.write(response.text)
        print(f"\n✅ Strategy saved to: {out_path}")
        
    except Exception as e:
        print(f"\n❌ API Error: {e}")
        if "404" in str(e) or "not found" in str(e).lower():
            print(f"⚠️ Model '{MODEL_NAME}' not found.")
            list_available_models()
            print("\n💡 Please update MODEL_NAME in the script to one of the above.")


if __name__ == "__main__":
    main()
