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
TASK: Analyze the attached 1-minute candle data for recent Bybit Listings to reverse-engineer the "Winning Algorithm".

CONTEXT:
- You are provided with normalized trading data (~24h) for multiple tokens.
- Some are labeled "WINNER" (Massive gains, e.g. SKR +300%, ZKP +80%).
- Some are labeled "LOSER" (Pump & Bleed, Bull Traps).
- Legend: 
  - TimeOffset: Minutes since listing.
  - Price: Normalized (1.0 = Listing Open).
  - DeltaRatio = BuyVolume / SellVolume.

OBJECTIVE:
Find the *hidden structural differences* between Winners and Losers in the first 4 hours.
Focus specifically on the "Re-Accumulation Phase" (usually min 60 to min 240) where "Smart Money" positions itself before the second pump.

ANALYSIS GUIDELINES (Chain of Thought):

1. **Phase 1: The Initial Dump/Volatility (0-60 min)**
   - Do Winners drop differently than Losers? 
   - Look at Volume Decay: Do Winners see volume dry up faster?

2. **Phase 2: The Silent Accumulation (60-240 min)**
   - Compare DeltaRatio (Buy/Sell Vol). 
   - Hypothesize: Do Winners have DeltaRatio > 1.0 continuously while price is flat/down? (Limit orders absorbing sells).
   - Hypothesize: Do Losers have DeltaRatio < 0.9 (Distribution)?
   
3. **Phase 3: The Breakout Trigger**
   - What specifically signals the start of the "God Candle"? 
   - Is it a break of VWAP? A spike in Delta?

OUTPUT FORMAT (Strict):

## 1. DATA INSIGHTS
- "Winners Avg Delta (Hour 2-4): X.XX" vs "Losers Avg Delta: Y.YY"
- "Winners Volatility Profile: [Description]"

## 2. THE ALGORITHM (Python-Ready Logic)
Define exact conditions for a bot.
- **ENTRY_CONDITION**: 
  - IF time > 60 mins 
  - AND Price < X (drawdown limit)
  - AND DeltaRatio (last 1h) > Y.YY (Critical filter!)
  - AND Price crosses above [Level: VWAP / Session High]
  
- **INVALIDATION (Cut Loss)**:
  - IF DeltaRatio drops below Z.ZZ...

## 3. TRAP IDENTIFICATION
- How to spot a "Fake Winner" (a loser that triggered entry rules but failed)?
- Give one specific rule to filter these out.

Think deeply. Don't give generic advice ("buy low"). Give math.
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
