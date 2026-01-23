#!/usr/bin/env python3
"""
Export 5-minute candle data for LLM Analysis.
Goal: Feed this to Gemini-1.5-Pro / Gemini-3-Preview to find hidden patterns.

Format:
SYMBOL | OUTCOME | TIME | OPEN | HIGH | LOW | CLOSE | VOL | DELTA_RATIO
...
"""

import sys
import warnings
from pathlib import Path
import pandas as pd
import numpy as np

# Add scripts_v3 to path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root / 'scripts_v3'))

from pump_analysis_lib import get_db_connection

warnings.simplefilter("ignore", UserWarning)

def get_outcome(df):
    """
    Classify based on 24h performance.
    Winner = Max Pump > 15% AND didn't close at -50%.
    Loser = Pumped and Bleeding or just Bleeding.
    """
    if df.empty: return "UNKNOWN"
    start_price = df.iloc[0]['open_price']
    max_price = df['high_price'].max()
    end_price = df.iloc[-1]['close_price']
    
    max_gain = (max_price - start_price) / start_price * 100
    end_change = (end_price - start_price) / start_price * 100
    
    if max_gain > 20 and end_change > -30:
        return "WINNER"
    elif max_gain < 10 and end_change < -10:
        return "LOSER_FLAT" # Never pumped
    else:
        return "LOSER_PUMP_DUMP" # Pumped but crashed

def load_and_resample(conn, listing_id):
    query = """
        SELECT timestamp_s, open_price, high_price, low_price, close_price, volume, buy_volume, sell_volume
        FROM bybit_trade.candles_1s
        WHERE listing_id = %s
        ORDER BY timestamp_s ASC
        LIMIT 86400
    """
    df = pd.read_sql(query, conn, params=(listing_id,))
    if df.empty: return df
    
    df['timestamp'] = pd.to_datetime(df['timestamp_s'], unit='s')
    df.set_index('timestamp', inplace=True)
    
    # Resample to 1 min (or keep as is since source is 1s, but we aggregate to reduce noise slightly if needed, 
    # but 1s to 1min is standard).
    df_1m = df.resample('1min').agg({
        'open_price': 'first',
        'high_price': 'max',
        'low_price': 'min',
        'close_price': 'last',
        'volume': 'sum',
        'buy_volume': 'sum',
        'sell_volume': 'sum'
    })
    
    # Calculate Delta Ratio per candle
    df_1m['delta_ratio'] = df_1m['buy_volume'] / df_1m['sell_volume'].replace(0, 1)
    
    return df_1m

def main():
    conn = get_db_connection()
    output_lines = []
    
    try:
        listings = pd.read_sql(
            "SELECT id, symbol FROM bybit_trade.listings WHERE data_fetched = TRUE ORDER BY listing_date DESC LIMIT 25",
            conn
        )
        
        output_lines.append("DATASET: Bybit Spot Listings (First 24h)")
        output_lines.append("INTERVAL: 1 Minute")
        output_lines.append("COLUMNS: TimeOffset(m), Open, High, Low, Close, Volume, DeltaRatio")
        output_lines.append("-" * 50)
        
        for _, row in listings.iterrows():
            lid, name = row['id'], row['symbol']
            df = load_and_resample(conn, lid)
            if df.empty: continue
            
            outcome = get_outcome(df)
            
            output_lines.append(f"\nTOKEN: {name}")
            output_lines.append(f"OUTCOME: {outcome}")
            output_lines.append("DATA:")
            
            start_time = df.index[0]
            
            for t, r in df.iterrows():
                offset_m = int((t - start_time).total_seconds() / 60)
                # Format to save tokens: Integers/Short floats
                # Price normalized to start = 1.0 (to make patterns comparable across tokens)
                start_p = df.iloc[0]['open_price']
                op = f"{r['open_price']/start_p:.3f}"
                hi = f"{r['high_price']/start_p:.3f}"
                lo = f"{r['low_price']/start_p:.3f}"
                cl = f"{r['close_price']/start_p:.3f}"
                vo = f"{int(r['volume'])}"
                dr = f"{r['delta_ratio']:.2f}"
                
                line = f"{offset_m},{op},{hi},{lo},{cl},{vo},{dr}"
                output_lines.append(line)
                
    finally:
        conn.close()
        
    # Write to file
    out_file = project_root / 'analysis_results' / 'llm_market_data.txt'
    out_file.parent.mkdir(exist_ok=True, parents=True)
    
    with open(out_file, 'w') as f:
        f.write("\n".join(output_lines))
        
    print(f"Data exported to {out_file}")
    print(f"Total Lines: {len(output_lines)}")
    print(f"Estimated Tokens: {len(' '.join(output_lines)) / 4:.0f}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
