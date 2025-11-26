import sys
import os
from pathlib import Path
import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Setup path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
config_dir = project_root / 'config'
sys.path.append(str(config_dir))

import settings

# Explicitly load .env
env_path = project_root / '.env'
load_dotenv(env_path)

# Constants
SCORE_THRESHOLD = 250
ENTRY_OFFSET_MINUTES = 17

async def fetch_candles(conn, pair_id, start_ts, end_ts):
    query = """
        SELECT timestamp, open, high, low, close
        FROM public.market_candles
        WHERE trading_pair_id = $1
          AND timestamp BETWEEN $2 AND $3
        ORDER BY timestamp ASC
    """
    return await conn.fetch(query, pair_id, start_ts, end_ts)

async def analyze_overheating(days=30):
    print(f"🔥 Analyzing Overheating vs Performance for last {days} days...")
    
    db_config = settings.DATABASE
    # Use readonly user if possible, or default from settings
    conn_params = {
        'host': db_config['host'],
        'port': db_config['port'],
        'user': 'elcrypto_readonly',
        'password': 'LohNeMamont@)11',
        'database': db_config['dbname'],
    }
    
    try:
        conn = await asyncpg.connect(**conn_params)
        
        # 1. Fetch Signals
        print("Fetching signals...")
        signals_query = f"""
            SELECT 
                sh.id, 
                sh.trading_pair_id,
                tp.pair_symbol,
                sh.timestamp as signal_ts, 
                sh.total_score
            FROM fas_v2.scoring_history sh
            JOIN public.trading_pairs tp ON sh.trading_pair_id = tp.id
            WHERE sh.total_score > {SCORE_THRESHOLD}
              AND sh.timestamp >= NOW() - INTERVAL '{days} days'
              AND tp.contract_type_id = 1
            ORDER BY sh.timestamp DESC
        """
        signals = await conn.fetch(signals_query)
        print(f"Found {len(signals)} signals.")
        
        results = []
        
        for i, sig in enumerate(signals):
            if i % 50 == 0:
                print(f"Processing {i}/{len(signals)}...")
                
            signal_ts = sig['signal_ts']
            entry_time = signal_ts + timedelta(minutes=ENTRY_OFFSET_MINUTES)
            
            # Fetch candles for Pre-Entry Analysis (1h before entry) AND Post-Entry (24h after)
            # Range: Entry - 65 min TO Entry + 24h
            fetch_start = entry_time - timedelta(minutes=65)
            fetch_end = entry_time + timedelta(hours=24)
            
            rows = await fetch_candles(conn, sig['trading_pair_id'], fetch_start, fetch_end)
            if not rows:
                continue
                
            # Convert to DataFrame for easier lookup
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close'])
            df.set_index('timestamp', inplace=True)
            
            # --- 1. Calculate Overheating ---
            # We need price at Entry Time
            # Find candle closest to entry_time (should be exact if 1m candles exist)
            try:
                # Use 'asof' or direct lookup. Since index is unique, direct is best.
                # But timestamps might be slightly off? Assume 1m alignment.
                # Let's use nearest match within tolerance
                idx_entry = df.index.get_indexer([entry_time], method='nearest')[0]
                entry_candle = df.iloc[idx_entry]
                
                # Check if it's close enough (within 2 mins)
                if abs((entry_candle.name - entry_time).total_seconds()) > 120:
                    continue
                    
                entry_price = float(entry_candle['open']) # Entry at Open of the minute
                
                # Helper to get price N mins before
                def get_price_change(minutes_before):
                    target_ts = entry_time - timedelta(minutes=minutes_before)
                    idx = df.index.get_indexer([target_ts], method='nearest')[0]
                    candle = df.iloc[idx]
                    if abs((candle.name - target_ts).total_seconds()) > 120:
                        return None
                    past_price = float(candle['open']) # Compare Open to Open? Or Close to Open?
                    # Usually "change over last 5 mins" is (Current - Price 5m ago).
                    return (entry_price - past_price) / past_price * 100

                oh_5m = get_price_change(5)
                oh_15m = get_price_change(15)
                oh_1h = get_price_change(60)
                
            except Exception as e:
                # print(f"Error calc overheating for {sig['pair_symbol']}: {e}")
                continue
                
            # --- 2. Calculate Performance ---
            # Look at candles AFTER entry_time
            post_entry_df = df[df.index > entry_time]
            if post_entry_df.empty:
                continue
                
            # Simple simulation: Max Profit and Max Drawdown in next 24h
            # We assume Long position
            min_low = post_entry_df['low'].min()
            max_high = post_entry_df['high'].max()
            
            max_drawdown = (float(min_low) - entry_price) / entry_price * 100
            max_profit = (float(max_high) - entry_price) / entry_price * 100
            
            # Time to peak (optional, but good for context)
            peak_time = post_entry_df['high'].idxmax()
            hours_to_peak = (peak_time - entry_time).total_seconds() / 3600
            
            results.append({
                'symbol': sig['pair_symbol'],
                'oh_5m': oh_5m,
                'oh_15m': oh_15m,
                'oh_1h': oh_1h,
                'max_profit': max_profit,
                'max_drawdown': max_drawdown,
                'hours_to_peak': hours_to_peak
            })
            
        # --- 3. Analysis & Reporting ---
        df_res = pd.DataFrame(results)
        
        print("\n" + "="*80)
        print(f"📊 OVERHEATING ANALYSIS REPORT ({len(df_res)} signals)")
        print("="*80)
        
        # Define buckets
        buckets = [-np.inf, -2, 0, 1, 2, 3, 5, np.inf]
        labels = ['< -2%', '-2% to 0%', '0% to 1%', '1% to 2%', '2% to 3%', '3% to 5%', '> 5%']
        
        for interval, col_name in [('5 min', 'oh_5m'), ('15 min', 'oh_15m'), ('1 Hour', 'oh_1h')]:
            print(f"\n🔹 Interval: {interval} Before Entry")
            print("-" * 95)
            print(f"{'Range':<15} | {'Count':<5} | {'Avg Profit':<10} | {'Avg DD':<10} | {'WinRate (>1%)':<12} | {'Risk/Reward':<10}")
            print("-" * 95)
            
            df_res['bucket'] = pd.cut(df_res[col_name], bins=buckets, labels=labels)
            grouped = df_res.groupby('bucket', observed=False)
            
            for name, group in grouped:
                if group.empty:
                    continue
                count = len(group)
                avg_profit = group['max_profit'].mean()
                avg_dd = group['max_drawdown'].mean()
                win_rate = (group['max_profit'] > 1.0).mean() * 100
                rr_ratio = abs(avg_profit / avg_dd) if avg_dd != 0 else 0
                
                print(f"{name:<15} | {count:<5} | {avg_profit:>9.2f}% | {avg_dd:>9.2f}% | {win_rate:>11.1f}% | {rr_ratio:>9.2f}")
                
            # Correlation
            corr_profit = df_res[col_name].corr(df_res['max_profit'])
            corr_dd = df_res[col_name].corr(df_res['max_drawdown'])
            print(f"\nCorrelation with Profit: {corr_profit:.2f}")
            print(f"Correlation with Drawdown: {corr_dd:.2f}")
            print("." * 95)

        await conn.close()

    except Exception as e:
        print(f"Critical Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(analyze_overheating())
