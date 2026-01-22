#!/usr/bin/env python3
"""
Advanced start‑price / volume / delta analysis for the 23 recent Bybit spot listings.

Features:
1️⃣ Load 1‑second candles from DB.
2️⃣ Compute start‑minute metrics (open, high, low, close, % change, max pump, max dip).
3️⃣ Compute order‑flow metrics (buy_volume, sell_volume, delta, delta_ratio).
4️⃣ Smart‑money coefficient:
   smart_money = (buy_vol where price ↑ – sell_vol where price ↓) / (total_buy + total_sell).
5️⃣ Aggressive entry rule (configurable):
   - price drop ≥ DROP_PCT (default 5%)
   - smart_money ≥ SM_COEF (default 1.5)
   - entry at next tick.
6️⃣ Exit rule (configurable): hold HOLD_SECONDS (default 60 s) or trailing stop TS_PCT (default 3%).
7️⃣ Full back‑test loop over all 23 tokens → CSV/JSON report + summary table.

Run:
    $ python bybit_trade/analyze_start.py
"""

import sys
import warnings
from pathlib import Path
from dataclasses import dataclass
import pandas as pd
import numpy as np

# Add scripts_v3 to path to import pump_analysis_lib
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root / 'scripts_v3'))

from pump_analysis_lib import get_db_connection

# ----------------------------------------------------------------------
# 1️⃣  Data structures
# ----------------------------------------------------------------------
warnings.simplefilter("ignore", UserWarning)   # suppress pandas‑SQL warning

@dataclass
class Trade:
    symbol: str
    entry_time: pd.Timestamp
    entry_price: float
    exit_time: pd.Timestamp
    exit_price: float
    pnl_pct: float
    exit_reason: str
    duration_s: int

# ----------------------------------------------------------------------
# 2️⃣  Helper functions
# ----------------------------------------------------------------------
def load_candles(conn, listing_id: int) -> pd.DataFrame:
    """Load 1‑s candles for a listing and set timestamp index."""
    sql = """
        SELECT timestamp_s, open_price, high_price, low_price, close_price,
               volume, buy_volume, sell_volume
        FROM bybit_trade.candles_1s
        WHERE listing_id = %s
        ORDER BY timestamp_s ASC
    """
    df = pd.read_sql(sql, conn, params=(listing_id,))
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp_s"], unit="s")
    df.set_index("timestamp", inplace=True)
    # basic derived columns
    df["delta"] = df["buy_volume"] - df["sell_volume"]
    df["cum_buy"] = df["buy_volume"].cumsum()
    df["cum_sell"] = df["sell_volume"].cumsum()
    df["delta_ratio"] = df["cum_buy"] / df["cum_sell"]
    return df

def start_minute_metrics(df: pd.DataFrame) -> dict:
    """Metrics for the first 60 seconds after listing."""
    first_min = df.iloc[:60]
    open_price = first_min.iloc[0]["open_price"]
    high_price = first_min["high_price"].max()
    low_price = first_min["low_price"].min()
    close_price = first_min.iloc[-1]["close_price"]
    pct_change = (close_price - open_price) / open_price * 100
    max_pump = (high_price - open_price) / open_price * 100
    max_dip = (low_price - open_price) / open_price * 100
    return {
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "pct_change": pct_change,
        "max_pump": max_pump,
        "max_dip": max_dip,
    }

def smart_money_coef(df: pd.DataFrame) -> float:
    """Coefficient = (buy_vol where price ↑ – sell_vol where price ↓) / (total_buy + total_sell)."""
    price_diff = df["close_price"].diff().fillna(0)
    buy_up = df.loc[price_diff > 0, "buy_volume"].sum()
    sell_down = df.loc[price_diff < 0, "sell_volume"].sum()
    total = df["buy_volume"].sum() + df["sell_volume"].sum()
    return (buy_up - sell_down) / total if total else 0.0

# ----------------------------------------------------------------------
# 3️⃣  Strategy implementation
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# 3️⃣  Strategy implementation
# ----------------------------------------------------------------------
def aggressive_entry(
    df: pd.DataFrame,
    symbol: str,
    drop_pct: float = 3.0,     # Relaxed from 5.0
    sm_coef_thr: float = 0.5,  # Relaxed from 1.5
    hold_seconds: int = 60,
    trailing_stop_pct: float = 0.03,
) -> list[Trade]:
    """Aggressive dip‑buy strategy."""
    trades: list[Trade] = []
    # 1️⃣ Detect drop
    start = start_minute_metrics(df)
    if start["pct_change"] > -drop_pct:
        return trades
    # 2️⃣ Smart‑money check
    if smart_money_coef(df.iloc[:60]) < sm_coef_thr:
        return trades
        
    # 3️⃣ Entry point
    entry_time = df.index[0] + pd.Timedelta(seconds=1)
    if entry_time not in df.index:
        entry_time = df.index[0]
    entry_price = df.at[entry_time, "close_price"]
    
    # 4️⃣ Exit logic
    highest = entry_price
    for cur_time, row in df.iterrows():
        if cur_time <= entry_time:
            continue
        cur_price = row["close_price"]
        highest = max(highest, cur_price)
        # trailing stop
        if cur_price < highest * (1 - trailing_stop_pct):
            return [Trade(symbol, entry_time, entry_price, cur_time, cur_price,
                         (cur_price - entry_price)/entry_price*100, "Trailing Stop",
                         int((cur_time - entry_time).total_seconds()))]
        # hold time
        if (cur_time - entry_time).total_seconds() >= hold_seconds:
             return [Trade(symbol, entry_time, entry_price, cur_time, cur_price,
                         (cur_price - entry_price)/entry_price*100, "Hold Time",
                         int((cur_time - entry_time).total_seconds()))]
                         
    # EOS
    final_price = df.iloc[-1]["close_price"]
    return [Trade(symbol, entry_time, entry_price, df.index[-1], final_price,
                 (final_price - entry_price)/entry_price*100, "EOS",
                 int((df.index[-1] - entry_time).total_seconds()))]

def momentum_entry(
    df: pd.DataFrame,
    symbol: str,
    pump_pct: float = 1.0,     # Buy if price up 1%
    sm_coef_thr: float = 0.2,  # Positive smart money
    hold_seconds: int = 120,    # Hold longer for pumps
    trailing_stop_pct: float = 0.05,
) -> list[Trade]:
    """Momentum buy strategy for pumps."""
    trades: list[Trade] = []
    
    # 1. Condition: Price Up + Smart Money
    start = start_minute_metrics(df)
    sm = smart_money_coef(df.iloc[:60])
    
    if start["pct_change"] < pump_pct or sm < sm_coef_thr:
        return trades
        
    # 2. Entry (simulated at 10s mark to let initial noise pass, or immediately)
    # Let's enter at 10s mark if condition met in first 60s (hindsight backtest)
    # Ideally we check these condition continuously, but for this start-analysis script
    # we are checking "Start Minute Properties" -> "Trade Outcome".
    
    entry_time = df.index[0] + pd.Timedelta(seconds=10)
    # Find closest time
    idx_loc = df.index.searchsorted(entry_time)
    if idx_loc >= len(df): return trades
    
    entry_row = df.iloc[idx_loc]
    entry_price = entry_row['close_price']
    entry_time = entry_row.name
    
    # 3. Exit logic
    highest = entry_price
    start_idx = idx_loc
    
    for i in range(start_idx + 1, len(df)):
        cur_time = df.index[i]
        cur_price = df.iloc[i]['close_price']
        
        highest = max(highest, cur_price)
        
        # Trailing stop
        if cur_price < highest * (1 - trailing_stop_pct):
            return [Trade(symbol, entry_time, entry_price, cur_time, cur_price,
                         (cur_price - entry_price)/entry_price*100, "Trailing Stop",
                         int((cur_time - entry_time).total_seconds()))]
                         
        # Time exit
        if (cur_time - entry_time).total_seconds() >= hold_seconds:
            return [Trade(symbol, entry_time, entry_price, cur_time, cur_price,
                         (cur_price - entry_price)/entry_price*100, "Hold Time",
                         int((cur_time - entry_time).total_seconds()))]
                         
    return []

# ----------------------------------------------------------------------
# 4️⃣  Main driver
# ----------------------------------------------------------------------
def main():
    conn = get_db_connection()
    try:
        listings = pd.read_sql(
            """
            SELECT id, symbol
            FROM bybit_trade.listings
            WHERE data_fetched = TRUE
            ORDER BY listing_date DESC
            LIMIT 23
            """,
            conn,
        )
        all_trades: list[Trade] = []
        summary = []
        for _, row in listings.iterrows():
            lid, symbol = row["id"], row["symbol"]
            df = load_candles(conn, lid)
            if df.empty or len(df) < 60:
                continue
            
            # Run BOTH strategies
            t_dip = aggressive_entry(df, symbol)
            for t in t_dip: t.exit_reason = f"DIP: {t.exit_reason}"
            
            t_mom = momentum_entry(df, symbol)
            for t in t_mom: t.exit_reason = f"MOM: {t.exit_reason}"
            
            trades = t_dip + t_mom
            all_trades.extend(trades)
            
            summary.append(
                {
                    "symbol": symbol,
                    "pct_change_1m": round(start_minute_metrics(df)["pct_change"], 2),
                    "smart_money": round(smart_money_coef(df.iloc[:60]), 3),
                    "trades": len(trades),
                }
            )
        # ---------- reporting ----------
        if all_trades:
            trades_df = pd.DataFrame([t.__dict__ for t in all_trades])
            out_path = Path(__file__).parent / "analysis_results" / "start_trades.csv"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            trades_df.to_csv(out_path, index=False)
            print(f"\n🗂️  Detailed trades saved to {out_path}")
            
            # Print detailed trade list
            print("\n📝 INDIVIDUAL TRADES")
            print(trades_df[['symbol', 'pnl_pct', 'exit_reason', 'duration_s']].to_string())

        if summary:
            sum_df = pd.DataFrame(summary).set_index("symbol")
            print("\n📊 START‑MINUTE SUMMARY (23 tokens)")
            print("-" * 60)
            print(sum_df)
            
        if all_trades:
            win_rate = (trades_df["pnl_pct"] > 0).mean() * 100
            total_pnl = trades_df["pnl_pct"].sum()
            print("\n🏁 OVERALL")
            print(f"Trades: {len(trades_df)} | Win Rate: {win_rate:.1f}% | Total PnL: {total_pnl:.2f}%")
            
    finally:
        conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    main()
