#!/usr/bin/env python3
"""
Backtest Emulator - Source of Truth
Strict implementation of logic audited in `audit_optimize_unified.md`.

Dependencies:
- composite_strategy.json
- scripts_v2/pump_analysis_lib.py (DB connection)
- scripts_v2/db_batch_utils.py (Bar fetching)
"""

import sys
import json
import csv
import math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional, Any, NamedTuple

# Add parent directory to path for imports
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
sys.path.append(str(PROJECT_ROOT))

# Import utilities
try:
    from scripts_v2.pump_analysis_lib import get_db_connection
    from scripts_v2.db_batch_utils import fetch_bars_batch_extended
except ImportError:
    print("CRITICAL ERROR: Could not import scripts_v2 modules. Run from project root?")
    sys.exit(1)

# ==============================================================================
# AUDITED CONSTANTS (From logic audit)
# ==============================================================================
ENTRY_DELAY_MINUTES = 17
POSITION_SIZE_USD = 100.0  # Fixed capital per trade for PnL calculation

# Default fallbacks if missing from strategy JSON (Parity with optimizer)
DEFAULT_BASE_ACTIVATION = 10.0
DEFAULT_BASE_CALLBACK = 4.0
DEFAULT_BASE_REENTRY_DROP = 5.0
DEFAULT_BASE_COOLDOWN = 300
DEFAULT_COMMISSION_PCT = 0.04

# ==============================================================================
# DATA STRUCTURES
# ==============================================================================

class TradeResult(NamedTuple):
    entry_ts: int
    exit_ts: int
    entry_price: float
    exit_price: float
    pnl_raw_pct: float     # Price change %
    pnl_realized_pct: float # Post-leverage & commission
    exit_reason: str
    is_liquidated: bool

class SimulationResult(NamedTuple):
    signal_id: int
    symbol: str
    entry_time_ts: int     # Expected entry time
    actual_entry_price: float
    final_exit_ts: int     # When the pair becomes free
    final_exit_price: float
    total_pnl_pct: float   # Sum of all trades' PnL
    trade_count: int
    trades: List[TradeResult]

# ==============================================================================
# CONFIGURATION LOADER
# ==============================================================================

class StrategyConfig:
    def __init__(self, json_path: Path):
        self.rules = []
        self._load(json_path)

    def _load(self, path: Path):
        print(f"[CONFIG] Loading strategy from {path}")
        with open(path, "r") as f:
            data = json.load(f)
        
        for rule in data.get("rules", []):
            f = rule["filter"]
            s = rule["strategy"]
            self.rules.append({
                "range": (f.get("score_min", 0), f.get("score_max", 9999)),
                "filter_criteria": {
                    "rsi_min": f.get("rsi_min", 0),
                    "vol_min": f.get("vol_min", 0),
                    "oi_min": f.get("oi_min", 0),
                },
                "params": {
                    "leverage": s.get("leverage", 10),
                    "sl_pct": s.get("sl_pct", 10),
                    "delta_window": s.get("delta_window", 300),
                    "threshold_mult": s.get("threshold_mult", 1.0),
                    "base_activation": s.get("base_activation", DEFAULT_BASE_ACTIVATION),
                    "base_callback": s.get("base_callback", DEFAULT_BASE_CALLBACK),
                    "base_reentry_drop": s.get("base_reentry_drop", DEFAULT_BASE_REENTRY_DROP),
                    "base_cooldown": s.get("base_cooldown", DEFAULT_BASE_COOLDOWN),
                    "max_reentry_seconds": s.get("max_reentry_hours", 24) * 3600,
                    "max_position_seconds": s.get("max_position_hours", 24) * 3600,
                }
            })
        print(f"[CONFIG] Loaded {len(self.rules)} strategy rules.")

    def get_strategy(self, score: int, rsi: float, vol: float, oi: float) -> Tuple[Optional[Dict], Optional[str]]:
        """Find matching strategy for signal. Returns (params, rejection_reason)."""
        for rule in self.rules:
            r_min, r_max = rule["range"]
            if r_min <= score < r_max:
                # Check filters
                f = rule["filter_criteria"]
                if rsi < f["rsi_min"]: return None, f"RSI {rsi:.1f} < {f['rsi_min']}"
                if vol < f["vol_min"]: return None, f"Vol {vol:.1f} < {f['vol_min']}"
                if oi < f["oi_min"]:   return None, f"OI {oi:.1f} < {f['oi_min']}"
                
                # Match found
                return rule["params"], None
        
        return None, f"Score {score} not in any range"

# ==============================================================================
# STRATEGY ENGINE
# ==============================================================================

class StrategyEmulator:
    @staticmethod
    def run_simulation(bars: List[tuple], params: Dict, start_timestamp: int) -> Optional[SimulationResult]:
        """
        Run trade simulation for a single signal.
        
        Args:
            bars: List of (ts, price, delta, _, large_buy, large_sell, ...)
            params: Strategy parameters
            start_timestamp: Unix timestamp when TRADING MUST START (Audit 4.1)
        """
        if not bars: return None

        # 1. Locate Entry Index (Audit 4.1)
        # We must find the first bar where ts >= start_timestamp
        entry_idx = -1
        for i, bar in enumerate(bars):
            if bar[0] >= start_timestamp:
                entry_idx = i
                break
        
        if entry_idx == -1:
            return None # No data after start time

        n = len(bars)
        
        # Precompute Delta Data (Audit 4.2 / 7.3) for performance
        cumsum_delta = [0.0] * (n + 1)
        cumsum_abs_delta = [0.0] * (n + 1)
        for i in range(n):
            d = bars[i][2]
            cumsum_delta[i+1] = cumsum_delta[i] + d
            cumsum_abs_delta[i+1] = cumsum_abs_delta[i] + abs(d)
            
        avg_delta_arr = [0.0] * n
        lookback = 100
        for i in range(n):
            lb = min(i, lookback)
            if lb > 0:
                avg_delta_arr[i] = (cumsum_abs_delta[i] - cumsum_abs_delta[i-lb]) / lb

        # State Variables
        trades: List[TradeResult] = []
        is_in_position = True
        
        # Initial Position (Audit 4.1)
        # "Open LONG position properties"
        current_entry_price = bars[entry_idx][1]
        current_entry_ts = bars[entry_idx][0]
        max_price = current_entry_price
        position_open_ts = current_entry_ts # Time of FIRST entry in sequence
        
        # Parameters extraction
        lev = params["leverage"]
        sl_pct = params["sl_pct"]
        activation = params["base_activation"]
        callback = params["base_callback"]
        delta_window = params["delta_window"]
        threshold_mult = params["threshold_mult"]
        cooldown = params["base_cooldown"]
        reentry_drop = params["base_reentry_drop"]
        max_pos_dur = params["max_position_seconds"]
        max_reentry_dur = params["max_reentry_seconds"]
        
        liquidation_pct = 100.0 / lev
        comm_cost = DEFAULT_COMMISSION_PCT * 2 * lev

        last_exit_ts = 0
        total_pnl = 0.0

        # Loop bars starting from entry
        for i in range(entry_idx, n):
            bar = bars[i]
            ts, price, delta, _, large_buy, large_sell = bar[0], bar[1], bar[2], bar[3], bar[4], bar[5]

            if is_in_position:
                # Update High Water Mark
                if price > max_price: max_price = price
                
                # Check metrics
                pnl_pct = (price - current_entry_price) / current_entry_price * 100
                drawdown = (max_price - price) / max_price * 100
                duration = ts - position_open_ts

                exit_reason = None
                
                # Priority 1: Max Duration (Audit 4.2.1)
                if max_pos_dur > 0 and duration >= max_pos_dur:
                    exit_reason = "TIMEOUT"
                    
                # Priority 2: Liquidation (Audit 4.2.2)
                elif pnl_pct <= -liquidation_pct:
                    exit_reason = "LIQUIDATION"
                    
                # Priority 3: Stop Loss (Audit 4.2.3)
                elif pnl_pct <= -sl_pct:
                    exit_reason = "STOP_LOSS"
                    
                # Priority 4: Trailing + Momentum (Audit 4.2.4)
                elif pnl_pct >= activation and drawdown >= callback:
                    # Check Momentum
                    # "If rolling_delta is NOT > threshold AND rolling_delta is NOT >= 0" -> Close
                    
                    start_win = max(0, i - delta_window)
                    actual_win = i - start_win
                    rolling_delta = cumsum_delta[i+1] - cumsum_delta[start_win]
                    
                    avg_delta = avg_delta_arr[i]
                    threshold = avg_delta * threshold_mult
                    
                    # Partial window scaling (Audit 7.5)
                    if actual_win < delta_window and delta_window > 0:
                        threshold *= (actual_win / delta_window)
                        
                    if not (rolling_delta > threshold) and not (rolling_delta >= 0):
                        exit_reason = "TRAILING_MOMENTUM"

                # EXECUTE EXIT
                if exit_reason:
                    is_liquidated = (exit_reason == "LIQUIDATION")
                    
                    if is_liquidated:
                        realized = -100.0
                    else:
                        raw_lev = pnl_pct * lev
                        realized = max(raw_lev, -100.0) - comm_cost
                    
                    trades.append(TradeResult(
                        current_entry_ts, ts, current_entry_price, price,
                        pnl_pct, realized, exit_reason, is_liquidated
                    ))
                    
                    total_pnl += realized
                    is_in_position = False
                    last_exit_ts = ts
                    # Do not continue loop, process re-entry on NEXT bar (or same if logic allows, typically next)
            
            else:
                # RE-ENTRY LOGIC (Audit 4.3)
                # Conditions:
                # 1. Window: current - signal_start <= max_reentry
                # 2. Cooldown: current - last_exit >= base
                # 3. Pullback: price < max_price AND drop >= base_reentry_drop
                # 4. Micro: delta > 0 AND large_buy > large_sell
                
                time_since_start = ts - position_open_ts # Use original position start? No, "signal_start_ts"
                # Audit 4.3.1 explicitly says "current_ts - signal_start_ts <= max_reentry_hours"
                # "signal_start_ts" refers to the initial entry time of the first position? 
                # Actually Audit 3.1 says "entry_time: Signal Timestamp + 17m".
                # Let's interpret "signal_start_ts" as the timestamp of the very first entry attempt.
                
                can_reenter = True
                if max_reentry_dur > 0 and (ts - bars[entry_idx][0]) > max_reentry_dur:
                    can_reenter = False
                
                if can_reenter:
                    if (ts - last_exit_ts) >= cooldown:
                        if price < max_price:
                            drop_pct = (max_price - price) / max_price * 100
                            if drop_pct >= reentry_drop:
                                if delta > 0 and large_buy > large_sell:
                                    # EXECUTE RE-ENTRY
                                    is_in_position = True
                                    current_entry_price = price
                                    current_entry_ts = ts
                                    # Reset max price (Audit 4.3 Action: "Reset entry_price, max_price")
                                    max_price = price
                                    # "Update position_entry_ts to current time"
                                    # Note: This affects Duration check for the NEW position.
                                    # But for Re-entry Window check, we still use initial signal start.
                                    position_open_ts = ts 
                else:
                    # Re-entry window closed, and we are not in position.
                    # Nothing more to do.
                    pass 

        # End of data cleanup
        if is_in_position:
            # Audit 4.2: "Force Close" at end of data? 
            # Usually treated as exit at current price.
            final_price = bars[-1][1]
            pnl_pct = (final_price - current_entry_price) / current_entry_price * 100
            
            # Check liquidation one last time
            if pnl_pct <= -liquidation_pct:
                realized = -100.0
                reason = "LIQUIDATION_EOF"
                is_liq = True
            else:
                realized = max(pnl_pct * lev, -100.0) - comm_cost
                reason = "EOF_CLOSE"
                is_liq = False
                
            trades.append(TradeResult(
                current_entry_ts, bars[-1][0], current_entry_price, final_price,
                pnl_pct, realized, reason, is_liq
            ))
            total_pnl += realized
            last_exit_ts = bars[-1][0]

        if not trades:
            return None

        return SimulationResult(
            signal_id=0, # Filled by caller
            symbol="",   # Filled by caller
            entry_time_ts=bars[entry_idx][0],
            actual_entry_price=trades[0].entry_price,
            final_exit_ts=last_exit_ts,
            final_exit_price=trades[-1].exit_price,
            total_pnl_pct=total_pnl,
            trade_count=len(trades),
            trades=trades
        )

# ==============================================================================
# MAIN BACKTESTER CLASS
# ==============================================================================

class Backtester:
    def __init__(self, db_conn, strategy_file: Path):
        self.conn = db_conn
        self.config = StrategyConfig(strategy_file)
        self.active_positions = {} # {symbol: free_at_timestamp} (Audit 5)
        self.results = []
        self.stats = {
            "processed": 0,
            "skipped_position": 0,
            "skipped_strategy": 0,
            "skipped_no_data": 0,
            "errors": 0
        }

    def fetch_signals(self) -> List[tuple]:
        """Fetch signals using EXACT query from optimize_unified.py"""
        print("[DATA] Fetching signals...")
        query = """
            SELECT sa.id, sa.pair_symbol, sa.signal_timestamp, sa.entry_time, sa.total_score,
                   i.rsi, i.volume_zscore, i.oi_delta_pct
            FROM web.signal_analysis AS sa
            JOIN fas_v2.indicators AS i ON (
                i.trading_pair_id = sa.trading_pair_id 
                AND i.timestamp = sa.signal_timestamp
                AND i.timeframe = '15m'
            )
            WHERE sa.total_score >= 100 AND sa.total_score < 950
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            
        print(f"[DATA] Fetched {len(rows)} signals.")
        # Sort by timestamp for Global Position Tracking (Audit 5.1)
        # Row: 0=id, 1=sym, 2=ts, 3=entry_time
        return sorted(rows, key=lambda x: x[2])

    def run(self, limit: int = 0):
        signals = self.fetch_signals()
        
        if limit > 0:
            print(f"[TEST] Limiting to first {limit} signals for verification.")
            signals = signals[:limit]
        
        # Process in batches to save memory
        BATCH_SIZE = 100
        total_signals = len(signals)
        
        for i in range(0, total_signals, BATCH_SIZE):
            batch = signals[i:i+BATCH_SIZE]
            signal_ids = [r[0] for r in batch]
            
            # Fetch bars for batch
            print(f"[Run] Processing batch {i//BATCH_SIZE + 1} ({len(batch)} signals)...")
            try:
                # max_seconds=172800 (48h) ensures coverage for max_reentry(24h) + max_hold(24h)
                # This significantly speeds up data loading compared to fetching generic large windows
                bars_map = fetch_bars_batch_extended(self.conn, signal_ids, max_seconds=172800)
            except Exception as e:
                print(f"[ERROR] Failed to fetch bars for batch: {e}")
                self.stats["errors"] += len(batch)
                continue

            for row in batch:
                self._process_signal(row, bars_map)
                
            self.stats["processed"] += len(batch)
            
        self._generate_report()

    def _process_signal(self, row, bars_map):
        sid, sym, ts, entry_time, score, rsi, vol, oi = row
        
        # 1. Global Position Check (Audit 5.2)
        # "If signal_timestamp < position_tracker_ts: SKIP SIGNAL"
        signal_ts_epoch = ts.timestamp()
        
        if sym in self.active_positions:
            free_at = self.active_positions[sym]
            if signal_ts_epoch < free_at:
                self.stats["skipped_position"] += 1
                return

        # 2. Strategy Lookup (Audit 3.2 / 6)
        params, reject_reason = self.config.get_strategy(score, rsi, vol, oi)
        if not params:
            # print(f"Skipped {sid}: {reject_reason}")
            self.stats["skipped_strategy"] += 1
            return

        # 3. Data Check (Audit 7.3)
        bars = bars_map.get(sid, [])
        if len(bars) < 100:
            self.stats["skipped_no_data"] += 1
            return

        # 4. Simulation (Audit 4)
        # "entry_time: Signal Timestamp + 17 minutes"
        # Database 'entry_time' column already has this calculation?
        # Audit 3.1: "entry_time: Signal Timestamp + 17 minutes"
        # We use the 'entry_time' from DB.
        entry_ts_epoch = entry_time.timestamp()
        
        result = StrategyEmulator.run_simulation(bars, params, entry_ts_epoch)
        
        if result:
            # Update result metadata
            result = result._replace(signal_id=sid, symbol=sym)
            self.results.append(result)
            
            # Update Global Tracker (Audit 5.2)
            # "Update position_tracker_ts = last_exit_ts of the final trade"
            self.active_positions[sym] = result.final_exit_ts
        else:
            self.stats["skipped_no_data"] += 1 # Simulation failed (e.g. no bars after entry)

    def _generate_report(self):
        print("\n" + "="*80)
        print("BACKTEST RESULTS (AUDITED LOGIC)")
        print("="*80)
        
        total_pnl_usd = 0.0
        total_trades = 0
        winning_trades = 0
        
        csv_rows = []
        
        for res in self.results:
            # Calculate USD PnL
            # Audit: "Simulates the trade lifecycle"
            # We assume fixed capital sizing for reporting
            net_pnl_usd = 0.0
            
            for t in res.trades:
                pnl_dollars = POSITION_SIZE_USD * (t.pnl_realized_pct / 100.0)
                net_pnl_usd += pnl_dollars
                total_trades += 1
                if t.pnl_realized_pct > 0:
                    winning_trades += 1
                
                # Add to CSV
                csv_rows.append({
                    "SignalID": res.signal_id,
                    "Symbol": res.symbol,
                    "EntryTime": datetime.fromtimestamp(t.entry_ts, tz=timezone.utc),
                    "ExitTime": datetime.fromtimestamp(t.exit_ts, tz=timezone.utc),
                    "Type": "TRADE",
                    "EntryPrice": t.entry_price,
                    "ExitPrice": t.exit_price,
                    "PnL_Pct": t.pnl_realized_pct,
                    "PnL_USD": pnl_dollars,
                    "Reason": t.exit_reason
                })
            
            total_pnl_usd += net_pnl_usd

        # Summary
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        roi = 0 # Need accurate capital base for ROI, using simplified PnL sum
        
        print(f"Total Signals Processed: {len(self.results)}")
        print(f"Total Trades Executed:   {total_trades}")
        print(f"Win Rate:                {win_rate:.2f}%")
        print(f"Total PnL (USD):         ${total_pnl_usd:.2f} (Base ${POSITION_SIZE_USD}/trade)")
        print("-" * 80)
        print(f"Skipped (Position Busy): {self.stats['skipped_position']}")
        print(f"Skipped (Strategy Filt): {self.stats['skipped_strategy']}")
        print(f"Skipped (No Data):       {self.stats['skipped_no_data']}")
        print("="*80)
        
        # Save CSV
        out_file = PROJECT_ROOT / "backtest_trades.csv"
        with open(out_file, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "SignalID", "Symbol", "EntryTime", "ExitTime", "Type", 
                "EntryPrice", "ExitPrice", "PnL_Pct", "PnL_USD", "Reason"
            ])
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"Detailed trade log saved to: {out_file}")

# ==============================================================================
# MAIN ENTRY
# ==============================================================================

if __name__ == "__main__":
    db = get_db_connection()
    try:
        idx_arg = -1
        limit_val = 0
        if "--limit" in sys.argv:
            idx_arg = sys.argv.index("--limit")
            if idx_arg + 1 < len(sys.argv):
                limit_val = int(sys.argv[idx_arg+1])
        
        strategy_path = PROJECT_ROOT / "composite_strategy.json"
        if not strategy_path.exists():
            print(f"Error: {strategy_path} not found.")
            sys.exit(1)
            
        tester = Backtester(db, strategy_path)
        tester.run(limit=limit_val)
    finally:
        db.close()
