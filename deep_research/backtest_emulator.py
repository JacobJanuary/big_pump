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

# ==============================================================================
# PARALLEL BACKTESTER
# ==============================================================================

class Backtester:
    def __init__(self, db_conn, strategy_file: Path):
        self.conn = db_conn  # Only used for initial signal fetch in main thread
        self.config = StrategyConfig(strategy_file)
        self.active_positions = {} 
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
            
        print(f"[DATA] Fetched {len(rows)} raw signals.")
        
        # Pre-filter: keep only signals that match AT LEAST ONE rule in composite_strategy.json
        filtered = self._pre_filter_signals(rows)
        print(f"[DATA] After pre-filter: {len(filtered)} signals match strategy rules.")
        
        return sorted(filtered, key=lambda x: x[2])
    
    def _pre_filter_signals(self, signals: List[tuple]) -> List[tuple]:
        """Filter signals to only those matching at least one rule in composite_strategy.json.
        
        This dramatically reduces the number of signals we need to fetch bars for.
        Signal tuple: (id, pair, signal_ts, entry_time, score, rsi, vol_zscore, oi_delta)
        """
        matched = []
        for signal in signals:
            sid, pair, sig_ts, entry_time, score, rsi, vol_zscore, oi_delta = signal
            
            # Check if signal matches ANY rule
            for rule in self.config.rules:
                r_min, r_max = rule["range"]
                flt = rule["filter_criteria"]
                if (r_min <= score < r_max and
                    rsi >= flt["rsi_min"] and
                    vol_zscore >= flt["vol_min"] and
                    oi_delta >= flt["oi_min"]):
                    matched.append(signal)
                    break  # No need to check other rules once matched
        
        return matched

    def preload_bars_parallel(self, signal_ids: List[int], workers: int = 8) -> Dict[int, List[tuple]]:
        """Load ALL bars for all signals in chunks (PARALLEL).
        
        EXACT COPY from optimize_unified.py (lines 304-369) for proven performance.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        print(f"[PRELOAD] Loading bars for {len(signal_ids)} signals using {workers} workers...")
        bars_map: Dict[int, List[tuple]] = {}
        bars_lock = threading.Lock()
        chunk_size = 5  # Smaller chunks for FASTER feedback
        total_chunks = (len(signal_ids) + chunk_size - 1) // chunk_size
        
        print(f"[PRELOAD] Total chunks: {total_chunks} (chunk_size={chunk_size})")
        
        # Split into chunks
        chunks = []
        for i in range(0, len(signal_ids), chunk_size):
            chunks.append((i // chunk_size, signal_ids[i:i + chunk_size]))
        
        completed = [0]  # Mutable counter for progress
        
        def load_chunk(args):
            chunk_idx, chunk = args
            local_bars = {}
            try:
                print(f"   [W] Chunk {chunk_idx+1}/{total_chunks} starting...", flush=True)
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT signal_analysis_id, second_ts, close_price, delta,
                                   large_buy_count, large_sell_count
                            FROM web.agg_trades_1s
                            WHERE signal_analysis_id = ANY(%s)
                            ORDER BY signal_analysis_id, second_ts
                        """, (chunk,))
                        rows = cur.fetchall()
                        for row in rows:
                            sid, ts, price, delta, buy, sell = row
                            # Extended tuple format for strategy emulator
                            local_bars.setdefault(sid, []).append(
                                (ts, float(price), float(delta), 0.0, buy, sell, 0.0, 0.0)
                            )
                print(f"   [W] Chunk {chunk_idx+1}/{total_chunks} done: {len(rows):,} rows", flush=True)
                return (chunk_idx, local_bars, len(rows))
            except Exception as e:
                print(f"[PRELOAD] Chunk {chunk_idx} error: {e}", flush=True)
                return (chunk_idx, {}, 0)
        
        # Parallel execution
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(load_chunk, chunk) for chunk in chunks]
            
            for future in as_completed(futures):
                chunk_idx, local_bars, row_count = future.result()
                with bars_lock:
                    for sid, bars in local_bars.items():
                        bars_map.setdefault(sid, []).extend(bars)
                    completed[0] += 1
                
                # Progress every 10 chunks or at completion
                if completed[0] % 20 == 0 or completed[0] == total_chunks:
                    print(f"[PRELOAD] Progress: {completed[0]}/{total_chunks} chunks ({completed[0]*100//total_chunks}%)", flush=True)
        
        print(f"[PRELOAD] Bars loaded for {len(bars_map)} signals")
        return bars_map

    def run(self, limit: int = 0, workers: int = 2):
        """Run backtest. Default workers=2 to avoid overwhelming DB."""

        import time
        t0 = time.time()
        
        signals = self.fetch_signals()
        
        if limit > 0:
            print(f"[TEST] Limiting to first {limit} signals.")
            signals = signals[:limit]
            
        total_signals = len(signals)
        signal_ids = [r[0] for r in signals]
        
        # 1. PRELOAD EVERYTHING
        # Using parallel DB fetch
        bars_map = self.preload_bars_parallel(signal_ids, workers=workers)
        
        t1 = time.time()
        print(f"[PERF] Data load took {t1-t0:.1f}s")
        
        # 2. PROCESS SIGNALS
        # Processing is sequential due to Global Position Tracking (dependency on previous results)
        # We cannot easily parallelize the strategy execution because trade N depends on trade N-1's exit time for the SAME PAIR.
        # But we CAN parallelize *across* pairs if we group them? 
        # Actually, Python execution of 2000 signals on in-memory data should be instant (<10s).
        # Optimization: Group by pair, run pairs in parallel? 
        # Let's stick to sequential first, it should be blazing fast with in-memory data.
        
        print("[CPU] Running simulations...")
        
        for row in signals:
            self._process_signal(row, bars_map)
            
        t2 = time.time()
        print(f"[PERF] Simulation took {t2-t1:.1f}s")
        print(f"[PERF] Total time: {t2-t0:.1f}s")
            
        self._generate_report()

    def _process_signal(self, row, bars_map):
        sid, sym, ts, entry_time, score, rsi, vol, oi = row
        
        # 1. Global Position Check
        if sym in self.active_positions:
            # Check if pair is free at this signal's timestamp
            free_at = self.active_positions[sym]
            if ts.timestamp() < free_at:
                self.stats["skipped_position"] += 1
                return

        # 2. Strategy Lookup
        params, reject_reason = self.config.get_strategy(score, rsi, vol, oi)
        if not params:
            self.stats["skipped_strategy"] += 1
            return

        # 3. Data Check
        bars = bars_map.get(sid, [])
        if len(bars) < 100:
            self.stats["skipped_no_data"] += 1
            return

        # 4. Simulation
        entry_ts_epoch = entry_time.timestamp()
        result = StrategyEmulator.run_simulation(bars, params, entry_ts_epoch)
        
        if result:
            result = result._replace(signal_id=sid, symbol=sym)
            self.results.append(result)
            # Update Global Tracker
            self.active_positions[sym] = result.final_exit_ts
            self.stats["processed"] += 1
        else:
            self.stats["skipped_no_data"] += 1 

    def _generate_report(self):
        from collections import defaultdict
        
        print("\n" + "="*120)
        print("DETAILED TRADE LOG")
        print("="*120)
        print(f"{'#':<5} {'Symbol':<12} {'Type':<10} {'Entry Time':<20} {'Entry $':<12} {'Exit Time':<20} {'Exit $':<12} {'Duration':<10} {'PnL %':<10} {'PnL $':<10} {'Reason':<15}")
        print("-"*120)
        
        total_pnl_usd = 0.0
        total_trades = 0
        winning_trades = 0
        
        csv_rows = []
        daily_pnl = defaultdict(float)  # {date_str: pnl_usd}
        trade_num = 0
        
        for res in self.results:
            trade_seq = 0  # 0 = Initial entry, 1+ = Re-entry
            for t in res.trades:
                trade_num += 1
                pnl_dollars = POSITION_SIZE_USD * (t.pnl_realized_pct / 100.0)
                total_pnl_usd += pnl_dollars
                total_trades += 1
                if t.pnl_realized_pct > 0:
                    winning_trades += 1
                
                # Extract date for daily grouping
                entry_dt = datetime.fromtimestamp(t.entry_ts, tz=timezone.utc)
                exit_dt = datetime.fromtimestamp(t.exit_ts, tz=timezone.utc)
                date_str = exit_dt.strftime("%Y-%m-%d")
                daily_pnl[date_str] += pnl_dollars
                
                # Calculate duration
                duration_sec = t.exit_ts - t.entry_ts
                hours = int(duration_sec // 3600)
                mins = int((duration_sec % 3600) // 60)
                duration_str = f"{hours}h {mins}m"
                
                # Determine trade type
                trade_type = "INITIAL" if trade_seq == 0 else f"REENTRY_{trade_seq}"
                
                # Print trade details
                pnl_sign = "+" if t.pnl_realized_pct >= 0 else ""
                usd_sign = "+" if pnl_dollars >= 0 else ""
                print(f"{trade_num:<5} {res.symbol:<12} {trade_type:<10} {entry_dt.strftime('%Y-%m-%d %H:%M'):<20} {t.entry_price:<12.4f} {exit_dt.strftime('%Y-%m-%d %H:%M'):<20} {t.exit_price:<12.4f} {duration_str:<10} {pnl_sign}{t.pnl_realized_pct:<9.2f} {usd_sign}${abs(pnl_dollars):<9.2f} {t.exit_reason:<15}")
                
                csv_rows.append({
                    "SignalID": res.signal_id,
                    "Symbol": res.symbol,
                    "TradeSeq": trade_seq,
                    "TradeType": trade_type,
                    "EntryTime": entry_dt.isoformat(),
                    "ExitTime": exit_dt.isoformat(),
                    "ExitDate": date_str,
                    "DurationSec": duration_sec,
                    "EntryPrice": f"{t.entry_price:.6f}",
                    "ExitPrice": f"{t.exit_price:.6f}",
                    "PnL_Raw_Pct": f"{t.pnl_raw_pct:.2f}",
                    "PnL_Realized_Pct": f"{t.pnl_realized_pct:.2f}",
                    "PnL_USD": f"{pnl_dollars:.2f}",
                    "ExitReason": t.exit_reason,
                    "IsLiquidation": t.is_liquidated
                })
                trade_seq += 1
        
        print("="*120)
        
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        # Count re-entries
        initial_trades = sum(1 for r in csv_rows if r["TradeSeq"] == 0)
        reentry_trades = total_trades - initial_trades
        
        print(f"Total Signals Processed: {len(self.results)}")
        print(f"Total Trades Executed:   {total_trades}")
        print(f"  - Initial Entries:     {initial_trades}")
        print(f"  - Re-entries:          {reentry_trades}")
        print(f"Win Rate:                {win_rate:.2f}%")
        print(f"Total PnL (USD):         ${total_pnl_usd:.2f} (Base ${POSITION_SIZE_USD}/trade)")
        print("-" * 80)
        print(f"Skipped (Position Busy): {self.stats['skipped_position']}")
        print(f"Skipped (Strategy Filt): {self.stats['skipped_strategy']}")
        print(f"Skipped (No Data):       {self.stats['skipped_no_data']}")
        print("="*80)
        
        # === DAILY PnL REPORT ===
        print("\n" + "="*80)
        print("DAILY PROFIT/LOSS REPORT")
        print("="*80)
        print(f"{'Date':<12} | {'Daily PnL':>12} | {'Cumulative':>12}")
        print("-" * 42)
        
        cumulative = 0.0
        for date_str in sorted(daily_pnl.keys()):
            daily = daily_pnl[date_str]
            cumulative += daily
            sign = "+" if daily >= 0 else ""
            cum_sign = "+" if cumulative >= 0 else ""
            print(f"{date_str:<12} | {sign}${daily:>10.2f} | {cum_sign}${cumulative:>10.2f}")
        
        print("="*80)
        
        # === SAVE CSV ===
        out_file = PROJECT_ROOT / "backtest_trades.csv"
        with open(out_file, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "SignalID", "Symbol", "TradeSeq", "TradeType", 
                "EntryTime", "ExitTime", "ExitDate",
                "EntryPrice", "ExitPrice", 
                "PnL_Raw_Pct", "PnL_Realized_Pct", "PnL_USD", 
                "ExitReason", "IsLiquidation"
            ])
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"\nDetailed trade log saved to: {out_file}")
        
        # === SAVE DAILY SUMMARY CSV ===
        daily_file = PROJECT_ROOT / "backtest_daily_summary.csv"
        with open(daily_file, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["Date", "DailyPnL_USD", "CumulativePnL_USD"])
            writer.writeheader()
            cumulative = 0.0
            for date_str in sorted(daily_pnl.keys()):
                cumulative += daily_pnl[date_str]
                writer.writerow({
                    "Date": date_str,
                    "DailyPnL_USD": f"{daily_pnl[date_str]:.2f}",
                    "CumulativePnL_USD": f"{cumulative:.2f}"
                })
        print(f"Daily summary saved to: {daily_file}")

# ==============================================================================
# MAIN ENTRY
# ==============================================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Audited Backtest Emulator")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of signals")
    parser.add_argument("--workers", type=int, default=2, help="Parallel workers for data loading (default: 2 to avoid DB overload)")
    args = parser.parse_args()

    db = get_db_connection()
    try:
        strategy_path = PROJECT_ROOT / "composite_strategy.json"
        if not strategy_path.exists():
            print(f"Error: {strategy_path} not found.")
            sys.exit(1)
            
        tester = Backtester(db, strategy_path)
        tester.run(limit=args.limit, workers=args.workers)
    finally:
        db.close()
