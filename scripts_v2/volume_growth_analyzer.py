#!/usr/bin/env python3
"""
Volume Growth Analyzer
Analyzes 24-hour volume growth for all trading pairs across exchanges and contract types.
Compares last 24 hours vs previous 24 hours using 1-hour candles.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import psycopg
from psycopg.rows import dict_row
from typing import List, Dict, Optional
from dataclasses import dataclass

# Add config directory to path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
config_dir = parent_dir / 'config'
sys.path.append(str(config_dir))

import settings

DB_CONFIG = settings.DATABASE

# Exchange and contract type mappings
EXCHANGES = {
    1: 'Binance',
    2: 'Bybit'
}

CONTRACT_TYPES = {
    1: 'Futures',
    2: 'Spot'
}

# Interval ID for 1-hour candles
INTERVAL_1H = 3

@dataclass
class VolumeAnalysis:
    """Volume analysis result for a trading pair+exchange+contract combination"""
    pair_symbol: str
    exchange_name: str
    contract_type: str
    volume_today: float
    volume_yesterday: float
    growth_percent: float
    growth_absolute: float
    trading_pair_id: int
    
    def __repr__(self):
        return (f"{self.pair_symbol:15s} | {self.exchange_name:10s} | {self.contract_type:10s} | "
                f"Today: ${self.volume_today:15,.0f} | Yesterday: ${self.volume_yesterday:15,.0f} | "
                f"Growth: {self.growth_percent:+8.2f}%")


def get_db_connection():
    """Get database connection"""
    conn_params = [
        f"host={DB_CONFIG['host']}",
        f"port={DB_CONFIG['port']}",
        f"dbname={DB_CONFIG['dbname']}",
        f"user={DB_CONFIG['user']}",
        "sslmode=disable"
    ]
    
    if DB_CONFIG.get('password'):
        conn_params.append(f"password={DB_CONFIG['password']}")
        
    conn_str = " ".join(conn_params)
    return psycopg.connect(conn_str)


def get_all_trading_pairs(conn) -> List[Dict]:
    """
    Get all active trading pairs from the database.
    Returns list of dicts with: id, pair_symbol, exchange_id, contract_type_id
    """
    query = """
        SELECT 
            id,
            pair_symbol,
            exchange_id,
            contract_type_id
        FROM public.trading_pairs
        WHERE is_active = TRUE
        ORDER BY pair_symbol, exchange_id, contract_type_id
    """
    
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)
        pairs = cur.fetchall()
    
    return pairs


def calculate_volume_growth(conn, trading_pair_id: int, min_candles: int = 20, min_yesterday_volume: float = 10000.0) -> Optional[tuple]:
    """
    Calculate volume growth for a specific trading pair with data quality checks.
    
    Args:
        conn: Database connection
        trading_pair_id: Trading pair ID
        min_candles: Minimum number of candles required for each period (default: 20)
        min_yesterday_volume: Minimum volume yesterday in USDT to consider (default: 10,000)
    
    Returns: (volume_today, volume_yesterday, growth_percent, growth_absolute, 
              candles_today_count, candles_yesterday_count) or None
    
    Logic:
    - "Today" = last 24 1-hour candles (most recent)
    - "Yesterday" = 24 1-hour candles before that (from 48h ago to 24h ago)
    - Quality filters:
      * Requires minimum number of candles in each period
      * Requires minimum volume yesterday to avoid false positives
    """
    # Get current time
    now = datetime.now(timezone.utc)
    
    # Calculate time boundaries
    # Today: from 24 hours ago to now
    end_today = now
    start_today = end_today - timedelta(hours=24)
    
    # Yesterday: from 48 hours ago to 24 hours ago
    end_yesterday = start_today
    start_yesterday = end_yesterday - timedelta(hours=24)
    
    # Convert to milliseconds
    end_today_ms = int(end_today.timestamp() * 1000)
    start_today_ms = int(start_today.timestamp() * 1000)
    end_yesterday_ms = int(end_yesterday.timestamp() * 1000)
    start_yesterday_ms = int(start_yesterday.timestamp() * 1000)
    
    # Enhanced query with candle count
    query = """
        SELECT 
            COALESCE(SUM(quote_asset_volume), 0) as total_volume,
            COUNT(*) as candle_count
        FROM public.candles
        WHERE trading_pair_id = %s
          AND interval_id = %s
          AND open_time >= %s
          AND open_time < %s
    """
    
    with conn.cursor(row_factory=dict_row) as cur:
        # Get today's volume and candle count
        cur.execute(query, (trading_pair_id, INTERVAL_1H, start_today_ms, end_today_ms))
        result_today = cur.fetchone()
        volume_today = float(result_today['total_volume'])
        candles_today = int(result_today['candle_count'])
        
        # Get yesterday's volume and candle count
        cur.execute(query, (trading_pair_id, INTERVAL_1H, start_yesterday_ms, end_yesterday_ms))
        result_yesterday = cur.fetchone()
        volume_yesterday = float(result_yesterday['total_volume'])
        candles_yesterday = int(result_yesterday['candle_count'])
    
    # Data quality checks
    # 1. Check minimum number of candles
    if candles_today < min_candles or candles_yesterday < min_candles:
        return None  # Insufficient data
    
    # 2. Check minimum volume yesterday (avoid false positives from illiquid pairs)
    if volume_yesterday < min_yesterday_volume:
        return None  # Too low liquidity yesterday
    
    # Calculate growth
    if volume_yesterday == 0:
        if volume_today > 0:
            growth_percent = float('inf')  # Infinite growth (from 0)
        else:
            growth_percent = 0.0  # No volume on both days
    else:
        growth_percent = ((volume_today - volume_yesterday) / volume_yesterday) * 100
    
    growth_absolute = volume_today - volume_yesterday
    
    return (volume_today, volume_yesterday, growth_percent, growth_absolute, candles_today, candles_yesterday)


def analyze_all_pairs(conn, min_candles: int = 20, min_yesterday_volume: float = 10000.0) -> List[VolumeAnalysis]:
    """
    Analyze volume growth for all trading pairs with quality filters.
    
    Args:
        min_candles: Minimum candles required per period (default: 20 out of 24)
        min_yesterday_volume: Minimum volume yesterday in USDT (default: $10,000)
    
    Returns list of VolumeAnalysis objects.
    """
    pairs = get_all_trading_pairs(conn)
    results = []
    
    total_pairs = len(pairs)
    filtered_count = 0
    
    print(f"\n🔍 Analyzing {total_pairs} trading pair configurations...")
    print(f"📋 Filters: min_candles={min_candles}, min_yesterday_volume=${min_yesterday_volume:,.0f}\n")
    
    for idx, pair in enumerate(pairs, 1):
        pair_id = pair['id']
        pair_symbol = pair['pair_symbol']
        exchange_id = pair['exchange_id']
        contract_type_id = pair['contract_type_id']
        
        exchange_name = EXCHANGES.get(exchange_id, f"Unknown({exchange_id})")
        contract_type = CONTRACT_TYPES.get(contract_type_id, f"Unknown({contract_type_id})")
        
        # Progress indicator
        if idx % 100 == 0 or idx == total_pairs:
            print(f"  Progress: {idx}/{total_pairs} pairs analyzed, {len(results)} passed filters...")
        
        # Calculate volume growth with quality checks
        volume_data = calculate_volume_growth(conn, pair_id, min_candles, min_yesterday_volume)
        
        if volume_data is None:
            filtered_count += 1
            continue
        
        volume_today, volume_yesterday, growth_percent, growth_absolute, candles_today, candles_yesterday = volume_data
        
        # Skip pairs with no volume
        if volume_today == 0 and volume_yesterday == 0:
            filtered_count += 1
            continue
        
        analysis = VolumeAnalysis(
            pair_symbol=pair_symbol,
            exchange_name=exchange_name,
            contract_type=contract_type,
            volume_today=volume_today,
            volume_yesterday=volume_yesterday,
            growth_percent=growth_percent,
            growth_absolute=growth_absolute,
            trading_pair_id=pair_id
        )
        
        results.append(analysis)
    
    print(f"\n✅ Analysis complete!")
    print(f"   Passed filters: {len(results)} pairs")
    print(f"   Filtered out: {filtered_count} pairs (low liquidity or insufficient data)\n")
    
    return results


def display_top_100(results: List[VolumeAnalysis]):
    """
    Display top 100 pairs by volume growth.
    """
    # Sort by growth percentage (descending)
    sorted_results = sorted(results, key=lambda x: x.growth_percent, reverse=True)
    
    # Take top 100
    top_100 = sorted_results[:100]
    
    print("=" * 120)
    print("📊 TOP 100 TRADING PAIRS BY 24H VOLUME GROWTH")
    print("=" * 120)
    print(f"{'Rank':<6} {'Pair':<15} {'Exchange':<10} {'Type':<10} {'Volume Today (USDT)':<20} "
          f"{'Volume Yesterday (USDT)':<23} {'Growth %':<12}")
    print("-" * 120)
    
    for rank, analysis in enumerate(top_100, 1):
        # Handle infinite growth
        if analysis.growth_percent == float('inf'):
            growth_str = "+∞"
        else:
            growth_str = f"+{analysis.growth_percent:.2f}%" if analysis.growth_percent >= 0 else f"{analysis.growth_percent:.2f}%"
        
        print(f"{rank:<6} {analysis.pair_symbol:<15} {analysis.exchange_name:<10} {analysis.contract_type:<10} "
              f"${analysis.volume_today:>18,.0f} ${analysis.volume_yesterday:>21,.0f} {growth_str:<12}")
    
    print("=" * 120)
    
    # Summary statistics
    print("\n📈 SUMMARY STATISTICS")
    print("-" * 50)
    
    # Filter out infinite values for stats
    finite_growth = [r.growth_percent for r in top_100 if r.growth_percent != float('inf')]
    
    if finite_growth:
        avg_growth = sum(finite_growth) / len(finite_growth)
        max_growth = max(finite_growth)
        min_growth = min(finite_growth)
        
        print(f"Average growth (top 100): {avg_growth:+.2f}%")
        print(f"Maximum growth: {max_growth:+.2f}%")
        print(f"Minimum growth: {min_growth:+.2f}%")
    
    # Count by exchange and contract type
    binance_futures = sum(1 for r in top_100 if r.exchange_name == 'Binance' and r.contract_type == 'Futures')
    binance_spot = sum(1 for r in top_100 if r.exchange_name == 'Binance' and r.contract_type == 'Spot')
    bybit_futures = sum(1 for r in top_100 if r.exchange_name == 'Bybit' and r.contract_type == 'Futures')
    bybit_spot = sum(1 for r in top_100 if r.exchange_name == 'Bybit' and r.contract_type == 'Spot')
    
    print(f"\n📊 Distribution:")
    print(f"  Binance Futures: {binance_futures}")
    print(f"  Binance Spot:    {binance_spot}")
    print(f"  Bybit Futures:   {bybit_futures}")
    print(f"  Bybit Spot:      {bybit_spot}")
    print()


def save_to_file(results: List[VolumeAnalysis], output_dir: Path):
    """
    Save results to a CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"volume_growth_top100_{timestamp}.csv"
    filepath = output_dir / filename
    
    # Sort by growth percentage (descending)
    sorted_results = sorted(results, key=lambda x: x.growth_percent, reverse=True)[:100]
    
    with open(filepath, 'w') as f:
        # Write header
        f.write("Rank,Pair,Exchange,Contract Type,Volume Today (USDT),Volume Yesterday (USDT),Growth %,Growth Absolute (USDT)\n")
        
        # Write data
        for rank, analysis in enumerate(sorted_results, 1):
            growth_str = "INF" if analysis.growth_percent == float('inf') else f"{analysis.growth_percent:.2f}"
            
            f.write(f"{rank},{analysis.pair_symbol},{analysis.exchange_name},{analysis.contract_type},"
                   f"{analysis.volume_today:.2f},{analysis.volume_yesterday:.2f},"
                   f"{growth_str},{analysis.growth_absolute:.2f}\n")
    
    print(f"💾 Results saved to: {filepath}")


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Volume Growth Analyzer - Find top trading pairs by 24h volume growth',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default filters (min 20 candles, min $10k yesterday volume)
  python3 volume_growth_analyzer.py
  
  # More strict filters (crypto veterans only)
  python3 volume_growth_analyzer.py --min-candles 23 --min-volume 100000
  
  # More relaxed filters (include newer/smaller pairs)
  python3 volume_growth_analyzer.py --min-candles 15 --min-volume 1000
        """
    )
    
    parser.add_argument(
        '--min-candles',
        type=int,
        default=20,
        help='Minimum number of 1h candles required per 24h period (default: 20 out of 24)'
    )
    
    parser.add_argument(
        '--min-volume',
        type=float,
        default=10000.0,
        help='Minimum volume yesterday in USDT to consider (default: 10000)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 120)
    print("🚀 VOLUME GROWTH ANALYZER")
    print("=" * 120)
    print("\n📅 Analysis Period:")
    
    now = datetime.now(timezone.utc)
    today_start = now - timedelta(hours=24)
    yesterday_start = now - timedelta(hours=48)
    
    print(f"  Today:     {today_start.strftime('%Y-%m-%d %H:%M:%S UTC')} to {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Yesterday: {yesterday_start.strftime('%Y-%m-%d %H:%M:%S UTC')} to {today_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    
    try:
        # Connect to database
        conn = get_db_connection()
        print("✅ Database connection established\n")
        
        # Analyze all pairs with user-specified filters
        results = analyze_all_pairs(conn, min_candles=args.min_candles, min_yesterday_volume=args.min_volume)
        
        # Display top 100
        display_top_100(results)
        
        # Save to file
        output_dir = Path(__file__).parent.parent / 'reports'
        save_to_file(results, output_dir)
        
        conn.close()
        print("\n✅ Analysis complete!\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

