"""
Report Signal Performance (Last 30 Days)
Generates a summary of signal quality based on pre-calculated metrics in web.signal_analysis.
"""
import sys
from pathlib import Path
import argparse
from datetime import datetime, timedelta

# Add scripts directory to path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

from pump_analysis_lib import get_db_connection

def generate_report(days=30):
    print("="*100)
    print(f"SIGNAL PERFORMANCE REPORT (LAST {days} DAYS)")
    print("="*100)
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Fetch stats
            query = f"""
                SELECT 
                    pair_symbol,
                    signal_timestamp,
                    total_score,
                    max_growth_pct,
                    max_drawdown_pct,
                    time_to_peak_seconds
                FROM web.signal_analysis
                WHERE signal_timestamp >= NOW() - INTERVAL '{days} days'
                ORDER BY signal_timestamp DESC
            """
            cur.execute(query)
            rows = cur.fetchall()
            
            if not rows:
                print("No signals found in the last 30 days.")
                print("Tip: Run 'python3 scripts/populate_signal_analysis.py' to update data.")
                return

            # Calculate aggregates
            total_signals = len(rows)
            avg_max_growth = sum(r[3] for r in rows) / total_signals
            avg_max_drawdown = sum(r[4] for r in rows) / total_signals
            
            # Win rates (theoretical max potential)
            wins_5pct = len([r for r in rows if r[3] >= 5])
            wins_10pct = len([r for r in rows if r[3] >= 10])
            wins_20pct = len([r for r in rows if r[3] >= 20])
            
            # Time to peak
            avg_time_to_peak_hours = (sum(r[5] for r in rows) / total_signals) / 3600
            
            # Best/Worst
            best_signal = max(rows, key=lambda x: x[3])
            worst_drawdown = min(rows, key=lambda x: x[4])

            print(f"Total Signals: {total_signals}")
            print(f"Avg Max Potential Profit: {avg_max_growth:.2f}%")
            print(f"Avg Max Drawdown: {avg_max_drawdown:.2f}%")
            print(f"Avg Time to Peak: {avg_time_to_peak_hours:.1f} hours")
            print("-" * 50)
            print(f"Potential Win Rate (>5%): {wins_5pct} ({wins_5pct/total_signals*100:.1f}%)")
            print(f"Potential Win Rate (>10%): {wins_10pct} ({wins_10pct/total_signals*100:.1f}%)")
            print(f"Potential Win Rate (>20%): {wins_20pct} ({wins_20pct/total_signals*100:.1f}%)")
            print("-" * 50)
            print(f"Best Signal: {best_signal[0]} (+{best_signal[3]:.2f}%) on {best_signal[1].strftime('%Y-%m-%d %H:%M')}")
            print(f"Max Drawdown: {worst_drawdown[0]} ({worst_drawdown[4]:.2f}%) on {worst_drawdown[1].strftime('%Y-%m-%d %H:%M')}")
            
            print("\n" + "="*100)
            print(f"{'Date':<18} {'Symbol':<12} {'Score':<8} {'Max Gain':<12} {'Max DD':<12} {'Time to Peak':<15}")
            print("-" * 100)
            
            # Print top 20 recent signals
            for r in rows[:20]:
                ts = r[1].strftime('%Y-%m-%d %H:%M')
                symbol = r[0]
                score = r[2]
                gain = r[3]
                dd = r[4]
                peak_time_h = r[5] / 3600
                
                print(f"{ts:<18} {symbol:<12} {score:<8} {gain:>+8.2f}%   {dd:>+8.2f}%   {peak_time_h:>5.1f}h")
            
            if total_signals > 20:
                print(f"... and {total_signals - 20} more signals.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Report signal performance stats.')
    parser.add_argument('--days', type=int, default=30, help='Days to look back')
    args = parser.parse_args()
    
    generate_report(args.days)
