
import sys
import os
from pathlib import Path

# Add current directory to path so we can import optimize_unified
sys.path.append(os.getcwd())

try:
    from optimize_unified import generate_strategy_grid
except ImportError as e:
    print(f"Error importing optimize_unified: {e}")
    # Try adding parent dir if run from deep_research
    sys.path.append(str(Path(os.getcwd()).parent))
    from optimize_unified import generate_strategy_grid

def verify():
    print("Generating strategy grid...")
    grid = generate_strategy_grid()
    print(f"Total strategies generated: {len(grid)}")
    
    # Analyze parameters
    activations = set()
    callbacks = set()
    activations_callbacks = set()
    reentry_hours = set()
    pos_hours = set()
    
    for s in grid:
        activations.add(s.get('base_activation'))
        callbacks.add(s.get('base_callback'))
        activations_callbacks.add((s.get('base_activation'), s.get('base_callback')))
        reentry_hours.add(s.get('max_reentry_hours'))
        pos_hours.add(s.get('max_position_hours'))
        
    print("\n--- Parameter Verification ---")
    print(f"Unique Base Activations: {sorted(list(activations))}")
    print(f"Unique Base Callbacks: {sorted(list(callbacks))}")
    print(f"Unique Profiles (Act, Cb): {sorted(list(activations_callbacks))}")
    print(f"Unique Max Reentry Hours: {sorted(list(reentry_hours))}")
    print(f"Unique Max Position Hours: {sorted(list(pos_hours))}")
    
    # Verify explicitly if 0.4 is present
    has_scalping = 0.4 in activations
    print(f"\n[VERDICT] Scalping param (0.4) present? {'✅ YES' if has_scalping else '❌ NO'}")
    
    has_degen = 10.0 in activations
    print(f"[VERDICT] Degen param (10.0) present? {'✅ YES' if has_degen else '❌ NO'}")

if __name__ == "__main__":
    verify()
