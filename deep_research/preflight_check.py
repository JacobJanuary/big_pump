#!/usr/bin/env python3
"""
Pre-flight checker for optimize_unified.py
Simulates critical paths without running the full 10-hour job
"""
import sys
import os
from pathlib import Path

# Setup path - run from big_pump directory
script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
os.chdir(project_dir)
sys.path.insert(0, str(script_dir))
sys.path.insert(0, str(project_dir))

print("=" * 70)
print("PRE-FLIGHT VERIFICATION FOR optimize_unified.py")
print("=" * 70)

errors = []
warnings = []

# Test 1: Import check
print("\n[1/8] Checking imports...")
try:
    import optimize_unified
    from optimize_unified import (
        generate_filter_grid, generate_strategy_grid,
        preload_all_signals, filter_signals_in_memory,
        evaluate_filter_lookup, WORKER_SIGNALS, WORKER_RESULTS,
        OUTPUT_JSONL_FILE
    )
    print("  ✅ All imports successful")
except Exception as e:
    errors.append(f"Import failed: {e}")
    print(f"  ❌ Import failed: {e}")

# Test 2: Grid sizes
print("\n[2/8] Checking grid sizes...")
try:
    fg = generate_filter_grid()
    sg = generate_strategy_grid()
    print(f"  Filter grid: {len(fg):,} combinations")
    print(f"  Strategy grid: {len(sg):,} combinations")
    if len(sg) != 77760:
        warnings.append(f"Strategy grid size {len(sg)} != expected 77760")
    else:
        print("  ✅ Strategy grid size matches expected 77,760")
except Exception as e:
    errors.append(f"Grid generation failed: {e}")

# Test 3: Signal count metric presence
print("\n[3/8] Checking signal_count in output...")
try:
    import inspect
    from deep_research import optimize_unified
    source = inspect.getsource(optimize_unified.main)
    if '"signal_count": count' in source or "'signal_count': count" in source:
        print("  ✅ signal_count metric present in output")
    else:
        errors.append("signal_count NOT found in main() output")
        print("  ❌ signal_count NOT found!")
except Exception as e:
    errors.append(f"Source inspection failed: {e}")

# Test 4: Backup mechanism
print("\n[4/8] Checking backup mechanism...")
try:
    source = inspect.getsource(optimize_unified.main)
    if 'backup_jsonl' in source and 'rename' in source:
        print("  ✅ Backup mechanism present")
    else:
        errors.append("Backup mechanism NOT found")
        print("  ❌ Backup mechanism NOT found!")
except Exception as e:
    errors.append(f"Backup check failed: {e}")

# Test 5: Fork-inherited globals for Phase 2
print("\n[5/8] Checking Phase 2 Pool creation...")
try:
    source = inspect.getsource(optimize_unified.main)
    # Check that we DON'T use initargs in Pool for Phase 2
    if 'global WORKER_SIGNALS, WORKER_RESULTS' in source and 'WORKER_SIGNALS = signals_with_bars' in source:
        print("  ✅ Phase 2 uses fork-inherited globals (no pickle)")
    else:
        errors.append("Phase 2 might still use initargs (pickle)")
        print("  ⚠️ Could not confirm fork-inherited globals")
except Exception as e:
    errors.append(f"Phase 2 check failed: {e}")

# Test 6: Tuple unpacking in Phase 2
print("\n[6/8] Checking tuple unpacking...")
try:
    source = inspect.getsource(optimize_unified.main)
    if 'cfg, agg, count = _evaluate_filter_wrapper_lookup' in source or 'for cfg, agg, count in iterator' in source:
        print("  ✅ Correct 3-tuple unpacking found")
    else:
        warnings.append("Tuple unpacking pattern not confirmed")
except Exception as e:
    errors.append(f"Tuple check failed: {e}")

# Test 7: evaluate_filter_lookup returns 3 values
print("\n[7/8] Checking evaluate_filter_lookup return signature...")
try:
    source = inspect.getsource(optimize_unified.evaluate_filter_lookup)
    if 'return filter_cfg, aggregated, total_processed_signals' in source:
        print("  ✅ Returns 3 values (filter_cfg, aggregated, total_processed_signals)")
    elif 'return filter_cfg, {}, 0' in source:
        print("  ✅ Returns 3 values (empty case: filter_cfg, {}, 0)")
    else:
        errors.append("evaluate_filter_lookup signature unclear")
except Exception as e:
    errors.append(f"Return signature check failed: {e}")

# Test 8: Syntax check
print("\n[8/8] Checking Python syntax...")
try:
    import py_compile
    py_compile.compile('deep_research/optimize_unified.py', doraise=True)
    print("  ✅ Syntax OK")
except py_compile.PyCompileError as e:
    errors.append(f"Syntax error: {e}")
    print(f"  ❌ Syntax error: {e}")

# Summary
print("\n" + "=" * 70)
print("VERIFICATION SUMMARY")
print("=" * 70)

if errors:
    print(f"\n❌ ERRORS ({len(errors)}):")
    for e in errors:
        print(f"  - {e}")
else:
    print("\n✅ No critical errors found")

if warnings:
    print(f"\n⚠️ WARNINGS ({len(warnings)}):")
    for w in warnings:
        print(f"  - {w}")

if not errors:
    print("\n🚀 READY TO RUN!")
    print("   python3 deep_research/optimize_unified.py --workers 12")
else:
    print("\n⛔ FIX ERRORS BEFORE RUNNING!")

sys.exit(1 if errors else 0)
