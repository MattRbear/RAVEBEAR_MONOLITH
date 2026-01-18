"""
PRE-FLIGHT CHECKLIST
====================
Must pass ALL checks before foundation is considered production-ready.

Run: python preflight.py

Exit codes:
  0 = All checks passed
  1 = Critical failures
  2 = Warnings only
"""
import sys
import importlib
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path("data")
REQUIRED_MODULES = [
    "aiohttp",
    "pyarrow",
    "pandas",
    "yaml",
]


def check(name: str, condition: bool, critical: bool = True) -> tuple:
    """Run a check, return (passed, is_critical)."""
    status = "✅" if condition else ("❌" if critical else "⚠️")
    print(f"  {status} {name}")
    return condition, critical


def main():
    print("=" * 60)
    print("  PRE-FLIGHT CHECKLIST")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)
    
    failures = 0
    warnings = 0
    
    # ============================================
    print("\n[DEPENDENCIES]")
    # ============================================
    
    for mod in REQUIRED_MODULES:
        try:
            importlib.import_module(mod)
            passed = True
        except ImportError:
            passed = False
        
        ok, crit = check(f"Module: {mod}", passed)
        if not ok:
            failures += 1
    
    # ============================================
    print("\n[CONFIGURATION]")
    # ============================================
    
    config_path = Path("config.yaml")
    ok, _ = check("config.yaml exists", config_path.exists())
    if not ok:
        failures += 1
    else:
        import yaml
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        
        ok, _ = check("venues configured", "venues" in cfg and len(cfg["venues"]) > 0)
        if not ok:
            failures += 1
        
        ok, _ = check("storage path configured", "storage" in cfg)
        if not ok:
            failures += 1
    
    # ============================================
    print("\n[DIRECTORY STRUCTURE]")
    # ============================================
    
    ok, _ = check("data/ directory exists", DATA_DIR.exists())
    if not ok:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        print("      → Created data/")
    
    logs_dir = Path("logs")
    ok, _ = check("logs/ directory exists", logs_dir.exists())
    if not ok:
        logs_dir.mkdir(parents=True, exist_ok=True)
        print("      → Created logs/")
    
    # ============================================
    print("\n[SOURCE FILES]")
    # ============================================
    
    required_files = [
        "collector/__init__.py",
        "collector/adapters/coinbase.py",
        "collector/adapters/kraken.py",
        "collector/adapters/okx.py",
        "collector/storage/parquet_writer.py",
        "collector/validation/validators.py",
        "collector/core/health.py",
        "collector/runtime_multi_fixed.py",
        "start_collector_fixed.py",
    ]
    
    for f in required_files:
        ok, _ = check(f"File: {f}", Path(f).exists())
        if not ok:
            failures += 1
    
    # ============================================
    print("\n[IMPORT TESTS]")
    # ============================================
    
    try:
        from collector.adapters.coinbase import CoinbaseAdapter
        ok = True
    except Exception as e:
        ok = False
        print(f"      Error: {e}")
    check("Import CoinbaseAdapter", ok)
    if not ok:
        failures += 1
    
    try:
        from collector.adapters.kraken import KrakenAdapter
        ok = True
    except Exception as e:
        ok = False
        print(f"      Error: {e}")
    check("Import KrakenAdapter", ok)
    if not ok:
        failures += 1
    
    try:
        from collector.adapters.okx import OKXAdapter
        ok = True
    except Exception as e:
        ok = False
        print(f"      Error: {e}")
    check("Import OKXAdapter", ok)
    if not ok:
        failures += 1
    
    try:
        from collector.storage.parquet_writer import ParquetStorage
        ok = True
    except Exception as e:
        ok = False
        print(f"      Error: {e}")
    check("Import ParquetStorage", ok)
    if not ok:
        failures += 1
    
    try:
        from collector.validation.validators import CandleValidator
        ok = True
    except Exception as e:
        ok = False
        print(f"      Error: {e}")
    check("Import CandleValidator", ok)
    if not ok:
        failures += 1
    
    # ============================================
    print("\n[RUNTIME TEST]")
    # ============================================
    
    try:
        from collector.runtime_multi_fixed import MultiVenueRuntime
        from collector.config import load_config  # FIXED: correct import path
        
        cfg = load_config("config.yaml")
        runtime = MultiVenueRuntime(cfg)
        ok = True
    except Exception as e:
        ok = False
        print(f"      Error: {e}")
    check("Runtime initializes", ok)
    if not ok:
        failures += 1
    
    # ============================================
    print("\n[DATA INTEGRITY TOOLS]")
    # ============================================
    
    ok, _ = check("audit_data.py exists", Path("audit_data.py").exists(), critical=False)
    if not ok:
        warnings += 1
    
    ok, _ = check("monitor_integrity.py exists", Path("monitor_integrity.py").exists(), critical=False)
    if not ok:
        warnings += 1
    
    ok, _ = check("check_data.py exists", Path("check_data.py").exists(), critical=False)
    if not ok:
        warnings += 1
    
    # ============================================
    # SUMMARY
    # ============================================
    print("\n" + "=" * 60)
    if failures == 0 and warnings == 0:
        print("  ✅ ALL CHECKS PASSED - READY FOR PRODUCTION")
        exit_code = 0
    elif failures == 0:
        print(f"  ⚠️  {warnings} WARNING(S) - Review before production")
        exit_code = 2
    else:
        print(f"  ❌ {failures} FAILURE(S) - NOT READY")
        exit_code = 1
    print("=" * 60)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
