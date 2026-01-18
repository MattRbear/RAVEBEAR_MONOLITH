# BEAR CORE UNIFIED - Code Extraction Manifest
**Created:** 2026-01-14  
**Purpose:** Extract ONLY proven, working code from 10+ parallel systems

---

## PHILOSOPHY
**Trust is earned, not assumed.**  
Only code that has PROVEN reliability gets migrated.  
No "it should work" - only "it DID work".

---

## EXTRACTION MAP

### 01_FOUNDATION (from Titan)
**Source:** `/New folder/titan/`  
**Why:** Clean architecture, never broke trust  
**Files to extract:**
- `core/orchestrator.py` → Self-healing orchestrator with auto-restart
- `core/health_check.py` → Heartbeat monitoring (60s interval)
- `core/rate_limiter.py` → Exponential backoff rate limiting
- `data/models.py` → Pydantic validation models
- `data/database.py` → ACID SQLite with WAL mode
- `strategies/base_strategy.py` → Strategy abstraction

**Modifications needed:**
- Adapt models.py to include all event types from other systems
- Add Poor High/Low, Box events from Candle_collector

---

### 02_DATA_COLLECTORS (from RaveQuant + ALPHA)
**Sources:** 
- `/New folder/RaveQuant/Trades_Bot/trades_exporter.py`
- `/New folder/ALPHA/feeds/okx_trades.py`
- `/New folder/ALPHA/feeds/whale_alert.py`
- `/New folder/Candle_collector/okx_collector.py`

**Why:** Proven to collect data without corruption  
**Files to extract:**
- OKX WebSocket connector (use ALPHA version - has reconnect logic)
- Whale Alert integration (ALPHA only)
- Stablecoin flow tracker (Candle_collector)
- Coinalyze derivatives feed (RaveQuant)

**Merge strategy:**
- Single `okx_feed.py` - merge best parts of ALPHA + RaveQuant
- Keep whale_alert.py as-is from ALPHA
- Add backfill logic from microstructure-engine

---

### 03_SIGNAL_ENGINES (from ALPHA + RaveQuant + wick_collector_v4_l2)
**Sources:**
- `/New folder/ALPHA/detectors/wick_detector.py`
- `/New folder/ALPHA/analysis/scorer.py` (Magnet Score)
- `/New folder/RaveQuant/Untouch_Wick/wick_detector.py`
- `/New folder/CVD DASH/cvd_core.py`
- `/New folder/RaveQuant/VWAP/vwap_calculator.py`
- `/New folder/Candle_collector/wick_factory.py`

**Why:** These have proven to detect patterns correctly  
**Files to extract:**
- `wick_engine.py` - Merge ALPHA scorer + RaveQuant detector
- `cvd_engine.py` - Use CVD DASH implementation (cleanest)
- `vwap_engine.py` - RaveQuant session-anchored version
- `liquidity_engine.py` - Extract L2 bucket logic from RaveQuant

**Merge strategy:**
- Wick engine: Use ALPHA's Magnet Score + RaveQuant's quality metrics
- Keep untouched wick tracking from ALPHA (proven reliable)
- Add geometry calculations from Candle_collector analytics

---

### 04_FILTERS_GATES (from microstructure-engine + INDEX)
**Sources:**
- `/New folder/microstructure-engine/institutional_gates.py`
- `/New folder/microstructure-engine/alpha.py` (OBI calculation)
- `/New folder/INDEX/consensus.py` (Geometric mean spreads)
- `/New folder/wick_collector_v4_l2/core/zone_defense.py`

**Why:** Best noise filters - prevent bad trades  
**Files to extract:**
- `institutional_gates.py` - Full module as-is
- `orderbook_filters.py` - OBI, ladder stability, wall detection
- `consensus_validator.py` - Cross-exchange spread checks
- `zone_defense.py` - L2 wall detection

**Modifications:**
- Make thresholds configurable per symbol (BTC vs PEPE differ)
- Add fail-closed mode (block trades if gates fail to calculate)

---

### 05_BACKTESTER (from Backtester)
**Sources:**
- `/New folder/Backtester/truth_engine.py`
- `/New folder/Backtester/institutional.py`
- `/New folder/Backtester/position.py`

**Why:** Correct simulation of exchange reality  
**Files to extract:**
- `truth_engine.py` - Full module (funding, slippage, liquidations)
- `institutional.py` - FVG, BOS, structure breaks
- `position.py` - Position tracking with PnL

**Modifications:**
- Create `vault_data_loader.py` - Load JSONL from RaveQuant vault
- Make fees configurable (OKX tier-based)
- Add latency simulation (variable, not constant 100ms)

---

### 06_ANALYTICS (from Candle_collector)
**Sources:**
- `/New folder/Candle_collector/bot_wick_touch_times.py`
- `/New folder/Candle_collector/level_factory.py`
- `/New folder/Candle_collector/box_factory.py`
- `/New folder/Candle_collector/event_ledger.py`

**Why:** Proven statistical analysis, correlation tracking  
**Files to extract:**
- `object_factories.py` - Poor levels, boxes, stacks
- `correlation_tracker.py` - Event ledger with market state
- `session_analyzer.py` - Hourly volatility patterns

**Modifications:**
- Integrate with Titan's database instead of JSONL
- Add proper state persistence (atomic writes)

---

### 07_UTILITIES (from multiple)
**Sources:**
- `/New folder/ALPHA/storage/jsonl_writer.py`
- `/New folder/microstructure-engine/persistence.py`
- `/New folder/Backtester/results_manager.py`
- `/New folder/Candle_collector/utils/logger.py`

**Why:** Proven utility functions  
**Files to extract:**
- `atomic_writer.py` - temp + fsync + rename pattern
- `structured_logger.py` - JSON logging with context
- `time_utils.py` - Timezone handling, ms/s conversion

---

### 08_CONFIG (new - unified config)
**Create from scratch:**
- `settings.yaml` - Master config (symbols, timeframes, thresholds)
- `secrets.yaml` - API keys (gitignored, load from env)
- `strategies.yaml` - Entry/exit parameters per strategy

**Why:** Single source of truth, no path hardcoding

---

## EXTRACTION RULES

### ✅ DO EXTRACT:
- Code with proven reliability (ran 24h+ without crash)
- Math/logic that has passed backtests
- Database/file operations with atomic writes
- Reconnection logic with exponential backoff
- Validation that has caught real errors

### ❌ DO NOT EXTRACT:
- Experimental code with "TODO: test this"
- Hardcoded paths to specific folders
- Config scattered across files
- Copy-pasted logic (extract once)
- Code with commented-out sections (sign of uncertainty)

---

## MIGRATION VERIFICATION

**For each extracted file, RUN:**
1. Linting: `ruff check file.py`
2. Type check: `mypy file.py`
3. Unit test: Create `test_file.py` with edge cases
4. Integration test: Run with real (but small) data sample

**No file goes into BEAR_CORE_UNIFIED without passing all 4.**

---

## TRUST LADDER

**Tier 1 (Deploy Immediately):**
- Titan orchestrator
- microstructure-engine gates
- Backtester TruthEngine

**Tier 2 (Deploy After Testing):**
- ALPHA wick detector + scorer
- RaveQuant confluence scoring
- CVD calculation

**Tier 3 (Rebuild If Needed):**
- Data collectors (merge required)
- Discord alerts (consolidate)
- Dashboard UI (choose one)

---

## NEXT STEPS

1. Extract Tier 1 foundation (Titan core)
2. Extract Tier 1 filters (microstructure gates)
3. Extract Tier 1 backtester (TruthEngine)
4. Test foundation + filters + backtester together
5. Only THEN add Tier 2 signal engines
6. Only THEN add Tier 3 data collection

**Build from reliability outward, not features inward.**

---

## REJECTION LOG

**Files NOT extracted and why:**
- `RaveQuant/run_all.py` - Path hardcoding breaks immediately
- `INDEX/main.py` - Never deployed, untested
- `wick_collector_v4_l2/main_ws.py` - Too many "FIXES NEEDED" flags
- Any file with "AUDIT_FINDINGS.txt" pointing to P0 failures

**Rejection means: extract the IDEA, rewrite from scratch with proper guards.**
