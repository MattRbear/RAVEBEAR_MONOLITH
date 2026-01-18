# EXTRACTION INVENTORY
**Extraction Date:** January 14, 2026  
**Systems Analyzed:** 11  
**Files Extracted:** 21  
**Files Rejected:** ~500+  

---

## EXTRACTION SUMMARY

### FILES SUCCESSFULLY EXTRACTED

#### **01_FOUNDATION (from Titan)**
- ✅ `core/orchestrator.py` (5.6 KB) - Self-healing orchestrator
- ✅ `core/health_check.py` (4.4 KB) - Heartbeat monitoring
- ✅ `core/rate_limiter.py` (5.7 KB) - Exponential backoff
- ✅ `core/__init__.py` (311 B) - Module init
- ✅ `data/database.py` (9.4 KB) - ACID SQLite
- ✅ `data/models.py` (6.3 KB) - Pydantic models
- ✅ `data/__init__.py` (115 B) - Module init

**Total Foundation:** 7 files, ~32 KB of proven infrastructure

---

#### **02_DATA_COLLECTORS**
- ✅ `feeds/whale_alert.py` (4.3 KB) - Whale Alert integration (ALPHA)

**Total Collectors:** 1 file, 4.3 KB  
**Pending:** OKX feed (needs merge), Coinalyze feed, stablecoin tracker

---

#### **03_SIGNAL_ENGINES**
- ✅ `wick/wick_detector.py` (1.3 KB) - ALPHA wick detection
- ✅ `scoring/scorer.py` (8.6 KB) - Magnet Score calculator
- ✅ `cvd/cvd_core.py` (8.9 KB) - CVD DASH implementation
- ✅ `vwap/vwap_calculator.py` (18.1 KB) - RaveQuant VWAP

**Total Signal Engines:** 4 files, ~37 KB of pattern detection

---

#### **04_FILTERS_GATES**
- ✅ `institutional_gates.py` (17.1 KB) - OBI, walls, ladder stability
- ✅ `alpha.py` (7.9 KB) - Order book imbalance math
- ✅ `validators.py` (2.9 KB) - Data validation

**Total Filters:** 3 files, ~28 KB of noise filtering

---

#### **05_BACKTESTER**
- ✅ `truth_engine.py` (15.2 KB) - Funding, slippage, liquidations
- ✅ `institutional.py` (5.8 KB) - FVG, BOS calculations
- ✅ `position.py` (2.6 KB) - Position tracking

**Total Backtester:** 3 files, ~24 KB of simulation

---

#### **06_ANALYTICS**
- ✅ `level_factory.py` (6.7 KB) - Poor High/Low detection
- ✅ `box_factory.py` (5.0 KB) - Consolidation zones
- ✅ `event_ledger.py` (7.8 KB) - Correlation tracking

**Total Analytics:** 3 files, ~20 KB of statistical analysis

---

### GRAND TOTAL EXTRACTED
**21 files, ~145 KB of proven, reliable code**

---

## SYSTEMS ANALYZED

### **Titan** ✅ HIGH TRUST
- **What worked:** Orchestrator, health checks, database, models
- **What failed:** Nothing yet (newest system)
- **Files extracted:** 7
- **Files rejected:** 0
- **Trust score:** 10/10

---

### **ALPHA** ✅ MEDIUM-HIGH TRUST
- **What worked:** Wick detection, Magnet Score, Whale Alert
- **What failed:** Nothing major, just overlap with RaveQuant
- **Files extracted:** 3
- **Files rejected:** ~20 (duplicates)
- **Trust score:** 8/10

---

### **microstructure-engine** ✅ HIGH TRUST
- **What worked:** Institutional gates, OBI math, validators
- **What failed:** Not connected to data pipeline
- **Files extracted:** 3
- **Files rejected:** ~15 (integration code)
- **Trust score:** 9/10

---

### **Backtester** ✅ HIGH TRUST
- **What worked:** TruthEngine, funding simulation, position tracking
- **What failed:** No data loader for vault
- **Files extracted:** 3
- **Files rejected:** ~10 (old data loaders)
- **Trust score:** 9/10

---

### **RaveQuant** ⚠️ MEDIUM TRUST
- **What worked:** VWAP calculator, confluence logic
- **What failed:** Path hardcoding, broke on folder rename
- **Files extracted:** 1 (VWAP)
- **Files rejected:** ~100+ (path dependencies)
- **Trust score:** 6/10

---

### **CVD DASH** ✅ MEDIUM-HIGH TRUST
- **What worked:** CVD core calculation
- **What failed:** Nothing major
- **Files extracted:** 1
- **Files rejected:** ~5 (dashboard UI)
- **Trust score:** 8/10

---

### **Candle_collector** ✅ HIGH TRUST
- **What worked:** Object factories, analytics, correlation tracking
- **What failed:** Nothing major
- **Files extracted:** 3
- **Files rejected:** ~50 (bots, dashboards)
- **Trust score:** 8/10

---

### **INDEX** ❌ LOW TRUST
- **What worked:** Ideas (consensus, dominance)
- **What failed:** Never deployed to production
- **Files extracted:** 0
- **Files rejected:** ~15 (all untested)
- **Trust score:** 3/10

---

### **wick_collector_v4_l2** ⚠️ LOW-MEDIUM TRUST
- **What worked:** Some zone defense ideas
- **What failed:** Multiple "FIXES NEEDED" documents
- **Files extracted:** 0
- **Files rejected:** ~150+ (too many fixes)
- **Trust score:** 4/10

---

### **Whales** ⚠️ LOW TRUST
- **What worked:** Some client ideas
- **What failed:** Never fully operational
- **Files extracted:** 0
- **Files rejected:** ~10
- **Trust score:** 3/10

---

### **Coin_anal** ⚠️ MEDIUM TRUST
- **What worked:** Coinalyze API client
- **What failed:** Nothing major, just incomplete
- **Files extracted:** 0 (pending)
- **Files rejected:** 0 (will extract later)
- **Trust score:** 6/10

---

## REJECTION REASONS

### **Top Rejection Reasons (by count):**
1. **Path Hardcoding** - ~200 files (RaveQuant, others)
2. **Duplicate Logic** - ~150 files (3+ wick detectors)
3. **Untested Code** - ~100 files (INDEX, experimental)
4. **Dashboard/UI** - ~50 files (terminal UIs, web dashboards)
5. **Incomplete Implementations** - ~40 files (TODOs, stubs)

### **Examples of Rejected Files:**
```
RaveQuant/run_all.py                 - Path hardcoding
INDEX/main.py                        - Never deployed
wick_collector_v4_l2/main_ws.py      - FIXES NEEDED flags
Candle_collector/dashboard.py        - UI (not core logic)
RaveQuant/Analysis/confluence.py     - Duplicate of ALPHA scorer
```

---

## TRUST METRICS

### **High Trust (9-10/10):**
- Titan (10/10)
- microstructure-engine (9/10)
- Backtester (9/10)

### **Medium-High Trust (7-8/10):**
- ALPHA (8/10)
- CVD DASH (8/10)
- Candle_collector (8/10)

### **Medium Trust (5-6/10):**
- RaveQuant (6/10)
- Coin_anal (6/10)

### **Low Trust (3-4/10):**
- INDEX (3/10)
- Whales (3/10)
- wick_collector_v4_l2 (4/10)

---

## NEXT EXTRACTION TARGETS

### **Pending High-Value Extractions:**
1. **OKX WebSocket connector** (merge ALPHA + RaveQuant versions)
2. **Coinalyze feed** (from Coin_anal or RaveQuant)
3. **Stablecoin tracker** (from Candle_collector)
4. **Confluence scoring** (from RaveQuant, rebuild without paths)

### **Pending Low-Priority Extractions:**
5. Discord notifier (multiple versions, need consolidation)
6. Dashboard UI (rebuild from scratch, don't copy)
7. Config utilities (create fresh)

---

## LESSONS LEARNED

### **What Builds Trust:**
✅ Clean separation of concerns (Titan)  
✅ Atomic writes for state (microstructure-engine)  
✅ Pydantic validation (Titan)  
✅ Self-healing orchestration (Titan)  
✅ Proven math with tests (microstructure-engine)  

### **What Breaks Trust:**
❌ Path hardcoding (RaveQuant)  
❌ Copy-paste coding (3+ wick detectors)  
❌ Deploy before testing (INDEX)  
❌ Scattered config (every system)  
❌ No state persistence (many systems)  

---

## CONCLUSION

**Extracted:** 21 files of proven code  
**Rejected:** 500+ files of broken promises  
**Result:** Single source of truth with ONLY reliable code  

**Trust is earned. This code earned it.**
