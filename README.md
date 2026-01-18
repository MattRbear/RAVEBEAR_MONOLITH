# RAVEBEAR MONOLITH

## Unified Trading Intelligence System

**Created:** January 17, 2026
**Purpose:** Single source of truth for all proven trading infrastructure
**Mission:** AI Debate Room + Premium Discord + Live Copilot + Streaming Analysis

---

## ARCHITECTURE OVERVIEW

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         RAVEBEAR MONOLITH                                    │
│                    (Telemetry-First Market Intelligence)                     │
└─────────────────────────────────────────────────────────────────────────────┘

01_FOUNDATION ────────► Self-healing orchestrator, ACID database, rate limiting
        │
02_DATA_COLLECTORS ───► OKX feeds, whale tracking, derivatives, validators
        │
03_SIGNAL_ENGINES ────► Wick detector, CVD, VWAP, Magnet Score
        │
04_FILTERS_GATES ─────► Institutional gates, OBI, ladder stability
        │
05_BACKTESTER ────────► TruthEngine (funding, slippage, liquidations)
        │
06_ANALYTICS ─────────► Object factories, pattern detection, session analysis
        │
07_CANDLE_COLLECTOR ──► Multi-venue parquet storage (data spine)
        │
08_WHALE_INTEL ───────► Whale Alert, Etherscan, Moralis, rate limiting
        │
09_CORRELATION ───────► USDT.D tracker, derivatives correlation, event ledger
        │
10_DISCORD ───────────► Alert system for premium users
```

---

## DIRECTORY STRUCTURE

```
RAVEBEAR_MONOLITH/
├── 01_FOUNDATION/         # Core infrastructure (self-healing, database)
│   ├── core/              # Orchestrator, health checks, rate limiting
│   └── data/              # ACID SQLite, Pydantic models
│
├── 02_DATA_COLLECTORS/    # All data ingestion
│   └── feeds/             # OKX, whale, derivatives feeds
│
├── 03_SIGNAL_ENGINES/     # Pattern detection
│   ├── wick/              # Untouched wick detector
│   ├── scoring/           # Magnet Score calculator
│   ├── cvd/               # CVD divergence
│   └── vwap/              # Session-anchored VWAP
│
├── 04_FILTERS_GATES/      # Noise reduction
│   └── institutional_gates.py  # OBI, walls, ladder stability
│
├── 05_BACKTESTER/         # Strategy validation
│   └── truth_engine.py    # Realistic simulation
│
├── 06_ANALYTICS/          # Statistical analysis
│   ├── level_factory.py   # Poor High/Low detection
│   ├── box_factory.py     # Consolidation zones
│   └── scoring_engine.py  # Quality scoring
│
├── 07_CANDLE_COLLECTOR/   # Production data spine
│   ├── collector/         # WebSocket handlers
│   └── src/               # Core modules
│
├── 08_WHALE_INTEL/        # On-chain tracking
│   ├── clients/           # API clients
│   └── rate_limiting/     # Budget management
│
├── 09_CORRELATION/        # Cross-market analysis
│   ├── usdt_d_collector.py
│   └── correlation_analyzer_v2.py
│
├── 10_DISCORD/            # Alert delivery
│   └── discord_alerts_v2.py
│
└── _DOCS/                 # Documentation
    ├── SYSTEM_ATLAS.md
    ├── INVENTORY.md
    └── EXTRACTION_MANIFEST.md
```

---

## DEVELOPMENT RULE

> **fail-closed + tests required per module**
>
> Every module must have passing tests before merge. If a test is missing or failing, the module is considered broken. No exceptions.

---

## WHAT'S INCLUDED

### TIER S - Mission Critical

- **Orchestrator** - Self-healing process supervisor
- **Database Layer** - ACID SQLite with WAL mode
- **Institutional Gates** - OBI, ladder stability, wall detection
- **TruthEngine** - Backtesting with funding/slippage simulation

### TIER A - High Value

- **Wick Detector** - Untouched wick edge (68K+ events proven)
- **Magnet Score** - Signal quality calculator
- **CVD Core** - Orderflow divergence detection
- **VWAP Calculator** - Session-anchored retail zones
- **Object Factories** - Poor H/L, Boxes, Stacks

### TIER A - Data Collection

- **Multi-Venue Candle Collector** - Production parquet storage
- **OKX WebSocket** - Real-time trades/orderbook
- **Whale Alert Client** - Large transaction tracking
- **USDT.D Tracker** - Stablecoin dominance correlation

---

## USE CASES

### 1. AI Debate Room

Multiple AI agents analyzing market using:

- Real telemetry from 07_CANDLE_COLLECTOR
- Signal detection from 03_SIGNAL_ENGINES
- Filtering through 04_FILTERS_GATES
- Correlation context from 09_CORRELATION

### 2. Premium Discord

- High-confidence alerts only (Magnet Score ≥85)
- Whale movement notifications
- USDT.D correlation warnings
- Zone defense triggers

### 3. Live Copilot

- Streaming market commentary
- Real-time confluence detection
- Institutional gate status
- Risk assessment

### 4. Backtesting

- Validate strategies before capital
- TruthEngine with realistic simulation
- Historical event replay

---

## EDGE FORMULA

Entry confluence required:

1. ✅ USDT.D near path end
2. ✅ Whale activity aligned
3. ✅ Untouched wicks (1H/15M/5M) aligned
4. ✅ Institutional gates pass
5. = SEND IT

---

## NOT INCLUDED (Rejected)

- Path-hardcoded RaveQuant modules
- Untested INDEX system
- Broken wick_collector_v4_l2 main files
- Copy-paste duplicates
- Experimental TODOs

**Trust is earned. This code earned it.**

---

## NEXT STEPS

1. [ ] Create unified config.yaml
2. [ ] Build orchestrator entry point
3. [ ] Wire signal engines to data collectors
4. [ ] Connect Discord alerts
5. [ ] Test end-to-end flow
6. [ ] Deploy AI debate room

---

_Built by RaveBear / Astral Bear-ly Projected_
_"The market hunts me. I hunt back."_
