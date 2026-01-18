# RAVEBEAR TRADING INFRASTRUCTURE
## System Overview - December 2025

---

## DIRECTORY STRUCTURE

```
C:\Users\M.R Bear\Documents\
├── Candle_collector\         # Main analysis system
├── CVD DASH\                 # Cumulative Volume Delta
├── Coin_anal\                # Coinalyze derivatives data
├── Data_Vault\               # All stored data
│   ├── 1m_Candles\           # BTC 1m OHLCV (43K+ candles)
│   ├── Analytics\            # Bot analysis outputs
│   ├── Correlations\         # Market snapshots & events
│   ├── Objects\              # Level/wick/box objects
│   └── Stablecoin_Flow\      # USDT/USDC supply data
└── clones\                   # Backups
```

---

## DASHBOARDS (Terminal UIs)

| Dashboard | Location | Command | What it does |
|-----------|----------|---------|--------------|
| **Level Dashboard** | Candle_collector\dashboard.py | `python dashboard.py` | Tracks wicks & poor levels, logs touches with full market state |
| **CVD Dashboard** | CVD DASH\cvd_dashboard.py | `python cvd_dashboard.py` | Real-time CVD from Kraken trades |
| **Coinalyze Dashboard** | Coin_anal\coinalyze_dashboard.py | `python coinalyze_dashboard.py` | OI, Funding, L/S Ratio, Liquidations |

---

## DATA COLLECTORS

| Collector | Location | Command | Data Source |
|-----------|----------|---------|-------------|
| **OKX Candles** | Candle_collector\okx_collector.py | `python okx_collector.py --continuous` | OKX REST API |
| **Stablecoin Flow** | Candle_collector\stablecoin_collector.py | `python stablecoin_collector.py --continuous` | Base44 API |
| **CVD Trades** | CVD DASH\cvd_collector.py | (runs via dashboard) | Kraken WebSocket |
| **Derivatives Tracker** | Candle_collector\derivatives_tracker.py | `python derivatives_tracker.py --continuous 5` | Coinalyze API |

---

## ANALYSIS BOTS (run once to process data)

| Bot | What it analyzes |
|-----|------------------|
| bot_candle_stats.py | Range, body, wick percentiles |
| bot_wick_touch_times.py | Time from wick creation to touch |
| bot_session_analysis.py | Hourly volatility patterns |
| bot_big_moves.py | Large candles (>0.3% range) |
| bot_volume_spikes.py | Volume explosions (>3x avg) |
| bot_level_clusters.py | Wick density zones |
| bot_consolidations.py | Tight range periods |

**Run all:** `python run_all_bots.py`

---

## OBJECT FACTORIES (generate tradeable objects)

| Factory | What it creates | Scoring |
|---------|-----------------|---------|
| level_factory.py | Poor Highs/Lows | Quality (taps, volume, cleanliness) |
| wick_factory.py | Untouched Wicks | Quality + Freshness |
| box_factory.py | Consolidation Boxes | Tightness + Duration |
| origin_factory.py | Displacement Zones | Magnitude + Volume |
| stack_detector.py | Confluence Stacks | Density + Diversity |
| pressure_map.py | Above/Below summary | Resistance vs Support |

**Run all:** `python run_object_system.py`

---

## CORRELATION TRACKING

| File | Purpose |
|------|---------|
| event_ledger.py | Logs level touches with full market state |
| derivatives_tracker.py | Takes periodic market snapshots |
| correlation_analyzer_v2.py | Finds patterns in historical data |

**Data logged per event:**
- BTC price
- USDT supply & net flow
- Open Interest
- Funding rate
- Long/Short ratio
- Which level was touched

---

## API KEYS USED

| API | Key Location | Rate Limit |
|-----|--------------|------------|
| Base44 (Stablecoin) | In code | Unknown |
| Coinalyze | all env.txt | 40/min |
| OKX | Public | Standard |
| Kraken | Public WebSocket | None |

---

## QUICK START

**1. Collect data:**
```
cd "C:\Users\M.R Bear\Documents\Candle_collector"
python okx_collector.py --continuous
```

**2. Run dashboards (separate terminals):**
```
# Terminal 1: Levels
python dashboard.py

# Terminal 2: CVD
cd "C:\Users\M.R Bear\Documents\CVD DASH"
python cvd_dashboard.py

# Terminal 3: Derivatives
cd "C:\Users\M.R Bear\Documents\Coin_anal"
python coinalyze_dashboard.py
```

**3. Run correlation tracker (background):**
```
python derivatives_tracker.py --continuous 5
```

**4. Check correlations:**
```
python correlation_analyzer_v2.py
```

---

## KEY FINDINGS FROM DATA

**From 43K+ 1m candles:**
- Median wick touch time: 3 minutes
- 84.4% of wicks touched within 30 minutes
- Most volatile hours: 14:00-16:00 UTC
- Big moves tend to reverse, not continue
- Volume spikes = 45.6% continuation (coin flip)

**Current market state (live):**
- 69.3% longs / 30.7% shorts = LONG HEAVY
- Funding: +1% (longs paying shorts)
- Interpretation: Overcrowded long, squeeze potential

---

## EDGE FORMULA (from user)

Entry confluence required:
1. USDT.D near path end
2. Nansen whales aligned
3. Whale Alert activity
4. Untouched wicks (1H/15M/5M) aligned
5. = SEND IT

VWAP = retail activation zones (MM handoff points)
