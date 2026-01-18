# Multi-Venue Candle Collector

**Production-grade 1-minute OHLCV data collection from multiple crypto exchanges.**

## Philosophy

This is a **data foundation layer**. Every downstream system (signals, bots, analysis) depends on this being correct. We are **anal** about:

- **No silent failures** - Errors are loud, not swallowed
- **No corrupt data** - Validation at every stage
- **No gaps without knowing** - Gap detection runs continuously
- **No duplicates** - Dedup is built-in
- **Fail-closed** - If uncertain, reject the candle

## Quick Start

```powershell
# 1. Activate environment
cd "C:\Users\M.R Bear\Documents\Muklti-candles-collection"
.\.venv\Scripts\Activate.ps1

# 2. Run pre-flight checks
python preflight.py

# 3. Start collector
python start_collector_fixed.py --config config.yaml

# 4. (Optional) Run integrity monitor in separate terminal
python monitor_integrity.py
```

## Verification Commands

| Command | Purpose |
|---------|---------|
| `python preflight.py` | Pre-flight checks - run before starting |
| `python audit_data.py` | Full data integrity audit |
| `python monitor_integrity.py` | Live continuous monitoring |
| `python check_data.py` | Quick data freshness check |

## Expected Output

```
============================================================
  MULTI-VENUE CANDLE COLLECTOR
  Venues: coinbase, kraken, okx
============================================================

  Starting WebSocket streams...

======================================================================
  STATUS: HEALTHY  |  Uptime: 5 min  |  12:00:00
======================================================================
  VENUE        STATUS             RECV    SAVED   RECONN
----------------------------------------------------------------------
  COINBASE     [OK]                 50       45        0
  KRAKEN       [OK]                 30       28        0
  OKX          [OK]                 60       55        0
======================================================================
```

## Status Indicators

| Status | Meaning |
|--------|---------|
| `[OK]` | Connected, receiving and saving data |
| `[WAIT]` | Connected, waiting for first candle |
| `[DISC]` | Disconnected, attempting reconnect |
| `[STALE (Xm)]` | No data received for X minutes |

## Columns

| Column | Meaning |
|--------|---------|
| RECV | Candles received from WebSocket |
| SAVED | Candles written to parquet |
| RECONN | Total reconnection attempts |

## Data Schema

Each candle has these fields:

```python
{
    "venue": str,           # "coinbase", "kraken", "okx"
    "symbol": str,          # "BTC-USD", "BTC/USD", "BTC-USDT"
    "timeframe": str,       # "1m", "5m", "15m", "1h", "4h", "1d"
    "open_time_ms": int,    # Unix timestamp (ms) of candle open
    "close_time_ms": int,   # Unix timestamp (ms) of candle close
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "volume": float,
    "quote_volume": float,  # May be null
    "vwap": float,          # May be null
    "trades_count": int,    # May be null
    "is_closed": bool,
    "source": str,          # "websocket", "rest", "aggregated"
    "ingest_time_ms": int,  # When we received it
}
```

## Directory Structure

```
data/
├── coinbase/
│   ├── BTC-USD/
│   │   ├── 1m/
│   │   │   └── 2026-01-10.parquet
│   │   └── 5m/
│   └── ETH-USD/
├── kraken/
│   ├── BTC-USD/     # Note: stored with normalized symbol
│   └── ETH-USD/
└── okx/
    ├── BTC-USDT/
    └── ETH-USDT/
```

## Validation Rules

Candles are **rejected** if:

- `high < low`
- `high < open` or `high < close`
- `low > open` or `low > close`
- Any price ≤ 0
- Volume < 0
- `open_time_ms` not aligned to timeframe
- Duplicate (same venue/symbol/timeframe/open_time)
- Out of order (older than last saved)

## Fail-Closed Behavior

| Scenario | Behavior |
|----------|----------|
| Invalid OHLC | Candle rejected, logged |
| Duplicate | Silently skipped |
| WebSocket disconnect | Auto-reconnect with backoff |
| Parse error | Message skipped, logged |
| File write error | Exception raised, process continues |

## Logging

- **Console**: Warnings and errors only (clean output)
- **File**: `logs/collector.log` (full DEBUG level)
- **Status**: Every 30 seconds to console

## Configuration (config.yaml)

```yaml
venues:
  coinbase:
    enabled: true
    symbols:
      - BTC-USD
      - ETH-USD
      # ...

gap_detection:
  enabled: false  # Enable after initial data collection
  lookback_days: 3

aggregation:
  enabled: true
  base_timeframe: "1m"
```

## Troubleshooting

### "Status shows [DISC] constantly"

1. Check internet connection
2. Check WebSocket URLs in config
3. Look at `logs/collector.log` for errors

### "SAVED count not increasing"

1. This is normal for first 1-2 minutes (buffering)
2. Check that RECV is increasing
3. Run `python check_data.py` to verify files

### "Duplicate rejection spam"

1. This was fixed - duplicates are now silently skipped
2. If still seeing errors, restart collector

### "Gap detection errors on startup"

1. Gap detection is disabled by default
2. Enable only after you have initial data

## Production Checklist

Before considering this layer "locked":

- [ ] `python preflight.py` passes
- [ ] Collector runs 10+ minutes with no RECONN
- [ ] `python audit_data.py` passes
- [ ] All three venues showing [OK]
- [ ] SAVED counts incrementing every minute
- [ ] No ERROR messages in console

## Next Steps (After Foundation Locked)

1. Enable gap detection/backfill
2. Add more symbols
3. Bolt on signal modules
4. Connect to trading systems

---

**Remember: This is the data spine. If it's wrong, everything is wrong.**
