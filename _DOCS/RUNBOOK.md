# RUNBOOK - Multi-Venue Candle Collector

**Version**: 1.0  
**Last Updated**: 2026-01-10

---

## TABLE OF CONTENTS

1. [Installation](#installation)
2. [Running the Collector](#running-the-collector)
3. [Compaction](#compaction)
4. [Daily QA Checks](#daily-qa-checks)
5. [Crash Recovery Drill](#crash-recovery-drill)
6. [Health State Interpretation](#health-state-interpretation)
7. [Common Issues](#common-issues)
8. [Monitoring](#monitoring)

---

## INSTALLATION

### Prerequisites
- Python 3.12+ installed
- Windows PowerShell 5.1+
- 10GB+ free disk space
- Internet connection for API access

### Bootstrap

```powershell
# 1. Clone/download repository
cd "C:\path\to\Muklti-candles-collection"

# 2. Run bootstrap script
.\bootstrap.ps1
```

**What it does**:
- Creates virtual environment (`.venv`)
- Installs pinned dependencies
- Validates/creates `config.yaml`
- Runs full test suite (45+ tests)

**Expected output**: `✓ Bootstrap complete!`

### Configuration

Edit `config.yaml`:

```yaml
venues:
  - coinbase
  - kraken
  - okx

symbols:
  - BTC-USD
  - ETH-USD
  - SOL-USD

timeframes:
  - 1m
  - 5m
  - 1h

data_dir: data
log_level: INFO
```

---

## RUNNING THE COLLECTOR

### Start Collector

```powershell
.\run.ps1
```

**What it does**:
- Activates virtual environment
- Validates config
- Creates `data/` and `logs/` directories
- Starts collector with logging

**Expected output**:
```
INFO: Collector started
INFO: Venues: coinbase, kraken, okx
INFO: Streams: 45 (3 venues × 5 symbols × 3 timeframes)
INFO: Health: HEALTHY
INFO: WS connections: 3/3 active
```

### Stop Collector

Press `Ctrl+C` to gracefully stop.

**What happens**:
- WebSocket connections closed
- In-flight candles flushed to disk
- Gap detector state saved
- Health monitor state saved

---

## COMPACTION

### Compact All Partitions

```powershell
.\compact.ps1 -All
```

**What it does**:
- Scans all partitions in `data/`
- Compacts CLOSED partitions only (not current day)
- Deduplicates by PK: `(venue, symbol, timeframe, open_time_ms)`
- Sorts by `open_time_ms`
- Generates `_manifest.json` with SHA256 hash

**Expected output**:
```
[3/3] Running compaction...
  - coinbase/BTC-USD/1m/2026-01-09: ✓ 120 files → 1 file
  - kraken/ETH-USD/1m/2026-01-09: ✓ 95 files → 1 file
  ...
✓ Compaction complete
```

### Compact Specific Stream

```powershell
.\compact.ps1 -Venue coinbase -Symbol BTC-USD -Timeframe 1m
```

### Force Compact Current Day (USE WITH CAUTION)

```powershell
.\compact.ps1 -All -Force
```

**⚠️ WARNING**: This can cause race conditions with active writers. Only use when collector is stopped.

---

## DAILY QA CHECKS

### Run QA Script

```powershell
.\qa.ps1
```

**What it checks**:
1. **PK Uniqueness**: No cross-file duplicates per stream
2. **Time Alignment**: All timestamps aligned to timeframe grid
3. **Data Quality**: No NaN/inf/negative values
4. **OHLC Validity**: `high >= max(open, close)`, `low <= min(open, close)`
5. **Finality**: All candles have `is_closed=True`
6. **Gap Detection**: No gaps > 5 minutes
7. **Manifest Validation**: SHA256 hashes match

**Expected output**:
```
CHECK 1: PK Uniqueness (Cross-File)
  ✅ PASS: No cross-file duplicate PKs found (45 streams checked)
...
✅ PASS: All integrity checks passed
```

### Custom QA Parameters

```powershell
# Scan last 7 days
.\qa.ps1 -Days 7

# Custom data directory
.\qa.ps1 -DataDir "C:\backups\data"

# Custom report file
.\qa.ps1 -Report "weekly_qa.json"
```

---

## CRASH RECOVERY DRILL

### Procedure

1. **Start collector** and let run for 30 minutes:
   ```powershell
   .\run.ps1
   ```

2. **Record last timestamps** (check logs or data files)

3. **Simulate crash**:
   ```powershell
   # Force kill process
   taskkill /F /IM python.exe
   ```

4. **Wait 5 minutes** (simulate downtime)

5. **Restart collector**:
   ```powershell
   .\run.ps1
   ```

6. **Verify gap detection** (check logs):
   ```
   INFO: event=gap_detected stream=coinbase/BTC-USD/1m gap_minutes=5
   INFO: event=backfill_start stream=coinbase/BTC-USD/1m
   ```

7. **Wait for backfill** to complete (10-15 minutes)

8. **Run QA**:
   ```powershell
   .\qa.ps1
   ```

### Pass Criteria

✅ Gap detector identifies all gaps  
✅ Backfill completes within 15 minutes  
✅ QA checks all PASS  
✅ No duplicate PKs  
✅ No missing candles in gap window  

---

## HEALTH STATE INTERPRETATION

### State Machine

```
HEALTHY → DEGRADED → DOWN → HEALTHY
   ↓          ↓         ↓
 (3 fails) (10 fails) (3 successes)
```

### States

**HEALTHY**:
- All operations succeeding
- < 3 failures in last 60 minutes
- **Action**: None, normal operation

**DEGRADED**:
- 3-9 failures in last 60 minutes
- Still retrying
- **Action**: Monitor closely, check logs for patterns

**DOWN**:
- 10+ failures in last 60 minutes
- Stopped retrying
- **Action**: Investigate immediately, check:
  - Venue API status pages
  - Network connectivity
  - API keys/credentials
  - Rate limits

### Recovery

**DOWN → HEALTHY**:
- Requires 3 consecutive successes
- Automatic when venue recovers
- Gap backfill triggers automatically

### Alerts

Alerts sent on state transitions only (no spam):

```
ALERT [WARNING] coinbase: State transition: HEALTHY → DEGRADED | Failures: 3/10 in 60m window | Reason: timeout
ALERT [CRITICAL] coinbase: State transition: DEGRADED → DOWN | Failures: 10/10 in 60m window | Reason: connection_error
ALERT [INFO] coinbase: State transition: DOWN → HEALTHY | Failures: 0/10 in 60m window | Reason: 3 consecutive successes
```

---

## COMMON ISSUES

### Issue 1: Venue DOWN

**Symptoms**:
- State transitions to DOWN
- No new candles from venue
- Logs show connection errors

**Diagnosis**:
1. Check venue status page (e.g., status.coinbase.com)
2. Review logs: `Get-Content logs\collector.log -Tail 100`
3. Check health state: Look for `ALERT [CRITICAL]` messages

**Resolution**:
- If venue API down: Wait for recovery, backfill will auto-trigger
- If config issue: Fix `config.yaml`, restart collector
- If rate limited: Wait for cooldown, check retry logs

### Issue 2: Disk Full

**Symptoms**:
- Write failures in logs
- Collector crashes
- `ALERT [CRITICAL] disk_full`

**Diagnosis**:
```powershell
Get-PSDrive C | Select-Object Used,Free
```

**Resolution**:
1. Run compaction: `.\compact.ps1 -All`
2. Delete old test data: `Remove-Item test_output -Recurse`
3. Archive old partitions to external storage
4. Add disk space

### Issue 3: Memory Leak

**Symptoms**:
- Memory usage grows over time
- Collector slows down
- Eventually crashes

**Diagnosis**:
```powershell
Get-Process python | Select-Object WS,PM
```

**Resolution**:
1. Restart collector
2. Review logs for excessive buffering
3. Reduce stream count in `config.yaml`
4. Report issue with logs

### Issue 4: Duplicate PKs

**Symptoms**:
- QA check fails: `❌ FAIL: PK Uniqueness`
- `qa_report.json` shows duplicates

**Diagnosis**:
```powershell
.\qa.ps1
# Review qa_report.json
```

**Resolution**:
1. Stop collector
2. Run compaction (deduplicates automatically):
   ```powershell
   .\compact.ps1 -All
   ```
3. Verify with QA:
   ```powershell
   .\qa.ps1
   ```
4. Restart collector

### Issue 5: Missing Candles (Gaps)

**Symptoms**:
- QA check fails: `❌ FAIL: Gap Detection`
- Gaps > 5 minutes detected

**Diagnosis**:
```powershell
.\qa.ps1
# Review qa_report.json for gap details
```

**Resolution**:
1. Check if exchange was online during gap (status pages)
2. If exchange was online:
   - Gap detector should auto-backfill on next run
   - Check cooldown state: `data/.gap_detector_state.json`
3. If gap persists:
   - Manually trigger backfill (future feature)
   - Report issue with logs

---

## MONITORING

### Log Files

**Location**: `logs/collector.log`

**Key Events**:
```
INFO: event=candle_written venue=coinbase symbol=BTC-USD timeframe=1m
WARNING: event=rate_limited status=429 retry_after=60s
ERROR: event=connection_error venue=kraken error=timeout
CRITICAL: event=state_transition old_state=DEGRADED new_state=DOWN
```

### Health Checks

**Every 30 seconds** (logged):
```
INFO: HEALTH CHECK | Overall: HEALTHY
INFO: - coinbase: HEALTHY | WS: ✓ | Recv: 1234 | Written: 1200
INFO: - kraken: DEGRADED | WS: ✓ | Recv: 890 | Written: 850
INFO: - okx: DOWN | WS: ✗ | Recv: 0 | Written: 0
```

### Metrics to Track

- **Memory**: Should be < 500MB stable
- **CPU**: < 10% idle, < 50% during backfill
- **File count**: < 200 files/day with compaction
- **Error rate**: < 1% after warmup
- **Gap count**: 0 after initial backfill

### Daily Routine

1. **Morning**: Run QA check
   ```powershell
   .\qa.ps1
   ```

2. **Review logs** for errors:
   ```powershell
   Get-Content logs\collector.log | Select-String "ERROR|CRITICAL"
   ```

3. **Check disk space**:
   ```powershell
   Get-PSDrive C
   ```

4. **Run compaction** (if needed):
   ```powershell
   .\compact.ps1 -All
   ```

---

## SUPPORT

**Documentation**:
- `README.md`: Overview and quick start
- `BUILD_PLAN.txt`: Architecture and contracts
- `REMEDIATION.md`: Security fixes and test evidence
- `BOOTSTRAP_PLAN.txt`: Installation steps

**Test Suite**:
```powershell
pytest -v  # Run all tests
pytest test_finality.py -v  # Run specific test
```

**Logs**:
- Collector: `logs/collector.log`
- QA reports: `qa_report.json`
- Gap state: `data/.gap_detector_state.json`
