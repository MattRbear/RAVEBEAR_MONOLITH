# Flint's Whale Intelligence System

Institutional-grade whale analysis via hybrid API infrastructure.

## Overview

This system implements a **Waterfall Validation** architecture where API credits are treated as scarce currency. Each layer gates the next, ensuring maximum alpha extraction with minimum resource consumption.

```
[Whale Alert WS] → Trigger (>$500k)
        ↓
[Alchemy] → Validate state
        ↓ Gate
[Etherscan] → Decode contracts  
        ↓ Gate (>$10M)
[Moralis] → Cross-chain check
        ↓ Gate (unidentified)
[Dune] → Historical forensics
        ↓
[Alpha Score] → Filter → Alert
```

## Quickstart

### 1. Prerequisites

- Python 3.11+
- PostgreSQL 15+ (with TimescaleDB extension recommended)
- API keys (see `.env` file)

### 2. Installation

```bash
cd "C:\Users\M.R Bear\Documents\Whales_New_Flint's"
pip install -r requirements.txt
```

### 3. Configuration

Edit `.env` file with your API keys:

```bash
# Required
WHALE_ALERT_API_KEY=your_key_here
ETHERSCAN_API_KEY=your_key_here
MORALIS_API_KEY=your_key_here
DUNE_API_KEY=your_key_here
TOKEN_METRICS_API_KEY=your_key_here

# Optional but recommended
ALCHEMY_API_KEY=your_key_here  # Free at alchemy.com
DISCORD_WEBHOOK_WHALE=your_webhook_url
```

### 4. Test Configuration

```bash
python config.py
```

Expected output:
```
✅ Configuration loaded successfully!
  Whale Alert:   ✅
  Etherscan:     ✅
  ...
```

### 5. Database Setup

```bash
# Create database
createdb whale_intel

# Run schema migration (when available)
python -m db.schema
```

### 6. Run System

```bash
python main.py
```

## Data Integrity / Fail-Closed Behavior

This system is designed to **fail-closed** on any data integrity issue:

- **Missing API response**: Logged, queued for retry, no downstream processing
- **Rate limit hit**: Backoff with jitter, no silent drops
- **Database write failure**: Atomic rollback, explicit exception
- **Invalid data**: Validation rejects, logged with context
- **Stale data**: Drift detection triggers alerts

### No Silent Fallbacks

The system will NEVER:
- Guess at missing data
- Continue processing with partial information
- Silently drop events
- Use cached data past TTL without flagging

## Troubleshooting

### Config Test Fails

```
❌ Configuration Error: WHALE_ALERT_API_KEY is required
```
→ Ensure `.env` file exists and contains the key

### Rate Limit Errors

```
429 Too Many Requests from Etherscan
```
→ Token bucket should prevent this. Check `rate_limiting/` logs

### Database Connection Failed

```
psycopg2.OperationalError: could not connect
```
→ Ensure PostgreSQL is running: `pg_isready -h localhost`

### WebSocket Disconnects

```
Whale Alert WS disconnected, reconnecting...
```
→ Normal behavior. Exponential backoff handles reconnection.

## API Rate Limits

| Provider      | Limit                    | Our Budget Strategy          |
|---------------|--------------------------|------------------------------|
| Whale Alert   | Premium (unlimited)      | Primary trigger              |
| Alchemy       | 30M CU/month, 330/sec    | WebSocket subs, Multicall    |
| Etherscan     | 5 RPS, 100k/day          | Token bucket, ABI cache      |
| Moralis       | 40k CU/day               | >$10M transactions only      |
| Dune          | 2500 credits/month       | ~8 queries/day, top 1%       |
| Token Metrics | 500/month                | >$5M, non-stablecoin only    |

## File Structure

```
Whales_New_Flint's/
├── FLINT_MASTER_PLAN.txt   # Claude context file
├── .env                    # API keys (DO NOT COMMIT)
├── config.py               # Configuration loader
├── requirements.txt        # Dependencies
├── README.md               # This file
├── clients/                # API clients
├── db/                     # Database layer
├── rate_limiting/          # Rate limit infrastructure
├── engines/                # Analysis engines
├── alerts/                 # Alert delivery
└── tests/                  # Test suite
```

## Development Status

- [x] Phase 0: Config & Environment
- [ ] Phase 1: Database Foundation
- [ ] Phase 2: Rate Limiting Infrastructure
- [ ] Phase 3: Whale Alert Client
- [ ] Phase 4: Alchemy Client
- [ ] Phase 5: Etherscan Client
- [ ] Phase 6: Moralis Client
- [ ] Phase 7: Dune Client
- [ ] Phase 8: Token Metrics Client
- [ ] Phase 9: Clustering Engine
- [ ] Phase 10: Alpha Scoring
- [ ] Phase 11: Alert System
- [ ] Phase 12: Dashboard & Orchestrator

## License

Private use only. RaveBear / Astral Bear-ly Projected.
