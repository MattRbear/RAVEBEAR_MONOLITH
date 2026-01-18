import time
from datetime import datetime, timezone


TIMEFRAME_MS = {
    "1m": 60000,
    "5m": 300000,
    "15m": 900000,
    "1h": 3600000,
    "4h": 14400000,
    "1d": 86400000,
}

ALLOWED_TIMEFRAMES = set(TIMEFRAME_MS.keys())


def timeframe_to_ms(timeframe):
    if timeframe not in TIMEFRAME_MS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return TIMEFRAME_MS[timeframe]


def floor_time(ms, duration):
    return (int(ms) // int(duration)) * int(duration)


def utc_date_parts(ms):
    dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    return dt.year, dt.month, dt.day


def now_ms():
    return int(time.time() * 1000)
