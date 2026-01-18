import time
import requests
import sys
from config import settings, secrets
from utils import get_logger, AlarmManager, rate_limiter

logger = get_logger("derivatives")

class HardenedDerivativesCollector:
    def __init__(self):
        self.alarms = AlarmManager()
        self.limiter = rate_limiter.limiter_coinalyze
        self.consecutive_failures = 0
        self.max_failures = 10

    def fetch_oi(self):
        """Fetch Open Interest safely."""
        if not self.limiter.acquire():
            return None

        try:
            url = f"{secrets.COINALYZE_API_URL}/open-interest"
            params = {"symbols": "BTCUSDT_PERP.A", "convert_to_usd": "true", "api_key": secrets.COINALYZE_API_KEY}
            
            resp = requests.get(url, params=params, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                # Validate response structure
                if data and isinstance(data, list) and len(data) > 0:
                    self.consecutive_failures = 0
                    return data[0] # Return first symbol
                else:
                    self.alarms.send("ERROR", "Empty derivatives data")
            
            elif resp.status_code == 429:
                self.limiter.report_429()
            elif resp.status_code == 401:
                self.alarms.send("CRITICAL", "Coinalyze API Key Invalid (401)")
                # In a real app we might trigger shutdown here
            else:
                self.alarms.send("ERROR", f"Coinalyze HTTP {resp.status_code}")
                
        except Exception as e:
            self.alarms.send("ERROR", f"Derivatives fetch failed: {e}")
            
        self.consecutive_failures += 1
        return None

if __name__ == "__main__":
    collector = HardenedDerivativesCollector()
    print(collector.fetch_oi())