import time
import requests
import json
from datetime import datetime, timezone
import sys

from config import settings, secrets
from utils import get_logger, AlarmManager, safe_write_json, safe_read_json

logger = get_logger("discord_alerts")

class HardenedAlertSystem:
    def __init__(self):
        self.queue_path = settings.DATA_VAULT_DIR / "discord_queue.json"
        self.webhook_url = secrets.DISCORD_WEBHOOK_URL
        self.queue = self._load_queue()
        
        # Cooldown state: {alert_type_id: last_sent_ts}
        self.cooldowns = {} 
        self.cooldown_config = {
            'whale_dump': 3600,      # 1 hour
            'funding_extreme': 7200, # 2 hours
            'level_approach': 1800,  # 30 min
            'confluence': 7200,      # 2 hours
            'liq_spike': 3600,       # 1 hour
            'price_alert': 300,      # 5 min
            'system_error': 600,     # 10 min
        }

    def _load_queue(self):
        return safe_read_json(self.queue_path, default=[])

    def _save_queue(self):
        safe_write_json(self.queue_path, self.queue)

    def send_alert(self, alert_type: str, message: str, data: dict = None, level: str = "INFO"):
        """Enqueue an alert."""
        # 1. Check Cooldown
        # Use a unique key for cooldowns (e.g., 'level_approach:87000')
        cooldown_key = alert_type
        if data and 'id' in data:
             cooldown_key = f"{alert_type}:{data['id']}"
        
        if self._is_cooled_down(cooldown_key, alert_type):
            logger.info(f"Alert suppressed by cooldown: {cooldown_key}")
            return

        # 2. Enqueue
        alert = {
            'id': f"{alert_type}:{int(time.time()*1000)}",
            'type': alert_type,
            'level': level,
            'message': message,
            'data': data or {},
            'created_at': datetime.now(timezone.utc).isoformat(),
            'attempts': 0,
            'sent': False
        }
        
        self.queue.append(alert)
        self._save_queue()
        
        # 3. Update Cooldown
        self.cooldowns[cooldown_key] = time.time()
        
        # 4. Process immediately
        self._process_queue()

    def _is_cooled_down(self, key: str, type_name: str) -> bool:
        limit = self.cooldown_config.get(type_name, 300)
        last = self.cooldowns.get(key, 0)
        return (time.time() - last) < limit

    def _process_queue(self):
        """Process pending alerts."""
        if not self.webhook_url:
            return

        # Filter pending
        pending = [a for a in self.queue if not a['sent']]
        
        for alert in pending:
            if alert['attempts'] >= 5:
                continue # Give up
            
            success = self._send_to_discord(alert)
            alert['attempts'] += 1
            
            if success:
                alert['sent'] = True
                alert['sent_at'] = datetime.now(timezone.utc).isoformat()
            else:
                # Stop processing if network fails to preserve order/backoff
                break
        
        # Cleanup old sent alerts (keep last 50 or 1 hour)
        cutoff = time.time() - 3600
        self.queue = [
            a for a in self.queue 
            if not a['sent'] or 
            (datetime.fromisoformat(a['sent_at'].replace("Z", "+00:00")).timestamp() > cutoff)
        ]
        self._save_queue()

    def _send_to_discord(self, alert):
        try:
            color_map = {
                'CRITICAL': 0xFF0000,
                'ERROR': 0xE74C3C,
                'WARNING': 0xF1C40F,
                'INFO': 0x3498DB
            }
            color = color_map.get(alert['level'], 0x95A5A6)

            embed = {
                "title": f"[{alert['level']}] {alert['type'].replace('_', ' ').title()}",
                "description": alert['message'],
                "color": color,
                "timestamp": alert['created_at'],
                "fields": []
            }
            
            if alert['data']:
                for k, v in alert['data'].items():
                    embed["fields"].append({"name": k, "value": str(v)[:1024], "inline": True})

            resp = requests.post(self.webhook_url, json={"embeds": [embed]}, timeout=5)
            
            if resp.status_code == 429:
                logger.warning("Discord Rate Limit hit")
                time.sleep(int(resp.headers.get("Retry-After", 5)))
                return False
                
            return resp.status_code in [200, 204]
            
        except Exception as e:
            logger.error(f"Discord send failed: {e}")
            return False

if __name__ == "__main__":
    # Test
    system = HardenedAlertSystem()
    system.send_alert("system_test", "Hardened Alert System Online", level="INFO")