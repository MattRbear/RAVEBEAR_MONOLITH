"""
RaveBear Discord Alert System
Monitors: Whale Flow, Derivatives, Levels, Confluence
Sends alerts to Discord webhook
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(r"C:\Users\M.R Bear\Documents\Whales")))
sys.path.insert(0, str(Path(r"C:\Users\M.R Bear\Documents\Coin_anal")))
sys.path.insert(0, str(Path(r"C:\Users\M.R Bear\Documents\Candle_collector")))

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("pip install pandas numpy requests")
    sys.exit(1)

# Force unbuffered output with safe encoding
import functools
import io
import sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
print = functools.partial(print, flush=True)

# Discord Webhook
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1435559676916797442/p-CVNHGuGGnmieCxuSZvddT0eTsa3P6QjLt-gjyDiKFAet98JlJI7MajVbeDmC-4R34v"

# Data paths
CANDLE_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles\BTC_USDT_SWAP_1m.parquet")
LEVELS_PATH = Path(r"C:\Users\M.R Bear\Documents\Candle_collector\levels_state.json")
ALERT_LOG_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\alert_log.json")

# Thresholds
WHALE_FLOW_THRESHOLD = 10_000_000      # $10M
FUNDING_EXTREME_THRESHOLD = 0.0005      # 0.05%
LS_IMBALANCE_THRESHOLD = 70             # 70% one side
OI_SPIKE_THRESHOLD = 0.03               # 3% change
LEVEL_APPROACH_DISTANCE = 150           # $150 from level
LIQUIDATION_SPIKE_THRESHOLD = 5_000_000 # $5M in period


class DiscordAlerter:
    def __init__(self, webhook_url=DISCORD_WEBHOOK):
        self.webhook_url = webhook_url
        self.sent_alerts = self.load_alert_log()
        self.last_state = {}
    
    def load_alert_log(self):
        if ALERT_LOG_PATH.exists():
            with open(ALERT_LOG_PATH) as f:
                return json.load(f)
        return {'alerts': [], 'last_values': {}}
    
    def save_alert_log(self):
        self.sent_alerts['alerts'] = self.sent_alerts['alerts'][-500:]
        with open(ALERT_LOG_PATH, 'w') as f:
            json.dump(self.sent_alerts, f, indent=2)
    
    def send_discord(self, title, description, color=0x00ff00, fields=None):
        """Send embed to Discord."""
        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": "RaveBear Alert System"}
        }
        
        if fields:
            embed["fields"] = fields
        
        payload = {"embeds": [embed]}
        
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            if resp.status_code == 204:
                print(f"  [DISCORD] Sent: {title}")
                return True
            else:
                print(f"  [DISCORD] Failed: {resp.status_code}")
                return False
        except Exception as e:
            print(f"  [DISCORD] Error: {e}")
            return False
    
    def already_alerted(self, alert_type, key, cooldown_minutes=30):
        """Check if we already sent this alert recently."""
        alert_id = f"{alert_type}:{key}"
        
        for alert in self.sent_alerts['alerts'][-100:]:
            if alert.get('id') == alert_id:
                alert_time = datetime.fromisoformat(alert.get('time', '2000-01-01'))
                if datetime.now(timezone.utc) - alert_time < timedelta(minutes=cooldown_minutes):
                    return True
        return False
    
    def log_alert(self, alert_type, key):
        """Log that we sent an alert."""
        self.sent_alerts['alerts'].append({
            'id': f"{alert_type}:{key}",
            'type': alert_type,
            'time': datetime.now(timezone.utc).isoformat()
        })
        self.save_alert_log()


class MarketMonitor:
    def __init__(self):
        self.alerter = DiscordAlerter()
        self.whale_client = None
        self.coinalyze_client = None
        
        # Try imports
        try:
            from whale_client import WhaleAlertClient
            self.whale_client = WhaleAlertClient()
        except:
            print("Whale client not available")
        
        try:
            from coinalyze_client import CoinalyzeClient
            self.coinalyze_client = CoinalyzeClient()
        except:
            print("Coinalyze client not available")
        
        self.last_check = {}
    
    def get_btc_price(self):
        try:
            df = pd.read_parquet(CANDLE_PATH)
            return float(df.iloc[-1]['close'])
        except:
            return None
    
    def get_levels(self):
        try:
            with open(LEVELS_PATH) as f:
                return json.load(f)
        except:
            return {'wicks': [], 'poors': []}
    
    def check_whale_alerts(self):
        """Check for whale flow alerts."""
        if not self.whale_client:
            return
        
        try:
            btc_txs = self.whale_client.get_btc_transactions(hours=1, min_value=5000000)
            if not btc_txs:
                return
            
            analysis = self.whale_client.analyze_flow(btc_txs)
            net_flow = analysis.get('net_exchange_flow', 0)
            inflow = analysis.get('exchange_inflow', 0)
            outflow = analysis.get('exchange_outflow', 0)
            
            # Whale dump alert
            if inflow > WHALE_FLOW_THRESHOLD:
                if not self.alerter.already_alerted('whale_dump', 'inflow', 60):
                    self.alerter.send_discord(
                        "🐋 WHALE DUMP DETECTED",
                        f"**${inflow/1e6:.1f}M BTC** moved TO exchanges in last hour",
                        color=0xff0000,
                        fields=[
                            {"name": "Net Flow", "value": f"${net_flow/1e6:+.1f}M", "inline": True},
                            {"name": "Signal", "value": "BEARISH - Selling pressure incoming", "inline": True}
                        ]
                    )
                    self.alerter.log_alert('whale_dump', 'inflow')
            
            # Whale accumulation alert
            if outflow > WHALE_FLOW_THRESHOLD:
                if not self.alerter.already_alerted('whale_accum', 'outflow', 60):
                    self.alerter.send_discord(
                        "🐋 WHALE ACCUMULATION",
                        f"**${outflow/1e6:.1f}M BTC** withdrawn FROM exchanges in last hour",
                        color=0x00ff00,
                        fields=[
                            {"name": "Net Flow", "value": f"${net_flow/1e6:+.1f}M", "inline": True},
                            {"name": "Signal", "value": "BULLISH - Smart money buying", "inline": True}
                        ]
                    )
                    self.alerter.log_alert('whale_accum', 'outflow')
        
        except Exception as e:
            print(f"Whale check error: {e}")
    
    def check_derivatives_alerts(self):
        """Check funding, L/S, OI alerts."""
        if not self.coinalyze_client:
            return
        
        try:
            # Get current data
            oi_data = self.coinalyze_client.get_open_interest()
            funding_data = self.coinalyze_client.get_funding_rate()
            ls_history = self.coinalyze_client.get_long_short_ratio_history(interval="1hour", hours=2)
            liq_history = self.coinalyze_client.get_liquidation_history(interval="1hour", hours=2)
            
            # Funding extreme
            if funding_data:
                funding = funding_data[0].get('value', 0)
                
                if abs(funding) > FUNDING_EXTREME_THRESHOLD:
                    direction = "POSITIVE" if funding > 0 else "NEGATIVE"
                    signal = "Longs paying shorts - LONG SQUEEZE potential" if funding > 0 else "Shorts paying longs - SHORT SQUEEZE potential"
                    
                    if not self.alerter.already_alerted('funding_extreme', direction, 120):
                        self.alerter.send_discord(
                            "💰 EXTREME FUNDING RATE",
                            f"Funding: **{funding*100:.4f}%** ({direction})",
                            color=0xffaa00,
                            fields=[
                                {"name": "Signal", "value": signal, "inline": False}
                            ]
                        )
                        self.alerter.log_alert('funding_extreme', direction)
            
            # L/S Imbalance
            if ls_history:
                latest = ls_history[-1]
                long_pct = latest.get('l', 50)
                short_pct = latest.get('s', 50)
                
                if long_pct > LS_IMBALANCE_THRESHOLD:
                    if not self.alerter.already_alerted('ls_imbalance', 'long_heavy', 120):
                        self.alerter.send_discord(
                            "⚖️ LONG HEAVY POSITIONING",
                            f"**{long_pct:.1f}%** of positions are LONG",
                            color=0xff6600,
                            fields=[
                                {"name": "Ratio", "value": f"{long_pct:.1f}% / {short_pct:.1f}%", "inline": True},
                                {"name": "Signal", "value": "Crowded long - SQUEEZE TARGET", "inline": True}
                            ]
                        )
                        self.alerter.log_alert('ls_imbalance', 'long_heavy')
                
                elif short_pct > LS_IMBALANCE_THRESHOLD:
                    if not self.alerter.already_alerted('ls_imbalance', 'short_heavy', 120):
                        self.alerter.send_discord(
                            "⚖️ SHORT HEAVY POSITIONING", 
                            f"**{short_pct:.1f}%** of positions are SHORT",
                            color=0x0066ff,
                            fields=[
                                {"name": "Ratio", "value": f"{long_pct:.1f}% / {short_pct:.1f}%", "inline": True},
                                {"name": "Signal", "value": "Crowded short - SQUEEZE TARGET", "inline": True}
                            ]
                        )
                        self.alerter.log_alert('ls_imbalance', 'short_heavy')
            
            # OI Spike
            if oi_data:
                current_oi = oi_data[0].get('value', 0)
                last_oi = self.alerter.sent_alerts.get('last_values', {}).get('oi', current_oi)
                
                if last_oi > 0:
                    oi_change = (current_oi - last_oi) / last_oi
                    
                    if abs(oi_change) > OI_SPIKE_THRESHOLD:
                        direction = "INCREASE" if oi_change > 0 else "DECREASE"
                        
                        if not self.alerter.already_alerted('oi_spike', direction, 60):
                            self.alerter.send_discord(
                                "📊 OI SPIKE DETECTED",
                                f"Open Interest **{oi_change*100:+.1f}%** in last check",
                                color=0x9900ff,
                                fields=[
                                    {"name": "Current OI", "value": f"${current_oi/1e9:.2f}B", "inline": True},
                                    {"name": "Signal", "value": f"Big positions {'opening' if oi_change > 0 else 'closing'}", "inline": True}
                                ]
                            )
                            self.alerter.log_alert('oi_spike', direction)
                
                self.alerter.sent_alerts.setdefault('last_values', {})['oi'] = current_oi
            
            # Liquidation spike
            if liq_history:
                recent_liqs = liq_history[-2:] if len(liq_history) >= 2 else liq_history
                total_long_liq = sum(l.get('l', 0) for l in recent_liqs)
                total_short_liq = sum(l.get('s', 0) for l in recent_liqs)
                total_liq = total_long_liq + total_short_liq
                
                if total_liq > LIQUIDATION_SPIKE_THRESHOLD:
                    dominant = "LONGS" if total_long_liq > total_short_liq else "SHORTS"
                    
                    if not self.alerter.already_alerted('liq_spike', dominant, 60):
                        self.alerter.send_discord(
                            "🔥 LIQUIDATION CASCADE",
                            f"**${total_liq/1e6:.1f}M** liquidated in last 2 hours",
                            color=0xff0066,
                            fields=[
                                {"name": "Long Liqs", "value": f"${total_long_liq/1e6:.1f}M", "inline": True},
                                {"name": "Short Liqs", "value": f"${total_short_liq/1e6:.1f}M", "inline": True},
                                {"name": "Dominant", "value": f"{dominant} getting rekt", "inline": False}
                            ]
                        )
                        self.alerter.log_alert('liq_spike', dominant)
        
        except Exception as e:
            print(f"Derivatives check error: {e}")
    
    def check_level_alerts(self):
        """Check for level approach and touch alerts."""
        try:
            price = self.get_btc_price()
            if not price:
                return
            
            levels = self.get_levels()
            all_levels = levels.get('wicks', []) + levels.get('poors', [])
            
            for lvl in all_levels:
                lvl_price = lvl.get('price', 0)
                lvl_type = lvl.get('type', 'WICK')
                lvl_dir = lvl.get('dir', '')
                distance = abs(price - lvl_price)
                
                # Level approach alert
                if distance < LEVEL_APPROACH_DISTANCE and distance > 50:
                    direction = "above" if lvl_price > price else "below"
                    alert_key = f"{lvl_price:.0f}"
                    
                    if not self.alerter.already_alerted('level_approach', alert_key, 30):
                        self.alerter.send_discord(
                            "🎯 LEVEL APPROACHING",
                            f"Price within **${distance:.0f}** of {lvl_type} level",
                            color=0x00ffff,
                            fields=[
                                {"name": "Level", "value": f"${lvl_price:,.2f}", "inline": True},
                                {"name": "Current", "value": f"${price:,.2f}", "inline": True},
                                {"name": "Direction", "value": f"Level is {direction}", "inline": True}
                            ]
                        )
                        self.alerter.log_alert('level_approach', alert_key)
        
        except Exception as e:
            print(f"Level check error: {e}")
    
    def check_confluence_alert(self):
        """Check for multiple signals aligning."""
        signals = []
        
        try:
            # Check whale flow
            if self.whale_client:
                btc_txs = self.whale_client.get_btc_transactions(hours=2, min_value=5000000)
                if btc_txs:
                    analysis = self.whale_client.analyze_flow(btc_txs)
                    net = analysis.get('net_exchange_flow', 0)
                    if net > 5_000_000:
                        signals.append(("Whale Inflow", "BEARISH"))
                    elif net < -5_000_000:
                        signals.append(("Whale Outflow", "BULLISH"))
            
            # Check derivatives
            if self.coinalyze_client:
                funding_data = self.coinalyze_client.get_funding_rate()
                ls_history = self.coinalyze_client.get_long_short_ratio_history(interval="1hour", hours=1)
                
                if funding_data:
                    funding = funding_data[0].get('value', 0)
                    if funding > 0.0003:
                        signals.append(("High Funding", "BEARISH"))
                    elif funding < -0.0003:
                        signals.append(("Negative Funding", "BULLISH"))
                
                if ls_history:
                    long_pct = ls_history[-1].get('l', 50)
                    if long_pct > 65:
                        signals.append(("Long Heavy", "BEARISH"))
                    elif long_pct < 35:
                        signals.append(("Short Heavy", "BULLISH"))
            
            # Check for confluence
            bullish = len([s for s in signals if s[1] == "BULLISH"])
            bearish = len([s for s in signals if s[1] == "BEARISH"])
            
            if bullish >= 3:
                if not self.alerter.already_alerted('confluence', 'bullish', 120):
                    signal_text = "\n".join([f"✅ {s[0]}" for s in signals if s[1] == "BULLISH"])
                    self.alerter.send_discord(
                        "⚡ BULLISH CONFLUENCE",
                        f"**{bullish} bullish signals aligned!**",
                        color=0x00ff00,
                        fields=[
                            {"name": "Signals", "value": signal_text, "inline": False}
                        ]
                    )
                    self.alerter.log_alert('confluence', 'bullish')
            
            elif bearish >= 3:
                if not self.alerter.already_alerted('confluence', 'bearish', 120):
                    signal_text = "\n".join([f"❌ {s[0]}" for s in signals if s[1] == "BEARISH"])
                    self.alerter.send_discord(
                        "⚡ BEARISH CONFLUENCE", 
                        f"**{bearish} bearish signals aligned!**",
                        color=0xff0000,
                        fields=[
                            {"name": "Signals", "value": signal_text, "inline": False}
                        ]
                    )
                    self.alerter.log_alert('confluence', 'bearish')
        
        except Exception as e:
            print(f"Confluence check error: {e}")
    
    def run(self, check_interval=120):
        """Run the alert monitor."""
        print("=" * 60)
        print("  RAVEBEAR DISCORD ALERT SYSTEM")
        print("=" * 60)
        print(f"  Check interval: {check_interval}s")
        print(f"  Webhook: ...{self.alerter.webhook_url[-20:]}")
        print("=" * 60)
        
        # Send startup message
        self.alerter.send_discord(
            "ALERT SYSTEM STARTED",
            "RaveBear monitoring active",
            color=0x00ff00,
            fields=[
                {"name": "Monitoring", "value": "Whales, Derivatives, Levels, Confluence", "inline": False}
            ]
        )
        
        try:
            while True:
                now = datetime.now().strftime('%H:%M:%S')
                print(f"\n[{now}] Running checks...")
                
                # Whale checks (every 3 min due to rate limits)
                if time.time() - self.last_check.get('whale', 0) > 180:
                    print("  Checking whale flow...")
                    self.check_whale_alerts()
                    self.last_check['whale'] = time.time()
                
                # Derivatives checks (every 2 min)
                if time.time() - self.last_check.get('derivs', 0) > 120:
                    print("  Checking derivatives...")
                    self.check_derivatives_alerts()
                    self.last_check['derivs'] = time.time()
                
                # Level checks (every 1 min)
                if time.time() - self.last_check.get('levels', 0) > 60:
                    print("  Checking levels...")
                    self.check_level_alerts()
                    self.last_check['levels'] = time.time()
                
                # Confluence check (every 5 min)
                if time.time() - self.last_check.get('confluence', 0) > 300:
                    print("  Checking confluence...")
                    self.check_confluence_alert()
                    self.last_check['confluence'] = time.time()
                
                print(f"  Next check in {check_interval}s...")
                time.sleep(check_interval)
        
        except KeyboardInterrupt:
            print("\n\nStopping alert system...")
            self.alerter.send_discord(
                "🛑 Alert System Stopped",
                "RaveBear monitoring paused",
                color=0xff0000
            )


def main():
    monitor = MarketMonitor()
    monitor.run(check_interval=60)


if __name__ == '__main__':
    main()
