"""
Whale Dashboard
Combines: Whale Alert + Etherscan + CoinGecko + Fear/Greed
Real-time whale flow analysis for trading edge.
"""

import os
import sys
import time
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add current dir to path
sys.path.insert(0, str(Path(__file__).parent))

from whale_client import WhaleAlertClient
from etherscan_client import EtherscanClient
from market_client import CoinGeckoClient

DATA_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Whale_Flow")
DATA_DIR.mkdir(parents=True, exist_ok=True)

SNAPSHOT_PATH = DATA_DIR / "whale_snapshots.json"
ALERTS_PATH = DATA_DIR / "whale_alerts.json"


def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


def format_usd(value, decimals=0):
    if value is None:
        return "--"
    if abs(value) >= 1_000_000_000:
        return f"${value/1e9:.2f}B"
    elif abs(value) >= 1_000_000:
        return f"${value/1e6:.2f}M"
    elif abs(value) >= 1_000:
        return f"${value/1e3:.1f}K"
    return f"${value:,.{decimals}f}"


def load_json(path):
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


def save_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


class WhaleDashboard:
    def __init__(self):
        self.whale = WhaleAlertClient()
        self.etherscan = EtherscanClient()
        self.coingecko = CoinGeckoClient()
        
        self.data = {
            'btc_flow': {},
            'eth_flow': {},
            'usdt_flow': {},
            'prices': {},
            'global': {},
            'fear_greed': {},
            'recent_txs': [],
        }
        
        self.last_update = {}
        self.snapshots = load_json(SNAPSHOT_PATH)
        self.alerts = load_json(ALERTS_PATH)
    
    def update_whale_data(self):
        """Fetch whale transaction data."""
        print("  Fetching whale data...")
        
        # BTC whale txs (last 4 hours)
        btc_txs = self.whale.get_btc_transactions(hours=4, min_value=1000000)
        if btc_txs:
            self.data['btc_flow'] = self.whale.analyze_flow(btc_txs)
            self.data['recent_txs'] = btc_txs[:20]
        
        # ETH whale txs
        eth_txs = self.whale.get_eth_transactions(hours=4, min_value=1000000)
        if eth_txs:
            self.data['eth_flow'] = self.whale.analyze_flow(eth_txs)
        
        # USDT whale txs
        usdt_txs = self.whale.get_usdt_transactions(hours=4, min_value=5000000)
        if usdt_txs:
            self.data['usdt_flow'] = self.whale.analyze_flow(usdt_txs)
        
        self.last_update['whale'] = time.time()
    
    def update_market_data(self):
        """Fetch market data."""
        print("  Fetching market data...")
        
        # Prices
        self.data['prices'] = self.coingecko.get_price() or {}
        
        # Global
        self.data['global'] = self.coingecko.get_global() or {}
        
        # Fear & Greed
        self.data['fear_greed'] = self.coingecko.get_fear_greed() or {}
        
        # ETH gas
        self.data['gas'] = self.etherscan.get_gas_price() or {}
        
        self.last_update['market'] = time.time()
    
    def take_snapshot(self):
        """Take a snapshot for correlation analysis."""
        btc_price = self.data['prices'].get('bitcoin', {}).get('usd', 0)
        
        snapshot = {
            'timestamp': int(time.time() * 1000),
            'datetime': datetime.now(timezone.utc).isoformat(),
            'btc_price': btc_price,
            'btc_net_flow': self.data['btc_flow'].get('net_exchange_flow', 0),
            'btc_inflow': self.data['btc_flow'].get('exchange_inflow', 0),
            'btc_outflow': self.data['btc_flow'].get('exchange_outflow', 0),
            'eth_net_flow': self.data['eth_flow'].get('net_exchange_flow', 0),
            'usdt_volume': self.data['usdt_flow'].get('total_volume', 0),
            'fear_greed': int(self.data['fear_greed'].get('value', 50)),
            'btc_dominance': self.data['global'].get('market_cap_percentage', {}).get('btc', 0),
        }
        
        # Classify signal
        btc_net = snapshot['btc_net_flow']
        if btc_net > 10_000_000:
            snapshot['signal'] = 'BEARISH'
        elif btc_net < -10_000_000:
            snapshot['signal'] = 'BULLISH'
        else:
            snapshot['signal'] = 'NEUTRAL'
        
        self.snapshots.append(snapshot)
        self.snapshots = self.snapshots[-500:]  # Keep last 500
        save_json(SNAPSHOT_PATH, self.snapshots)
        
        return snapshot
    
    def check_alerts(self):
        """Check for notable whale activity."""
        new_alerts = []
        
        for tx in self.data.get('recent_txs', [])[:10]:
            amount_usd = tx.get('amount_usd', 0)
            tx_class = self.whale.classify_transaction(tx)
            
            # Alert on large exchange flows
            if amount_usd >= 50_000_000:
                alert = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'type': tx_class,
                    'amount_usd': amount_usd,
                    'currency': tx.get('symbol', 'BTC'),
                    'from': tx.get('from', {}).get('owner', 'unknown'),
                    'to': tx.get('to', {}).get('owner', 'unknown'),
                }
                
                # Check if we already alerted this
                tx_hash = tx.get('hash', '')
                existing = [a for a in self.alerts if a.get('hash') == tx_hash]
                if not existing:
                    alert['hash'] = tx_hash
                    new_alerts.append(alert)
                    self.alerts.append(alert)
        
        if new_alerts:
            self.alerts = self.alerts[-200:]
            save_json(ALERTS_PATH, self.alerts)
        
        return new_alerts
    
    def render(self):
        """Render the dashboard."""
        clear()
        ts_str = datetime.now().strftime('%H:%M:%S')
        
        print("=" * 72)
        print(f"  WHALE FLOW DASHBOARD  |  {ts_str}")
        print("=" * 72)
        
        # === PRICES ===
        prices = self.data.get('prices', {})
        btc = prices.get('bitcoin', {})
        eth = prices.get('ethereum', {})
        
        print(f"\n  PRICES")
        print(f"  {'-'*60}")
        print(f"  BTC:  ${btc.get('usd', 0):>12,.2f}  ({btc.get('usd_24h_change', 0):+.2f}%)")
        print(f"  ETH:  ${eth.get('usd', 0):>12,.2f}  ({eth.get('usd_24h_change', 0):+.2f}%)")
        
        # Fear & Greed
        fng = self.data.get('fear_greed', {})
        if fng:
            fng_val = fng.get('value', 50)
            fng_class = fng.get('value_classification', 'Neutral')
            print(f"\n  Fear & Greed: {fng_val} ({fng_class})")
        
        # BTC Dominance
        btc_dom = self.data.get('global', {}).get('market_cap_percentage', {}).get('btc', 0)
        if btc_dom:
            print(f"  BTC Dominance: {btc_dom:.2f}%")
        
        # === BTC WHALE FLOW ===
        btc_flow = self.data.get('btc_flow', {})
        if btc_flow:
            print(f"\n  BTC WHALE FLOW (4H)")
            print(f"  {'-'*60}")
            print(f"  Total Volume:     {format_usd(btc_flow.get('total_volume', 0)):>15}")
            print(f"  Exchange Inflow:  {format_usd(btc_flow.get('exchange_inflow', 0)):>15}  ({btc_flow.get('inflow_count', 0)} txs)")
            print(f"  Exchange Outflow: {format_usd(btc_flow.get('exchange_outflow', 0)):>15}  ({btc_flow.get('outflow_count', 0)} txs)")
            
            net = btc_flow.get('net_exchange_flow', 0)
            direction = "TO" if net > 0 else "FROM"
            signal = "BEARISH" if net > 0 else "BULLISH"
            print(f"  Net Flow:         {format_usd(abs(net)):>15}  {direction} exchanges")
            print(f"  -> {signal} SIGNAL")
        
        # === ETH WHALE FLOW ===
        eth_flow = self.data.get('eth_flow', {})
        if eth_flow:
            print(f"\n  ETH WHALE FLOW (4H)")
            print(f"  {'-'*60}")
            print(f"  Total Volume:     {format_usd(eth_flow.get('total_volume', 0)):>15}")
            print(f"  Exchange Inflow:  {format_usd(eth_flow.get('exchange_inflow', 0)):>15}")
            print(f"  Exchange Outflow: {format_usd(eth_flow.get('exchange_outflow', 0)):>15}")
            
            net = eth_flow.get('net_exchange_flow', 0)
            direction = "TO" if net > 0 else "FROM"
            print(f"  Net Flow:         {format_usd(abs(net)):>15}  {direction} exchanges")
        
        # === USDT FLOW ===
        usdt_flow = self.data.get('usdt_flow', {})
        if usdt_flow:
            print(f"\n  USDT WHALE FLOW (4H)")
            print(f"  {'-'*60}")
            print(f"  Total Volume:     {format_usd(usdt_flow.get('total_volume', 0)):>15}")
            inflow = usdt_flow.get('exchange_inflow', 0)
            if inflow > 50_000_000:
                print(f"  -> Large USDT to exchanges = dry powder ready to buy")
        
        # === RECENT LARGE TXS ===
        recent = self.data.get('recent_txs', [])[:8]
        if recent:
            print(f"\n  RECENT WHALE TRANSACTIONS")
            print(f"  {'-'*60}")
            print(f"  {'TIME':<8} {'TYPE':<18} {'AMOUNT':>12} {'FROM/TO':<20}")
            print(f"  {'-'*60}")
            
            for tx in recent:
                tx_time = datetime.fromtimestamp(tx.get('timestamp', 0), tz=timezone.utc)
                time_str = tx_time.strftime('%H:%M')
                tx_class = self.whale.classify_transaction(tx)
                amount = tx.get('amount_usd', 0)
                
                from_owner = tx.get('from', {}).get('owner', 'unknown')[:10]
                to_owner = tx.get('to', {}).get('owner', 'unknown')[:10]
                
                if tx_class == 'EXCHANGE_INFLOW':
                    flow_str = f"-> {to_owner}"
                elif tx_class == 'EXCHANGE_OUTFLOW':
                    flow_str = f"{from_owner} ->"
                else:
                    flow_str = f"{from_owner[:8]}"
                
                print(f"  {time_str:<8} {tx_class:<18} {format_usd(amount):>12} {flow_str:<20}")
        
        # === GAS ===
        gas = self.data.get('gas', {})
        if gas:
            print(f"\n  ETH GAS: Safe={gas.get('safe', 0)} | Fast={gas.get('fast', 0)} gwei")
        
        # === SIGNAL SUMMARY ===
        print(f"\n{'='*72}")
        btc_net = btc_flow.get('net_exchange_flow', 0) if btc_flow else 0
        
        signals = []
        if btc_net > 10_000_000:
            signals.append("BTC->Exchange (SELL)")
        elif btc_net < -10_000_000:
            signals.append("BTC<-Exchange (ACCUM)")
        
        fng_val = int(fng.get('value', 50)) if fng else 50
        if fng_val < 25:
            signals.append("Extreme Fear (contrarian BUY)")
        elif fng_val > 75:
            signals.append("Extreme Greed (contrarian SELL)")
        
        if signals:
            print(f"  SIGNALS: {' | '.join(signals)}")
        else:
            print(f"  SIGNALS: Neutral / No strong whale activity")
        
        print("=" * 72)
        
        whale_age = int(time.time() - self.last_update.get('whale', 0))
        market_age = int(time.time() - self.last_update.get('market', 0))
        print(f"  Whale: {whale_age}s ago  |  Market: {market_age}s ago  |  Snapshots: {len(self.snapshots)}")
        print(f"  Data: {DATA_DIR}")
        print("=" * 72)
    
    def run(self, whale_interval=120, market_interval=60):
        """Run the dashboard."""
        print("Starting Whale Dashboard...")
        print("Initial data fetch...\n")
        
        self.update_market_data()
        self.update_whale_data()
        self.take_snapshot()
        
        try:
            while True:
                self.render()
                
                now = time.time()
                
                # Update market data every 60s
                if now - self.last_update.get('market', 0) >= market_interval:
                    self.update_market_data()
                
                # Update whale data every 120s (rate limit friendly)
                if now - self.last_update.get('whale', 0) >= whale_interval:
                    self.update_whale_data()
                    self.take_snapshot()
                    
                    # Check for alerts
                    alerts = self.check_alerts()
                    if alerts:
                        print(f"\n  !! NEW WHALE ALERT: {alerts[0]['type']} {format_usd(alerts[0]['amount_usd'])}")
                
                time.sleep(10)
        
        except KeyboardInterrupt:
            print("\n\nStopping...")
            print(f"Saved {len(self.snapshots)} snapshots")


def main():
    dashboard = WhaleDashboard()
    dashboard.run()


if __name__ == '__main__':
    main()
