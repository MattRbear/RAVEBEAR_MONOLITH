import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Callable, Any, Union
from dataclasses import dataclass, field
from collections import deque
import logging
from position import Position

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TruthEngine")

@dataclass
class Order:
    symbol: str
    side: str
    size: float
    order_type: str # LIMIT or MARKET
    price: Optional[float] = None
    timestamp_created: Optional[pd.Timestamp] = None
    # For simulation
    execution_time: Optional[pd.Timestamp] = None
    leverage: float = 1.0

@dataclass
class Trade:
    symbol: str
    side: str
    entry_price: float
    exit_price: float
    size: float
    pnl: float
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    fee: float = 0.0
    exit_reason: str = "SIGNAL" # SIGNAL, LIQUIDATION, STOP_LOSS, TAKE_PROFIT

class TruthEngine:
    def __init__(self, 
                 initial_capital: float = 10000.0, 
                 latency_ms: int = 100,
                 maker_fee: float = 0.0002,
                 taker_fee: float = 0.0005,
                 slippage_model: bool = True):
        
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.equity = initial_capital
        self.latency_ms = latency_ms
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.use_slippage_model = slippage_model
        
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.pending_orders: deque = deque() # Queue for latency simulation
        
        # Funding tracking
        self.last_funding_check = None
        
        # Metrics
        self.peak_equity = initial_capital
        self.max_drawdown = 0.0
        self.equity_history = []  # List of {'timestamp': t, 'equity': e}

    def load_data(self, data_dict: Dict[str, pd.DataFrame]):
        """
        Ingests a dictionary of DataFrames.
        Expects index to be DatetimeIndex.
        Example: {"BTC": df_btc, "ETH": df_eth}
        
        Ensures columns can vary (Nansen Requirement).
        Synchronizes timestamps.
        """
        self.data = data_dict
        # Get union of all timestamps
        all_indices = sorted(set().union(*[df.index for df in data_dict.values()]))
        self.timeline = all_indices
        logger.info(f"Loaded {len(data_dict)} assets. Total timeline bars: {len(self.timeline)}")

    def run(self, strategy_func: Callable):
        """
        Main Event Loop.
        strategy_func(timestamp, current_data, engine) -> None
        """
        logger.info("Starting Backtest...")
        
        for timestamp in self.timeline:
            self._update_market_state(timestamp)
            
            # 1. Check Funding (00:00, 08:00, 16:00 UTC)
            self._check_funding(timestamp)
            
            # 2. Process Pending Orders (Latency check)
            self._process_orders(timestamp)
            
            # 3. Liquidation Check
            self._check_liquidations(timestamp)
            
            # 4. User Strategy Signal
            # Construct a slice of data for the current timestamp
            current_data = {}
            for symbol, df in self.data.items():
                if timestamp in df.index:
                    current_data[symbol] = df.loc[timestamp]
            
            if current_data:
                strategy_func(timestamp, current_data, self)
            
            # 5. Update Equity Curve
            self._update_equity(timestamp)
            
            # 6. Bankruptcy Check
            if self.equity <= 0:
                logger.warning(f"BANKRUPTCY at {timestamp} - Stopping Engine.")
                self.positions.clear() # Force close all (conceptual)
                break

        logger.info("Backtest Complete.")
        return self._generate_report()

    def _update_market_state(self, timestamp):
        self.current_time = timestamp
        # Assume valid prices are available in self.data lookup for 'Close' or 'Open'
        # For efficiency, we rely on strategy or order process to look up exact prices

    def _check_funding(self, timestamp):
        """
        Applies funding every 8 hours.
        """
        # Simple check: if hour is 0, 8, 16 and minute is 0. 
        # CAUTION: If data is 15m bars, we hit 00:00 exact. If hourly, we hit 00:00.
        # If funding happened already for this 8h block, skip.
        
        if timestamp.hour in [0, 8, 16] and timestamp.minute == 0:
            # Avoid double counting if we have multiple ticks in the same minute (rare in vectorized but possible in tick)
            if self.last_funding_check != timestamp:
                self.last_funding_check = timestamp
                funding_rate = 0.0001 # Default baseline or fetch from data column 'fundingRate' if exists
                
                total_funding_fee = 0.0
                for symbol, pos in self.positions.items():
                    # Create dynamic funding rate lookup if column exists
                    try:
                        rate = self.data[symbol].loc[timestamp]['funding_rate']
                    except (KeyError, AttributeError):
                        rate = funding_rate # Fallback
                    
                    # Get Current Mark Price for Funding Calc
                    try:
                        current_price = self.data[symbol].loc[timestamp]['close']
                    except KeyError:
                        current_price = pos.entry_price # Fallback to entry if no data (unlikely)

                    fee = pos.apply_funding(current_price, rate)
                    total_funding_fee += fee
                
                self.capital -= total_funding_fee # Deduct from cash
                # logger.debug(f"Funding Applied at {timestamp}: {total_funding_fee}")

    def submit_order(self, order: Order):
        """
        Pushes order to latency queue.
        """
        order.timestamp_created = self.current_time
        order.execution_time = self.current_time + pd.Timedelta(milliseconds=self.latency_ms)
        self.pending_orders.append(order)

    def _process_orders(self, timestamp):
        """
        Checks if orders in queue are ready to execute.
        """
        while self.pending_orders and self.pending_orders[0].execution_time <= timestamp:
            order = self.pending_orders.popleft()
            self._execute_order(order, timestamp)

    def _execute_order(self, order: Order, timestamp):
        # 1. Get current price
        try:
            # Use 'Open' price on the bar of execution as strict fill if latency pushes it next bar
            # Or 'Close' of current bar? 
            # Realism: If we act at T+latency, we fill at the market price at T+latency.
            # If our loop is at T_actual, we use Open/Close/High/Low logic.
            # Simplified: Use 'Open' of the current bar (representing T+latency entry)
            current_bar = self.data[order.symbol].loc[timestamp]
            market_price = current_bar['open'] 
            high = current_bar['high']
            low = current_bar['low']
            volatility = current_bar.get('atr', market_price * 0.01) # Default 1% vol if no ATR
        except KeyError:
            # No data for this symbol at this timestamp
            return 

        # 2. Limit Order Logic (Maker)
        if order.order_type == "LIMIT":
            if order.side == "LONG" and low <= order.price:
                fill_price = order.price
                is_maker = True
            elif order.side == "SHORT" and high >= order.price:
                fill_price = order.price
                is_maker = True
            else:
                return # Not filled, keep in queue? Or discard? Simplified: Discard or Retry. For now, assume IOC-like or manual management required.
                # To keep it simple for v3 prototype: Limit orders only fill if price crossed. We won't re-queue loops here.
        
        # 3. Market Order Logic (Taker)
        else:
            is_maker = False
            base_price = market_price
            
            # Slippage Model
            if self.use_slippage_model:
                # slippage = base_slippage * vol_factor
                # Simple impact model
                vol_impact = volatility / base_price
                slippage_pct = 0.0005 * (1 + vol_impact * 10) 
                
                if order.side == "LONG":
                    fill_price = base_price * (1 + slippage_pct)
                else:
                    fill_price = base_price * (1 - slippage_pct)
            else:
                fill_price = base_price

        # 4. Update Position
        self._update_position(order.symbol, order.side, order.size, fill_price, is_maker, timestamp, leverage=order.leverage)

    def _update_position(self, symbol, side, size, price, is_maker, timestamp, leverage=1.0):
        fee_rate = self.maker_fee if is_maker else self.taker_fee
        commission = (price * size) * fee_rate
        self.capital -= commission
        
        if symbol in self.positions:
            pos = self.positions[symbol]
            # Closing or flipping
            if pos.side != side:
                # Reduce or close
                if size >= pos.size:
                    # Full close + potential reverse
                    pnl = pos.calculate_pnl(price)
                    self.capital += pnl
                    
                    entry_t = getattr(pos, 'entry_time', "?")
                    self.trades.append(Trade(symbol, pos.side, pos.entry_price, price, pos.size, pnl, entry_t, timestamp, commission, "SIGNAL"))
                    
                    remaining = size - pos.size
                    del self.positions[symbol]
                    
                    if remaining > 0:
                        # New position in opposite direction
                        new_pos = Position(symbol, side, price, remaining, leverage, entry_time=timestamp)
                        self.positions[symbol] = new_pos
                else:
                    # Partial close
                    # PnL on partial
                    portion_ratio = size / pos.size
                    # PnL Realized on the portion
                    realized_pnl = (price - pos.entry_price) * size if pos.side == "LONG" else (pos.entry_price - price) * size
                    self.capital += realized_pnl
                    pos.size -= size
                    entry_t = getattr(pos, 'entry_time', "?")
                    self.trades.append(Trade(symbol, pos.side, pos.entry_price, price, size, realized_pnl, entry_t, timestamp, commission, "PARTIAL_SIGNAL"))

            else:
                # Adding to position (Pyramiding)
                # Weighted average entry price
                total_val = (pos.entry_price * pos.size) + (price * size)
                new_size = pos.size + size
                pos.entry_price = total_val / new_size
                pos.size = new_size
                pos.update_liquidation_price()
        else:
            # New Position
            self.positions[symbol] = Position(symbol, side, price, size, leverage, entry_time=timestamp)
    
    def _check_liquidations(self, timestamp):
        to_liquidate = []
        for symbol, pos in self.positions.items():
            try:
                current_bar = self.data[symbol].loc[timestamp]
                # Check Low for Longs, High for Shorts (Worst case)
                worst_price = current_bar['low'] if pos.side == "LONG" else current_bar['high']
                
                if pos.check_liquidation(worst_price):
                    to_liquidate.append((symbol, pos.liquidation_price))
            except KeyError:
                continue
        
        for symbol, liq_price in to_liquidate:
            pos = self.positions[symbol]
            
            # Liquidation Loss Logic (Fixed v1.5)
            # You lose your initial margin (isolated). 
            # In Cross, you could lose more, but we assume Isolated for safety.
            pnl = -str(pos.initial_margin) if hasattr(pos, 'initial_margin') else -((pos.entry_price * pos.size) / pos.leverage)
            pnl = float(pnl) 
            
            # Actual realization
            self.capital += pnl 
            
            # Record Trade
            # entry_time should be tracked in Position
            entry_t = getattr(pos, 'entry_time', "?")
            trade = Trade(
                symbol=symbol, 
                side=pos.side, 
                entry_price=pos.entry_price, 
                exit_price=liq_price, 
                size=pos.size, 
                pnl=pnl, 
                entry_time=entry_t, 
                exit_time=timestamp, 
                fee=0.0, 
                exit_reason="LIQUIDATION"
            )
            self.trades.append(trade)
            
            del self.positions[symbol]
            logger.warning(f"[LIQUIDATION] {symbol} at {timestamp} @ {liq_price} | Loss: ${pnl:,.2f}")

    def _update_equity(self, timestamp):
        unrealized_pnl = 0.0
        for symbol, pos in self.positions.items():
            try:
                price = self.data[symbol].loc[timestamp]['close']
                unrealized_pnl += pos.calculate_pnl(price)
            except KeyError:
                pass
        
        total_equity = self.capital + unrealized_pnl
        self.equity = total_equity
        self.peak_equity = max(self.peak_equity, total_equity)
        
        # DD Calc
        dd = (self.peak_equity - total_equity) / self.peak_equity
        self.max_drawdown = max(self.max_drawdown, dd)
        
        # Record History (Sample every bar or as needed)
        self.equity_history.append({'timestamp': timestamp, 'equity': total_equity})

    def _generate_report(self):
        # Calc Stats
        wins = [t.pnl for t in self.trades if t.pnl > 0]
        losses = [t.pnl for t in self.trades if t.pnl <= 0]
        
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)
        win_rate = len(wins) / len(self.trades) if self.trades else 0.0
        
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        
        return {
            "Final Equity": self.equity,
            "Max Drawdown": self.max_drawdown,
            "Total Trades": len(self.trades),
            "PnL": self.equity - self.initial_capital,
            "Win Rate": win_rate,
            "Profit Factor": profit_factor,
            "Avg Win": avg_win,
            "Avg Loss": avg_loss,
            "Equity Curve": self.equity_history
        }
