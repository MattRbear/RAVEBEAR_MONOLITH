import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass
class LiquidityLevel:
    price: float
    type: str # 'POOR_HIGH', 'POOR_LOW', 'SWING_HIGH', 'SWING_LOW'
    timestamp: pd.Timestamp
    strength: int = 1

class SmartMoneyLogic:
    @staticmethod
    def detect_swings(df: pd.DataFrame, window: int = 3):
        """
        Identifies Fractal Highs and Lows.
        Returns two Series: is_swing_high, is_swing_low
        """
        high = df['high']
        low = df['low']
        
        # Shift logic to find peaks/valleys
        # A swing high is higher than 'window' bars before and after
        # rolling_max centers the window. calculate on full range.
        roll_max = high.rolling(window=2*window+1, center=True).max()
        roll_min = low.rolling(window=2*window+1, center=True).min()
        
        is_high = (high == roll_max)
        is_low = (low == roll_min)
        
        return is_high, is_low

    @staticmethod
    def detect_structure(df: pd.DataFrame, lookback: int = 20):
        """
        Detects BOS (Break of Structure) and CHoCH (Change of Character).
        Logic: BOS = Close breaks previous Swing Point in trend direction.
        """
        is_high, is_low = SmartMoneyLogic.detect_swings(df, window=5)
        
        # We need to act on copies or return new series to avoid SettingWithCopy warnings if df is a slice
        structure_events = []
        last_swing_high = df['high'].iloc[0]
        last_swing_low = df['low'].iloc[0]
        
        for i in range(len(df)):
            close = df['close'].iloc[i]
            if is_high.iloc[i]: last_swing_high = df['high'].iloc[i]
            if is_low.iloc[i]: last_swing_low = df['low'].iloc[i]
            
            # Simple BOS Logic
            if close > last_swing_high:
                structure_events.append('BOS_BULL')
            elif close < last_swing_low:
                structure_events.append('BOS_BEAR')
            else:
                structure_events.append(None)
                
        return structure_events

    @staticmethod
    def detect_poor_highs_lows(df: pd.DataFrame, threshold_pct: float = 0.0005):
        """
        Finds 'Equal Highs/Lows' (Liquidity Pools).
        """
        # Look for highs that are within 0.05% of previous recent highs
        # This implies algos are defending a level or building liquidity
        df = df.copy() # Safe copy
        df['poor_high'] = False
        df['poor_low'] = False
        
        window = 20
        # Optimization: Identify potential levels first could be faster, but loop is explicit as req.
        for i in range(window, len(df)):
            current_high = df['high'].iloc[i]
            past_highs = df['high'].iloc[i-window:i]
            
            # If any past high is essentially equal to current high
            matches = past_highs[abs(past_highs - current_high) / current_high < threshold_pct]
            if not matches.empty:
                df.at[df.index[i], 'poor_high'] = True

            current_low = df['low'].iloc[i]
            past_lows = df['low'].iloc[i-window:i]
            matches_low = past_lows[abs(past_lows - current_low) / current_low < threshold_pct]
            if not matches_low.empty:
                df.at[df.index[i], 'poor_low'] = True
        
        return df

    @staticmethod
    def detect_fvg(df: pd.DataFrame):
        """
        Fair Value Gaps (Imbalance).
        Bullish FVG: Low of candle i > High of candle i-2
        """
        df = df.copy()
        df['fvg_bull'] = (df['low'] > df['high'].shift(2))
        df['fvg_bear'] = (df['high'] < df['low'].shift(2))
        return df

class OrderFlow:
    @staticmethod
    def calculate_cvd(df: pd.DataFrame):
        """
        Cumulative Volume Delta.
        If 'taker_buy_volume' exists, use it.
        Else, use Tick Rule Approximation.
        """
        if 'taker_buy_volume' in df.columns:
            buy_vol = df['taker_buy_volume']
            sell_vol = df['volume'] - buy_vol
            delta = buy_vol - sell_vol
        else:
            # Tick Rule: Close > Open = Buy Vol, Close < Open = Sell Vol
            # More precise: Compare to PREVIOUS Close
            price_change = df['close'] - df['close'].shift(1)
            delta = np.where(price_change > 0, df['volume'], 
                    np.where(price_change < 0, -df['volume'], 0))
            
        return pd.Series(delta).cumsum()

    @staticmethod
    def rolling_vwap(df: pd.DataFrame, window: int = 20):
        """
        Rolling Volume Weighted Average Price.
        Standard VWAP resets daily; Rolling tracks last N bars (e.g. 4H window).
        """
        v = df['volume']
        tp = (df['high'] + df['low'] + df['close']) / 3
        
        # Rolling sum of (TP * Vol) / Rolling sum of Vol
        vwap = (tp * v).rolling(window=window).sum() / v.rolling(window=window).sum()
        return vwap

    @staticmethod
    def detect_liquidation_candles(df: pd.DataFrame):
        """
        'Institutional Raping': High Volume + Long Wicks + Displacement.
        """
        avg_vol = df['volume'].rolling(50).mean()
        atr = (df['high'] - df['low']).rolling(50).mean()
        
        # Criteria: Vol > 3x Avg AND Range > 2x ATR
        is_high_vol = df['volume'] > (avg_vol * 3.0)
        is_wide_range = (df['high'] - df['low']) > (atr * 2.0)
        
        # Wick analysis: Wick is > 50% of candle?
        body_size = abs(df['close'] - df['open'])
        wick_size = (df['high'] - df['low']) - body_size
        is_wicky = wick_size > body_size
        
        return is_high_vol & is_wide_range & is_wicky
