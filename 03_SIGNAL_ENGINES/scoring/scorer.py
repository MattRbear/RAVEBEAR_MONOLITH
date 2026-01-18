import logging
from typing import Dict, Any, Optional
from features import WickEvent

class WickScorer:
    """
    PORTED EnhancedWickScorer
    Adapts legacy V4 logic to the new WickEvent/WickFeatures architecture.
    """
    
    def __init__(self, cfg: Dict[str, Any] = None):
        self.cfg = cfg or {}
        self.logger = logging.getLogger("analysis.scorer")
        
        # Scoring weights for Wick Magnet Score (Preserved from V4)
        self.weights = {
            'virgin_status': 15,
            'distance': 20,
            'approach_velocity': 20,
            'sweep_probability': 15,
            'liquidity_density': 10,
            'vwap_alignment': 10,
            'oi_conviction': 5,
            'age_maturity': 5
        }
    
    def score_wick(self, wick: WickEvent) -> Dict[str, Any]:
        """
        Generate complete scoring output for a wick.
        Adapts WickFeatures to legacy scoring factors.
        """
        output = {}
        features = wick.features
        
        # === 1. WICK MAGNET SCORE (0-100) ===
        magnet_score, score_breakdown = self._compute_magnet_score(features)
        output['wick_magnet_score'] = magnet_score
        output['score_breakdown'] = score_breakdown
        
        # === 2. ATTACK WINDOW ===
        attack_window = self._compute_attack_window(features)
        output['attack_window_seconds'] = attack_window
        output['attack_window_minutes'] = round(attack_window / 60, 2)
        
        # === 3. TRAP MODE ===
        output['trap_mode'] = self._classify_trap_mode(features)
        
        # === 4. EXECUTION BIAS ===
        output['execution_bias'] = self._determine_execution_bias(wick, features)
        
        # === 5. TIMING CLASSIFICATION ===
        output['timing_classification'] = self._classify_timing(features, magnet_score)
        
        # === 6. CONFIDENCE SCORE ===
        output['confidence'] = self._compute_confidence(features, magnet_score)
        
        return output
    
    def _compute_magnet_score(self, features) -> tuple[float, dict]:
        """Compute Wick Magnet Score (0-100)"""
        scores = {}
        
        # 1. Virgin Status (0-15 points)
        # New arch: fresh_sd_zone_flag indicates freshness
        if features.fresh_sd_zone_flag:
            scores['virgin_status'] = 15
        else:
            scores['virgin_status'] = 8  # Assume touched if not fresh
            
        # 2. Distance (0-20 points)
        # New arch: newly detected wick is at price (distance=0).
        # We give full points for proximity if this is a fresh signal.
        # Alternatively, use session_vwap_distance as a proxy for "extension".
        # For now, max score as it's actionable immediately.
        scores['distance'] = 20
        
        # 3. Approach Velocity (0-20 points)
        # New arch: rejection_velocity (how fast we rejected).
        # V4 used approach velocity. We map rejection_velocity as a proxy for volatility.
        velocity = abs(features.rejection_velocity)
        # Normalize: assuming velocity is price/sec or similar.
        # Cap at 15 points
        scores['approach_velocity'] = min(15, velocity * 10) 
        
        # 4. Sweep Probability (0-15 points)
        # New arch: imbalance_trap_score (0-100)
        scores['sweep_probability'] = (features.imbalance_trap_score / 100) * 15
        
        # 5. Liquidity Density (0-10 points) - INVERSE
        # New arch: l1_depth_bid/ask + depth_imbalance
        # V4: Lower liquidity = Higher Score (vacuum magnet)
        # We normalize depth to a 0-100 scale (approx)
        total_depth = features.l5_depth_bid + features.l5_depth_ask
        
        if total_depth <= 0:
            scores['liquidity_density'] = 0 # Penalize missing data (avoid false vacuum)
        else:
            # Heuristic normalization
            density_norm = min(100, total_depth / 10.0) # Arbitrary scaling needed
            scores['liquidity_density'] = (1 - density_norm / 100) * 10
        
        # 6. VWAP Alignment (0-10 points)
        # New arch: vwap_mean_reversion_score (0-100) or band flags
        if features.vwap_mean_reversion_score > 70:
            scores['vwap_alignment'] = 10
        elif features.vwap_mean_reversion_score > 40:
            scores['vwap_alignment'] = 5
        else:
            scores['vwap_alignment'] = 0
            
        # 7. OI Conviction (0-5 points)
        # New arch: oi_change_pct (fractional, e.g. 0.01 = 1%)
        if abs(features.oi_change_pct) > 0.01: # Significant change (>1%)
            scores['oi_conviction'] = 5
        elif abs(features.oi_change_pct) > 0.003: # Moderate (>0.3%)
            scores['oi_conviction'] = 3
        else:
            scores['oi_conviction'] = 0
            
        # 8. Age Maturity (0-5 points)
        # New arch: Wick is brand new (0 min)
        # V4 scored 10-30 mins as prime (5 pts). New/0 min was lower.
        # We give 1 pt for brand new.
        scores['age_maturity'] = 1
        
        total_score = sum(scores.values())
        return round(total_score, 2), scores

    def _compute_attack_window(self, features) -> int:
        """Compute attack window in seconds"""
        # V4 Logic: Distance / Velocity
        # Since distance is 0 (new wick), we default to "Immediate" window
        base_window = 60 # 1 minute default for new wicks
        
        # Adjust by Rejection Velocity (V4: Approach Velocity)
        velocity = abs(features.rejection_velocity)
        if velocity > 10: # High velocity
            base_window = 30
        elif velocity < 1: # Low velocity grind
            base_window = 180
            
        return base_window

    def _classify_trap_mode(self, features) -> str:
        """
        Classify trap mode: NO_TRAP, SOFT_TRAP, HARD_TRAP, LIQUIDATE_REVERSE
        """
        trap_score = features.imbalance_trap_score
        divergence = features.delta_divergence_flag
        absorption = features.absorption_flag
        
        if trap_score < 30:
            return 'NO_TRAP'
        elif trap_score < 50:
            return 'SOFT_TRAP'
        elif trap_score < 70:
            return 'HARD_TRAP'
        else:
            # High trap score + divergence/absorption = Liquidate Reverse
            if divergence or absorption:
                return 'LIQUIDATE_REVERSE'
            else:
                return 'HARD_TRAP'

    def _determine_execution_bias(self, wick: WickEvent, features) -> str:
        """
        Determine execution bias: LONG, SHORT, NEUTRAL
        """
        direction = wick.wick_side # "upper" or "lower"
        trap_mode = self._classify_trap_mode(features)
        
        # Base bias
        if direction == 'lower': # Bull wick
            base_bias = 'LONG'
        else:
            base_bias = 'SHORT'
            
        # Reverse on Liquidation Trap
        if trap_mode == 'LIQUIDATE_REVERSE':
            return 'SHORT' if base_bias == 'LONG' else 'LONG'
            
        # Divergence check
        if features.delta_divergence_flag:
            # If divergence exists, it often confirms the reversal (base_bias)
            # e.g. Price Lower, Delta Higher (Bull Div) -> Long
            return base_bias
            
        # Magnet score check
        # We need to re-calc magnet score or pass it. 
        # For simplicity, if trap score is high but no divergence, Neutral.
        if features.imbalance_trap_score > 50 and not features.delta_divergence_flag:
            return 'NEUTRAL'
            
        return base_bias

    def _classify_timing(self, features, magnet_score: float) -> str:
        """Classify timing: EARLY, PRIME, LATE"""
        # For a newly detected wick (Distance ~ 0)
        
        # High score -> PRIME
        if magnet_score > 60:
            return 'PRIME'
        
        # Very high velocity rejection -> LATE (missed it?)
        if features.rejection_velocity > 50:
            return 'LATE'
            
        return 'EARLY' # Default

    def _compute_confidence(self, features, magnet_score: float) -> float:
        """Compute confidence 0-100"""
        confidence = 50.0
        
        # Data completeness (proxied by non-zero fields)
        if features.oi_change_pct != 0: confidence += 10
        if features.l1_depth_bid != 0: confidence += 10
        
        # Magnet score influence
        if magnet_score > 70: confidence += 20
        elif magnet_score > 50: confidence += 10
        
        # Penalize conflicts
        if features.delta_divergence_flag and features.imbalance_trap_score < 30:
            confidence -= 10
            
        return max(0.0, min(100.0, round(confidence, 2)))
