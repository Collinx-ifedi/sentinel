# agent_engine/safety_sentinel.py
import os
import sys
from pathlib import Path
import yaml
import logging
import asyncio
import httpx
from typing import Tuple, Dict, Any, Optional, List

# --- CORE INTEL IMPORTS ---
# Importing from your established legacy modules to maintain the "Brain's" historical context
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
from src.core_intel.macro_economy_indicator import analyze_macro_indicators, MACRO_INDICATORS
from src.core_intel.sentiment_analysis import get_sentiment_snapshot

# Setup logging
log = logging.getLogger("SafetySentinel")

class SafetySentinel:
    """
    The Final Decision Gate for the Solana Sentinel Agent.
    Merges on-chain security (RugCheck) with Macro, Sentiment, and TA Data.
    Acts as a Circuit Breaker to veto dangerous transactions.
    """
    def __init__(self, config_path: str = "config.yaml"):
        self.rugcheck_url = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"
        
        # Load Risk Thresholds
        self.config = self._load_config(config_path)
        self.max_rug_score = self.config.get("max_rug_score", 500)
        self.base_max_total_risk = self.config.get("max_total_risk", 65.0)
        
    def _load_config(self, path: str) -> Dict[str, Any]:
        """Loads thresholds from config.yaml, falling back to safe defaults."""
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return yaml.safe_load(f).get("sentinel", {})
        except Exception as e:
            log.warning(f"Failed to load config.yaml: {e}. Using default risk thresholds.")
        return {}

    async def get_security_report(self, mint_address: str) -> Dict[str, Any]:
        """Fetches and parses the token report from RugCheck.xyz."""
        url = self.rugcheck_url.format(mint=mint_address)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                
                score = data.get("score", 999) # Default to high risk if missing
                risks = data.get("risks", [])
                
                # Check for critical danger flags
                mint_authority_enabled = any(
                    r.get("name", "").lower() == "mint authority still enabled" for r in risks
                )
                is_rugged = any(r.get("level") == "danger" for r in risks)
                
                # Try to extract total market liquidity (for Pump & Dump checks)
                liquidity = 0.0
                markets = data.get("markets", [])
                if markets:
                    liquidity = sum(float(m.get("liquidity", 0)) for m in markets)
                
                return {
                    "score": score,
                    "mint_authority_enabled": mint_authority_enabled,
                    "is_rugged": is_rugged,
                    "liquidity": liquidity,
                    "raw_risks": risks
                }
        except Exception as e:
            log.error(f"RugCheck API failed for {mint_address}: {e}")
            # If the security check fails, default to a HIGH RISK state for safety
            return {"score": 999, "mint_authority_enabled": False, "is_rugged": True, "liquidity": 0.0}

    async def _get_macro_risk(self, symbol: str) -> Tuple[float, float]:
        """
        Runs the legacy synchronous Macro Indicator in a separate thread.
        Returns: (risk_0_to_100, raw_sentiment_score)
        """
        try:
            # Run sync function in thread pool to prevent blocking async loop
            results = await asyncio.to_thread(analyze_macro_indicators, MACRO_INDICATORS, symbol)
            
            if not results:
                return 50.0, 0.0 # Neutral fallback
                
            # Average the sentiment scores (-1.0 to 1.0)
            valid_scores = [r.get("sentiment", 0.0) for r in results if isinstance(r.get("sentiment"), (int, float))]
            avg_sentiment = sum(valid_scores) / len(valid_scores) if valid_scores else 0.0
            
            # Map sentiment [-1.0, 1.0] to Risk [100.0, 0.0]
            macro_risk = (1.0 - avg_sentiment) * 50.0
            return float(macro_risk), float(avg_sentiment)
            
        except Exception as e:
            log.error(f"Macro analysis failed: {e}")
            return 50.0, 0.0

    async def _get_sentiment_risk(self, symbol: str) -> Tuple[float, float]:
        """
        Fetches the comprehensive social/news sentiment snapshot.
        Returns: (risk_0_to_100, raw_sentiment_score)
        """
        try:
            snapshot = await get_sentiment_snapshot(symbol)
            avg_sentiment = snapshot.get("average_social_sentiment_score", 0.0)
            
            # Map sentiment [-1.0, 1.0] to Risk [100.0, 0.0]
            sentiment_risk = (1.0 - avg_sentiment) * 50.0
            return float(sentiment_risk), float(avg_sentiment)
        except Exception as e:
            log.error(f"Sentiment analysis failed: {e}")
            return 50.0, 0.0

    async def evaluate_total_risk(self, 
                                  mint_address: str, 
                                  symbol: str, 
                                  ta_summary: Optional[Dict] = None, 
                                  pattern_summary: Optional[Dict] = None) -> Tuple[bool, str, float]:
        """
        The Master Risk Logic Gate.
        Evaluates RugCheck, Macro, Sentiment, and optional TA/Patterns to Output Go/No-Go.
        """
        log.info(f"Sentinel initiating security sweep for {symbol} ({mint_address})...")
        
        # 1. Fetch Data Concurrently
        rug_data, macro_data, sentiment_data = await asyncio.gather(
            self.get_security_report(mint_address),
            self._get_macro_risk(symbol),
            self._get_sentiment_risk(symbol)
        )
        
        macro_risk, raw_macro_sentiment = macro_data
        social_risk, raw_social_sentiment = sentiment_data
        rug_score = rug_data["score"]
        
        # ---------------------------------------------------------
        # HARD VETOES (Immediate Termination)
        # ---------------------------------------------------------
        if rug_data["mint_authority_enabled"]:
            return False, "[SENTINEL] HARD VETO: Mint Authority Enabled. Highly susceptible to inflation rug.", 100.0
            
        if rug_data["is_rugged"] or rug_score > self.max_rug_score:
            reason = f"[SENTINEL] HARD VETO: On-chain danger detected (Rug Score: {rug_score})."
            return False, reason, 100.0
            
        # ---------------------------------------------------------
        # ANOMALY DETECTION (Pump & Dump Filter)
        # ---------------------------------------------------------
        # High social hype (> 0.5) combined with a moderate/bad rug score (> 300) or low liquidity
        if raw_social_sentiment > 0.5 and rug_score > 300:
            return False, "[SENTINEL] ANOMALY VETO: Probable Pump & Dump detected. High social hype, but suspicious on-chain metrics.", 90.0

        # ---------------------------------------------------------
        # DYNAMIC RISK ADJUSTMENT & SCORING
        # ---------------------------------------------------------
        # Normalize Rug Score (0-1000) to Risk Component (0-100)
        normalized_rug_risk = min(100.0, rug_score / 5.0)
        
        # Calculate Total Weighted Risk
        # 50% On-Chain, 25% Macro, 25% Sentiment
        total_risk_score = (normalized_rug_risk * 0.50) + (macro_risk * 0.25) + (social_risk * 0.25)
        
        # MACRO CORRELATION: If the global market is bearish, tighten the risk allowance
        max_allowed_risk = self.base_max_total_risk
        if raw_macro_sentiment < -0.1: # Bearish
            max_allowed_risk *= 0.8 # Reduce allowed risk by 20%
            log.info(f"Macro is Bearish. Tightening Max Allowed Risk to {max_allowed_risk:.2f}")

        # ---------------------------------------------------------
        # TA & PATTERN INTEGRATION (The Technical Veto)
        # ---------------------------------------------------------
        if ta_summary or pattern_summary:
            ta_sentiment = ta_summary.get("sentiment", {}).get("numeric_score", 0.0) if ta_summary else 0.0
            pattern_sentiment = pattern_summary.get("sentiment", "Neutral") if pattern_summary else "Neutral"
            latest_patterns = pattern_summary.get("latest_patterns", []) if pattern_summary else []
            
            # 1. Base Trend Penalization
            if ta_sentiment < -0.4 and pattern_sentiment == "Bearish":
                total_risk_score += 10.0 
                log.info("Chart setup is highly bearish (TA & Patterns). Increasing total risk penalty.")
            elif ta_sentiment < -0.2:
                total_risk_score += 5.0
                log.info(f"TA Sentiment is bearish ({ta_sentiment:.2f}). Applying moderate risk penalty.")
                
            # 2. Immediate Pattern Threat Detection (Candle Geometry Veto)
            if latest_patterns:
                bearish_keywords = ["Bearish", "Shooting", "Evening", "Dark", "Three Black", "Gravestone", "Tweezer Top", "On Neck", "Above Neck", "Below Neck"]
                # Check if any of the patterns on the absolute latest unclosed candle are bearish
                is_immediate_threat = any(any(k in p for k in bearish_keywords) for p in latest_patterns)
                
                if is_immediate_threat:
                    total_risk_score += 15.0
                    log.warning(f"Immediate bearish pattern detected on the last candle {latest_patterns}. Applying +15.0 risk penalty!")
                    
            # 3. Bullish Confluence Mitigation
            if ta_sentiment > 0.4 and pattern_sentiment == "Bullish":
                total_risk_score -= 10.0 # Reward a pristine chart setup
                # Guardrail: Ensure risk score doesn't drop below the base on-chain risk (don't mask rug danger)
                total_risk_score = max(total_risk_score, (normalized_rug_risk * 0.50))
                log.info("Strong Bullish Confluence (TA & Patterns) detected. Reducing overall risk score safely.")

        # ---------------------------------------------------------
        # FINAL DECISION
        # ---------------------------------------------------------
        if total_risk_score > max_allowed_risk:
            reason = f"[SENTINEL] SOFT VETO: Total Risk Score ({total_risk_score:.1f}) exceeds allowed threshold ({max_allowed_risk:.1f}). Macro/Social/Chart environment does not support trade."
            return False, reason, total_risk_score
            
        success_msg = f"[SENTINEL] ALL CLEAR: Security Verified. (Risk Score: {total_risk_score:.1f}/100)"
        return True, success_msg, total_risk_score

    # ==============================================================================
    # PORTFOLIO HEALTH ANALYSIS (DASHBOARD SUPPORT)
    # ==============================================================================
    async def evaluate_portfolio_risk(self, token_mints: List[str]) -> Dict[str, Any]:
        """
        Calculates a 'Portfolio Health Score' by checking the RugCheck 
        status of all assets in the user's War Chest concurrently.
        """
        # Blue-chip / Safe mints to ignore to save API calls and false positives
        safe_mints = {
            "So11111111111111111111111111111111111111112", # SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", # USDT
            "J1toso1uKSpDdVN6qsQp96aX53pA1d3A86d3Y1A4T",    # JitoSOL
            "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqkVmBw",   # mSOL
            "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piV1",   # bSOL
        }
        
        assets_to_check = [m for m in token_mints if m not in safe_mints]
        
        if not assets_to_check:
            # If the portfolio is entirely SOL and stablecoins, it's perfectly safe
            return {"health_score": 100.0, "high_risk_assets": [], "status": "SAFE", "scanned_count": 0}
            
        # 1. Fire off concurrent security reports for all unverified altcoins
        tasks = [self.get_security_report(mint) for mint in assets_to_check]
        reports = await asyncio.gather(*tasks, return_exceptions=True)
        
        high_risk_mints = []
        total_rug_score = 0.0
        valid_reports = 0
        
        # 2. Analyze the returning reports
        for mint, report in zip(assets_to_check, reports):
            if isinstance(report, Exception):
                log.warning(f"Portfolio Risk Sweep: Failed to analyze mint {mint}")
                continue
                
            score = report.get("score", 0)
            
            # If the token is fundamentally compromised, flag it immediately
            if report.get("is_rugged") or score > self.max_rug_score:
                high_risk_mints.append(mint)
                
            total_rug_score += score
            valid_reports += 1
            
        avg_rug_score = (total_rug_score / valid_reports) if valid_reports > 0 else 0
        
        # 3. Map average rug score (0-5000+) to a Health Score (0-100)
        # Lower rug score = Higher health. A 0 average rug score = 100 health.
        health_score = max(0.0, min(100.0, 100.0 - (avg_rug_score / 20.0)))
        
        # 4. Determine overall status color/state for the dashboard UI
        status = "CRITICAL" if health_score < 40 else "WARNING" if health_score < 75 else "SAFE"
        
        return {
            "health_score": round(health_score, 2),
            "high_risk_assets": high_risk_mints,
            "status": status,
            "scanned_count": valid_reports
        }