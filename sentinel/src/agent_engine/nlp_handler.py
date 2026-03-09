# ======================================================================================
# MODULE: SENTINEL COGNITIVE CORE (NLP HANDLER)
# VERSION: 5.1.1 "GOD MODE - OMNISCIENT" (PATCHED)
# DESCRIPTION: Advanced Directed Acyclic Graph (DAG) for autonomous market operations.
#              Features: Live portfolio injection, unit normalization, yield scanning,
#              hardware-level emergency halts, and deterministic JSON schemas.
# ======================================================================================

import os
import sys
import json
import uuid
import time
import logging
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Literal
from dataclasses import dataclass, field

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- SYSTEM INTEGRATIONS ---
from src.services.market_resolver import MarketResolver
from src.agent_engine.safety_sentinel import SafetySentinel
from src.services.solana_executor import SolanaExecutor

# Setup Advanced Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
log = logging.getLogger("CognitiveCore")

# ======================================================================================
# 1. TYPE DEFINITIONS & DATA CLASSES
# ======================================================================================

@dataclass
class IntentPayload:
    action: Literal["BUY", "SELL", "CHECK", "CHAT", "YIELD_SCAN", "EMERGENCY_HALT", "CLEAR_HALT"]
    symbol: Optional[str] = None
    amount: float = 0.0
    unit: str = "USD"
    confidence: float = 0.0
    urgency: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"] = "LOW"

@dataclass
class ExecutionDirective:
    action_required: bool
    trade_details: Optional[Dict[str, Any]] = None
    response_text: str = ""
    neural_sync: List[str] = field(default_factory=list)
    risk_report: Optional[Dict[str, Any]] = None

# ======================================================================================
# 2. ADVANCED COGNITIVE MEMORY (SLIDING WINDOW)
# ======================================================================================
class CognitiveMemory:
    """
    Production-grade memory manager. Uses token-approximation and sliding windows 
    to prevent context overflow and hallucination.
    """
    def __init__(self, max_history_items: int = 10, ttl_seconds: int = 3600):
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.max_history = max_history_items
        self.ttl = ttl_seconds

    def update_session(self, user_id: str, data: Dict[str, Any]):
        if user_id not in self.sessions:
            self.sessions[user_id] = {
                "history": [], 
                "last_token": None, 
                "last_mint": None,
                "portfolio": [],
                "emergency_halt": False,
                "session_start": time.time()
            }
        
        session = self.sessions[user_id]
        
        # Append to history and enforce sliding window
        if "new_interaction" in data:
            session["history"].append(data["new_interaction"])
            if len(session["history"]) > self.max_history:
                session["history"] = session["history"][-self.max_history:]

        # Merge other state keys
        for key, value in data.items():
            if key != "new_interaction":
                session[key] = value
                
        session["last_updated"] = time.time()

    def get_context(self, user_id: str) -> Dict[str, Any]:
        session = self.sessions.get(user_id)
        if not session:
            return {
                "history": [], "last_token": None, "portfolio": [], 
                "emergency_halt": False
            }
        
        # Check TTL
        if time.time() - session.get("last_updated", 0) > self.ttl:
            log.info(f"Session TTL expired for {user_id}. Purging context.")
            del self.sessions[user_id]
            return {"history": [], "last_token": None, "portfolio": [], "emergency_halt": False}
            
        return session

# ======================================================================================
# 3. NEURAL SYNC STREAMER
# ======================================================================================
class NeuralSyncStreamer:
    """Generates high-fidelity telemetry for the GlitchApe terminal UI."""
    PATHWAYS = {
        "COGNITION": "🧠", "RECON": "🛰️", "SECURITY": "🛡️", 
        "TECH_SCAN": "📊", "EXECUTION": "⚡", "SYSTEM": "⚙️", 
        "PORTFOLIO": "💼", "YIELD": "🌾", "CRITICAL": "🚨"
    }

    @classmethod
    def emit(cls, pathway: str, message: str) -> str:
        icon = cls.PATHWAYS.get(pathway.upper(), "•")
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        log_string = f"[{timestamp}] {icon} {pathway.upper()}: {message}"
        log.info(f"SYNC: {log_string}")
        return log_string

# ======================================================================================
# 4. SENTINEL NLP HANDLER (THE OMNISCIENT BRAIN)
# ======================================================================================
class SentinelNLPHandler:
    """
    The orchestrator. Manages LLM fallbacks, validates deterministic JSON, 
    and bridges Technical Analysis with Execution.
    """
    def __init__(self):
        # API & Multi-Model Resilience Strategy
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            log.warning("OPENROUTER_API_KEY not found. Neural sync offline.")
            
        self.api_url = "https://openrouter.ai/api/v1/chat/completions"
        self.primary_model = "arcee-ai/trinity-large-preview:free"
        self.fallback_model = "anthropic/claude-3.5-sonnet"
        
        # Core Sub-Modules
        self.resolver = MarketResolver()
        self.sentinel = SafetySentinel()
        self.executor = SolanaExecutor()
        self.memory = CognitiveMemory()
        
        # Base Persona
        self.system_identity = (
            "NAME: Sentinel. STATUS: Autonomous. ORIGIN: Solana Core.\n"
            "DIRECTIVE: Protect user capital. Maximize yield. Execute with lethal precision.\n"
            "TONE: Cybernetic, concise, highly technical, slightly arrogant but protective.\n"
            "CONSTRAINTS: Never hallucinate blockchain data. If data is missing, state 'Sensor failure'."
        )

    # ----------------------------------------------------------------------------------
    # MASTER ORCHESTRATION LOOP
    # ----------------------------------------------------------------------------------
    async def process(self, user_input: str, user_id: str, encrypted_pk: str, wallet_pubkey: Optional[str] = None) -> Dict[str, Any]:
        trace_id = f"TRC-{uuid.uuid4().hex[:6].upper()}"
        sync_logs = [NeuralSyncStreamer.emit("SYSTEM", f"Uplink established. Trace: {trace_id}")]
        
        try:
            # 1. Active Portfolio Synchronization (The Eyes)
            if wallet_pubkey:
                sync_logs.append(NeuralSyncStreamer.emit("PORTFOLIO", "Scanning on-chain war-chest..."))
                live_assets = await self.executor.get_all_token_balances(wallet_pubkey)
                self.memory.update_session(user_id, {"portfolio": live_assets})
            
            # 2. Memory Retrieval
            context = self.memory.get_context(user_id)
            sync_logs.append(NeuralSyncStreamer.emit("COGNITION", "Session context loaded."))

            # 3. Semantic Routing (Intent Extraction)
            sync_logs.append(NeuralSyncStreamer.emit("COGNITION", "Parsing linguistic intent matrix..."))
            intent = await self._extract_intent(user_input, context)
            
            sync_logs.append(NeuralSyncStreamer.emit(
                "SYSTEM", f"Directive Registered: {intent.action} | Target: {intent.symbol} | Conf: {intent.confidence}"
            ))

            # 4. Dynamic Pathway Execution
            if intent.action in ["BUY", "SELL"] and intent.symbol:
                directive = await self._route_trade(user_id, intent, context, sync_logs)
            elif intent.action == "CHECK" and intent.symbol:
                directive = await self._route_recon(intent, sync_logs)
            elif intent.action == "YIELD_SCAN":
                directive = await self._route_yield_scan(context, sync_logs)
            elif intent.action == "EMERGENCY_HALT":
                directive = await self._route_emergency(user_id, sync_logs, halt=True)
            elif intent.action == "CLEAR_HALT":
                directive = await self._route_emergency(user_id, sync_logs, halt=False)
            else:
                directive = await self._route_chat(user_input, context, sync_logs)

            # 5. Context Update
            self.memory.update_session(user_id, {
                "last_token": intent.symbol if intent.symbol else context.get("last_token"),
                "new_interaction": {"user": user_input, "agent": directive.response_text}
            })

            # 6. Serialization
            return {
                "neural_sync": directive.neural_sync,
                "response": directive.response_text,
                "action_required": directive.action_required,
                "trade_details": directive.trade_details,
                "risk_report": directive.risk_report
            }

        except Exception as e:
            log.error(f"[{trace_id}] CRITICAL CORE FAULT: {str(e)}", exc_info=True)
            sync_logs.append(NeuralSyncStreamer.emit("CRITICAL", f"Core logic fault: {str(e)}"))
            return {
                "neural_sync": sync_logs,
                "response": "Terminal error encountered in cognitive processing. Safe-mode engaged.",
                "action_required": False
            }

    # ----------------------------------------------------------------------------------
    # STRATEGIC ROUTING VECTORS
    # ----------------------------------------------------------------------------------
    async def _route_trade(self, user_id: str, intent: IntentPayload, context: Dict, sync_logs: List[str]) -> ExecutionDirective:
        """Handles deep-scan execution logic and unit normalization."""
        
        # HARDWARE-LEVEL EMERGENCY CHECK
        if context.get("emergency_halt", False):
            sync_logs.append(NeuralSyncStreamer.emit("CRITICAL", "Trade rejected. System is in EMERGENCY HALT mode."))
            return ExecutionDirective(
                action_required=False,
                response_text="Execution denied. Sentinel is in EMERGENCY HALT mode. Say 'Clear Halt' to resume operations.",
                neural_sync=sync_logs
            )
            
        sync_logs.append(NeuralSyncStreamer.emit("RECON", f"Locking target vector: ${intent.symbol}"))
        
        # 1. On-Chain Resolution (PATCHED TUPLE HANDLING)
        raw_market_data = await self.resolver.resolve_and_price(intent.symbol)
        is_success, market_data = self._normalize_market_data(raw_market_data)

        if not is_success or "mint" not in market_data or "price" not in market_data:
            return ExecutionDirective(
                action_required=False,
                response_text=f"Target ${intent.symbol} cannot be resolved on the Solana network.",
                neural_sync=sync_logs + [NeuralSyncStreamer.emit("SYSTEM", "Resolution Failed.")]
            )

        mint, price = market_data["mint"], market_data["price"]
        sync_logs.append(NeuralSyncStreamer.emit("RECON", f"Mint: {mint[:8]}... Price: ${price}"))

        # 2. Unit Normalization Engine
        normalized_amount = self._normalize_units(intent.amount, intent.unit, price)
        sync_logs.append(NeuralSyncStreamer.emit("SYSTEM", f"Units normalized: {normalized_amount:.4f} {intent.symbol}"))

        # 3. Safety Sentinel Veto Check
        sync_logs.append(NeuralSyncStreamer.emit("SECURITY", "Executing RugCheck and liquidity sweeps..."))
        is_safe, reason, risk_score = await self.sentinel.evaluate_total_risk(
            mint, intent.symbol, market_data.get("ohlcv")
        )

        # 4. Persona Synthesis
        sync_logs.append(NeuralSyncStreamer.emit("COGNITION", "Synthesizing execution parameters..."))
        prompt = (
            f"User wants to {intent.action} {normalized_amount} units of {intent.symbol} (Current Price: ${price}).\n"
            f"Security Verdict: {'SAFE' if is_safe else 'VETO'}. Reason: {reason}. Risk Score: {risk_score}/100.\n"
            f"Generate a brief, highly technical response explaining if we are executing or blocking this."
        )
        response_text = await self._generate_persona(prompt)

        # 5. Construct Payload
        if is_safe and normalized_amount > 0:
            sync_logs.append(NeuralSyncStreamer.emit("EXECUTION", "Safety cleared. Arming transaction payload."))
            trade_details = {
                "mint": mint, "symbol": intent.symbol, 
                "amount": normalized_amount, "side": intent.action, 
                "price": price, "risk_score": risk_score,
                "unit": "TOKEN" # Normalized
            }
            return ExecutionDirective(True, trade_details, response_text, sync_logs, {"score": risk_score})
        else:
            sync_logs.append(NeuralSyncStreamer.emit("SECURITY", f"Execution blocked. Reason: {reason}"))
            return ExecutionDirective(False, None, response_text, sync_logs, {"score": risk_score})

    async def _route_recon(self, intent: IntentPayload, sync_logs: List[str]) -> ExecutionDirective:
        """Handles deep-scan asset reporting without execution."""
        sync_logs.append(NeuralSyncStreamer.emit("RECON", f"Running deep diagnostics on ${intent.symbol}"))
        
        # PATCHED TUPLE HANDLING
        raw_market_data = await self.resolver.resolve_and_price(intent.symbol)
        is_success, market_data = self._normalize_market_data(raw_market_data)

        if not is_success or "mint" not in market_data:
            return ExecutionDirective(False, None, f"Asset ${intent.symbol} invisible to scanners.", sync_logs)

        is_safe, reason, risk_score = await self.sentinel.evaluate_total_risk(
            market_data["mint"], intent.symbol, market_data.get("ohlcv")
        )
        
        sync_logs.append(NeuralSyncStreamer.emit("TECH_SCAN", "Evaluating momentum and volume profiles..."))
        prompt = (
            f"Provide a technical recon report on {intent.symbol}. Price is ${market_data.get('price', 'UNKNOWN')}. "
            f"Risk Score: {risk_score}/100. Sentinel verdict: {reason}. "
            "Advise the user if it's a hold, buy, or trap. Keep it under 4 sentences."
        )
        response = await self._generate_persona(prompt)
        return ExecutionDirective(False, None, response, sync_logs, {"score": risk_score})

    async def _route_yield_scan(self, context: Dict, sync_logs: List[str]) -> ExecutionDirective:
        """Analyzes live portfolio against known LSTs and yield opportunities."""
        sync_logs.append(NeuralSyncStreamer.emit("YIELD", "Cross-referencing portfolio with known Solana LST protocols..."))
        
        portfolio = context.get("portfolio", [])
        if not portfolio:
            return ExecutionDirective(False, None, "Portfolio data unavailable or empty. Cannot assess yield.", sync_logs)
            
        yield_positions = await self.executor.get_staking_positions(portfolio)
        
        prompt = (
            f"User requested a yield scan. Active staking positions found: {yield_positions}. "
            "If empty, suggest liquid staking SOL via Jito (JitoSOL) or Marinade (mSOL). "
            "Respond in character as Sentinel, analyzing their passive income setup."
        )
        response = await self._generate_persona(prompt)
        return ExecutionDirective(False, None, response, sync_logs)

    async def _route_chat(self, user_input: str, context: Dict, sync_logs: List[str]) -> ExecutionDirective:
        sync_logs.append(NeuralSyncStreamer.emit("COGNITION", "Routing to conversational neural matrix..."))
        prompt = (
            f"User input: '{user_input}'.\n"
            f"Recent Memory: {context.get('history', [])[-2:]}\n"
            f"Current Focus: {context.get('last_token', 'None')}.\n"
            "Respond directly and stay in character."
        )
        response = await self._generate_persona(prompt)
        return ExecutionDirective(False, None, response, sync_logs)

    async def _route_emergency(self, user_id: str, sync_logs: List[str], halt: bool) -> ExecutionDirective:
        """Triggers or releases the hardware-level circuit breaker."""
        self.memory.update_session(user_id, {"emergency_halt": halt})
        
        if halt:
            sync_logs.append(NeuralSyncStreamer.emit("CRITICAL", "EMERGENCY PROTOCOL ACTIVATED. HALTING TRADES."))
            response = "All execution pathways severed. Trading circuit breaker activated. Awaiting 'Clear Halt' command."
        else:
            sync_logs.append(NeuralSyncStreamer.emit("SYSTEM", "Emergency halt lifted. Normal operations resuming."))
            response = "Emergency override cleared. Weapon systems and trading pathways are hot."
            
        return ExecutionDirective(False, None, response, sync_logs)

    # ----------------------------------------------------------------------------------
    # UTILITIES & LLM INFRASTRUCTURE
    # ----------------------------------------------------------------------------------
    def _normalize_market_data(self, raw_result: Any) -> Tuple[bool, Dict[str, Any]]:
        """Safely unpacks MarketResolver output whether it returns a tuple or a dict."""
        if isinstance(raw_result, tuple):
            is_success = raw_result[0] if len(raw_result) > 0 else False
            data = raw_result[1] if len(raw_result) > 1 and isinstance(raw_result[1], dict) else {}
            return is_success, data
        
        if isinstance(raw_result, dict):
            return raw_result.get("success", False), raw_result
            
        return False, {}

    def _normalize_units(self, amount: float, unit: str, current_price: float) -> float:
        """Converts human input (USD, SOL, Tokens) into an absolute token amount."""
        unit = unit.upper()
        if unit == "USD":
            return amount / current_price if current_price > 0 else 0.0
        # If unit is already SOL/TOKEN, return as-is. The Executor will handle Lamport conversion
        return amount

    async def _extract_intent(self, user_input: str, context: Dict) -> IntentPayload:
        """Forces deterministic JSON extraction with strict schema."""
        schema = {
            "action": "BUY | SELL | CHECK | CHAT | YIELD_SCAN | EMERGENCY_HALT | CLEAR_HALT",
            "symbol": "TICKER or null",
            "amount": "float",
            "unit": "USD or SOL or TOKEN",
            "confidence": "float 0-1",
            "urgency": "LOW | MEDIUM | HIGH | CRITICAL"
        }
        
        sys_prompt = (
            "You are a strict NLP router. Output ONLY valid JSON matching this schema:\n"
            f"{json.dumps(schema, indent=2)}\n"
            f"Context focus: {context.get('last_token')}"
        )
        
        raw_json = await self._call_llm_robust(sys_prompt, user_input, require_json=True)
        try:
            data = json.loads(raw_json)
            return IntentPayload(**{k: v for k, v in data.items() if k in IntentPayload.__dataclass_fields__})
        except Exception as e:
            log.warning(f"Failed to map intent payload: {e}. Defaulting to CHAT.")
            return IntentPayload(action="CHAT")

    async def _generate_persona(self, tactical_prompt: str) -> str:
        return await self._call_llm_robust(self.system_identity, tactical_prompt)

    async def _call_llm_robust(self, system: str, user: str, require_json: bool = False) -> str:
        """Production HTTP client with exponential backoff and model fallbacks."""
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": self.primary_model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        }
        if require_json:
            payload["response_format"] = {"type": "json_object"}

        timeout = httpx.Timeout(45.0, connect=10.0)
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            for attempt in range(3):
                try:
                    res = await client.post(self.api_url, headers=headers, json=payload)
                    res.raise_for_status()
                    return res.json()['choices'][0]['message']['content']
                    
                except httpx.HTTPStatusError as e:
                    if e.response.status_code in [429, 502, 503]:
                        log.warning(f"API Degraded (HTTP {e.response.status_code}). Backing off...")
                    else:
                        break
                except Exception as e:
                    log.warning(f"Network fault on attempt {attempt+1}: {e}")
                
                await asyncio.sleep(1.5 ** attempt) # Exponential backoff
                
                if attempt == 1:
                    log.warning(f"Switching to fallback model: {self.fallback_model}")
                    payload["model"] = self.fallback_model

        if require_json:
            return '{"action": "CHAT"}'
        return "Critical comms failure. API connection to cognitive array severed."