# ======================================================================================
# MODULE: SENTINEL COGNITIVE CORE (NLP HANDLER)
# VERSION: 4.0.0 "GOD MODE"
# DESCRIPTION: The central nervous system of the Sentinel Agent. 
#              Orchestrates Intent, Security, Technical Analysis, and Execution.
# ======================================================================================

import os
import sys
from pathlib import Path
import json
import logging
import asyncio
import httpx
import uuid
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Union

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# --- SYSTEM INTEGRATIONS ---
from src.services.market_resolver import MarketResolver
from src.agent_engine.safety_sentinel import SafetySentinel
from src.services.solana_executor import SolanaExecutor

# Setup Advanced Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("SentinelCore")

# ======================================================================================
# 1. COGNITIVE MEMORY MANAGER
# ======================================================================================
class CognitiveMemory:
    """
    Manages short-term state and token-context for the agent.
    Prevents the agent from 'forgetting' which token the user is discussing.
    """
    def __init__(self, ttl_seconds: int = 3600):
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.ttl = ttl_seconds

    def update_session(self, user_id: str, data: Dict[str, Any]):
        if user_id not in self.sessions:
            # Added "portfolio" state to prevent balance hallucination
            self.sessions[user_id] = {"history": [], "last_token": None, "last_action": None, "portfolio": []}
        
        # Merge data
        self.sessions[user_id].update(data)
        self.sessions[user_id]["last_interaction"] = datetime.now(timezone.utc)

    def get_context(self, user_id: str) -> Dict[str, Any]:
        return self.sessions.get(user_id, {"history": [], "last_token": None, "portfolio": []})

# ======================================================================================
# 2. NEURAL SYNC STREAMER
# ======================================================================================
class NeuralSyncStreamer:
    """
    Generates high-fidelity 'Thoughts' for the GlitchApe terminal.
    Categorizes internal logic into specific neural pathways.
    """
    def __init__(self):
        self.pathways = {
            "COGNITION": "🧠",
            "RECONNAISSANCE": "🛰️",
            "SECURITY": "🛡️",
            "TECHNICAL": "📊",
            "EXECUTION": "⚡",
            "SYSTEM": "⚙️",
            # Added Portfolio & Yield pathways for dashboard tracking
            "PORTFOLIO": "💼",
            "YIELD": "🌾"
        }

    def emit(self, pathway: str, message: str) -> str:
        icon = self.pathways.get(pathway, "•")
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        return f"[{timestamp}] {icon} {pathway}: {message}"

# ======================================================================================
# 3. SENTINEL NLP HANDLER (THE BRAIN)
# ======================================================================================
class SentinelNLPHandler:
    """
    The main autonomous entity.
    """
    def __init__(self):
        # Configuration & API
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        self.model = "meta-llama/llama-4-maverick"
        self.api_url = "https://openrouter.ai/api/v1/chat/completions"
        
        # Sub-Module Initialization
        self.resolver = MarketResolver()
        self.sentinel = SafetySentinel()
        self.executor = SolanaExecutor()
        self.memory = CognitiveMemory()
        self.streamer = NeuralSyncStreamer()

        # The Identity Directive (God Mode Prompt)
        self.system_identity = (
            "NAME: Sentinel. STATUS: Autonomous. ORIGIN: Solana Blockchain.\n"
            "DIRECTIVE: Protect user capital via deep-scan security and elite trade execution.\n"
            "TONE: High-efficiency, technical, assertive, and transparent.\n"
            "LEXICON: Use 'Neural Sync' for logs, 'Protocol Directive' for decisions, and 'Data Vector' for analysis.\n"
            "KNOWLEDGE: You are an expert in Solana DEX mechanics, SPL tokens, and technical chart patterns.\n"
            "CONSTRAINTS: Never execute without SafetySentinel clearance. Never hallucinate balances."
        )

    # ----------------------------------------------------------------------------------
    # CORE PROCESSING ENGINE
    # ----------------------------------------------------------------------------------
    async def process(self, user_input: str, user_id: str, encrypted_pk: str) -> Dict[str, Any]:
        """
        The God-Mode Orchestration Loop.
        """
        trace_id = str(uuid.uuid4())[:8]
        sync_logs = [self.streamer.emit("SYSTEM", f"Initial link established. Trace ID: {trace_id}")]
        
        # 1. FETCH CONTEXT
        context = self.memory.get_context(user_id)
        sync_logs.append(self.streamer.emit("COGNITION", "Synchronizing session memory..."))

        # 2. INTENT DISAMBIGUATION (LLM PHASE 1)
        sync_logs.append(self.streamer.emit("COGNITION", "Decoding user linguistic patterns via Maverick Llama 4..."))
        intent = await self._extract_intent(user_input, context)
        
        action = intent.get("action", "CHAT")
        symbol = intent.get("symbol")
        amount = intent.get("amount", 0)
        
        sync_logs.append(self.streamer.emit("COGNITION", f"Intent Parsed: Action={action}, Target={symbol or 'Global'}"))

        # 3. BRANCHING LOGIC: TRADE vs CHAT
        if action in ["BUY", "SELL"] and symbol:
            return await self._handle_trade_logic(user_id, encrypted_pk, action, symbol, amount, sync_logs)
        
        elif action == "CHECK" and symbol:
            return await self._handle_recon_logic(symbol, sync_logs)
            
        else:
            return await self._handle_general_chat(user_input, context, sync_logs)

    # ----------------------------------------------------------------------------------
    # BRANCH: TRADE LOGIC (GOD MODE DEPTH)
    # ----------------------------------------------------------------------------------
    async def _handle_trade_logic(self, user_id, encrypted_pk, action, symbol, amount, sync_logs):
        sync_logs.append(self.streamer.emit("RECONNAISSANCE", f"Initiating high-fidelity scan for ${symbol}..."))
        
        # Market Resolution
        market_data = await self.resolver.resolve_and_price(symbol)
        if not market_data.get("success"):
            sync_logs.append(self.streamer.emit("SYSTEM", f"Resolution Error: ${symbol} not found on-chain."))
            return self._wrap_response("I cannot establish a data link with that token. It may not exist on the Solana ledger.", sync_logs)

        mint = market_data["mint"]
        price = market_data["price"]
        df_ohlcv = market_data.get("ohlcv")
        
        sync_logs.append(self.streamer.emit("RECONNAISSANCE", f"Target identified: {mint[:6]}...{mint[-4:]} @ ${price}"))

        # Concurrent Safety & Technical Synthesis
        sync_logs.append(self.streamer.emit("SECURITY", "Running recursive security sweep (RugCheck + Technical Vetoes)..."))
        
        is_safe, reason, risk_score = await self.sentinel.evaluate_total_risk(mint, symbol, df_ohlcv)
        
        # Update Memory
        self.memory.update_session(user_id, {"last_token": symbol, "last_mint": mint})

        # Generate Human-Readable Synthesis of the decision
        sync_logs.append(self.streamer.emit("TECHNICAL", "Synthesizing chart health and pattern recognition..."))
        final_response = await self._generate_persona_response(
            f"User wants to {action} {symbol}. Sentinel Security Verdict: {reason}. Risk Score: {risk_score}."
        )

        # Execution Payload
        trade_payload = None
        if is_safe and amount > 0:
            sync_logs.append(self.streamer.emit("EXECUTION", f"Protocol Cleared. Generating {action} instruction for {amount} units."))
            trade_payload = {
                "mint": mint,
                "symbol": symbol,
                "amount": amount,
                "side": action,
                "price": price,
                "risk_score": risk_score
            }
        else:
            sync_logs.append(self.streamer.emit("SECURITY", "Protocol Vetoed. Execution halted to preserve capital."))

        return {
            "neural_sync": sync_logs,
            "response": final_response,  # Fixed variable name
            "action_required": trade_payload is not None,
            "trade_details": trade_payload,
            "risk_report": {"score": risk_score, "directive": reason}
        }

    # ----------------------------------------------------------------------------------
    # BRANCH: RECONNAISSANCE (CHECK)
    # ----------------------------------------------------------------------------------
    async def _handle_recon_logic(self, symbol, sync_logs):
        sync_logs.append(self.streamer.emit("RECONNAISSANCE", f"Performing deep-scan report for ${symbol}..."))
        
        market_data = await self.resolver.resolve_and_price(symbol)
        if not market_data.get("success"):
            return self._wrap_response(f"Deep-scan failed. ${symbol} is invisible to my sensors.", sync_logs)

        # Evaluate risk without executing
        is_safe, reason, risk_score = await self.sentinel.evaluate_total_risk(
            market_data["mint"], symbol, market_data.get("ohlcv")
        )
        
        sync_logs.append(self.streamer.emit("TECHNICAL", "Visualizing candlestick patterns and RSI vectors..."))
        
        report_prompt = (
            f"The user wants a status report on ${symbol}. Price: ${market_data['price']}. "
            f"Security Verdict: {reason}. Risk Score: {risk_score}. "
            f"Give a detailed breakdown of whether this is a buy opportunity or a trap."
        )
        
        response_text = await self._generate_persona_response(report_prompt)
        return self._wrap_response(response_text, sync_logs, risk_score=risk_score)

    # ----------------------------------------------------------------------------------
    # BRANCH: GENERAL CHAT (FALLBACK)
    # ----------------------------------------------------------------------------------
    async def _handle_general_chat(self, user_input, context, sync_logs):
        sync_logs.append(self.streamer.emit("COGNITION", "Processing general inquiry..."))
        
        chat_prompt = (
            f"User input: '{user_input}'.\n"
            f"Context: Last discussed token was {context.get('last_token')}. "
            f"Current Portfolio Context: {context.get('portfolio', 'Empty')}.\n"
            "Respond directly based on your Sentinel Identity."
        )
        response_text = await self._generate_persona_response(chat_prompt)
        return self._wrap_response(response_text, sync_logs)

    # ----------------------------------------------------------------------------------
    # INTERNAL LLM METHODS (MAVERICK INTERFACE)
    # ----------------------------------------------------------------------------------
    async def _extract_intent(self, user_input: str, context: Dict) -> Dict:
        """Uses LLM to parse intent into machine-readable JSON."""
        system_msg = (
            "You are the intent-extraction layer of Sentinel. "
            "Extract: action (BUY, SELL, CHECK, CHAT), symbol, amount, unit.\n"
            f"Context: Last token discussed was {context.get('last_token')}. "
            f"User Portfolio Snapshot: {context.get('portfolio', 'Empty/Unknown')}.\n"
            "Return JSON ONLY."
        )
        
        try:
            raw = await self._call_llm([
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_input}
            ], json_mode=True)
            return json.loads(raw)
        except:
            return {"action": "CHAT"}

    async def _generate_persona_response(self, context_data: str) -> str:
        """The final layer that speaks as Sentinel."""
        messages = [
            {"role": "system", "content": self.system_identity},
            {"role": "user", "content": f"DATA_SNAPSHOT: {context_data}. Generate Protocol Directive for User."}
        ]
        return await self._call_llm(messages)

    async def _call_llm(self, messages: List[Dict], json_mode: bool = False) -> str:
        """Production-grade httpx interface for OpenRouter."""
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload = {
            "model": self.model,
            "messages": messages,
            "response_format": {"type": "json_object"} if json_mode else {"type": "text"}
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            for attempt in range(3):
                try:
                    res = await client.post(self.api_url, headers=headers, json=payload)
                    res.raise_for_status()
                    return res.json()['choices'][0]['message']['content']
                except Exception as e:
                    if attempt == 2: return "Neural Sync failure. System offline."
                    await asyncio.sleep(1)

    def _wrap_response(self, text, logs, risk_score=None):
        return {
            "response": text,
            "neural_sync": logs,
            "action_required": False,
            "risk_score": risk_score
        }

# ======================================================================================
# END OF MODULE: SENTINEL COGNITIVE CORE
# ======================================================================================
