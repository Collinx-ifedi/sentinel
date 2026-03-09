# ======================================================================================
# MODULE: SENTINEL BRIDGE (FASTAPI SERVER)
# VERSION: 4.1.0 "GOD MODE"
# DESCRIPTION: High-performance async API gateway. Handles Argon2 + Brevo 2FA, 
#              WebSocket Neural Sync, static file serving, and live portfolio pricing.
# ======================================================================================

import os
import sys
import secrets
from pathlib import Path
import jwt
import httpx
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

from fastapi import FastAPI, Depends, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, EmailStr
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
# --- SENTINEL MODULE IMPORTS ---
from src.database.db import get_db, init_db
from src.database.models import User, AgentWallet, TradeHistory
from src.agent_engine.nlp_handler import SentinelNLPHandler
from src.services.solana_executor import SolanaExecutor
from src.services.market_resolver import MarketResolver
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

# Setup Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("SentinelServer")

# ======================================================================================
# 1. SECURITY & AUTHENTICATION CONFIGURATION
# ======================================================================================
# God-Mode Argon2 Configuration (High memory cost to defeat ASICs)
ph = PasswordHasher(
    time_cost=3, 
    memory_cost=65536, # 64MB RAM per hash
    parallelism=4, 
    hash_len=32, 
    salt_len=16
)

# CRITICAL SECURITY FIX: No default fallback. Server will not boot without a secure key.
SECRET_KEY = os.getenv("JWT_SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError("CRITICAL FAULT: JWT_SECRET_KEY is missing from environment variables.")

ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

# Brevo Configuration
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "auth@glitchape.fun")
SENDER_NAME = os.getenv("SENDER_NAME", "GlitchApe Protocol")

# ======================================================================================
# 2. WEBSOCKET MANAGER (NEURAL SYNC)
# ======================================================================================
class ConnectionManager:
    """Manages active WebSocket connections for real-time Neural Sync streaming."""
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, user_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[user_id] = websocket
        log.info(f"Neural Sync established for User: {user_id}")

    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]
            log.info(f"Neural Sync disconnected for User: {user_id}")

    async def stream_logs(self, user_id: str, logs: List[str]):
        """Streams thoughts to the frontend terminal one by one to simulate 'thinking'."""
        if user_id in self.active_connections:
            ws = self.active_connections[user_id]
            for log_entry in logs:
                try:
                    await ws.send_json({"type": "neural_sync", "data": log_entry})
                    await asyncio.sleep(0.3) # Artificial delay for terminal aesthetic
                except Exception as e:
                    log.error(f"WS Sync Error: {e}")
                    break

ws_manager = ConnectionManager()

# ======================================================================================
# 3. FASTAPI LIFESPAN & INSTANCE
# ======================================================================================
# Singletons for the AI Engine
ai_brain = SentinelNLPHandler()
solana_exec = SolanaExecutor()

async def lifespan(app: FastAPI):
    """Executes on server startup and shutdown."""
    log.info("Booting Sentinel Protocol...")
    await init_db()
    
    # Ensure frontend directory exists to prevent crash on mounting
    os.makedirs("frontend", exist_ok=True)
    
    yield
    log.info("Shutting down Sentinel Protocol. Closing executor RPC...")
    await solana_exec.close()

app = FastAPI(title="Sentinel Protocol API", version="4.1", lifespan=lifespan)

# RESTRICTED CORS FOR PRODUCTION
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:8000")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "https://glitchape.fun", "https://www.glitchape.fun"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

# ======================================================================================
# 4. PYDANTIC SCHEMAS
# ======================================================================================
class AuthRequest(BaseModel):
    email: EmailStr
    password: str

class OTPVerify(BaseModel):
    email: EmailStr
    otp_code: str

class ChatRequest(BaseModel):
    message: str

# ======================================================================================
# 5. AUTHENTICATION ROUTES (ARGON2 + BREVO 2FA)
# ======================================================================================
async def send_brevo_otp(email: str, otp: str):
    """Fires the 2FA code via Brevo API using environment configuration."""
    if not BREVO_API_KEY:
        log.warning(f"BREVO_API_KEY missing. DEV MODE OTP for {email}: {otp}")
        return

    headers = {"accept": "application/json", "api-key": BREVO_API_KEY, "content-type": "application/json"}
    payload = {
        "sender": {"name": SENDER_NAME, "email": SENDER_EMAIL},
        "to": [{"email": email}],
        "subject": "Sentinel Protocol: Authentication Matrix (2FA)",
        "htmlContent": f"<html><body><h3>Your Neural Sync Access Code is: <b style='font-family:monospace; font-size: 24px; color: #00F0FF;'>{otp}</b></h3><p>Valid for 10 minutes.</p></body></html>"
    }
    
    async with httpx.AsyncClient() as client:
        res = await client.post("https://api.brevo.com/v3/smtp/email", headers=headers, json=payload)
        if res.status_code != 201:
            log.error(f"Brevo API Error: {res.text}")

@app.post("/auth/register", summary="Create User & Solana Wallet")
async def register(req: AuthRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.email == req.email))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Identity already registered.")

    # 1. Argon2 Hashing
    hashed_pw = ph.hash(req.password)
    new_user = User(email=req.email, hashed_password=hashed_pw)
    db.add(new_user)
    await db.flush() # Get user ID before creating wallet

    # 2. Generate AES-256 Encrypted Solana Wallet
    pubkey, enc_privkey = solana_exec.generate_wallet()
    wallet = AgentWallet(user_id=new_user.id, public_key=pubkey, encrypted_privkey=enc_privkey)
    db.add(wallet)
    await db.commit()

    return {"status": "success", "message": "Identity registered. Wallet secured."}

@app.post("/auth/login", summary="Verify Password & Trigger 2FA")
async def login(req: AuthRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.email == req.email))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    # 1. Argon2 Verification
    try:
        ph.verify(user.hashed_password, req.password)
        # Auto-upgrade hash if Argon2 parameters changed in code
        if ph.check_needs_rehash(user.hashed_password):
            user.hashed_password = ph.hash(req.password)
    except VerifyMismatchError:
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    # 2. Generate Cryptographically Secure OTP
    otp_code = ''.join(secrets.choice("0123456789") for _ in range(6))
    
    user.otp_code = otp_code
    user.otp_expiry = datetime.now(timezone.utc) + timedelta(minutes=10)
    await db.commit()
    
    await send_brevo_otp(user.email, otp_code)
    return {"status": "pending_2fa", "message": "Argon2 verified. 2FA code dispatched via Brevo."}

@app.post("/auth/verify", summary="Verify OTP & Issue JWT")
async def verify_otp(req: OTPVerify, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.email == req.email))
    user = result.scalar_one_or_none()

    if not user or user.otp_code != req.otp_code:
        raise HTTPException(status_code=401, detail="Invalid or expired OTP.")
        
    if datetime.now(timezone.utc) > user.otp_expiry:
        raise HTTPException(status_code=401, detail="OTP expired.")

    # Clear OTP and verify
    user.otp_code = None
    user.is_verified = True
    await db.commit()

    # Issue JWT
    expire = datetime.now(timezone.utc) + timedelta(hours=24)
    token = jwt.encode({"sub": user.id, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)
    return {"access_token": token, "token_type": "bearer"}

# ======================================================================================
# 6. DEPENDENCIES & WEBSOCKETS
# ======================================================================================
async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid session token.")
        
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="User not found.")
    return user

@app.websocket("/ws/neural-sync")
async def websocket_endpoint(websocket: WebSocket, token: str):
    """Authenticates WS connection via token query param and maps to user."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        await ws_manager.connect(user_id, websocket)
        try:
            while True:
                # Keep connection alive
                await websocket.receive_text()
        except WebSocketDisconnect:
            ws_manager.disconnect(user_id)
    except jwt.PyJWTError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)

# ======================================================================================
# 7. THE SENTINEL ORCHESTRATION ROUTE
# ======================================================================================
@app.post("/api/chat", summary="Interact with Sentinel NLP Brain")
async def chat_with_sentinel(req: ChatRequest, user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """
    1. Sends user input to NLP Brain.
    2. Streams Neural Sync thoughts to WS.
    3. Executes trade if AI clears it.
    """
    # Get user's encrypted wallet for potential execution
    wallet_result = await db.execute(select(AgentWallet).where(AgentWallet.user_id == user.id))
    wallet = wallet_result.scalar_one_or_none()
    
    if not wallet:
        raise HTTPException(status_code=500, detail="Wallet architecture corrupted.")

    # Let the Brain process the input (Passing public key for real-time scans)
    ai_response = await ai_brain.process(req.message, user.id, wallet.encrypted_privkey, wallet.public_key)
    
    # Background Task: Stream thoughts to the user's terminal via WebSocket
    asyncio.create_task(ws_manager.stream_logs(user.id, ai_response.get("neural_sync", [])))

    # Execute Trade if the AI reached an "action_required" state
    trade_result = None
    if ai_response.get("action_required") and ai_response.get("trade_details"):
        trade = ai_response["trade_details"]
        
        # Convert SOL/USD amounts to lamports (Simplified representation)
        amount_lamports = int(trade["amount"] * 1_000_000_000) if trade["unit"] == "SOL" else int(trade["amount"] * 1_000_000)

        # Call the Hands (Solana Executor)
        trade_result = await solana_exec.execute_swap(
            user_id=user.id,
            encrypted_pk=wallet.encrypted_privkey,
            input_mint="So11111111111111111111111111111111111111112" if trade["side"] == "BUY" else trade["mint"],
            output_mint=trade["mint"] if trade["side"] == "BUY" else "So11111111111111111111111111111111111111112",
            amount_lamports=amount_lamports,
            is_safe=True, # Safety was already gated inside NLPHandler
            db_session=db
        )

        # Stream the execution result back to the terminal
        if trade_result["success"]:
            success_log = f"[EXECUTION] Trade Confirmed. Signature: {trade_result['signature'][:12]}..."
            asyncio.create_task(ws_manager.stream_logs(user.id, [success_log]))
        else:
            fail_log = f"[EXECUTION FAILED] {trade_result['error']}"
            asyncio.create_task(ws_manager.stream_logs(user.id, [fail_log]))

    return {
        "reply": ai_response.get("response"),
        "trade_status": trade_result
    }

# ======================================================================================
# 8. DASHBOARD API ROUTES (LIVE PORTFOLIO, YIELD, HISTORY)
# ======================================================================================

@app.get("/wallet/portfolio", summary="Fetch Portfolio Balances & Prices")
async def get_portfolio(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    wallet_result = await db.execute(select(AgentWallet).where(AgentWallet.user_id == user.id))
    wallet = wallet_result.scalar_one_or_none()
    if not wallet:
        raise HTTPException(status_code=404, detail="Wallet not found.")

    # 1. Fetch on-chain balances
    native_sol_balance = await solana_exec.get_native_balance(wallet.public_key)
    token_balances = await solana_exec.get_all_token_balances(wallet.public_key)
    
    # Compile list of all mints the user owns to resolve metadata
    all_mints = [t["mint"] for t in token_balances]
    metadata = await solana_exec.get_token_metadata(all_mints)

    # Instantiate resolver for live pricing
    resolver = MarketResolver()

    # 2. Build the UI response payload
    portfolio = []
    
    # Add Native SOL with live pricing
    _, sol_price = await resolver.resolve_and_price("SOL")
    sol_price = sol_price if sol_price is not None else 0.0
    
    portfolio.append({
        "name": "Solana",
        "symbol": "SOL",
        "balance": native_sol_balance,
        "mint": "So11111111111111111111111111111111111111112",
        "price_usd": sol_price, 
        "total_value_usd": native_sol_balance * sol_price
    })

    # Add SPL Tokens with live pricing
    for t in token_balances:
        mint = t["mint"]
        meta = metadata.get(mint, {})
        symbol = meta.get("symbol", f"UNK-{mint[:4]}")
        
        # Resolve dynamic price via MarketResolver using the mint address
        _, token_price = await resolver.resolve_and_price(mint)
        token_price = token_price if token_price is not None else 0.0
        
        portfolio.append({
            "name": meta.get("name", "Unknown Token"),
            "symbol": symbol,
            "balance": t["amount"],
            "mint": mint,
            "price_usd": token_price,
            "total_value_usd": t["amount"] * token_price
        })

    total_net_worth = sum(asset["total_value_usd"] for asset in portfolio)
    
    # Update AI Memory Context so the bot knows the user's holdings
    ai_brain.memory.update_session(user.id, {"portfolio": portfolio})

    return {
        "sol_balance": native_sol_balance,
        "total_net_worth_usd": total_net_worth,
        "assets": portfolio
    }

@app.get("/wallet/yield", summary="Fetch Active Staking & Yield Positions")
async def get_yield_positions(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    wallet_result = await db.execute(select(AgentWallet).where(AgentWallet.user_id == user.id))
    wallet = wallet_result.scalar_one_or_none()
    if not wallet:
        raise HTTPException(status_code=404, detail="Wallet not found.")

    token_balances = await solana_exec.get_all_token_balances(wallet.public_key)
    yield_positions = await solana_exec.get_staking_positions(token_balances)
    
    # Map mocked APY data for known LSTs
    enriched_yields = []
    for pos in yield_positions:
        apy = "7.5%" if pos["protocol"] == "JitoSOL" else "8.2%" if pos["protocol"] == "mSOL" else "6.8%"
        enriched_yields.append({
            "protocol": pos["protocol"],
            "balance": pos["amount"],
            "apy": apy,
            "status": "Active"
        })

    return {"yield_positions": enriched_yields}

@app.get("/wallet/history", summary="Fetch Trade/Acquisition Ledger")
async def get_trade_history(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    # Query trade history, latest first, limit 50
    result = await db.execute(
        select(TradeHistory)
        .where(TradeHistory.user_id == user.id)
        .order_by(desc(TradeHistory.timestamp))
        .limit(50)
    )
    history = result.scalars().all()

    # Collect unique mints to resolve metadata
    mints_to_resolve = list({tx.input_mint for tx in history} | {tx.output_mint for tx in history})
    metadata = await solana_exec.get_token_metadata(mints_to_resolve)

    formatted_history = []
    for tx in history:
        in_meta = metadata.get(tx.input_mint)