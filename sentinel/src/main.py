# ======================================================================================
# MODULE: SENTINEL IGNITION (MAIN ENTRY POINT)
# VERSION: 4.0.0 "GOD MODE"
# DESCRIPTION: Secure bootloader for the Sentinel Protocol. Validates cryptography,
#              tests external connections, and provisions the Uvicorn web workers.
# ======================================================================================

import os
import sys
import asyncio
import argparse
import logging
from urllib.parse import urlparse
from dotenv import load_dotenv

# --- HIGH-PERFORMANCE SERVER ---
import uvicorn
import httpx
from sqlalchemy.ext.asyncio import create_async_engine

# --- SETUP TERMINAL LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("SentinelBoot")

# ======================================================================================
# 1. PRE-FLIGHT DIAGNOSTICS (ENVIRONMENT & SECURITY)
# ======================================================================================
def _validate_environment() -> bool:
    """
    Enforces strict security checks on environment variables before boot.
    """
    load_dotenv()
    log.info("Initiating Level-1 Security Sweep...")
    
    critical_keys = [
        "OPENROUTER_API_KEY", "BREVO_API_KEY", "DATABASE_URL",
        "JWT_SECRET_KEY", "MASTER_ENCRYPTION_KEY", "SOLANA_RPC_URL"
    ]
    
    missing = [key for key in critical_keys if not os.getenv(key)]
    if missing:
        log.critical(f"Boot Aborted. Missing critical environment variables: {', '.join(missing)}")
        return False

    # 1. Cryptographic Key Strength Checks
    master_key = os.getenv("MASTER_ENCRYPTION_KEY")
    if len(master_key) < 32:
        log.critical("Boot Aborted. MASTER_ENCRYPTION_KEY must be at least 32 characters for secure AES-256 derivation.")
        return False

    jwt_key = os.getenv("JWT_SECRET_KEY")
    if len(jwt_key) < 32:
        log.critical("Boot Aborted. JWT_SECRET_KEY is too weak. Vulnerable to brute-force session hijacking.")
        return False
        
    # 2. Database URL Protocol Check (Async requirement)
    db_url = os.getenv("DATABASE_URL")
    if not db_url.startswith(("sqlite+aiosqlite://", "postgresql+asyncpg://")):
        log.critical(f"Boot Aborted. Database URL must use an async driver (aiosqlite/asyncpg). Found: {db_url}")
        return False

    log.info("Level-1 Security Sweep: PASSED. Cryptography meets Sentinel Standards.")
    return True

# ======================================================================================
# 2. PRE-FLIGHT DIAGNOSTICS (NETWORK & INFRASTRUCTURE)
# ======================================================================================
async def _verify_infrastructure():
    """
    Tests database availability and the Solana RPC connection asynchronously.
    """
    log.info("Initiating Level-2 Network Diagnostics...")
    
    # 1. Database Ping
    try:
        db_url = os.getenv("DATABASE_URL")
        engine = create_async_engine(db_url, echo=False)
        async with engine.begin() as conn:
            # Just opening a connection tests the URI and credentials
            pass
        await engine.dispose()
        log.info("Database Connection: OK.")
    except Exception as e:
        log.critical(f"Database Unreachable: {e}")
        sys.exit(1)

    # 2. Solana RPC Ping
    try:
        rpc_url = os.getenv("SOLANA_RPC_URL")
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(rpc_url, json=payload)
            if resp.status_code == 200:
                log.info("Solana Mainnet RPC Connection: OK.")
            else:
                log.warning(f"Solana RPC returned status {resp.status_code}. Execution may fail.")
    except Exception as e:
        log.critical(f"Solana RPC Unreachable: {e}")
        sys.exit(1)

    log.info("Level-2 Network Diagnostics: PASSED. All systems nominal.")

# ======================================================================================
# 3. SENTINEL BOOT SEQUENCE
# ======================================================================================
def start_server(host: str, port: int, environment: str):
    """
    Provisions the Uvicorn web server based on the operational environment.
    """
    is_dev = environment.lower() == "development"
    
    # In production, we drop the reload flag and scale up worker processes.
    workers = 1 if is_dev else int(os.getenv("WEB_CONCURRENCY", 4))
    
    print(f"""
    ========================================================
       ███████╗███████╗███╗   ██╗████████╗██╗███╗   ██╗███████╗██╗     
       ██╔════╝██╔════╝████╗  ██║╚══██╔══╝██║████╗  ██║██╔════╝██║     
       ███████╗█████╗  ██╔██╗ ██║   ██║   ██║██╔██╗ ██║█████╗  ██║     
       ╚════██║██╔══╝  ██║╚██╗██║   ██║   ██║██║╚██╗██║██╔══╝  ██║     
       ███████║███████╗██║ ╚████║   ██║   ██║██║ ╚████║███████╗███████╗
       ╚══════╝╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝╚═╝  ╚═══╝╚══════╝╚══════╝
    ========================================================
       PROTOCOL: ONLINE
       ENVIRONMENT: {environment.upper()}
       HOST: {host}:{port}
       NEURAL SYNC (WS): wss://{host}:{port}/ws/neural-sync
    ========================================================
    """)

    # Launch Uvicorn programmatic API
    uvicorn.run(
        "server:app",  # Application target
        host=host,
        port=port,
        reload=is_dev,
        workers=workers,
        log_level="info" if is_dev else "warning",
        proxy_headers=True,       # Crucial if running behind Nginx/Cloudflare
        forwarded_allow_ips="*"   # Trust X-Forwarded-For headers
    )

# ======================================================================================
# 4. COMMAND-LINE INTERFACE (CLI)
# ======================================================================================
def main():
    parser = argparse.ArgumentParser(description="Sentinel Protocol Management CLI")
    parser.add_argument("--start", action="store_true", help="Boot the Sentinel API Server")
    parser.add_argument("--check", action="store_true", help="Run system diagnostics without starting the server")
    
    args = parser.parse_args()

    # If no flags provided, default to --start
    if not (args.start or args.check):
        args.start = True

    # 1. Enforce Environment Security
    if not _validate_environment():
        sys.exit(1)

    # 2. Run Asynchronous Network Checks
    asyncio.run(_verify_infrastructure())

    # 3. Boot Action
    if args.check:
        log.info("System Check Complete. Ready for deployment.")
        sys.exit(0)

    if args.start:
        host = os.getenv("HOST", "0.0.0.0")
        port = int(os.getenv("PORT", 8000))
        env = os.getenv("ENVIRONMENT", "development")
        start_server(host, port, env)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received. Sentinel shutting down gracefully...")
        sys.exit(0)