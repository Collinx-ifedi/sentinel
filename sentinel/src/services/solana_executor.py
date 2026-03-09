# ======================================================================================
# MODULE: SOLANA EXECUTOR (THE HANDS & EYES)
# VERSION: 4.1.0 "GOD MODE"
# DESCRIPTION: Secure execution engine and on-chain oracle. Handles AES-256 wallet 
#              encryption, transaction signing, and deep-scan portfolio RPC queries.
# ======================================================================================

import os
import base64
import logging
import asyncio
import httpx
from decimal import Decimal
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction
from solders.message import to_bytes_versioned
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

# Assuming TradeHistory model is accessible here
from src.database.models import TradeHistory

log = logging.getLogger("SolanaExecutor")

# --- ON-CHAIN CONSTANTS ---
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
NATIVE_SOL_MINT = "So11111111111111111111111111111111111111112"

# Known Liquid Staking Tokens (LSTs) for the Yield Engine
KNOWN_LSTS = {
    "J1toso1uKSpDdVN6qsQp96aX53pA1d3A86d3Y1A4T": "JitoSOL",
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqkVmBw": "mSOL",
    "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piV1": "bSOL"
}

class SolanaExecutor:
    """
    Core Execution & Oracle Class for Sentinel Protocol.
    Manages cryptographic keys and async RPC interactions.
    """
    def __init__(self):
        self.rpc_url = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
        self.master_key = os.getenv("MASTER_ENCRYPTION_KEY", "DEFAULT_DEV_KEY_CHANGE_IN_PROD")
        self.client = httpx.AsyncClient(timeout=15.0)
        
        # Cache for token metadata (Mint -> Symbol/Name)
        self.metadata_cache: Dict[str, Dict] = {}

    def _derive_fernet_key(self, master_password: str, salt: bytes) -> Fernet:
        """Derives a secure AES-256 key from the environment master password using PBKDF2."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=600000, # Updated to modern OWASP recommendation
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_password.encode()))
        return Fernet(key)

    async def close(self):
        """Gracefully closes the persistent HTTPX client."""
        await self.client.aclose()

    # ==============================================================================
    # 1. CRYPTOGRAPHIC WALLET MANAGEMENT
    # ==============================================================================
    def generate_wallet(self) -> Tuple[str, str]:
        """Generates a new Solana Keypair and returns the Pubkey and Encrypted Private Key."""
        kp = Keypair()
        pubkey = str(kp.pubkey())
        secret_bytes = bytes(kp)
        
        # Generate a unique salt for this specific wallet
        salt = os.urandom(16)
        fernet = self._derive_fernet_key(self.master_key, salt)
        
        encrypted_bytes = fernet.encrypt(secret_bytes)
        
        # Combine salt and ciphertext for storage
        salt_b64 = base64.urlsafe_b64encode(salt).decode('utf-8')
        ciphertext_b64 = encrypted_bytes.decode('utf-8')
        
        encrypted_pk_payload = f"{salt_b64}:{ciphertext_b64}"
        return pubkey, encrypted_pk_payload

    def _decrypt_wallet(self, encrypted_pk_payload: str) -> Keypair:
        """Decrypts the AES-256 payload back into a functional Solders Keypair."""
        try:
            salt_b64, ciphertext_b64 = encrypted_pk_payload.split(':')
            salt = base64.urlsafe_b64decode(salt_b64.encode('utf-8'))
            
            fernet = self._derive_fernet_key(self.master_key, salt)
            decrypted_bytes = fernet.decrypt(ciphertext_b64.encode('utf-8'))
            
            return Keypair.from_bytes(decrypted_bytes)
        except ValueError:
            raise ValueError("Invalid encrypted payload format. Expected 'salt:ciphertext'.")

    # ==============================================================================
    # 2. ORACLE READ LAYER (PORTFOLIO & YIELD)
    # ==============================================================================
    async def get_native_balance(self, pubkey_str: str) -> float:
        """Fetches the native SOL balance for the dashboard."""
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getBalance",
            "params": [pubkey_str]
        }
        try:
            resp = await self.client.post(self.rpc_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            lamports = data.get("result", {}).get("value", 0)
            return lamports / 1_000_000_000.0
        except Exception as e:
            log.error(f"Failed to fetch native balance for {pubkey_str}: {e}")
            return 0.0

    async def get_all_token_balances(self, pubkey_str: str) -> List[Dict[str, Any]]:
        """Deep-scans the blockchain for all SPL token accounts owned by the wallet."""
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                pubkey_str,
                {"programId": TOKEN_PROGRAM_ID},
                {"encoding": "jsonParsed"}
            ]
        }
        
        assets = []
        try:
            resp = await self.client.post(self.rpc_url, json=payload)
            resp.raise_for_status()
            accounts = resp.json().get("result", {}).get("value", [])
            
            for acc in accounts:
                info = acc["account"]["data"]["parsed"]["info"]
                mint = info["mint"]
                token_amt = info["tokenAmount"]
                
                amount = float(token_amt["uiAmount"] or 0)
                if amount > 0:
                    assets.append({
                        "mint": mint,
                        "amount": amount,
                        "decimals": token_amt["decimals"]
                    })
            return assets
        except Exception as e:
            log.error(f"Failed to fetch token accounts for {pubkey_str}: {e}")
            return []

    async def get_token_metadata(self, mints: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Resolves mint addresses to human-readable symbols using the Jupiter Token API.
        Fetches individually to save bandwidth and prevent rate-limiting.
        """
        resolved = {}
        missing_mints = [m for m in mints if m not in self.metadata_cache]
        
        for mint in missing_mints:
            if mint == NATIVE_SOL_MINT:
                continue
            try:
                # Surgical fetch per-mint instead of downloading 10MB of strict tokens
                resp = await self.client.get(f"https://tokens.jup.ag/token/{mint}")
                if resp.status_code == 200:
                    token = resp.json()
                    self.metadata_cache[mint] = {
                        "symbol": token.get("symbol", f"UNK-{mint[:4]}"),
                        "name": token.get("name", "Unknown Token"),
                        "logo_uri": token.get("logoURI", "")
                    }
            except Exception as e:
                log.warning(f"Failed to fetch metadata for {mint}: {e}")

        # Construct final response from cache (defaulting to truncated mint if unknown)
        for mint in mints:
            resolved[mint] = self.metadata_cache.get(mint, {
                "symbol": f"UNK-{mint[:4]}",
                "name": "Unknown Token",
                "logo_uri": ""
            })
            
        # Hardcode Native SOL for convenience
        resolved[NATIVE_SOL_MINT] = {"symbol": "SOL", "name": "Solana", "logo_uri": ""}
        
        return resolved

    async def get_staking_positions(self, token_balances: List[Dict]) -> List[Dict]:
        """Filters the token balances to identify Yield/LST positions."""
        yield_positions = []
        for asset in token_balances:
            if asset["mint"] in KNOWN_LSTS:
                yield_positions.append({
                    "mint": asset["mint"],
                    "amount": asset["amount"],
                    "protocol": KNOWN_LSTS[asset["mint"]]
                })
        return yield_positions

    # ==============================================================================
    # 3. EXECUTION LAYER (TRADE LOGIC)
    # ==============================================================================
    async def execute_swap(self, 
                           user_id: str, 
                           encrypted_pk: str, 
                           input_mint: str, 
                           output_mint: str, 
                           amount_lamports: int, 
                           is_safe: bool, 
                           db_session: AsyncSession) -> Dict[str, Any]:
        """
        The Master Execution Function. 
        Requires 'is_safe' clearance from the SafetySentinel before signing.
        Executes swap via Jupiter API V6.
        """
        if not is_safe:
            return {"success": False, "error": "VETO: Safety Protocol blocked execution."}

        try:
            # 1. Decrypt Wallet
            keypair = self._decrypt_wallet(encrypted_pk)
            user_pubkey = str(keypair.pubkey())
            
            log.info(f"EXECUTING SWAP: {amount_lamports} from {input_mint} to {output_mint} for {user_pubkey}")
            
            # 2. Get Quote from Jupiter V6
            quote_url = f"https://quote-api.jup.ag/v6/quote?inputMint={input_mint}&outputMint={output_mint}&amount={amount_lamports}&slippageBps=50"
            quote_resp = await self.client.get(quote_url)
            quote_resp.raise_for_status()
            quote_data = quote_resp.json()
            
            # 3. Get Swap Transaction from Jupiter V6
            swap_url = "https://quote-api.jup.ag/v6/swap"
            swap_payload = {
                "quoteResponse": quote_data,
                "userPublicKey": user_pubkey,
                "wrapAndUnwrapSol": True,
                "prioritizationFeeLamports": "auto" # Dynamic priority fees to ensure execution
            }
            swap_resp = await self.client.post(swap_url, json=swap_payload)
            swap_resp.raise_for_status()
            swap_data = swap_resp.json()
            
            # 4. Deserialize & Sign VersionedTransaction
            swap_transaction = swap_data["swapTransaction"]
            raw_tx_bytes = base64.b64decode(swap_transaction)
            
            tx = VersionedTransaction.from_bytes(raw_tx_bytes)
            signature = keypair.sign_message(to_bytes_versioned(tx.message))
            signed_tx = VersionedTransaction.populate(tx.message, [signature])
            
            # 5. Broadcast to Network with preflight commitment
            b64_signed_tx = base64.b64encode(bytes(signed_tx)).decode('utf-8')
            send_payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "sendTransaction",
                "params": [
                    b64_signed_tx,
                    {"encoding": "base64", "preflightCommitment": "confirmed"}
                ]
            }
            
            send_resp = await self.client.post(self.rpc_url, json=send_payload)
            send_data = send_resp.json()
            if "error" in send_data:
                raise Exception(f"RPC Error: {send_data['error']}")
                
            tx_signature = send_data["result"]
            log.info(f"Transaction broadcasted. Signature: {tx_signature}. Waiting for confirmation...")
            
            # 6. Wait for Confirmation 
            confirmed = False
            for _ in range(30): # Poll for up to ~60 seconds
                await asyncio.sleep(2)
                status_payload = {
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getSignatureStatuses",
                    "params": [[tx_signature], {"searchTransactionHistory": True}]
                }
                status_resp = await self.client.post(self.rpc_url, json=status_payload)
                status_data = status_resp.json()
                
                if "result" in status_data and status_data["result"]["value"][0]:
                    conf_status = status_data["result"]["value"][0].get("confirmationStatus")
                    err = status_data["result"]["value"][0].get("err")
                    
                    if err:
                        raise Exception(f"Transaction failed on-chain: {err}")
                        
                    if conf_status in ["confirmed", "finalized"]:
                        confirmed = True
                        break
                        
            if not confirmed:
                raise Exception("Transaction broadcasted but failed to confirm within timeout.")

            # 7. Write to Immutable Ledger (TradeHistory) - Using Decimal for precision
            ui_amount = float(Decimal(amount_lamports) / Decimal(1_000_000_000))
            trade_record = TradeHistory(
                user_id=user_id,
                signature=tx_signature,
                input_mint=input_mint,
                output_mint=output_mint,
                amount=ui_amount,
                side="BUY" if input_mint == NATIVE_SOL_MINT else "SELL",
                status="SUCCESS",
                timestamp=datetime.now(timezone.utc)
            )
            
            db_session.add(trade_record)
            await db_session.commit()

            return {
                "success": True, 
                "signature": tx_signature,
                "input": input_mint,
                "output": output_mint
            }

        except SQLAlchemyError as db_err:
            log.error(f"Ledger Commit Failed: {db_err}")
            await db_session.rollback()
            return {"success": False, "error": "Database serialization failure."}
        except Exception as e:
            log.error(f"Execution Failed: {e}")
            return {"success": False, "error": str(e)}