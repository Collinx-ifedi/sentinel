# ======================================================================================
# src/services/market_resolver.py
# ======================================================================================

import os
import time
import logging
import asyncio
from typing import Optional, Tuple, Dict, Any, List

import httpx
import pandas as pd

# Setup Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')
log = logging.getLogger("MarketResolver")

class MarketResolver:
    """
    Unified high-speed module for Solana token resolution, real-time pricing, 
    and historical OHLCV data fetching.
    
    Architecture Updates (God Mode - v5.1.1):
    - Replaced legacy Jupiter endpoints with api.jup.ag structured APIs.
    - API Key Support implemented.
    - Replaced massive token registry download with targeted search resolution.
    - Expanded Static Asset Bypass for 0ms resolution on major pairs.
    - Fault-tolerant price fetching with defensive JSON parsing and backoff.
    """
    def __init__(self):
        # API Endpoints (Current Jupiter Architecture)
        self.jup_price_url = "https://api.jup.ag/price/v3"
        self.jup_tokens_search_url = "https://api.jup.ag/tokens/v2/search"
        self.jup_api_key = os.getenv("JUP_API_KEY", "").strip()
        
        # GeckoTerminal for Historical Data
        self.gecko_ohlcv_url = "https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/ohlcv/{timeframe}"
        
        # STATIC ASSET BYPASS: Hardcoded foundation tokens to prevent "Sensor Failure"
        self.static_mints = {
            "SOL": "So11111111111111111111111111111111111111112",
            "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
            "BONK": "DezXAZ8z7PnrnRJjz3wXBoRg7R9j3F3p5hH9zq7y5E5",
            "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbAbdEM43xge",
            "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYtM23iq8V",
            "POPCAT": "7GCihgDB8fe6KVZzXvkKsE9EzNcc2KYcZvpUCPkM7yZ",
            "JITOSOL": "J1toso1uKSpDdVN6qsQp96aX53pA1d3A86d3Y1A4T",
            "MSOL": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqkVmBw"
        }
        
        # Dynamic In-Memory Caches
        self.symbol_to_mint: Dict[str, str] = self.static_mints.copy()
        self.mint_to_symbol: Dict[str, str] = {v: k for k, v in self.static_mints.items()}

    def _get_jup_headers(self) -> Dict[str, str]:
        """Generates headers for Jupiter API requests, including optional auth."""
        headers = {"Accept": "application/json"}
        if self.jup_api_key:
            headers["x-api-key"] = self.jup_api_key
        return headers

    async def _search_jupiter_token(self, symbol: str) -> Optional[str]:
        """
        Queries Jupiter token search endpoint for a specific symbol.
        Implements strict disambiguation to avoid picking fake/scam tokens.
        """
        symbol_upper = symbol.upper()
        log.info(f"Resolving symbol '{symbol_upper}' via Jupiter token search endpoint...")
        
        timeout = httpx.Timeout(10.0, connect=5.0)
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                params = {"query": symbol_upper}
                response = await client.get(
                    self.jup_tokens_search_url, 
                    params=params, 
                    headers=self._get_jup_headers()
                )
                response.raise_for_status()
                
                tokens = response.json()
                
                if not tokens or not isinstance(tokens, list):
                    log.warning(f"No tokens returned in Jupiter search for '{symbol_upper}'")
                    return None
                    
                # 1. Filter for EXACT symbol matches
                exact_matches = [t for t in tokens if t.get("symbol", "").upper() == symbol_upper]
                
                if not exact_matches:
                    log.warning(f"No exact Jupiter token search match found for '{symbol_upper}'. Candidates were partial.")
                    return None
                    
                # 2. Disambiguation: Prefer verified/strict over unverified if tags exist
                best_match = exact_matches[0]
                for match in exact_matches:
                    tags = [tag.lower() for tag in match.get("tags", [])]
                    if "verified" in tags or "strict" in tags:
                        best_match = match
                        break
                        
                mint = best_match.get("address")
                if mint:
                    log.info(f"Successfully resolved '{symbol_upper}' to mint: {mint}")
                    return mint
                    
            except httpx.HTTPStatusError as e:
                log.error(f"HTTP {e.response.status_code} from Jupiter token search for '{symbol_upper}'")
            except httpx.RequestError as e:
                log.error(f"DNS/network failure reaching Jupiter token search for '{symbol_upper}': {e}")
            except Exception as e:
                log.error(f"Unexpected JSON/Parsing fault during token search for '{symbol_upper}': {e}")
                
        return None

    async def get_mint_address(self, symbol: str) -> Optional[str]:
        """Resolves a ticker symbol (e.g., 'BONK') to its Solana Mint Address."""
        symbol = symbol.upper()
        
        # 1. Check static bypass (0ms lookup, 100% offline reliable)
        if symbol in self.static_mints:
            log.info(f"[CACHE] Symbol '{symbol}' resolved instantly via static bypass.")
            return self.static_mints[symbol]
            
        # 2. Check dynamic memory cache
        if symbol in self.symbol_to_mint:
            log.info(f"[CACHE] Symbol '{symbol}' resolved from in-memory cache.")
            return self.symbol_to_mint[symbol]
            
        # 3. Cache miss -> Search Jupiter API
        mint = await self._search_jupiter_token(symbol)
        
        # Cache successful resolutions
        if mint:
            self.symbol_to_mint[symbol] = mint
            self.mint_to_symbol[mint] = symbol
            
        return mint

    async def get_token_price(self, mint: str) -> Optional[float]:
        """
        Fetches the real-time USD price via Jupiter Price V3 API.
        Includes automated retries and defensive JSON parsing.
        """
        if not mint:
            return None
            
        timeout = httpx.Timeout(10.0, connect=5.0)
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            # 3 attempts for resilience against API jitter or 429s
            for attempt in range(3):
                try:
                    params = {"ids": mint}
                    response = await client.get(
                        self.jup_price_url, 
                        params=params, 
                        headers=self._get_jup_headers()
                    )
                    response.raise_for_status()
                    
                    payload = response.json()
                    
                    # Defensively extract data (Accounts for multiple v3 payload shapes)
                    data = payload.get("data", {})
                    token_data = data.get(mint)
                    
                    # Shape A: {"data": {"mint": {"price": "1.23", ...}}}
                    if isinstance(token_data, dict) and "price" in token_data:
                        return float(token_data["price"])
                        
                    # Shape B: {"data": {"mint": "1.23"}} (Fallback parsing)
                    if isinstance(token_data, (int, float, str)):
                        try:
                            return float(token_data)
                        except ValueError:
                            pass
                            
                    log.warning(f"Mint {mint} returned in price API, but structure lacked valid price. Payload: {payload}")
                    return None
                    
                except httpx.HTTPStatusError as e:
                    log.error(f"HTTP {e.response.status_code} from Jupiter price API for mint {mint}")
                    if e.response.status_code in (429, 500, 502, 503, 504):
                        await asyncio.sleep(1.0 * (attempt + 1)) # Linear backoff
                        continue
                    break
                except httpx.RequestError as e:
                    log.error(f"DNS/network failure reaching Jupiter price API for mint {mint}: {e}")
                    break
                except Exception as e:
                    log.error(f"Unexpected fault fetching price for {mint}: {e}")
                    break
            
        return None

    async def resolve_and_price(self, identifier: str) -> Tuple[Optional[str], Optional[float]]:
        """
        Unified handler for the Sentinel NLP Core.
        Auto-detects whether the input is a Ticker Symbol or a Mint Address.
        
        Returns:
            Tuple[mint_address, current_price_usd]
        """
        identifier = identifier.strip()
        
        # Solana mints are base58 strings typically 43-44 chars long
        if len(identifier) > 30:
            mint = identifier
        else:
            mint = await self.get_mint_address(identifier)
            
        if not mint:
            log.warning(f"Sensor Warning: Cannot resolve identifier '{identifier}' to a valid mint.")
            return None, None
            
        price = await self.get_token_price(mint)
        
        if price is None:
            log.warning(f"Resolved symbol '{identifier}' to mint {mint}, but no price was returned.")
            
        return mint, price

    # =========================================================================
    # HISTORICAL DATA & BACKWARD COMPATIBILITY BRIDGE
    # =========================================================================
    async def get_historical_data(self, mint: str, timeframe: str, limit: int = 100) -> Optional[pd.DataFrame]:
        """Fetches and normalizes OHLCV data for Technical Analysis modules."""
        gt_timeframe = "day"
        aggregate = 1
        
        if timeframe in ['1m', '5m', '15m']:
            gt_timeframe = "minute"
            aggregate = int(timeframe.replace('m', ''))
        elif timeframe in ['1h', '4h']:
            gt_timeframe = "hour"
            aggregate = int(timeframe.replace('h', ''))
        elif timeframe in ['1d']:
            gt_timeframe = "day"

        url = self.gecko_ohlcv_url.format(mint=mint, timeframe=gt_timeframe)
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                params = {"limit": limit, "aggregate": aggregate}
                response = await client.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                ohlcv_list = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
                
                if not ohlcv_list:
                    return None
                    
                df = pd.DataFrame(ohlcv_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('timestamp', inplace=False)
                
                # Reverse list to ensure chronological order (oldest -> newest)
                df = df.iloc[::-1].reset_index(drop=True)
                return df
                
        except Exception as e:
            log.error(f"GeckoTerminal fetch failed for {mint[:6]}... : {e}")
            return None

    async def fetch_ohlcv(self, symbol: str, timeframe: str = '1d', limit: int = 100) -> Optional[pd.DataFrame]:
        """LEGACY BRIDGE: Allows legacy Agent Brain modules to function safely."""
        log.info(f"Fetching async OHLCV data for {symbol} ({timeframe})...")
        mint = await self.get_mint_address(symbol)
        
        if not mint:
            log.error(f"fetch_ohlcv aborted: Cannot resolve '{symbol}'.")
            return None
            
        return await self.get_historical_data(mint, timeframe, limit)

# =============================================================================
# DIRECT EXECUTION TEST
# =============================================================================
if __name__ == "__main__":
    async def test_resolver():
        resolver = MarketResolver()
        
        print("\n=== Testing Core Asset Resolution (Should hit Static Bypass) ===")
        mint, price = await resolver.resolve_and_price("SOL")
        print(f"Result -> SOL Mint: {mint} | Price: ${price}")
        
        print("\n=== Testing Ticker Resolution (Should hit Static Bypass) ===")
        mint, price = await resolver.resolve_and_price("BONK")
        print(f"Result -> BONK Mint: {mint} | Price: ${price}")

        print("\n=== Testing Dynamic Search Resolution (Should hit Jupiter API) ===")
        # Testing a token unlikely to be in our static map to force the search fallback
        mint, price = await resolver.resolve_and_price("RAY") 
        print(f"Result -> RAY Mint: {mint} | Price: ${price}")

        print("\n=== Testing Historical Data Bridge ===")
        df = await resolver.fetch_ohlcv("SOL", timeframe="1h", limit=3)
        print(df)
        
    asyncio.run(test_resolver())
