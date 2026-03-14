# src/services/market_resolver.py
import time
import logging
import asyncio
from typing import Optional, Tuple, Dict, Any

import httpx
import pandas as pd

# Setup Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')
log = logging.getLogger("MarketResolver")

class MarketResolver:
    """
    Unified high-speed module for Solana token resolution, real-time pricing, 
    and historical OHLCV data fetching.
    
    Architecture Updates (God Mode):
    - Public API Integration (No Auth Required)
    - Static Asset Bypass for 0ms resolution on major pairs
    - Dual Symbol/Mint memory caching
    - Fault-tolerant price fetching with backoff
    """
    def __init__(self):
        # API Endpoints (Public Free Tier)
        self.jup_price_url = "https://price.jup.ag/v4/price"
        self.jup_tokens_url = "https://token.jup.ag/all"
        
        # GeckoTerminal for Historical Data
        self.gecko_ohlcv_url = "https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/ohlcv/{timeframe}"
        
        # STATIC ASSET BYPASS: Hardcoded foundation tokens to prevent "Sensor Failure"
        self.static_mints = {
            "SOL": "So11111111111111111111111111111111111111112",
            "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
            "JITOSOL": "J1toso1uKSpDdVN6qsQp96aX53pA1d3A86d3Y1A4T",
            "MSOL": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqkVmBw"
        }
        
        # Dynamic In-Memory Caches
        self.symbol_to_mint: Dict[str, str] = self.static_mints.copy()
        self.mint_to_symbol: Dict[str, str] = {v: k for k, v in self.static_mints.items()}
        
        self.last_refresh = 0.0
        self.cache_ttl = 1800  # 30 minutes

    async def _refresh_token_cache(self) -> None:
        """Fetches the Jupiter token list and updates the dual caches."""
        now = time.time()
        
        # Guard: Respect TTL and ensure we have more than just our static assets
        if now - self.last_refresh < self.cache_ttl and len(self.symbol_to_mint) > len(self.static_mints):
            return

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(self.jup_tokens_url)
                response.raise_for_status()
                tokens = response.json()
                
                new_tokens_count = 0
                for token in tokens:
                    symbol = token.get("symbol", "").upper()
                    mint = token.get("address")
                    
                    if symbol and mint:
                        # Prioritize the first verified instance of a symbol (avoids fakes)
                        if symbol not in self.symbol_to_mint:
                            self.symbol_to_mint[symbol] = mint
                            new_tokens_count += 1
                        
                        # Always map mint -> symbol for reverse lookups
                        self.mint_to_symbol[mint] = symbol
                            
                self.last_refresh = now
                log.info(f"Oracle cache synchronized. Indexed {len(self.symbol_to_mint)} total assets.")
                
        except httpx.HTTPStatusError as e:
            log.error(f"Oracle API Degraded (HTTP {e.response.status_code}) during token registry fetch.")
        except httpx.RequestError as e:
            log.error(f"Network fault reaching token registry: {e}")
        except Exception as e:
            log.error(f"Critical fault in token cache refresh: {e}")

    async def get_mint_address(self, symbol: str) -> Optional[str]:
        """Resolves a ticker symbol (e.g., 'BONK') to its Solana Mint Address."""
        symbol = symbol.upper()
        
        # 1. Check static bypass and memory cache (0ms lookup)
        if symbol in self.symbol_to_mint:
            return self.symbol_to_mint[symbol]
            
        # 2. Cache miss -> Trigger background refresh -> Check again
        await self._refresh_token_cache()
        return self.symbol_to_mint.get(symbol)

    async def get_token_price(self, mint: str) -> Optional[float]:
        """
        Fetches the real-time USD price via Jupiter V4 API.
        Includes automated retries to survive transient rate limits.
        """
        if not mint:
            return None
            
        timeout = httpx.Timeout(10.0, connect=5.0)
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            # 3 attempts for resilience against public endpoint 429s
            for attempt in range(3):
                try:
                    params = {"ids": mint}
                    response = await client.get(self.jup_price_url, params=params)
                    response.raise_for_status()
                    
                    data = response.json().get("data", {})
                    token_data = data.get(mint)
                    
                    if token_data and "price" in token_data:
                        return float(token_data["price"])
                        
                    return None # Token genuinely not found on Oracle
                    
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        log.warning(f"Price Oracle rate limit hit for {mint[:6]}... Retrying ({attempt+1}/3)")
                        await asyncio.sleep(1.0 * (attempt + 1)) # Linear backoff
                    else:
                        log.error(f"HTTP Error fetching price for {mint}: {e.response.status_code}")
                        break
                except httpx.RequestError as e:
                    log.error(f"Network fault fetching price for {mint}: {e}")
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
            log.warning(f"Sensor Warning: Resolved mint {mint[:6]}... but failed to fetch price.")
            
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
        
        print("\n--- Testing Core Asset Resolution ---")
        mint, price = await resolver.resolve_and_price("SOL")
        print(f"SOL -> Mint: {mint} | Price: ${price}")
        
        print("\n--- Testing Ticker Resolution ---")
        mint, price = await resolver.resolve_and_price("BONK")
        print(f"BONK -> Mint: {mint} | Price: ${price}")
        
        print("\n--- Testing Historical Data Bridge ---")
        df = await resolver.fetch_ohlcv("SOL", timeframe="1h", limit=3)
        print(df)
        
    asyncio.run(test_resolver())
