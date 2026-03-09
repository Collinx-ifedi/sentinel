# services/market_resolver.py
import os
import time
import logging
import asyncio
from typing import Optional, Tuple, Dict, Any

import httpx
import pandas as pd

# Setup Logger
log = logging.getLogger("MarketResolver")

class MarketResolver:
    """
    Unified high-speed module for Solana token resolution, real-time pricing, 
    and historical OHLCV data fetching.
    
    Replaces legacy synchronous ccxt logic with modern async HTTPX calls.
    """
    def __init__(self):
        # API Endpoints (Migrated to Jupiter Lite-API infrastructure)
        self.base_url = "https://lite-api.jup.ag"
        self.jup_price_url = f"{self.base_url}/price/v3"
        self.jup_tokens_url = f"{self.base_url}/tokens/v2" 
        
        # API Key Integration
        self.api_key = os.getenv("JUPITER_API_KEY")
        self.headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }
        
        # GeckoTerminal is used as a free, reliable source for Solana OHLCV
        self.gecko_ohlcv_url = "https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/ohlcv/{timeframe}"
        
        # In-Memory Cache
        self._token_cache: Dict[str, str] = {
            "SOL": "So11111111111111111111111111111111111111112",
            "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        }
        self._cache_last_updated = 0.0

    async def _refresh_token_cache(self) -> None:
        """Fetches the Jupiter token list and updates the local cache."""
        now = time.time()
        # Refresh cache only if it's older than 1 hour (3600 seconds)
        if now - self._cache_last_updated < 3600 and len(self._token_cache) > 2:
            return

        try:
            # Applied headers for API Key authentication and strictly enforce 15.0s timeout
            async with httpx.AsyncClient(timeout=15.0, headers=self.headers) as client:
                response = await client.get(self.jup_tokens_url)
                response.raise_for_status()
                tokens = response.json()
                
                for token in tokens:
                    symbol = token.get("symbol", "").upper()
                    mint = token.get("address") # Updated from 'id' to 'address' for Lite-API
                    if symbol and mint:
                        # Only map the first verified instance of a symbol to avoid spoofed tokens
                        if symbol not in self._token_cache:
                            self._token_cache[symbol] = mint
                            
                self._cache_last_updated = now
                log.info(f"Token cache refreshed. Loaded {len(self._token_cache)} tokens.")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                log.error("Jupiter API Rate Limited (429) - Backing off.")
            else:
                log.error(f"Jupiter API Authentication/HTTP Error: {e.response.status_code} - Check API Key.")
        except httpx.RequestError as e:
            log.error(f"Jupiter API DNS/Network Fault during token fetch: {e}")
        except Exception as e:
            log.error(f"Failed to refresh Jupiter token cache: {e}")

    async def get_mint_address(self, symbol: str) -> Optional[str]:
        """Resolves a symbol (e.g., 'JUP') into its Solana Mint Address."""
        symbol = symbol.upper()
        
        # Pre-check cache
        if symbol in self._token_cache:
            return self._token_cache[symbol]
            
        # Refresh cache and check again
        await self._refresh_token_cache()
        return self._token_cache.get(symbol)

    async def get_token_price(self, mint_address: str) -> float:
        """Fetches the real-time USD price via Jupiter V3 Price API."""
        if not mint_address:
            return 0.0
            
        try:
            # Applied headers for API Key authentication
            async with httpx.AsyncClient(timeout=5.0, headers=self.headers) as client:
                params = {"ids": mint_address}
                response = await client.get(self.jup_price_url, params=params)
                response.raise_for_status()
                data = response.json().get("data", {})
                
                token_data = data.get(mint_address)
                if token_data and "price" in token_data:
                    return float(token_data["price"])
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                log.error(f"Rate Limited (429) fetching price for {mint_address}.")
            else:
                log.error(f"HTTP Error fetching price for {mint_address}: {e.response.status_code}")
        except httpx.RequestError as e:
            log.error(f"DNS/Network Error fetching price for {mint_address}: {e}")
        except Exception as e:
            log.error(f"Price fetch failed for mint {mint_address}: {e}")
            
        return 0.0

    async def resolve_and_price(self, symbol: str) -> Tuple[Optional[str], float]:
        """
        Unified helper for the Agent Brain. 
        Returns (mint_address, current_price).
        """
        mint = await self.get_mint_address(symbol)
        if not mint:
            log.warning(f"Could not resolve symbol '{symbol}' to a mint address.")
            return None, 0.0
            
        price = await self.get_token_price(mint)
        return mint, price

    async def get_historical_data(self, mint: str, timeframe: str, limit: int = 100) -> Optional[pd.DataFrame]:
        """
        Fetches historical OHLCV data for Technical Analysis.
        """
        # Map legacy timeframe strings (e.g., '1h', '1d') to GeckoTerminal parameters
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
                    
                # Format exactly as the legacy ccxt DataFrame for backward compatibility
                df = pd.DataFrame(ohlcv_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                
                # GeckoTerminal returns timestamp in seconds. Convert to datetime.
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('timestamp', inplace=False) # technical_analysis.py expects timestamp as a column, not index
                
                # Reverse list because APIs often return newest first, but TA needs oldest first
                df = df.iloc[::-1].reset_index(drop=True)
                return df
                
        except Exception as e:
            log.error(f"Historical data fetch failed for {mint}: {e}")
            return None

    # =========================================================================
    # BACKWARD COMPATIBILITY BRIDGE
    # =========================================================================
    async def fetch_ohlcv(self, symbol: str, timeframe: str = '1d', limit: int = 100) -> Optional[pd.DataFrame]:
        """
        LEGACY BRIDGE: Replaces the exact method from the old market_data.py.
        This allows technical_analysis.py and pattern_analyzer.py to function 
        without changing a single line of their internal code.
        """
        log.info(f"Fetching async OHLCV data for {symbol} ({timeframe})...")
        mint = await self.get_mint_address(symbol)
        if not mint:
            log.error(f"fetch_ohlcv aborted: Cannot resolve '{symbol}'.")
            return None
            
        return await self.get_historical_data(mint, timeframe, limit)

# Example Usage Block for testing the module directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def test_resolver():
        resolver = MarketResolver()
        mint, price = await resolver.resolve_and_price("JUP")
        print(f"JUP Mint: {mint} | Price: ${price:.4f}")
        
        df = await resolver.fetch_ohlcv("JUP", timeframe="1h", limit=5)
        print("\nBackward Compatibility OHLCV DataFrame:")
        print(df)
        
    asyncio.run(test_resolver())