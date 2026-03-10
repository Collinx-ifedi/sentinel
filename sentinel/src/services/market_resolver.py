# src/services/market_resolver.py
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
    
    Updated for Sentinel Protocol architecture: 
    - Public endpoints (No API Keys)
    - 30-minute cache TTL
    - Dual Symbol/Mint caching system
    """
    def __init__(self):
        # API Endpoints (Public Jupiter Endpoints)
        self.jup_price_url = "https://price.jup.ag/v4/price"
        self.jup_tokens_url = "https://token.jup.ag/all"
        
        # GeckoTerminal is used as a free, reliable source for Solana OHLCV
        self.gecko_ohlcv_url = "https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/ohlcv/{timeframe}"
        
        # Dual In-Memory Caches
        self.symbol_to_mint: Dict[str, str] = {
            "SOL": "So11111111111111111111111111111111111111112",
            "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        }
        self.mint_to_symbol: Dict[str, str] = {
            "So11111111111111111111111111111111111111112": "SOL",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC"
        }
        
        self.last_refresh = 0.0
        self.cache_ttl = 1800  # 30 minutes

    async def _refresh_token_cache(self) -> None:
        """Fetches the Jupiter token list and updates the dual caches."""
        now = time.time()
        # Refresh cache only if it's older than 30 minutes (1800 seconds)
        if now - self.last_refresh < self.cache_ttl and len(self.symbol_to_mint) > 2:
            return

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(self.jup_tokens_url)
                response.raise_for_status()
                tokens = response.json()
                
                for token in tokens:
                    symbol = token.get("symbol", "").upper()
                    mint = token.get("address")
                    if symbol and mint:
                        # Only map the first instance of a symbol to avoid spoofed tokens
                        if symbol not in self.symbol_to_mint:
                            self.symbol_to_mint[symbol] = mint
                        
                        # Always map mint to symbol
                        self.mint_to_symbol[mint] = symbol
                            
                self.last_refresh = now
                log.info(f"Token cache refreshed. Loaded {len(self.symbol_to_mint)} tokens.")
        except httpx.HTTPStatusError as e:
            log.error(f"Jupiter API HTTP Error during token fetch: {e.response.status_code}")
        except httpx.RequestError as e:
            log.error(f"Jupiter API DNS/Network Fault during token fetch: {e}")
        except Exception as e:
            log.error(f"Failed to refresh Jupiter token cache: {e}")

    async def get_mint_address(self, symbol: str) -> Optional[str]:
        """Resolves a symbol (e.g., 'BONK') into its Solana Mint Address."""
        symbol = symbol.upper()
        
        # Pre-check cache
        if symbol in self.symbol_to_mint:
            return self.symbol_to_mint[symbol]
            
        # Refresh cache and check again
        await self._refresh_token_cache()
        return self.symbol_to_mint.get(symbol)

    async def get_token_price(self, mint: str) -> Optional[float]:
        """Fetches the real-time USD price via Jupiter V4 Price API (No Auth)."""
        if not mint:
            return None
            
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                params = {"ids": mint}
                response = await client.get(self.jup_price_url, params=params)
                response.raise_for_status()
                data = response.json().get("data", {})
                
                token_data = data.get(mint)
                if token_data and "price" in token_data:
                    return float(token_data["price"])
        except httpx.HTTPStatusError as e:
            log.error(f"HTTP Error fetching price for {mint}: {e.response.status_code}")
        except httpx.RequestError as e:
            log.error(f"DNS/Network Error fetching price for {mint}: {e}")
        except Exception as e:
            log.error(f"Price fetch failed for mint {mint}: {e}")
            
        return None

    async def resolve_and_price(self, identifier: str) -> Tuple[Optional[str], Optional[float]]:
        """
        Unified helper for the Sentinel Agent. 
        Automatically detects if identifier is a symbol or mint address.
        Returns (mint_address, current_price).
        """
        # If length is > 30, we treat it directly as a Solana mint address
        if len(identifier) > 30:
            mint = identifier
        else:
            mint = await self.get_mint_address(identifier)
            
        if not mint:
            log.warning(f"Could not resolve identifier '{identifier}' to a mint address.")
            return None, None
            
        price = await self.get_token_price(mint)
        return mint, price

    # =========================================================================
    # HISTORICAL DATA & BACKWARD COMPATIBILITY
    # =========================================================================
    async def get_historical_data(self, mint: str, timeframe: str, limit: int = 100) -> Optional[pd.DataFrame]:
        """
        Fetches historical OHLCV data for Technical Analysis.
        """
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
                
                # Reverse list to ensure oldest first
                df = df.iloc[::-1].reset_index(drop=True)
                return df
                
        except Exception as e:
            log.error(f"Historical data fetch failed for {mint}: {e}")
            return None

    async def fetch_ohlcv(self, symbol: str, timeframe: str = '1d', limit: int = 100) -> Optional[pd.DataFrame]:
        """
        LEGACY BRIDGE: Allows older modules to function without breaking.
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
        
        print("\n--- Testing Symbol Resolution ---")
        mint, price = await resolver.resolve_and_price("BONK")
        print(f"Symbol BONK -> Mint: {mint} | Price: ${price}")
        
        print("\n--- Testing Direct Mint Resolution ---")
        direct_mint = "DezXAZ8z7PnrnRJjz3wXBoRg7R9j3F3p5hH9zq7y5E5"
        mint, price = await resolver.resolve_and_price(direct_mint)
        print(f"Mint Address -> Mint: {mint} | Price: ${price}")
        
    asyncio.run(test_resolver())
