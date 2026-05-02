"""
analyzer.py — Token Analyzer dengan multi-source data fetching.

Priority chain:
  1. pump.fun API  → untuk token pre-graduation (MAYORITAS kasus)
  2. DexScreener   → untuk token yang sudah graduate ke Raydium/Orca
  3. GeckoTerminal → fallback gratis untuk DEX data (替代 Birdeye)
  4. Helius RPC    → holder count, token metadata & transaksi on-chain
  5. Jupiter Price → konfirmasi harga real-time
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from groq import AsyncGroq

logger = logging.getLogger(__name__)

# ─── ENV ──────────────────────────────────────────────────────────────────────
HELIUS_API_KEY  = os.environ.get("HELIUS_API_KEY", "").strip()
GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "").strip()
PUMP_THRESHOLD  = float(os.environ.get("PUMP_THRESHOLD", "30"))

HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""
DEXSCREENER_BASE = "https://api.dexscreener.com"
GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"
PUMPFUN_API      = "https://frontend-api.pump.fun"
JUPITER_PRICE    = "https://price.jup.ag/v6/price"

logger.info("=== TokenAnalyzer INIT ===")
logger.info("HELIUS_API_KEY set: %s", bool(HELIUS_API_KEY))
logger.info("GROQ_API_KEY set  : %s", bool(GROQ_API_KEY))


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            v = val.replace(",", "").replace("$", "").strip()
            if not v or v.lower() in ("none", "null", "nan", ""):
                return default
            if v.upper().endswith("K"):
                return float(v[:-1]) * 1_000
            if v.upper().endswith("M"):
                return float(v[:-1]) * 1_000_000
            if v.upper().endswith("B"):
                return float(v[:-1]) * 1_000_000_000
            return float(v)
        except (ValueError, AttributeError):
            return default
    if isinstance(val, dict):
        for key in ("usd", "value", "amount", "price", "current", "native"):
            if key in val and val[key] is not None:
                return _safe_float(val[key], default)
        return default
    return default


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(_safe_float(val, default))
    except Exception:
        return default


def _age_hours(created_ts: Optional[int]) -> Optional[float]:
    """Convert unix timestamp to age in hours."""
    if not created_ts:
        return None
    try:
        created = datetime.fromtimestamp(created_ts, tz=timezone.utc)
        return round((datetime.now(timezone.utc) - created).total_seconds() / 3600, 2)
    except Exception:
        return None


# ─── ANALYZER ─────────────────────────────────────────────────────────────────

class TokenAnalyzer:
    def __init__(self, db):
        self.db = db
        self.groq: Optional[AsyncGroq] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(3)

        if GROQ_API_KEY:
            try:
                import httpx
                self.groq = AsyncGroq(
                    api_key=GROQ_API_KEY,
                    http_client=httpx.AsyncClient(timeout=30)
                )
                logger.info("Groq client initialized")
            except Exception as e:
                logger.warning("Groq init failed: %s", e)

    # ── Session management ─────────────────────────────────────────────────────

    async def _session_get(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json",
                },
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Generic HTTP with retry ────────────────────────────────────────────────

    async def _get(self, url: str, **kwargs) -> Optional[Any]:
        """GET request with retry on 429/5xx."""
        session = await self._session_get()
        for attempt in range(1, 4):
            try:
                async with self.semaphore:
                    async with session.get(url, **kwargs) as resp:
                        if resp.status == 200:
                            return await resp.json(content_type=None)
                        if resp.status == 429:
                            wait = 3 ** attempt
                            logger.warning("429 rate-limit, sleeping %ss | %s", wait, url[:60])
                            await asyncio.sleep(wait)
                        elif 500 <= resp.status < 600:
                            await asyncio.sleep(2 ** attempt)
                        else:
                            logger.warning("HTTP %s | %s", resp.status, url[:80])
                            return None
            except asyncio.TimeoutError:
                logger.warning("Timeout attempt %s | %s", attempt, url[:60])
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.debug("GET error attempt %s: %s | %s", attempt, e, url[:60])
                await asyncio.sleep(2 ** attempt)
        return None

    async def _post(self, url: str, payload: dict, **kwargs) -> Optional[Any]:
        """POST request with retry on 429/5xx."""
        session = await self._session_get()
        for attempt in range(1, 4):
            try:
                async with self.semaphore:
                    async with session.post(url, json=payload, **kwargs) as resp:
                        if resp.status == 200:
                            return await resp.json(content_type=None)
                        if resp.status == 429:
                            wait = 3 ** attempt
                            logger.warning("429 rate-limit POST, sleeping %ss | %s", wait, url[:60])
                            await asyncio.sleep(wait)
                        elif 500 <= resp.status < 600:
                            await asyncio.sleep(2 ** attempt)
                        else:
                            logger.warning("HTTP %s POST | %s", resp.status, url[:80])
                            return None
            except asyncio.TimeoutError:
                logger.warning("Timeout POST attempt %s | %s", attempt, url[:60])
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.debug("POST error attempt %s: %s | %s", attempt, e, url[:60])
                await asyncio.sleep(2 ** attempt)
        return None

    # ── 1. pump.fun API ────────────────────────────────────────────────────────

    async def fetch_pumpfun(self, address: str) -> Optional[Dict]:
        """
        Fetch token data from pump.fun frontend API.
        Works for ALL pump.fun tokens — both pre-graduation and graduated.
        Returns raw pump.fun coin data or None.
        
        Tries multiple endpoints in order:
        1. frontend-api.pump.fun/coins/{address}
        2. pumpportal.fun/api/data/{address}
        """
        # Try primary endpoint
        url = f"{PUMPFUN_API}/coins/{address}"
        logger.info("pump.fun GET (primary): %s", url)
        data = await self._get(url)
        if data and isinstance(data, dict) and (data.get("mint") or data.get("name")):
            logger.info("pump.fun OK (primary): %s | mcap=$%.0f", address[:8], data.get("usd_market_cap", 0))
            return data
        
        # Fallback: pumpportal.fun API (alternative source)
        fallback_url = f"https://pumpportal.fun/api/data/{address}"
        logger.info("pump.fun GET (fallback): %s", fallback_url)
        data = await self._get(fallback_url)
        if data and isinstance(data, dict) and (data.get("mint") or data.get("symbol")):
            logger.info("pumpportal OK: %s | mcap=$%.0f", address[:8], data.get("market_cap_usd", 0))
            # Normalize field names to match pump.fun format
            normalized = {
                "mint": data.get("mint", address),
                "name": data.get("name", "Unknown"),
                "symbol": data.get("symbol", "???"),
                "usd_market_cap": _safe_float(data.get("market_cap_usd", 0)),
                "total_supply": _safe_float(data.get("supply", 1_000_000_000)),
                "real_sol_reserves": _safe_float(data.get("sol_reserve", 0)) * 1e9,  # convert SOL to lamports
                "created_timestamp": data.get("created"),
                "complete": data.get("is_graduated", False),
                "reply_count": _safe_int(data.get("transactions_1h", 0)),
            }
            return normalized
        
        logger.warning("pump.fun: no data from any endpoint for %s", address[:8])
        return None

    def _pumpfun_to_snapshot(self, data: dict, address: str) -> Dict[str, Any]:
        """
        Convert pump.fun API response to unified snapshot format.
        pump.fun doesn't provide volume/buy-sell directly, so those fields
        will be None. Price is calculated from bonding curve reserves.
        """
        mint = data.get("mint", address)
        name   = data.get("name", "Unknown")
        symbol = data.get("symbol", "???")

        # Market cap in USD
        usd_mcap = _safe_float(data.get("usd_market_cap"))

        # Price: pump.fun gives usd_market_cap / total_supply
        total_supply = _safe_float(data.get("total_supply", 1_000_000_000))
        price_usd    = (usd_mcap / total_supply) if (usd_mcap and total_supply) else 0.0

        # Liquidity: approximate from real_sol_reserves
        # real_sol_reserves is in lamports (1 SOL = 1e9 lamports)
        sol_reserves = _safe_float(data.get("real_sol_reserves", 0)) / 1e9
        # Use a rough SOL price estimate from market cap context
        # We'll enrich this later if needed; for now 0 is fine
        liq_usd = 0.0

        # Token age
        created_ts = data.get("created_timestamp")
        age_hours  = _age_hours(created_ts)

        # Graduation status
        graduated = bool(data.get("complete", False))

        return {
            "price_usd":          price_usd,
            "market_cap":         usd_mcap,
            "liquidity_usd":      liq_usd,
            "volume_1h":          None,
            "volume_6h":          None,
            "volume_24h":         None,
            "price_change_1h":    None,
            "price_change_6h":    None,
            "price_change_24h":   None,
            "holder_count":       None,   # enriched by Helius
            "top10_holder_pct":   None,   # enriched by Helius
            "token_age_hours":    age_hours,
            "buy_count_1h":       _safe_int(data.get("reply_count", 0)),  # rough proxy
            "sell_count_1h":      0,
            "buy_sell_ratio":     0.0,
            "tx_count_24h":       None,
            "dex_name":           "raydium" if graduated else "pumpfun_bonding_curve",
            "symbol":             symbol,
            "name":               name,
            "graduated":          graduated,
            "data_source":        "pumpfun",
            "raw_json":           json.dumps(data)[:2000],
        }

    # ── 2. DexScreener ────────────────────────────────────────────────────────

    async def fetch_dexscreener(self, address: str) -> Optional[Dict]:
        """
        Fetch best trading pair from DexScreener.
        Only works for tokens that have graduated / been listed on a DEX.
        
        Tries multiple endpoints:
        1. /tokens/v1/solana/{address} - new API v1
        2. /latest/dex/search?{query}=q - search fallback
        """
        # Try primary endpoint (API v1)
        url = f"{DEXSCREENER_BASE}/tokens/v1/solana/{address}"
        logger.info("DexScreener GET (v1): %s", url[:80])
        data = await self._get(url)
        if data:
            pairs = data if isinstance(data, list) else data.get("pairs", [])
            if pairs:
                pairs.sort(
                    key=lambda x: _safe_float((x.get("liquidity") or {}).get("usd"), 0),
                    reverse=True,
                )
                logger.info("DexScreener OK (v1): %s | liquidity=$%.0f", address[:8], _safe_float(pairs[0].get("liquidity", {}).get("usd"), 0))
                return pairs[0]
        
        # Fallback: search endpoint
        search_url = f"{DEXSCREENER_BASE}/latest/dex/search?q={address}"
        logger.info("DexScreener GET (search): %s", search_url[:80])
        data = await self._get(search_url)
        if data and isinstance(data, dict):
            pairs = data.get("pairs", [])
            if pairs:
                pairs.sort(
                    key=lambda x: _safe_float((x.get("liquidity") or {}).get("usd"), 0),
                    reverse=True,
                )
                logger.info("DexScreener OK (search): %s | liquidity=$%.0f", address[:8], _safe_float(pairs[0].get("liquidity", {}).get("usd"), 0))
                return pairs[0]
        
        logger.warning("DexScreener: no data for %s", address[:8])
        return None

    async def fetch_dexscreener_price_at_time(
        self, pair_address: str, target_time: datetime
    ) -> Optional[float]:
        """Get historical price at a specific time via candles."""
        if not pair_address:
            return None
        from_ts = int((target_time - timedelta(minutes=5)).timestamp())
        to_ts   = int((target_time + timedelta(minutes=5)).timestamp())
        url = (
            f"{DEXSCREENER_BASE}/latest/dex/candles"
            f"/solana/{pair_address}?from={from_ts}&to={to_ts}&resolution=1"
        )
        data = await self._get(url)
        if not data or not isinstance(data, dict):
            return None
        candles = data.get("candles", [])
        if not candles:
            return None
        target_ts = target_time.timestamp()
        closest   = min(candles, key=lambda c: abs(c.get("t", 0) - target_ts))
        return _safe_float(closest.get("c")) or None

    def _dexscreener_to_snapshot(
        self, pair: dict, address: str, price_at_call: Optional[float] = None
    ) -> Dict[str, Any]:
        """Convert DexScreener pair data to unified snapshot format."""
        liquidity    = pair.get("liquidity", {}) or {}
        volume       = pair.get("volume", {}) or {}
        price_change = pair.get("priceChange", {}) or {}
        txns         = pair.get("txns", {}) or {}
        base         = pair.get("baseToken", {}) or {}

        h1  = txns.get("h1",  {}) or {}
        h24 = txns.get("h24", {}) or {}
        buy_1h  = _safe_int(h1.get("buys",  0))
        sell_1h = _safe_int(h1.get("sells", 0))

        return {
            "price_usd":         price_at_call or _safe_float(pair.get("priceUsd")),
            "market_cap":        _safe_float(pair.get("marketCap")) or _safe_float(pair.get("fdv")),
            "liquidity_usd":     _safe_float(liquidity.get("usd")),
            "volume_1h":         _safe_float(volume.get("h1")),
            "volume_6h":         _safe_float(volume.get("h6")),
            "volume_24h":        _safe_float(volume.get("h24")),
            "price_change_1h":   _safe_float(price_change.get("h1")),
            "price_change_6h":   _safe_float(price_change.get("h6")),
            "price_change_24h":  _safe_float(price_change.get("h24")),
            "holder_count":      None,
            "top10_holder_pct":  None,
            "token_age_hours":   None,
            "buy_count_1h":      buy_1h,
            "sell_count_1h":     sell_1h,
            "buy_sell_ratio":    round(buy_1h / max(sell_1h, 1), 3),
            "tx_count_24h":      _safe_int(h24.get("buys", 0)) + _safe_int(h24.get("sells", 0)),
            "dex_name":          pair.get("dexId", ""),
            "symbol":            base.get("symbol", "???"),
            "name":              base.get("name", "Unknown"),
            "graduated":         True,
            "data_source":       "dexscreener",
        }

    # ── 3. Jupiter Price ──────────────────────────────────────────────────────

    async def fetch_jupiter_price(self, address: str) -> Optional[float]:
        """
        Get current price from Jupiter Price API.
        Works for any SPL token with active trading.
        Free, no auth needed.
        
        Tries multiple endpoints:
        1. /v6/price - latest API
        2. /v4/price - fallback for older tokens
        """
        # Try v6 API first
        url = f"{JUPITER_PRICE}?ids={address}"
        logger.info("Jupiter GET (v6): %s", url[:80])
        data = await self._get(url)
        if data and isinstance(data, dict):
            token_data = (data.get("data") or {}).get(address)
            if token_data:
                price = _safe_float(token_data.get("price"))
                if price and price > 0:
                    logger.info("Jupiter OK (v6): %s = $%.12f", address[:8], price)
                    return price
        
        # Fallback to v4 API
        fallback_url = f"https://price.jup.ag/v4/price?ids={address}"
        logger.info("Jupiter GET (v4): %s", fallback_url[:80])
        data = await self._get(fallback_url)
        if data and isinstance(data, dict):
            token_data = (data.get("data") or {}).get(address)
            if token_data:
                price = _safe_float(token_data.get("price"))
                if price and price > 0:
                    logger.info("Jupiter OK (v4): %s = $%.12f", address[:8], price)
                    return price
        
        logger.warning("Jupiter: no price for %s", address[:8])
        return None

    # ── 3b. Birdeye API ────────────────────────────────────────────────────────

    async def fetch_geckoterminal(self, address: str) -> Optional[Dict]:
        """
        Fetch token data from GeckoTerminal API (FREE).
        Good alternative to Birdeye for DEX tokens.
        No API key required for basic endpoints.
        """
        # Search for token by address
        url = f"{GECKOTERMINAL_BASE}/solana/tokens/{address}"
        logger.info("GeckoTerminal GET: %s", url[:80])
        data = await self._get(url)
        if data and isinstance(data, dict):
            token_data = data.get("data", {}).get("attributes")
            if token_data:
                price_usd = _safe_float(token_data.get("price_usd"))
                market_cap = _safe_float(token_data.get("market_cap_usd"))
                liquidity = _safe_float(token_data.get("liquidity_usd"))
                volume_24h = _safe_float(token_data.get("volume_usd_h24"))
                price_change_24h = _safe_float(token_data.get("price_change_percentage_h24"))
                
                logger.info("GeckoTerminal OK: %s | price=$%.12f | mcap=$%.0f", 
                           address[:8], price_usd, market_cap)
                return {
                    "address": address,
                    "price_usd": price_usd,
                    "market_cap": market_cap,
                    "liquidity": liquidity,
                    "volume_24h": volume_24h,
                    "price_change_24h": price_change_24h,
                    "symbol": token_data.get("symbol", "?"),
                    "name": token_data.get("name", "Unknown"),
                }
        
        logger.warning("GeckoTerminal: no data for %s", address[:8])
        return None

    def _geckoterminal_to_snapshot(self, data: dict, address: str) -> Dict[str, Any]:
        """Convert GeckoTerminal API response to unified snapshot format."""
        return {
            "price_usd": data.get("price_usd", 0),
            "market_cap": data.get("market_cap", 0),
            "liquidity_usd": data.get("liquidity", 0),
            "volume_1h": None,
            "volume_6h": None,
            "volume_24h": data.get("volume_24h", 0),
            "price_change_1h": None,
            "price_change_6h": None,
            "price_change_24h": data.get("price_change_24h", 0),
            "holder_count": None,
            "top10_holder_pct": None,
            "token_age_hours": None,
            "buy_count_1h": 0,
            "sell_count_1h": 0,
            "buy_sell_ratio": 0.0,
            "tx_count_24h": None,
            "dex_name": "geckoterminal",
            "symbol": data.get("symbol", "?"),
            "name": data.get("name", "Unknown"),
            "graduated": False,
            "data_source": "geckoterminal",
            "raw_json": json.dumps(data)[:2000],
        }

    async def fetch_birdeye(self, address: str) -> Optional[Dict]:
        """
        DEPRECATED: Birdeye API is now paid-only.
        This function is kept for backwards compatibility but returns None.
        Use GeckoTerminal instead.
        """
        logger.debug("Birdeye is deprecated, skipping for %s", address[:8])
        return None

    def _birdeye_to_snapshot(self, data: dict, address: str) -> Dict[str, Any]:
        """Convert Birdeye API response to unified snapshot format."""
        return {
            "price_usd": data.get("price_usd", 0),
            "market_cap": data.get("market_cap", 0),
            "liquidity_usd": data.get("liquidity", 0),
            "volume_1h": None,
            "volume_6h": None,
            "volume_24h": data.get("volume_24h", 0),
            "price_change_1h": None,
            "price_change_6h": None,
            "price_change_24h": data.get("price_change_24h", 0),
            "holder_count": None,
            "top10_holder_pct": None,
            "token_age_hours": None,
            "buy_count_1h": 0,
            "sell_count_1h": 0,
            "buy_sell_ratio": 0.0,
            "tx_count_24h": None,
            "dex_name": "birdeye",
            "symbol": "?",
            "name": "Unknown",
            "graduated": False,
            "data_source": "birdeye",
            "raw_json": json.dumps(data)[:2000],
        }

    # ── 4. Helius Enrichment ──────────────────────────────────────────────────

    async def fetch_helius_enrichment(self, address: str) -> Dict[str, Any]:
        """
        Fetch comprehensive on-chain data from Helius RPC.
        Returns holder_count, top10_holder_pct, token_age_hours, tx_count_24h.
        All fields are optional - returns None for failed lookups.
        """
        if not HELIUS_RPC:
            logger.debug("HELIUS_API_KEY not set, skipping enrichment")
            return {
                "holder_count": None,
                "top10_holder_pct": None,
                "token_age_hours": None,
                "tx_count_24h": None,
            }
        
        result = {
            "holder_count": None,
            "top10_holder_pct": None,
            "token_age_hours": None,
            "tx_count_24h": None,
        }
        
        # 1. Get holder count + top-10 concentration
        holders = await self.fetch_helius_holders(address)
        result["holder_count"] = holders.get("holder_count")
        result["top10_holder_pct"] = holders.get("top10_holder_pct")
        
        # 2. Get token age
        age = await self.fetch_token_age_helius(address)
        result["token_age_hours"] = age
        
        # 3. Get transaction count (last 24h)
        try:
            now_ts = int(datetime.now(timezone.utc).timestamp())
            day_ago_ts = now_ts - 86400
            
            payload = {
                "jsonrpc": "2.0",
                "id": "tx-count",
                "method": "getSignaturesForAddress",
                "params": [address, {"limit": 1000, "before": None, "until": None}],
            }
            data = await self._post(HELIUS_RPC, payload)
            if data and "result" in data:
                sigs = data["result"]
                # Count transactions within last 24h
                tx_24h = sum(
                    1 for s in sigs 
                    if s.get("blockTime") and s["blockTime"] >= day_ago_ts
                )
                result["tx_count_24h"] = tx_24h
        except Exception as e:
            logger.debug("Helius tx count error for %s: %s", address[:8], e)
        
        return result

    async def fetch_helius_holders(self, address: str) -> Dict[str, Any]:
        """
        Get holder count + top-10 concentration via Helius DAS API.
        Uses getTokenAccounts (Helius custom) which returns result.total.
        """
        if not HELIUS_RPC:
            return {"holder_count": None, "top10_holder_pct": None}

        # Step 1: Get total holder count (limit=1 is enough to get 'total')
        payload = {
            "jsonrpc": "2.0",
            "id": "htk-count",
            "method": "getTokenAccounts",
            "params": {"page": 1, "limit": 1000, "mint": address},
        }
        data = await self._post(HELIUS_RPC, payload)
        if not data:
            return {"holder_count": None, "top10_holder_pct": None}

        result = data.get("result", {})
        if not result:
            # Fallback: try standard getTokenLargestAccounts
            return await self._helius_largest_accounts(address)

        token_accounts = result.get("token_accounts", [])
        total = result.get("total") or len(token_accounts)

        # Step 2: Compute top-10 concentration from returned accounts
        if token_accounts:
            amounts = sorted(
                [_safe_float(a.get("amount", 0)) for a in token_accounts],
                reverse=True
            )
            total_amount = sum(amounts)
            top10_amount = sum(amounts[:10])
            top10_pct    = round(top10_amount / max(total_amount, 1) * 100, 2) if total_amount > 0 else None
        else:
            top10_pct = None

        return {"holder_count": total, "top10_holder_pct": top10_pct}

    async def _helius_largest_accounts(self, address: str) -> Dict[str, Any]:
        """Fallback: use getTokenLargestAccounts to compute top-10 pct."""
        if not HELIUS_RPC:
            return {"holder_count": None, "top10_holder_pct": None}
        payload = {
            "jsonrpc": "2.0",
            "id": "gla",
            "method": "getTokenLargestAccounts",
            "params": [address],
        }
        data = await self._post(HELIUS_RPC, payload)
        if not data:
            return {"holder_count": None, "top10_holder_pct": None}
        try:
            accounts = data.get("result", {}).get("value", [])
            if not accounts:
                return {"holder_count": None, "top10_holder_pct": None}
            amounts      = [_safe_float(a.get("uiAmount", 0)) for a in accounts]
            total_amount = sum(amounts)
            top10_amount = sum(amounts[:10])
            top10_pct    = round(top10_amount / max(total_amount, 1) * 100, 2)
            return {"holder_count": len(accounts), "top10_holder_pct": top10_pct}
        except Exception as e:
            logger.debug("Helius largest accounts error: %s", e)
            return {"holder_count": None, "top10_holder_pct": None}

    async def fetch_token_age_helius(self, address: str) -> Optional[float]:
        """
        Get token age in hours via Helius getSignaturesForAddress.
        Finds the oldest signature = mint/creation transaction.
        """
        if not HELIUS_RPC:
            return None
        payload = {
            "jsonrpc": "2.0",
            "id": "gsa",
            "method": "getSignaturesForAddress",
            "params": [address, {"limit": 1000, "commitment": "confirmed"}],
        }
        data = await self._post(HELIUS_RPC, payload)
        if not data:
            return None
        try:
            sigs = data.get("result", [])
            if not sigs:
                return None
            oldest     = sigs[-1]
            block_time = oldest.get("blockTime")
            if block_time:
                created   = datetime.fromtimestamp(block_time, tz=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
                return round(age_hours, 2)
        except Exception as e:
            logger.debug("Token age error: %s", e)
        return None

    # ── HELIUS TRANSACTION-BASED CANDLES ───────────────────────────────────────

    async def fetch_candles_from_transactions(
        self, address: str, call_time: datetime, lookback_minutes: int = 60
    ) -> Optional[Dict[str, List[Dict]]]:
        """
        Fetch transactions from Helius and reconstruct price candles at multiple timeframes.
        
        This is the KEY to getting historical price action at the exact call time.
        Helius provides transaction-level data which we can aggregate into candles.
        
        Timeframes: 5s, 15s, 30s, 1m, 5m, 10m
        Returns dict like: {"5s": [...], "15s": [...], "30s": [...], "1m": [...], "5m": [...], "10m": [...]}
        Each candle: {"timestamp": unix_ts, "open": price, "high": price, "low": price, "close": price, "volume": usd}
        """
        if not HELIUS_API_KEY:
            logger.warning("HELIUS_API_KEY not set, cannot fetch transaction candles")
            return None
        
        logger.info("=== FETCHING TX CANDLES for %s from %s ===", address[:8], call_time.strftime("%Y-%m-%d %H:%M"))
        
        # Calculate time range
        start_time = call_time
        end_time = call_time + timedelta(minutes=lookback_minutes)
        start_ts = int(start_time.timestamp())
        end_ts = int(end_time.timestamp())
        
        # Fetch transactions using Helius Enhanced Transactions API
        url = HELIUS_RPC
        payload = {
            "jsonrpc": "2.0",
            "id": "candles-tx-fetch",
            "method": "getTransactionHistory",
            "params": [
                address,
                {
                    "startTime": start_ts,
                    "endTime": end_ts,
                    "limit": 1000  # Max transactions to fetch
                }
            ]
        }
        
        try:
            session = await self._session_get()
            async with self.semaphore:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        logger.warning("Helius TX history HTTP %s", resp.status)
                        return None
                    result = await resp.json()
                    
            tx_list = result.get("result", [])
            if not tx_list:
                logger.warning("No transactions found for %s in time range", address[:8])
                return None
            
            logger.info("Found %s transactions for %s", len(tx_list), address[:8])
            
            # Extract price from each transaction
            # Parse transaction to get token amount and SOL amount for price calculation
            prices_with_time = []
            for tx in tx_list:
                try:
                    tx_time = tx.get("blockTime", 0)
                    if not tx_time:
                        continue
                    
                    # Parse transaction instructions to find token swaps
                    # This is simplified - real implementation needs deeper parsing
                    meta = tx.get("meta", {})
                    if not meta or meta.get("err"):
                        continue
                    
                    post_balances = meta.get("postTokenBalances", [])
                    pre_balances = meta.get("preTokenBalances", [])
                    
                    # Find balance change for our token
                    token_balance_change = 0
                    for pb in post_balances:
                        if pb.get("mint") == address:
                            for pre in pre_balances:
                                if pre.get("mint") == address and pre.get("owner") == pb.get("owner"):
                                    token_balance_change = pb.get("uiTokenAmount", {}).get("uiAmount", 0) - pre.get("uiTokenAmount", {}).get("uiAmount", 0)
                                    break
                            break
                    
                    if token_balance_change == 0:
                        continue
                    
                    # Get SOL balance change for price calculation
                    sol_pre = meta.get("preBalances", [])[0] if meta.get("preBalances") else 0
                    sol_post = meta.get("postBalances", [])[0] if meta.get("postBalances") else 0
                    sol_change_lamports = sol_post - sol_pre
                    sol_change = sol_change_lamports / 1e9  # Convert lamports to SOL
                    
                    # Price = SOL change / token change (absolute values)
                    if token_balance_change != 0:
                        price_sol = abs(sol_change) / abs(token_balance_change)
                        # Approximate USD price (we'll enrich with actual SOL/USD rate later)
                        price_usd = price_sol * 150  # Rough SOL price estimate
                        
                        prices_with_time.append({
                            "timestamp": tx_time,
                            "price": price_usd,
                            "volume_usd": abs(token_balance_change) * price_usd,
                            "is_buy": token_balance_change > 0  # Positive = bought token
                        })
                except Exception as e:
                    logger.debug("Error parsing tx: %s", e)
                    continue
            
            if not prices_with_time:
                logger.warning("Could not extract prices from transactions for %s", address[:8])
                return None
            
            # Sort by timestamp
            prices_with_time.sort(key=lambda x: x["timestamp"])
            
            # Aggregate into candles at different timeframes
            timeframes = {
                "5s": 5,
                "15s": 15,
                "30s": 30,
                "1m": 60,
                "5m": 300,
                "10m": 600
            }
            
            candles_by_tf = {}
            for tf_name, tf_seconds in timeframes.items():
                candles = self._aggregate_to_candles(prices_with_time, tf_seconds)
                candles_by_tf[tf_name] = candles
            
            logger.info("Generated candles for %s: %s", address[:8], {k: len(v) for k, v in candles_by_tf.items()})
            return candles_by_tf
            
        except Exception as e:
            logger.exception("Error fetching transaction candles: %s", e)
            return None
    
    def _aggregate_to_candles(
        self, prices: List[Dict], timeframe_seconds: int
    ) -> List[Dict]:
        """Aggregate individual trades into OHLCV candles."""
        if not prices:
            return []
        
        candles = []
        current_candle_start = prices[0]["timestamp"]
        candle_prices = []
        
        for trade in prices:
            ts = trade["timestamp"]
            
            # Check if we need to close current candle and start new one
            if ts >= current_candle_start + timeframe_seconds:
                # Close current candle
                if candle_prices:
                    candle = self._create_candle(candle_prices, current_candle_start)
                    candles.append(candle)
                
                # Start new candle
                current_candle_start = ((ts // timeframe_seconds) * timeframe_seconds)
                candle_prices = [trade]
            else:
                candle_prices.append(trade)
        
        # Don't forget the last candle
        if candle_prices:
            candle = self._create_candle(candle_prices, current_candle_start)
            candles.append(candle)
        
        return candles
    
    def _create_candle(self, trades: List[Dict], start_ts: int) -> Dict:
        """Create a single OHLCV candle from list of trades."""
        if not trades:
            return {}
        
        prices = [t["price"] for t in trades]
        volumes = [t["volume_usd"] for t in trades]
        
        return {
            "timestamp": start_ts,
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": sum(volumes),
            "trade_count": len(trades),
            "buys": sum(1 for t in trades if t.get("is_buy", False)),
            "sells": sum(1 for t in trades if not t.get("is_buy", False))
        }

    # ── MAIN SNAPSHOT ─────────────────────────────────────────────────────────

    async def fetch_snapshot(
        self, address: str, call_time: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch token snapshot WITH HISTORICAL PRICE ACTION from call time.
        
        NEW APPROACH:
        1. Use Helius Transaction API to fetch ALL transactions from call_time
        2. Reconstruct price candles at 5s, 15s, 30s, 1m, 5m, 10m timeframes
        3. Analyze patterns from these candles for AI prediction
        
        This gives us REAL historical data from the exact moment the token was called!
        
        Fallback chain (parallel fetch for speed):
          1. Helius Transaction Candles → PRIMARY SOURCE (historical price action)
          2. pump.fun API → untuk token metadata & current state
          3. DexScreener → untuk graduated tokens
          4. GeckoTerminal → fallback gratis
          5. Jupiter Price → konfirmasi harga
        """
        logger.info("=== FETCHING SNAPSHOT for %s at %s ===", address[:8], call_time.strftime("%Y-%m-%d %H:%M"))
        
        # PRIORITY 1: Fetch transaction-based candles from Helius (HISTORICAL DATA!)
        tx_candles = await self.fetch_candles_from_transactions(address, call_time, lookback_minutes=60)
        
        # Fetch other data in PARALLEL while processing candles
        pumpfun_task = asyncio.create_task(self.fetch_pumpfun(address))
        dex_task     = asyncio.create_task(self.fetch_dexscreener(address))
        gecko_task   = asyncio.create_task(self.fetch_geckoterminal(address))
        helius_task  = asyncio.create_task(self.fetch_helius_enrichment(address))
        jupiter_task = asyncio.create_task(self.fetch_jupiter_price(address))
        
        # Wait for all with timeout
        try:
            pumpfun_data, dex_pair, gecko_data, helius_data, jupiter_price = await asyncio.gather(
                pumpfun_task, dex_task, gecko_task, helius_task, jupiter_task,
                return_exceptions=True
            )
        except Exception as e:
            logger.warning("Parallel fetch error for %s: %s", address[:8], e)
            pumpfun_data = dex_pair = gecko_data = helius_data = jupiter_price = None
        
        # Handle exceptions from individual tasks
        if isinstance(pumpfun_data, Exception):
            logger.warning("pump.fun exception: %s", pumpfun_data)
            pumpfun_data = None
        if isinstance(dex_pair, Exception):
            logger.warning("DexScreener exception: %s", dex_pair)
            dex_pair = None
        if isinstance(gecko_data, Exception):
            logger.warning("GeckoTerminal exception: %s", gecko_data)
            gecko_data = None
        if isinstance(helius_data, Exception):
            logger.warning("Helius exception: %s", helius_data)
            helius_data = None
        if isinstance(jupiter_price, Exception):
            logger.warning("Jupiter exception: %s", jupiter_price)
            jupiter_price = None
        
        snapshot: Optional[Dict[str, Any]] = None
        
        # ── Build snapshot from Helius transaction candles (PRIMARY) ───────────
        if tx_candles:
            logger.info("✓ Got Helius transaction candles for %s", address[:8])
            
            # Get the first candle (closest to call time) as entry point
            first_candle_1m = tx_candles.get("1m", [{}])[0] if tx_candles.get("1m") else {}
            entry_price = first_candle_1m.get("open", 0)
            
            # Calculate metrics from candles
            price_5s = tx_candles.get("5s", [])
            price_1m = tx_candles.get("1m", [])
            price_10m = tx_candles.get("10m", [])
            
            # Volatility: % change from first to last candle in each timeframe
            vol_5s = self._calc_volatility(price_5s)
            vol_1m = self._calc_volatility(price_1m)
            vol_10m = self._calc_volatility(price_10m)
            
            # Volume analysis
            total_vol_10m = sum(c.get("volume", 0) for c in price_10m)
            avg_trade_size = total_vol_10m / max(sum(c.get("trade_count", 0) for c in price_10m), 1)
            
            # Buy/sell pressure from candles
            total_buys = sum(c.get("buys", 0) for c in price_10m)
            total_sells = sum(c.get("sells", 0) for c in price_10m)
            buy_sell_ratio = total_buys / max(total_sells, 1)
            
            # Get current/latest price from last candle
            latest_price = price_10m[-1].get("close", entry_price) if price_10m else entry_price
            
            snapshot = {
                "price_usd":          latest_price,
                "entry_price":        entry_price,
                "market_cap":         latest_price * 1_000_000_000,  # rough estimate
                "liquidity_usd":      None,
                "volume_1h":          total_vol_10m * 6,  # extrapolate
                "volume_6h":          None,
                "volume_24h":         None,
                "price_change_1h":    vol_1m * 100 if vol_1m else None,
                "price_change_6h":    None,
                "price_change_24h":   None,
                "holder_count":       helius_data.get("holder_count") if helius_data else None,
                "top10_holder_pct":   helius_data.get("top10_holder_pct") if helius_data else None,
                "token_age_hours":    helius_data.get("token_age_hours") if helius_data else None,
                "buy_count_1h":       total_buys * 6,  # extrapolate
                "sell_count_1h":      total_sells * 6,
                "buy_sell_ratio":     buy_sell_ratio,
                "tx_count_24h":       sum(c.get("trade_count", 0) for c in price_10m) * 144,  # extrapolate
                "dex_name":           "raydium",  # assume DEX
                "symbol":             "?",
                "name":               "Unknown",
                "graduated":          True,
                "data_source":        "helius_tx_candles",
                # Store candle data for AI analysis
                "candles_5s":         json.dumps(price_5s)[:5000],
                "candles_15s":        json.dumps(price_15s)[:5000] if (price_15s := tx_candles.get("15s")) else "[]",
                "candles_30s":        json.dumps(tx_candles.get("30s", []))[:5000],
                "candles_1m":         json.dumps(price_1m)[:5000],
                "candles_5m":         json.dumps(tx_candles.get("5m", []))[:5000],
                "candles_10m":        json.dumps(price_10m)[:5000],
                "volatility_5s":      vol_5s,
                "volatility_1m":      vol_1m,
                "volatility_10m":     vol_10m,
                "avg_trade_size":     avg_trade_size,
                "raw_json":           json.dumps({"tx_candles": True, "candle_count": len(price_1m)})[:2000],
            }
            
            # Enrich with pump.fun/DexScreener if available
            if pumpfun_data:
                snapshot["symbol"] = pumpfun_data.get("symbol", "?")
                snapshot["name"] = pumpfun_data.get("name", "Unknown")
                snapshot["graduated"] = pumpfun_data.get("complete", False)
                snapshot["data_source"] = "helius_tx_candles+pumpfun"
            
            if dex_pair:
                dex_mcap = _safe_float(dex_pair.get("marketCap")) or _safe_float(dex_pair.get("fdv"))
                if dex_mcap and dex_mcap > 0:
                    snapshot["market_cap"] = dex_mcap
                snapshot["liquidity_usd"] = _safe_float((dex_pair.get("liquidity") or {}).get("usd"))
                snapshot["data_source"] = "helius_tx_candles+dexscreener"

        # ── Fallback to pump.fun (no historical candles, just current state) ───
        if not snapshot and pumpfun_data:
            snapshot = self._pumpfun_to_snapshot(pumpfun_data, address)
            logger.info("✓ Got pump.fun data (fallback): mcap=$%.0f", snapshot.get("market_cap", 0))
            
            # If graduated, enrich with DexScreener
            if snapshot.get("graduated") and dex_pair:
                logger.info("%s graduated — enriching with DexScreener...", address[:8])
                dex_snap = self._dexscreener_to_snapshot(dex_pair, address, None)
                for field in ("volume_1h", "volume_6h", "volume_24h",
                              "price_change_1h", "price_change_6h", "price_change_24h",
                              "buy_count_1h", "sell_count_1h", "buy_sell_ratio",
                              "tx_count_24h", "dex_name", "liquidity_usd"):
                    dex_val = dex_snap.get(field)
                    if dex_val is not None and dex_val != 0:
                        snapshot[field] = dex_val
                if dex_snap.get("price_usd") and dex_snap["price_usd"] > 0:
                    snapshot["price_usd"]  = dex_snap["price_usd"]
                    snapshot["market_cap"] = dex_snap["market_cap"] or snapshot["market_cap"]
                snapshot["data_source"] = "pumpfun+dexscreener"

        # ── Fallback to DexScreener ────────────────────────────────────────────
        if not snapshot and dex_pair:
            snapshot = self._dexscreener_to_snapshot(dex_pair, address, None)
            logger.info("✓ Got DexScreener data (fallback): liquidity=$%.0f", snapshot.get("liquidity_usd", 0))

        # ── Fallback to GeckoTerminal ──────────────────────────────────────────
        if not snapshot and gecko_data:
            snapshot = self._geckoterminal_to_snapshot(gecko_data, address)
            logger.info("✓ Got GeckoTerminal data (fallback): price=$%.8f", snapshot.get("price_usd", 0))

        # ── Enrich with Helius on-chain data ───────────────────────────────────
        if snapshot and helius_data:
            logger.info("Enriching with Helius data...")
            for field in ("holder_count", "top10_holder_pct", "token_age_hours", "tx_count_24h"):
                helius_val = helius_data.get(field)
                if helius_val is not None:
                    snapshot[field] = helius_val
            if "helius" not in snapshot.get("data_source", ""):
                snapshot["data_source"] = f"{snapshot.get('data_source', '')}+helius"

        # ── Use Jupiter price if still no snapshot ─────────────────────────────
        if not snapshot and jupiter_price and jupiter_price > 0:
            snapshot = {
                "price_usd": jupiter_price,
                "market_cap": jupiter_price * 1_000_000_000,
                "liquidity_usd": None,
                "volume_1h": None,
                "volume_6h": None,
                "volume_24h": None,
                "price_change_1h": None,
                "price_change_6h": None,
                "price_change_24h": None,
                "holder_count": None,
                "top10_holder_pct": None,
                "token_age_hours": None,
                "buy_count_1h": 0,
                "sell_count_1h": 0,
                "buy_sell_ratio": 0.0,
                "tx_count_24h": None,
                "dex_name": "jupiter",
                "symbol": "?",
                "name": "Unknown",
                "graduated": False,
                "data_source": "jupiter",
                "raw_json": "{}",
            }
            logger.info("✓ Got Jupiter price only (fallback): $%.8f", jupiter_price)

        # ── No data from any source ────────────────────────────────────────────
        if not snapshot:
            logger.warning("✗ No data from ANY source for %s", address[:8])
            return None

        # ── Final validation ───────────────────────────────────────────────────
        price = snapshot.get("price_usd", 0) or 0
        mcap  = snapshot.get("market_cap", 0) or 0
        if price == 0 and mcap == 0:
            logger.warning("Zero price+mcap for %s, discarding snapshot", address[:8])
            return None

        # Build buy_sell_ratio if not set
        if not snapshot.get("buy_sell_ratio"):
            bc = _safe_int(snapshot.get("buy_count_1h", 0))
            sc = _safe_int(snapshot.get("sell_count_1h", 0))
            snapshot["buy_sell_ratio"] = round(bc / max(sc, 1), 3)
        
        logger.info("=== SNAPSHOT COMPLETE for %s | source=%s | price=$%.8f | mcap=$%.0f ===", 
                   address[:8], snapshot.get("data_source", "?"), 
                   snapshot.get("price_usd", 0), snapshot.get("market_cap", 0))

        return snapshot

    def _calc_volatility(self, candles: List[Dict]) -> float:
        """Calculate % price change from first to last candle."""
        if not candles or len(candles) < 2:
            return 0.0
        first = candles[0].get("open", 0)
        last = candles[-1].get("close", 0)
        if first == 0:
            return 0.0
        return (last - first) / first

    # ── CURRENT PRICE ─────────────────────────────────────────────────────────

    async def fetch_current_price(self, address: str) -> Optional[float]:
        """
        Get current price for labeling (PUMP / DUMP).
        Tries Jupiter → pump.fun → DexScreener in order.
        """
        # 1. Jupiter (fast, reliable for traded tokens)
        price = await self.fetch_jupiter_price(address)
        if price and price > 0:
            return price

        # 2. pump.fun (for bonding curve tokens)
        pf = await self.fetch_pumpfun(address)
        if pf:
            total_supply = _safe_float(pf.get("total_supply", 1_000_000_000))
            usd_mcap     = _safe_float(pf.get("usd_market_cap", 0))
            if usd_mcap and total_supply:
                return usd_mcap / total_supply

        # 3. DexScreener
        pair = await self.fetch_dexscreener(address)
        if pair:
            return _safe_float(pair.get("priceUsd")) or None

        return None

    # ── AI ANALYSIS ───────────────────────────────────────────────────────────

    def build_feature_summary(self, snapshot: Dict) -> str:
        age    = snapshot.get("token_age_hours")
        age_str = f"{age:.1f}h" if age is not None else "unknown"

        top10    = snapshot.get("top10_holder_pct")
        top10_str = f"{top10:.1f}%" if top10 is not None else "unknown"

        source = snapshot.get("data_source", "unknown")
        graduated = "YES" if snapshot.get("graduated") else "NO (bonding curve)"

        vol1h  = snapshot.get("volume_1h")
        vol24h = snapshot.get("volume_24h")
        vol1h_str  = f"${vol1h:,.0f}"  if vol1h  is not None else "N/A"
        vol24h_str = f"${vol24h:,.0f}" if vol24h is not None else "N/A"

        holders = snapshot.get("holder_count")
        holder_str = str(holders) if holders is not None else "unknown"

        return (
            f"Token: {snapshot.get('name', '?')} ({snapshot.get('symbol', '?')})\n"
            f"- Data Source:       {source.upper()}\n"
            f"- Graduated DEX:     {graduated}\n"
            f"- Market Cap:        ${snapshot.get('market_cap', 0):,.0f}\n"
            f"- Liquidity:         ${snapshot.get('liquidity_usd', 0):,.0f}\n"
            f"- Token Age:         {age_str}\n"
            f"- Volume 1h:         {vol1h_str}\n"
            f"- Volume 24h:        {vol24h_str}\n"
            f"- Price Change 1h:   {snapshot.get('price_change_1h') or 'N/A'}\n"
            f"- Price Change 24h:  {snapshot.get('price_change_24h') or 'N/A'}\n"
            f"- Buy/Sell Ratio:    {snapshot.get('buy_sell_ratio', 0):.2f}\n"
            f"- Buys 1h:           {snapshot.get('buy_count_1h', 0)}"
            f" | Sells 1h: {snapshot.get('sell_count_1h', 0)}\n"
            f"- Holders:           {holder_str}\n"
            f"- Top 10 Holders:    {top10_str}\n"
            f"- DEX:               {snapshot.get('dex_name', '?')}"
        )

    def build_historical_context(self) -> str:
        winners = self.db.get_labeled_tokens("PUMP", limit=50)
        losers  = self.db.get_labeled_tokens("DUMP",  limit=50)

        def summarize(tokens: List[Dict], label: str) -> str:
            if not tokens:
                return f"No {label} data yet."
            n = len(tokens)

            def avg(key):
                vals = [t.get(key) or 0 for t in tokens if t.get(key) is not None]
                return sum(vals) / len(vals) if vals else 0

            return (
                f"{label} tokens ({n} samples):\n"
                f"  Avg Market Cap:     ${avg('market_cap'):,.0f}\n"
                f"  Avg Liquidity:      ${avg('liquidity_usd'):,.0f}\n"
                f"  Avg Token Age:      {avg('token_age_hours'):.1f}h\n"
                f"  Avg Buy/Sell Ratio: {avg('buy_sell_ratio'):.2f}\n"
                f"  Avg Top10 Hold %:   {avg('top10_holder_pct'):.1f}%\n"
                f"  Avg Holders:        {avg('holder_count'):.0f}"
            )

        return (
            "HISTORICAL PATTERN SUMMARY:\n"
            f"{summarize(winners, 'PUMP')}\n\n"
            f"{summarize(losers, 'DUMP')}"
        )

    async def predict(self, address: str, snapshot: Dict) -> Dict[str, Any]:
        _default = {
            "score": 50, "verdict": "CAUTION",
            "reasoning": "AI not configured",
            "red_flags": [], "green_flags": [],
            "similar_winners": 0, "similar_losers": 0,
        }

        if not self.groq:
            logger.warning("Groq not configured, returning default prediction")
            return _default

        stats          = self.db.get_stats()
        historical_ctx = self.build_historical_context()
        token_summary  = self.build_feature_summary(snapshot)

        prompt = f"""You are an expert Solana memecoin on-chain analyst specializing in pump.fun tokens.

{historical_ctx}

CHANNEL STATS: {stats['pumps']} pumps, {stats['dumps']} dumps, {stats['winrate']}% winrate.

Now analyze this NEW token:
{token_summary}

Based on historical patterns, assess if this token is likely to PUMP (>{PUMP_THRESHOLD:.0f}% gain within 24h) or DUMP.
Key signals: low market cap + high buy/sell ratio + recent token + healthy holder distribution = potential PUMP.
Honeypot signals: very old age, zero volume, no holders data, only on bonding curve with no activity.

Respond ONLY with valid JSON (no markdown, no extra text):
{{
  "score": <0-100, where 100 = very likely PUMP>,
  "verdict": "<GO|CAUTION|SKIP>",
  "reasoning": "<2-3 sentence analysis>",
  "red_flags": ["<flag1>", "<flag2>"],
  "green_flags": ["<flag1>", "<flag2>"],
  "similar_winners": <int: how many historical PUMPs have similar profile>,
  "similar_losers": <int: how many historical DUMPs have similar profile>
}}"""

        try:
            resp = await self.groq.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500,
            )
            raw = resp.choices[0].message.content.strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            result = json.loads(raw)
            self.db.save_prediction(address, result)
            return result

        except json.JSONDecodeError as e:
            logger.error("Groq JSON parse error: %s", e)
            return _default
        except Exception as e:
            logger.error("Groq API error: %s", e)
            return {**_default, "reasoning": f"AI error: {str(e)[:80]}"}
