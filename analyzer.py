import os
import asyncio
import logging
import aiohttp
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from groq import AsyncGroq
import json

logger = logging.getLogger(__name__)

# ─── ENV ─────────────────────────────────────────────
HELIUS_API_KEY = os.environ.get('HELIUS_API_KEY', '').strip()
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip()
PUMP_THRESHOLD = float(os.environ.get('PUMP_THRESHOLD', '30'))

GMGN_API_KEY = os.environ.get('GMGN_API_KEY', '').strip()
GMGN_PROXY_URL = os.environ.get('GMGN_PROXY_URL', '').strip()

DEXSCREENER_BASE = "https://api.dexscreener.com"
HELIUS_BASE = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# GMGN: aktif kalau ada proxy URL
_USE_GMGN = bool(GMGN_PROXY_URL)
if _USE_GMGN:
    try:
        from gmgn_client import GMGNClient
        _HAS_GMGN = True
    except ImportError:
        _HAS_GMGN = False
        _USE_GMGN = False
else:
    _HAS_GMGN = False

logger.info(f"Helius API: {'SET' if HELIUS_API_KEY else 'MISSING'}")
logger.info(f"GMGN Proxy: {'ENABLED' if _USE_GMGN else 'DISABLED'}")


def _safe_float(val, default=0.0):
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val.replace(',', '').replace('$', '').replace('K', '000').replace('M', '000000'))
        except (ValueError, AttributeError):
            return default
    if isinstance(val, dict):
        for key in ('usd', 'value', 'amount', 'price', 'current', 'native'):
            if key in val and val[key] is not None:
                return _safe_float(val[key], default)
        return default
    return default


def _safe_int(val, default=0):
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        try:
            return int(float(val.replace(',', '').replace('K', '000')))
        except (ValueError, AttributeError):
            return default
    if isinstance(val, dict):
        for key in ('count', 'value', 'total', 'amount', 'holders'):
            if key in val and val[key] is not None:
                return _safe_int(val[key], default)
        return default
    return default


class TokenAnalyzer:
    def __init__(self, db):
        self.db = db
        self.groq = None
        self.session = None
        self.semaphore = asyncio.Semaphore(2)  # Lebih conservative

        self.gmgn = None
        if _USE_GMGN and _HAS_GMGN:
            self.gmgn = GMGNClient(api_key=GMGN_API_KEY, proxy_url=GMGN_PROXY_URL)
            logger.info(f"GMGN proxy: {GMGN_PROXY_URL[:50]}...")

    # ─── SESSION ──────────────────────────────────────

    async def get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json",
                }
            )
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # ─── RETRY ────────────────────────────────────────

    async def _fetch_with_retry(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        session = await self.get_session()
        for attempt in range(1, 4):
            try:
                async with self.semaphore:
                    async with session.request(method, url, **kwargs) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        elif resp.status == 429:
                            wait = 3 ** attempt
                            logger.warning(f"429, retry in {wait}s")
                            await asyncio.sleep(wait)
                        elif 500 <= resp.status < 600:
                            wait = 2 ** attempt
                            await asyncio.sleep(wait)
                        else:
                            logger.warning(f"HTTP {resp.status} | {url[:70]}")
                            return None
            except asyncio.TimeoutError:
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.debug(f"Req error: {e}")
                await asyncio.sleep(2 ** attempt)
        return None

    # ─── GMGN VIA PROXY ───────────────────────────────

    async def fetch_gmgn_proxy(self, path: str, payload: Optional[Dict] = None) -> Optional[Dict]:
        """Fetch via GMGN proxy worker."""
        if not GMGN_PROXY_URL:
            return None
        
        url = f"{GMGN_PROXY_URL}?path={path}"
        try:
            import aiohttp
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as s:
                if payload:
                    async with s.post(url, json=payload) as r:
                        text = await r.text()
                        if r.status == 200:
                            return json.loads(text)
                        logger.warning(f"GMGN proxy POST {r.status}: {text[:100]}")
                else:
                    async with s.get(url) as r:
                        text = await r.text()
                        if r.status == 200:
                            return json.loads(text)
                        logger.warning(f"GMGN proxy GET {r.status}: {text[:100]}")
        except Exception as e:
            logger.warning(f"GMGN proxy error: {e}")
        return None

    async def fetch_gmgn_token(self, address: str) -> Optional[Dict]:
        """Get token data from GMGN via proxy."""
        # Try multi-window endpoint
        data = await self.fetch_gmgn_proxy(
            "/api/v1/mutil_window_token_info",
            {"chain": "sol", "addresses": [address]}
        )
        if data:
            return self._extract_gmgn_token(data, address)
        
        # Fallback: single token endpoint
        for ep in ["/defi/quotation/v1/tokens/sol/", "/defi/quotation/v1/token/sol/"]:
            data = await self.fetch_gmgn_proxy(ep + address)
            if data:
                extracted = self._extract_gmgn_token(data, address)
                if extracted:
                    return extracted
        return None

    def _extract_gmgn_token(self, raw: Dict, address: str) -> Optional[Dict]:
        """Extract token object dari berbagai format GMGN response."""
        if not isinstance(raw, dict):
            return None
        
        # Format 1: {code: 0, data: {tokens: [{...}]}}
        if raw.get('code') == 0:
            data = raw.get('data', {})
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            if isinstance(data, dict):
                tokens = data.get('tokens', [])
                if tokens:
                    return tokens[0]
                # Cek kalau data langsung token
                if 'address' in data or 'price' in data:
                    return data
            return None
        
        # Format 2: Direct token object
        if 'address' in raw and ('price' in raw or 'market_cap' in raw):
            return raw
        
        # Format 3: {token: {...}}
        if 'token' in raw and isinstance(raw['token'], dict):
            return raw['token']
            
        return None

    async def fetch_gmgn_security(self, address: str) -> Optional[Dict]:
        """Get security data from GMGN."""
        data = await self.fetch_gmgn_proxy(f"/api/v1/token_security/sol/{address}")
        if not data:
            return None
        
        if isinstance(data, dict):
            if data.get('code') == 0 and data.get('data'):
                return data['data']
            # Direct security object
            if 'is_honeypot' in data or 'mintAuthority' in data:
                return data
        return None

    # ─── DEXSCREENER ──────────────────────────────────

    async def fetch_dexscreener(self, address: str) -> Optional[Dict]:
        url = f"{DEXSCREENER_BASE}/tokens/v1/solana/{address}"
        data = await self._fetch_with_retry("GET", url)
        if not data:
            return None
        
        pairs = data if isinstance(data, list) else data.get('pairs', [])
        if not pairs:
            return None
        
        pairs.sort(
            key=lambda x: _safe_float((x.get('liquidity') or {}).get('usd'), 0),
            reverse=True
        )
        return pairs[0]

    async def fetch_dexscreener_price_at_time(
        self, pair_address: str, target_time: datetime
    ) -> Optional[float]:
        if not pair_address:
            return None
        
        from_ts = int((target_time - timedelta(minutes=5)).timestamp())
        to_ts = int((target_time + timedelta(minutes=5)).timestamp())
        
        url = (
            f"{DEXSCREENER_BASE}/latest/dex/candles"
            f"/solana/{pair_address}?from={from_ts}&to={to_ts}&resolution=1"
        )
        
        data = await self._fetch_with_retry("GET", url)
        if not data or not isinstance(data, dict):
            return None
        
        candles = data.get('candles', [])
        if not candles:
            return None
        
        target_ts = target_time.timestamp()
        closest = min(candles, key=lambda c: abs(c.get('t', 0) - target_ts))
        return _safe_float(closest.get('c'))

    # ─── HELIUS ───────────────────────────────────────

    async def fetch_helius_holders(self, address: str) -> Dict[str, Any]:
        if not HELIUS_API_KEY:
            return {'holder_count': None, 'top10_holder_pct': None}
        
        payload = {
            "jsonrpc": "2.0",
            "id": "get-token-largest-accounts",
            "method": "getTokenLargestAccounts",
            "params": [address]
        }
        data = await self._fetch_with_retry("POST", HELIUS_BASE, json=payload)
        if not data:
            return {'holder_count': None, 'top10_holder_pct': None}
        
        try:
            accounts = data.get('result', {}).get('value', [])
            total_supply = sum(float(a.get('uiAmount', 0) or 0) for a in accounts)
            top10 = accounts[:10]
            top10_amount = sum(float(a.get('uiAmount', 0) or 0) for a in top10)
            top10_pct = (top10_amount / total_supply * 100) if total_supply > 0 else 0
            
            return {
                'holder_count': len(accounts),
                'top10_holder_pct': round(top10_pct, 2)
            }
        except Exception as e:
            logger.error(f"Helius holders: {e}")
            return {'holder_count': None, 'top10_holder_pct': None}

    async def fetch_token_age(self, address: str) -> Optional[float]:
        if not HELIUS_API_KEY:
            return None
        
        payload = {
            "jsonrpc": "2.0",
            "id": "get-signatures",
            "method": "getSignaturesForAddress",
            "params": [address, {"limit": 1000, "commitment": "confirmed"}]
        }
        data = await self._fetch_with_retry("POST", HELIUS_BASE, json=payload)
        if not data:
            return None
        
        try:
            sigs = data.get('result', [])
            if not sigs:
                return None
            oldest = sigs[-1]
            block_time = oldest.get('blockTime')
            if block_time:
                created = datetime.fromtimestamp(block_time, tz=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
                return round(age_hours, 2)
        except Exception as e:
            logger.error(f"Token age: {e}")
        return None

    # ─── MAIN SNAPSHOT ────────────────────────────────

    async def fetch_snapshot(self, address: str, call_time: datetime) -> Optional[Dict[str, Any]]:
        """
        Fetch snapshot AT CALL TIME.
        Priority: GMGN (via proxy) → DexScreener + Helius
        """
        snapshot = None
        
        # ── 1. GMGN via Proxy ──
        if _USE_GMGN:
            logger.info(f"GMGN proxy for {address[:8]}...")
            gmgn_token = await self.fetch_gmgn_token(address)
            if gmgn_token:
                gmgn_security = await self.fetch_gmgn_security(address)
                snapshot = self._gmgn_to_snapshot(gmgn_token, gmgn_security, address)
                if snapshot:
                    logger.info(f"✓ GMGN snapshot {address[:8]}")
                    snapshot['data_source'] = 'gmgn'
                    return snapshot
            logger.info(f"✗ GMGN failed {address[:8]}, fallback...")
        
        # ── 2. DexScreener + Helius ──
        pair, holders, age_hours = await asyncio.gather(
            self.fetch_dexscreener(address),
            self.fetch_helius_holders(address),
            self.fetch_token_age(address),
        )
        
        if not pair:
            logger.warning(f"No DEX pair {address[:8]}")
            return None
        
        pair_address = pair.get('pairAddress', '')
        price_at_call = await self.fetch_dexscreener_price_at_time(pair_address, call_time)
        
        liquidity = pair.get('liquidity', {}) or {}
        volume = pair.get('volume', {}) or {}
        price_change = pair.get('priceChange', {}) or {}
        txns = pair.get('txns', {}) or {}
        
        buy_1h = (txns.get('h1') or {}).get('buys', 0) or 0
        sell_1h = (txns.get('h1') or {}).get('sells', 0) or 0
        bs_ratio = round(buy_1h / max(sell_1h, 1), 3)
        
        snapshot = {
            'price_usd': price_at_call or _safe_float(pair.get('priceUsd')),
            'market_cap': _safe_float(pair.get('marketCap')) or _safe_float(pair.get('fdv')),
            'liquidity_usd': _safe_float(liquidity.get('usd')),
            'volume_1h': _safe_float(volume.get('h1')),
            'volume_6h': _safe_float(volume.get('h6')),
            'volume_24h': _safe_float(volume.get('h24')),
            'price_change_1h': _safe_float(price_change.get('h1')),
            'price_change_6h': _safe_float(price_change.get('h6')),
            'price_change_24h': _safe_float(price_change.get('h24')),
            'holder_count': holders.get('holder_count'),
            'top10_holder_pct': holders.get('top10_holder_pct'),
            'token_age_hours': age_hours,
            'buy_count_1h': buy_1h,
            'sell_count_1h': sell_1h,
            'buy_sell_ratio': bs_ratio,
            'tx_count_24h': (
                ((txns.get('h24') or {}).get('buys', 0) or 0) +
                ((txns.get('h24') or {}).get('sells', 0) or 0)
            ),
            'dex_name': pair.get('dexId', ''),
            'symbol': pair.get('baseToken', {}).get('symbol', '???'),
            'name': pair.get('baseToken', {}).get('name', 'Unknown'),
            'data_source': 'dexscreener',
        }
        
        return snapshot

    def _gmgn_to_snapshot(
        self, token: Dict, security: Optional[Dict], address: str
    ) -> Optional[Dict[str, Any]]:
        """Convert GMGN token data to snapshot format."""
        try:
            price = _safe_float(token.get('price'))
            mcap = _safe_float(token.get('market_cap')) or _safe_float(token.get('fdv'))
            liq = _safe_float(token.get('liquidity'))
            
            if price == 0 and mcap == 0:
                return None
            
            # Volume bisa nested
            vol_1h = _safe_float(token.get('volume_1h', token.get('volume1h')))
            vol_6h = _safe_float(token.get('volume_6h', token.get('volume6h')))
            vol_24h = _safe_float(token.get('volume_24h', token.get('volume24h')))
            
            # Kalau volume dict, coba ekstrak
            if isinstance(token.get('volume'), dict):
                v = token['volume']
                vol_1h = vol_1h or _safe_float(v.get('1h'))
                vol_6h = vol_6h or _safe_float(v.get('6h'))
                vol_24h = vol_24h or _safe_float(v.get('24h'))
            
            snapshot = {
                'price_usd': price,
                'market_cap': mcap,
                'liquidity_usd': liq,
                'volume_1h': vol_1h,
                'volume_6h': vol_6h,
                'volume_24h': vol_24h,
                'price_change_1h': _safe_float(token.get('price_change_1h', token.get('priceChange1h'))),
                'price_change_6h': _safe_float(token.get('price_change_6h', token.get('priceChange6h'))),
                'price_change_24h': _safe_float(token.get('price_change_24h', token.get('priceChange24h'))),
                'holder_count': _safe_int(token.get('holder_count', token.get('holders'))) or None,
                'top10_holder_pct': _safe_float(token.get('top10_holder_pct', token.get('top10Percentage'))) or None,
                'token_age_hours': None,
                'buy_count_1h': _safe_int(token.get('buy_1h', token.get('buy1h'))),
                'sell_count_1h': _safe_int(token.get('sell_1h', token.get('sell1h'))),
                'buy_sell_ratio': 0,
                'tx_count_24h': _safe_int(token.get('tx_count_24h', token.get('tx24h'))),
                'dex_name': token.get('dex', '') or token.get('platform', ''),
                'symbol': token.get('symbol', '???') or '???',
                'name': token.get('name', 'Unknown') or 'Unknown',
                'security': security or {},
            }
            return snapshot
            
        except Exception as e:
            logger.error(f"GMGN conversion: {e}")
            return None

    async def fetch_current_price(self, address: str) -> Optional[float]:
        """Current price for labeling."""
        # Try GMGN first
        if _USE_GMGN:
            try:
                gmgn = await self.fetch_gmgn_token(address)
                if gmgn:
                    price = gmgn.get('price') or gmgn.get('last_price')
                    if price:
                        return _safe_float(price)
            except Exception as e:
                logger.debug(f"GMGN price error: {e}")
        
        # Fallback DexScreener
        pair = await self.fetch_dexscreener(address)
        if pair:
            return _safe_float(pair.get('priceUsd'))
        return None

    # ─── AI ───────────────────────────────────────────

    def build_feature_summary(self, snapshot: Dict) -> str:
        age = snapshot.get('token_age_hours')
        age_str = f"{age:.1f}h" if age is not None else "unknown"
        
        top10 = snapshot.get('top10_holder_pct')
        top10_str = f"{top10:.1f}%" if top10 is not None else "unknown"
        
        security = snapshot.get('security', {})
        sec_lines = ""
        if security:
            flags = []
            if security.get('is_honeypot'):
                flags.append("🚨 HONEYPOT")
            if security.get('mintAuthority'):
                flags.append("MintAuth")
            if security.get('freezeAuthority'):
                flags.append("FreezeAuth")
            if flags:
                sec_lines = "\nSECURITY: " + " | ".join(flags)
        
        return (
            f"Token: {snapshot.get('name', '?')} ({snapshot.get('symbol', '?')})\n"
            f"- Market Cap:        ${snapshot.get('market_cap', 0):,.0f}\n"
            f"- Liquidity:         ${snapshot.get('liquidity_usd', 0):,.0f}\n"
            f"- Token Age:         {age_str}\n"
            f"- Volume 1h:         ${snapshot.get('volume_1h', 0):,.0f}\n"
            f"- Volume 24h:        ${snapshot.get('volume_24h', 0):,.0f}\n"
            f"- Price Change 1h:   {snapshot.get('price_change_1h', 0):+.1f}%\n"
            f"- Price Change 24h:  {snapshot.get('price_change_24h', 0):+.1f}%\n"
            f"- Buy/Sell Ratio 1h: {snapshot.get('buy_sell_ratio', 0):.2f}\n"
            f"- Buys 1h:           {snapshot.get('buy_count_1h', 0)} "
            f"| Sells 1h: {snapshot.get('sell_count_1h', 0)}\n"
            f"- Holders:           {snapshot.get('holder_count', '?')}\n"
            f"- Top 10 Holders:    {top10_str}\n"
            f"- DEX:               {snapshot.get('dex_name', '?')}"
            f"{sec_lines}"
        )

    def build_historical_context(self) -> str:
        winners = self.db.get_labeled_tokens('PUMP', limit=30)
        losers = self.db.get_labeled_tokens('DUMP', limit=30)

        def summarize_group(tokens: List[Dict], label: str) -> str:
            if not tokens:
                return f"No {label} data yet."

            avg_mcap = sum(t.get('market_cap', 0) or 0 for t in tokens) / len(tokens)
            avg_liq = sum(t.get('liquidity_usd', 0) or 0 for t in tokens) / len(tokens)
            avg_age = sum(t.get('token_age_hours', 0) or 0 for t in tokens) / len(tokens)
            avg_bs = sum(t.get('buy_sell_ratio', 0) or 0 for t in tokens) / len(tokens)
            top10_vals = [t.get('top10_holder_pct', 0) or 0 for t in tokens if t.get('top10_holder_pct')]
            avg_top10 = sum(top10_vals) / len(top10_vals) if top10_vals else 0

            return (
                f"{label} tokens ({len(tokens)} samples):\n"
                f"  Avg Market Cap:        ${avg_mcap:,.0f}\n"
                f"  Avg Liquidity:         ${avg_liq:,.0f}\n"
                f"  Avg Token Age:         {avg_age:.1f}h\n"
                f"  Avg Buy/Sell Ratio:    {avg_bs:.2f}\n"
                f"  Avg Top10 Holder %:    {avg_top10:.1f}%"
            )

        return (
            "HISTORICAL PATTERN SUMMARY:\n"
            f"{summarize_group(winners, 'PUMP')}\n\n"
            f"{summarize_group(losers, 'DUMP')}"
        )

    async def predict(self, address: str, snapshot: Dict) -> Dict[str, Any]:
        if not self.groq:
            logger.warning("Groq not configured")
            return {
                'score': 50, 'verdict': 'CAUTION',
                'reasoning': 'AI not configured',
                'red_flags': [], 'green_flags': [],
                'similar_winners': 0, 'similar_losers': 0,
            }

        stats = self.db.get_stats()
        historical_ctx = self.build_historical_context()
        token_summary = self.build_feature_summary(snapshot)

        prompt = f"""You are an expert Solana memecoin on-chain analyst...

{historical_ctx}

CHANNEL STATS: {stats['pumps']} pumps, {stats['dumps']} dumps, {stats['winrate']}% winrate.

Now analyze this NEW token:
{token_summary}

Based on historical patterns, assess if this token is likely to PUMP (>{PUMP_THRESHOLD:.0f}% gain within 24h) or DUMP.

Respond ONLY with valid JSON:
{{
  "score": <0-100>,
  "verdict": "<GO|CAUTION|SKIP>",
  "reasoning": "<2-3 sentences>",
  "red_flags": ["<flag1>"],
  "green_flags": ["<flag1>"],
  "similar_winners": <int>,
  "similar_losers": <int>
}}"""

        try:
            if not self.groq:
                import httpx
                from groq import AsyncGroq
                self.groq = AsyncGroq(api_key=GROQ_API_KEY, http_client=httpx.AsyncClient())

            response = await self.groq.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500
            )
            
            raw = response.choices[0].message.content.strip()
            raw = raw.replace('```json', '').replace('```', '').strip()
            result = json.loads(raw)

            self.db.save_prediction(address, result)
            return result

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse: {e}")
            return {
                'score': 50, 'verdict': 'CAUTION',
                'reasoning': 'Parse error', 'red_flags': [], 'green_flags': [],
                'similar_winners': 0, 'similar_losers': 0,
            }
        except Exception as e:
            logger.error(f"Groq: {e}")
            return {
                'score': 50, 'verdict': 'CAUTION',
                'reasoning': f'Error: {str(e)}', 'red_flags': [], 'green_flags': [],
                'similar_winners': 0, 'similar_losers': 0,
            }
