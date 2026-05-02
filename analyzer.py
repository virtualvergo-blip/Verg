import os
import asyncio
import logging
import aiohttp
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from groq import AsyncGroq
import json

# Import GMGN client
try:
    from gmgn_client import GMGNClient
    _HAS_GMGN = True
except ImportError:
    _HAS_GMGN = False

logger = logging.getLogger(__name__)

HELIUS_API_KEY = os.environ.get('HELIUS_API_KEY', '')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
PUMP_THRESHOLD = float(os.environ.get('PUMP_THRESHOLD', '30'))

# GMGN Config
GMGN_API_KEY = os.environ.get('GMGN_API_KEY', '')
GMGN_PROXY_URL = os.environ.get('GMGN_PROXY_URL', '')

DEXSCREENER_BASE = "https://api.dexscreener.com"
HELIUS_BASE = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"


class TokenAnalyzer:
    def __init__(self, db):
        self.db = db
        self.groq = None
        self.session = None
        self.semaphore = asyncio.Semaphore(3)

        # Init GMGN client kalau tersedia
        self.gmgn = None
        if _HAS_GMGN:
            self.gmgn = GMGNClient(
                api_key=GMGN_API_KEY,
                proxy_url=GMGN_PROXY_URL
            )
            logger.info("GMGN client initialized (proxy=%s)", bool(GMGN_PROXY_URL))
        else:
            logger.warning("gmgn_client.py not found, using DexScreener only")

    # ─────────────────────────────────────────────
    # SESSION MANAGEMENT
    # ─────────────────────────────────────────────

    async def get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json",
                }
            )
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # ─────────────────────────────────────────────
    # GENERIC RETRY WRAPPER
    # ─────────────────────────────────────────────

    async def _fetch_with_retry(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        session = await self.get_session()
        for attempt in range(1, 4):
            try:
                async with self.semaphore:
                    async with session.request(method, url, **kwargs) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        elif resp.status == 429:
                            wait = 2 ** attempt
                            logger.warning(f"Rate limit 429, retry in {wait}s")
                            await asyncio.sleep(wait)
                        elif 500 <= resp.status < 600:
                            wait = 2 ** attempt
                            logger.warning(f"Server {resp.status}, retry in {wait}s")
                            await asyncio.sleep(wait)
                        else:
                            logger.warning(f"HTTP {resp.status} on {url[:60]}")
                            return None
            except asyncio.TimeoutError:
                logger.warning(f"Timeout attempt {attempt}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Request error: {e}")
                await asyncio.sleep(2 ** attempt)
        return None

    # ─────────────────────────────────────────────
    # GMGN FETCHING (Primary)
    # ─────────────────────────────────────────────

    async def fetch_gmgn_data(self, address: str) -> Optional[Dict]:
        """Fetch comprehensive token data from GMGN."""
        if not self.gmgn:
            return None

        try:
            # 1. Token info (price, volume, holders, etc)
            token_info = await self.gmgn.token_info(address)
            if not token_info:
                logger.debug(f"GMGN token_info failed for {address[:8]}")
                return None

            # 2. Token security (honeypot, mint authority, etc)
            security = await self.gmgn.token_security(address)
            if security:
                token_info['security'] = security

            logger.info(f"GMGN data fetched for {address[:8]}")
            return token_info

        except Exception as e:
            logger.warning(f"GMGN fetch error for {address[:8]}: {e}")
            return None

    def _gmgn_to_snapshot(self, gmgn_data: Dict, address: str) -> Optional[Dict[str, Any]]:
        """Convert GMGN response format ke snapshot format internal."""
        try:
            # GMGN structure bisa beda-beda, handle beberapa varian
            token = gmgn_data

            # Kalau ada nested 'token' key
            if 'token' in token and isinstance(token['token'], dict):
                token = token['token']

            # Extract price
            price = 0.0
            if 'price' in token:
                price = float(token['price'])
            elif 'last_price' in token:
                price = float(token['last_price'])

            # Extract market cap
            mcap = 0.0
            if 'market_cap' in token:
                mcap = float(token['market_cap'])
            elif 'fdv' in token:
                mcap = float(token['fdv'])

            # Extract liquidity
            liq = 0.0
            if 'liquidity' in token:
                liq = float(token['liquidity'])

            # Extract volume (GMGN biasanya ada multiple timeframes)
            vol_1h = float(token.get('volume_1h', 0) or token.get('volume1h', 0) or 0)
            vol_6h = float(token.get('volume_6h', 0) or token.get('volume6h', 0) or 0)
            vol_24h = float(token.get('volume_24h', 0) or token.get('volume24h', 0) or 0)

            # Extract price change
            pc_1h = float(token.get('price_change_1h', 0) or token.get('priceChange1h', 0) or 0)
            pc_6h = float(token.get('price_change_6h', 0) or token.get('priceChange6h', 0) or 0)
            pc_24h = float(token.get('price_change_24h', 0) or token.get('priceChange24h', 0) or 0)

            # Extract holders
            holder_count = token.get('holder_count') or token.get('holders') or None
            top10_pct = token.get('top10_holder_pct') or token.get('top10Percentage') or None

            # Extract tx counts
            buy_1h = int(token.get('buy_1h', 0) or token.get('buy1h', 0) or 0)
            sell_1h = int(token.get('sell_1h', 0) or token.get('sell1h', 0) or 0)
            bs_ratio = round(buy_1h / max(sell_1h, 1), 3)

            tx_24h = int(token.get('tx_count_24h', 0) or token.get('tx24h', 0) or 0)

            # Token age
            age_hours = token.get('token_age_hours') or token.get('age_hours') or None
            if 'created_timestamp' in token and not age_hours:
                try:
                    created = datetime.fromtimestamp(token['created_timestamp'], tz=timezone.utc)
                    age_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
                except:
                    pass

            # Symbol & name
            symbol = token.get('symbol', '???')
            name = token.get('name', 'Unknown')

            # DEX info
            dex_name = token.get('dex', '') or token.get('dexId', '') or token.get('platform', '')

            snapshot = {
                'price_usd': price,
                'market_cap': mcap,
                'liquidity_usd': liq,
                'volume_1h': vol_1h,
                'volume_6h': vol_6h,
                'volume_24h': vol_24h,
                'price_change_1h': pc_1h,
                'price_change_6h': pc_6h,
                'price_change_24h': pc_24h,
                'holder_count': holder_count,
                'top10_holder_pct': top10_pct,
                'token_age_hours': round(age_hours, 2) if age_hours else None,
                'buy_count_1h': buy_1h,
                'sell_count_1h': sell_1h,
                'buy_sell_ratio': bs_ratio,
                'tx_count_24h': tx_24h,
                'dex_name': dex_name,
                'symbol': symbol,
                'name': name,
                # Extra GMGN-specific data
                'gmgn_raw': json.dumps(token),
                'security': token.get('security', {}),
            }

            return snapshot

        except Exception as e:
            logger.error(f"GMGN snapshot conversion error: {e}")
            return None

    # ─────────────────────────────────────────────
    # DEXSCREENER FALLBACK
    # ─────────────────────────────────────────────

    async def fetch_dexscreener(self, address: str) -> Optional[Dict]:
        url = f"{DEXSCREENER_BASE}/tokens/v1/solana/{address}"
        data = await self._fetch_with_retry("GET", url)

        if data is None:
            return None

        if isinstance(data, list):
            pairs = data
        elif isinstance(data, dict):
            pairs = data.get('pairs', [])
        else:
            return None

        if not pairs:
            return None

        pairs.sort(
            key=lambda x: float(x.get('liquidity', {}).get('usd', 0) or 0),
            reverse=True
        )
        return pairs[0]

    async def fetch_helius_holders(self, address: str) -> Dict[str, Any]:
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
            logger.error(f"Helius holders error: {e}")
            return {'holder_count': None, 'top10_holder_pct': None}

    async def fetch_helius_token_metadata(self, address: str) -> Dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "id": "get-asset",
            "method": "getAsset",
            "params": {"id": address}
        }
        data = await self._fetch_with_retry("POST", HELIUS_BASE, json=payload)
        if not data:
            return {}

        try:
            result = data.get('result', {})
            content_meta = result.get('content', {}).get('metadata', {})
            return {
                'name': content_meta.get('name', ''),
                'symbol': content_meta.get('symbol', ''),
                'created_at': None
            }
        except Exception as e:
            logger.error(f"Helius metadata error: {e}")
            return {}

    async def fetch_token_age(self, address: str) -> Optional[float]:
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
            logger.error(f"Token age error: {e}")
        return None

    # ─────────────────────────────────────────────
    # MAIN SNAPSHOT FETCHER (Hybrid: GMGN → DexScreener)
    # ─────────────────────────────────────────────

    async def fetch_snapshot(self, address: str, call_time: datetime) -> Optional[Dict[str, Any]]:
        """
        Fetch comprehensive token snapshot.
        Priority: GMGN (more accurate) → DexScreener + Helius (fallback)
        """
        snapshot = None

        # ── Try GMGN first ──
        if self.gmgn:
            logger.info(f"Trying GMGN for {address[:8]}...")
            gmgn_data = await self.fetch_gmgn_data(address)
            if gmgn_data:
                snapshot = self._gmgn_to_snapshot(gmgn_data, address)
                if snapshot:
                    logger.info(f"✓ GMGN success for {address[:8]}")
                    # Save snapshot with GMGN flag
                    snapshot['data_source'] = 'gmgn'
                    return snapshot
            logger.info(f"✗ GMGN failed for {address[:8]}, trying DexScreener...")

        # ── Fallback: DexScreener + Helius ──
        pair, holders, meta, age_hours = await asyncio.gather(
            self.fetch_dexscreener(address),
            self.fetch_helius_holders(address),
            self.fetch_helius_token_metadata(address),
            self.fetch_token_age(address),
        )

        if not pair:
            logger.warning(f"No DEX pair found for {address[:8]}")
            return None

        price_at_call = await self.get_price_at_time(address, call_time, pair)

        liquidity = pair.get('liquidity', {})
        volume = pair.get('volume', {})
        price_change = pair.get('priceChange', {})
        txns = pair.get('txns', {})

        buy_1h = txns.get('h1', {}).get('buys', 0) or 0
        sell_1h = txns.get('h1', {}).get('sells', 0) or 0
        bs_ratio = round(buy_1h / max(sell_1h, 1), 3)

        snapshot = {
            'price_usd': price_at_call or float(pair.get('priceUsd', 0) or 0),
            'market_cap': float(pair.get('marketCap', 0) or pair.get('fdv', 0) or 0),
            'liquidity_usd': float(liquidity.get('usd', 0) or 0),
            'volume_1h': float(volume.get('h1', 0) or 0),
            'volume_6h': float(volume.get('h6', 0) or 0),
            'volume_24h': float(volume.get('h24', 0) or 0),
            'price_change_1h': float(price_change.get('h1', 0) or 0),
            'price_change_6h': float(price_change.get('h6', 0) or 0),
            'price_change_24h': float(price_change.get('h24', 0) or 0),
            'holder_count': holders.get('holder_count'),
            'top10_holder_pct': holders.get('top10_holder_pct'),
            'token_age_hours': age_hours,
            'buy_count_1h': buy_1h,
            'sell_count_1h': sell_1h,
            'buy_sell_ratio': bs_ratio,
            'tx_count_24h': (
                (txns.get('h24', {}).get('buys', 0) or 0) +
                (txns.get('h24', {}).get('sells', 0) or 0)
            ),
            'dex_name': pair.get('dexId', ''),
            'symbol': meta.get('symbol') or pair.get('baseToken', {}).get('symbol', ''),
            'name': meta.get('name') or pair.get('baseToken', {}).get('name', ''),
            'data_source': 'dexscreener',
        }

        return snapshot

    async def get_price_at_time(
        self, address: str, target_time: datetime, current_pair: Dict
    ) -> Optional[float]:
        try:
            pair_address = current_pair.get('pairAddress', '')
            chain = current_pair.get('chainId', 'solana')
            if not pair_address:
                return None

            from_ts = int((target_time - timedelta(minutes=5)).timestamp())
            to_ts = int((target_time + timedelta(minutes=5)).timestamp())

            url = (
                f"{DEXSCREENER_BASE}/latest/dex/candles"
                f"/{chain}/{pair_address}?from={from_ts}&to={to_ts}&resolution=1"
            )

            data = await self._fetch_with_retry("GET", url)
            if data:
                candles = data.get('candles', []) if isinstance(data, dict) else []
                if candles:
                    target_ts = target_time.timestamp()
                    closest = min(candles, key=lambda c: abs(c.get('t', 0) - target_ts))
                    return float(closest.get('c', 0))
        except Exception as e:
            logger.debug(f"Candle fetch error: {e}")
        return None

    async def fetch_current_price(self, address: str) -> Optional[float]:
        # Try GMGN first
        if self.gmgn:
            try:
                gmgn_data = await self.gmgn.token_info(address)
                if gmgn_data:
                    token = gmgn_data.get('token', gmgn_data)
                    price = token.get('price') or token.get('last_price')
                    if price:
                        return float(price)
            except Exception as e:
                logger.debug(f"GMGN current price error: {e}")

        # Fallback DexScreener
        pair = await self.fetch_dexscreener(address)
        if pair:
            return float(pair.get('priceUsd', 0) or 0)
        return None

    # ─────────────────────────────────────────────
    # AI PATTERN ANALYSIS
    # ─────────────────────────────────────────────

    def build_feature_summary(self, snapshot: Dict) -> str:
        age = snapshot.get('token_age_hours')
        age_str = f"{age:.1f}h" if age is not None else "unknown"

        top10 = snapshot.get('top10_holder_pct')
        top10_str = f"{top10:.1f}%" if top10 is not None else "unknown"

        # Security info dari GMGN
        security = snapshot.get('security', {})
        sec_lines = ""
        if security:
            flags = []
            if security.get('is_honeypot'):
                flags.append("🚨 HONEYPOT DETECTED")
            if security.get('mintAuthority'):
                flags.append(f"Mint Authority: {security['mintAuthority']}")
            if security.get('freezeAuthority'):
                flags.append(f"Freeze Authority: {security['freezeAuthority']}")
            if security.get('top10Percentage'):
                flags.append(f"Top10: {security['top10Percentage']}%")
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
            logger.warning("Groq not configured, returning neutral prediction")
            return {
                'score': 50,
                'verdict': 'CAUTION',
                'reasoning': 'AI not configured (missing GROQ_API_KEY)',
                'red_flags': [],
                'green_flags': [],
                'similar_winners': 0,
                'similar_losers': 0,
            }

        stats = self.db.get_stats()
        historical_ctx = self.build_historical_context()
        token_summary = self.build_feature_summary(snapshot)

        prompt = f"""You are an expert Solana memecoin on-chain analyst. You have studied hundreds of tokens called by a signal channel and learned to distinguish PUMP tokens from DUMP tokens.

{historical_ctx}

CHANNEL STATS: {stats['pumps']} pumps, {stats['dumps']} dumps, {stats['winrate']}% winrate overall.

Now analyze this NEW token that was just called:
{token_summary}

Based on the historical patterns above, assess if this token is likely to PUMP (>{PUMP_THRESHOLD:.0f}% gain within 24h) or DUMP.

Respond ONLY with valid JSON, no markdown, no explanation outside JSON:
{{
  "score": <0-100, higher = more likely PUMP>,
  "verdict": "<GO|CAUTION|SKIP>",
  "reasoning": "<2-3 sentences explaining key signals>",
  "red_flags": ["<flag1>", "<flag2>"],
  "green_flags": ["<flag1>", "<flag2>"],
  "similar_winners": <estimated similar tokens that pumped>,
  "similar_losers": <estimated similar tokens that dumped>
}}"""

        try:
            if not self.groq:
                import httpx
                from groq import AsyncGroq
                self.groq = AsyncGroq(
                    api_key=GROQ_API_KEY,
                    http_client=httpx.AsyncClient()
                )

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
            logger.error(f"JSON parse error: {e}")
            return {
                'score': 50, 'verdict': 'CAUTION',
                'reasoning': 'Analysis parse error',
                'red_flags': [], 'green_flags': [],
                'similar_winners': 0, 'similar_losers': 0,
            }
        except Exception as e:
            logger.error(f"Groq prediction error: {e}")
            return {
                'score': 50, 'verdict': 'CAUTION',
                'reasoning': f'Analysis error: {str(e)}',
                'red_flags': [], 'green_flags': [],
                'similar_winners': 0, 'similar_losers': 0,
            }
