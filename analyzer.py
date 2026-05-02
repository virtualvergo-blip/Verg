import os
import asyncio
import logging
import aiohttp
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from groq import AsyncGroq
import json

logger = logging.getLogger(__name__)

HELIUS_API_KEY = os.environ.get('HELIUS_API_KEY', '')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
PUMP_THRESHOLD = float(os.environ.get('PUMP_THRESHOLD', '30'))  # % gain = PUMP

DEXSCREENER_BASE = "https://api.dexscreener.com/latest/dex"
HELIUS_BASE = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"


class TokenAnalyzer:
    def __init__(self, db):
        self.db = db
        self.groq = None # initialized lazily in preidct()

    # ─────────────────────────────────────────────
    # SESSION MANAGEMENT
    # ─────────────────────────────────────────────

    async def get_session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self.session

    async def close(self):
        """Gracefully close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()

    # ─────────────────────────────────────────────
    # DATA FETCHING
    # ─────────────────────────────────────────────

    async def fetch_dexscreener(self, address: str) -> Optional[Dict]:
        """Fetch token data from DexScreener"""
        try:
            session = await self.get_session()
            url = f"{DEXSCREENER_BASE}/tokens/{address}"
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                pairs = data.get('pairs', [])
                if not pairs:
                    return None
                # Get most liquid pair
                pairs.sort(
                    key=lambda x: float(x.get('liquidity', {}).get('usd', 0) or 0),
                    reverse=True
                )
                return pairs[0]
        except Exception as e:
            logger.error(f"DexScreener error for {address[:8]}: {e}")
            return None

    async def fetch_helius_holders(self, address: str) -> Dict[str, Any]:
        """Fetch holder data from Helius"""
        try:
            session = await self.get_session()
            payload = {
                "jsonrpc": "2.0",
                "id": "get-token-largest-accounts",
                "method": "getTokenLargestAccounts",
                "params": [address]
            }
            async with session.post(HELIUS_BASE, json=payload) as resp:
                data = await resp.json()
                accounts = data.get('result', {}).get('value', [])

                # Calculate top 10 holder concentration
                total_supply = sum(float(a.get('uiAmount', 0) or 0) for a in accounts)
                top10 = accounts[:10]
                top10_amount = sum(float(a.get('uiAmount', 0) or 0) for a in top10)
                top10_pct = (top10_amount / total_supply * 100) if total_supply > 0 else 0

                return {
                    'holder_count': len(accounts),
                    'top10_holder_pct': round(top10_pct, 2)
                }
        except Exception as e:
            logger.error(f"Helius holders error for {address[:8]}: {e}")
            return {'holder_count': None, 'top10_holder_pct': None}

    async def fetch_helius_token_metadata(self, address: str) -> Dict[str, Any]:
        """Fetch token creation time and metadata from Helius"""
        try:
            session = await self.get_session()
            payload = {
                "jsonrpc": "2.0",
                "id": "get-asset",
                "method": "getAsset",
                "params": {"id": address}
            }
            async with session.post(HELIUS_BASE, json=payload) as resp:
                data = await resp.json()
                result = data.get('result', {})
                content_meta = result.get('content', {}).get('metadata', {})

                return {
                    'name': content_meta.get('name', ''),
                    'symbol': content_meta.get('symbol', ''),
                    'created_at': None  # Requires separate tx history lookup
                }
        except Exception as e:
            logger.error(f"Helius metadata error for {address[:8]}: {e}")
            return {}

    async def fetch_token_age(self, address: str) -> Optional[float]:
        """Get token age in hours via Helius transaction history"""
        try:
            session = await self.get_session()
            payload = {
                "jsonrpc": "2.0",
                "id": "get-signatures",
                "method": "getSignaturesForAddress",
                "params": [
                    address,
                    {"limit": 1000, "commitment": "confirmed"}
                ]
            }
            async with session.post(HELIUS_BASE, json=payload) as resp:
                data = await resp.json()
                sigs = data.get('result', [])
                if not sigs:
                    return None
                # Oldest transaction = creation
                oldest = sigs[-1]
                block_time = oldest.get('blockTime')
                if block_time:
                    created = datetime.fromtimestamp(block_time, tz=timezone.utc)
                    age_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
                    return round(age_hours, 2)
        except Exception as e:
            logger.error(f"Token age error for {address[:8]}: {e}")
        return None

    async def fetch_snapshot(self, address: str, call_time: datetime) -> Optional[Dict[str, Any]]:
        """
        Fetch comprehensive token snapshot.
        DexScreener returns current data — for historical calls we use
        the candle data nearest to call_time for price reconstruction.
        """
        # Fetch concurrently
        pair, holders, meta, age_hours = await asyncio.gather(
            self.fetch_dexscreener(address),
            self.fetch_helius_holders(address),
            self.fetch_helius_token_metadata(address),
            self.fetch_token_age(address),
        )

        if not pair:
            logger.warning(f"No DEX pair found for {address[:8]}")
            return None

        # Reconstruct price at call_time using historical candle
        price_at_call = await self.get_price_at_time(address, call_time, pair)

        # Extract data from DexScreener
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
        }

        return snapshot

    async def get_price_at_time(
        self, address: str, target_time: datetime, current_pair: Dict
    ) -> Optional[float]:
        """
        Reconstruct price at call_time using DexScreener OHLCV candle data.
        Falls back to current price if candle data unavailable.
        """
        try:
            pair_address = current_pair.get('pairAddress', '')
            chain = current_pair.get('chainId', 'solana')

            if not pair_address:
                return None

            session = await self.get_session()
            from_ts = int((target_time - timedelta(minutes=5)).timestamp())
            to_ts = int((target_time + timedelta(minutes=5)).timestamp())

            url = (
                f"https://api.dexscreener.com/latest/dex/candles"
                f"/{chain}/{pair_address}?from={from_ts}&to={to_ts}&resolution=1"
            )

            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candles = data.get('candles', [])
                    if candles:
                        target_ts = target_time.timestamp()
                        closest = min(candles, key=lambda c: abs(c.get('t', 0) - target_ts))
                        return float(closest.get('c', 0))  # close price
        except Exception as e:
            logger.debug(f"Candle fetch error: {e}")

        return None

    async def fetch_current_price(self, address: str) -> Optional[float]:
        """Fetch current price for label checking"""
        pair = await self.fetch_dexscreener(address)
        if pair:
            return float(pair.get('priceUsd', 0) or 0)
        return None

    # ─────────────────────────────────────────────
    # AI PATTERN ANALYSIS
    # ─────────────────────────────────────────────

    def build_feature_summary(self, snapshot: Dict) -> str:
        """Build human-readable feature summary for AI"""
        age = snapshot.get('token_age_hours')
        age_str = f"{age:.1f}h" if age is not None else "unknown"

        top10 = snapshot.get('top10_holder_pct')
        top10_str = f"{top10:.1f}%" if top10 is not None else "unknown"

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
        )

    def build_historical_context(self) -> str:
        """Build context from labeled historical tokens"""
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
        """Run Groq AI analysis and return prediction"""
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
            logger.error(f"JSON parse error in prediction: {e}")
            return {'score': 50, 'verdict': 'CAUTION', 'reasoning': 'Analysis parse error', 'similar_winners': 0, 'similar_losers': 0}
        except Exception as e:
            logger.error(f"groq prediction error: {e}")
            return {'score': 50, 'verdict': 'CAUTION', 'reasoning': f'Analysis error: {str(e)}', 'similar_winners': 0, 'similar_losers': 0}
   
            raw = response.choices[0].message.content.strip()
            # Strip potential markdown fences
            raw = raw.replace('```json', '').replace('```', '').strip()
            result = json.loads(raw)

            self.db.save_prediction(address, result)
            return result

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error in prediction: {e}")
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
