import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import asyncio
import aiohttp
from groq import AsyncGroq

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TokenAnalyzer:
    def __init__(self):
        self.groq_key = os.getenv("GROQ_API_KEY")
        self.helius_key = os.getenv("HELIUS_API_KEY")
        
        if not self.groq_key:
            logger.warning("⚠️ GROQ_API_KEY not set! AI analysis will be skipped.")
        else:
            self.groq_client = AsyncGroq(api_key=self.groq_key)
            logger.info("✅ Groq client initialized")
            
        if not self.helius_key:
            logger.warning("⚠️ HELIUS_API_KEY not set! Transaction history analysis disabled.")
        else:
            logger.info(f"✅ Helius client initialized (Key: {self.helius_key[:8]}...)")

        self.session = None

    async def start_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_helius_transactions(self, token_address: str, timestamp: datetime) -> Optional[List[Dict]]:
        """Fetch raw transactions from Helius starting from the call time."""
        if not self.helius_key:
            return None
        
        url = f"https://mainnet.helius-rpc.com/?api-key={self.helius_key}"
        
        # Convert timestamp to Unix time
        before_timestamp = int(timestamp.timestamp())
        
        payload = {
            "jsonrpc": "2.0",
            "id": "helius-tx",
            "method": "getSignaturesForAddress",
            "params": [
                token_address,
                {
                    "before": None, # Start from latest and go back? No, we need specific time.
                    "limit": 1000
                }
            ]
        }
        
        # Note: Helius getSignaturesForAddress returns signatures in descending order (newest first).
        # We need to fetch and filter manually or use 'before' parameter carefully.
        # For simplicity in backfill, we fetch recent 1000 and filter by time locally 
        # OR use 'until' parameter if available (Helius supports 'until' for timestamp).
        
        # Better approach: Fetch signatures until we pass the target timestamp
        try:
            # First request
            params = [token_address, {"limit": 1000}]
            payload["params"] = params
            
            async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "result" in data:
                        # Filter transactions that happened AFTER the call time (we want data starting from call time)
                        # Actually, for pattern analysis, we want transactions IMMEDIATELY after the call.
                        # The list is newest -> oldest. We need to reverse it or process carefully.
                        # Let's just return the raw list for now, filtering happens in candle builder.
                        return data["result"]
            logger.warning(f"Helius TX history HTTP {resp.status}")
        except Exception as e:
            logger.error(f"Helius TX fetch error: {e}")
        return None

    async def build_candles_from_txs(self, txs: List[Dict], start_time: datetime) -> Dict[str, List[Dict]]:
        """
        Build OHLCV candles for 5s, 15s, 30s, 1m, 5m, 10m from raw transaction list.
        Returns dict: { "5s": [...], "1m": [...] }
        """
        if not txs:
            return {}

        # Parse transactions into a list of (timestamp, price, volume)
        # This is simplified. Real implementation needs to decode token transfers from TX instructions.
        # Since decoding on-chain instructions is heavy, we assume Helius 'parsed' field gives us token transfers.
        
        trades = []
        for tx in txs:
            tx_time = tx.get('blockTime')
            if not tx_time: continue
            
            # We only care about trades AFTER the signal call
            if tx_time < start_time.timestamp():
                continue
            
            # Extract price/volume (Simplified logic - requires full parsing in prod)
            # For this snippet, we simulate extraction or rely on parsed data if available
            # In a real scenario, you'd parse 'tokenTransfers' from tx['meta'] or 'transaction'
            
            # Placeholder for actual price extraction logic
            # price = extract_price(tx) 
            # volume = extract_volume(tx)
            # trades.append({'time': tx_time, 'price': price, 'volume': volume})
            pass

        # If no trades extracted (due to complexity of parsing in this snippet), return empty
        if not trades:
            logger.warning("No parsable trades found in Helius response (Parsing logic required).")
            return {}

        # Aggregate into buckets
        timeframes = {
            "5s": 5, "15s": 15, "30s": 30,
            "1m": 60, "5m": 300, "10m": 600
        }
        
        candles = {}
        
        for tf_name, seconds in timeframes.items():
            tf_candles = []
            if not trades: break
            
            current_bucket_start = trades[0]['time'] // seconds * seconds
            bucket_trades = []
            
            for trade in trades:
                bucket_start = trade['time'] // seconds * seconds
                
                if bucket_start != current_bucket_start:
                    # Close previous bucket
                    if bucket_trades:
                        ohlcv = self._create_ohlc(bucket_trades)
                        tf_candles.append(ohlcv)
                    
                    current_bucket_start = bucket_start
                    bucket_trades = []
                
                bucket_trades.append(trade)
            
            # Close last bucket
            if bucket_trades:
                ohlcv = self._create_ohlc(bucket_trades)
                tf_candles.append(ohlcv)
            
            candles[tf_name] = tf_candles
            
        return candles

    def _create_ohlc(self, trades: List[Dict]) -> Dict:
        if not trades: return {}
        prices = [t['price'] for t in trades]
        volumes = [t['volume'] for t in trades]
        
        return {
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": sum(volumes),
            "count": len(trades)
        }

    async def fetch_snapshot(self, token_address: str, call_time: datetime) -> Optional[Dict]:
        """Fetch current snapshot + historical transaction patterns."""
        await self.start_session()
        
        logger.info(f"=== FETCHING SNAPSHOT for {token_address[:8]} at {call_time.strftime('%H:%M')} ===")
        
        # 1. Fetch Transaction History from Helius (for pattern analysis)
        tx_candles = {}
        if self.helius_key:
            logger.info(f"=== FETCHING TX CANDLES for {token_address[:8]} from {call_time.strftime('%H:%M')} ===")
            txs = await self.fetch_helius_transactions(token_address, call_time)
            if txs:
                tx_candles = await self.build_candles_from_txs(txs, call_time)
                logger.info(f"✓ Built candles: {list(tx_candles.keys())}")
            else:
                logger.warning("No transaction history found or parsing failed.")
        else:
            logger.warning("HELIUS_API_KEY not set, cannot fetch transaction candles")

        # 2. Fetch Market Data (Parallel)
        tasks = {
            "pumpfun": self._fetch_pumpfun(token_address),
            "dexscreener": self._fetch_dexscreener(token_address),
            "gecko": self._fetch_gecko(token_address),
            "jupiter": self._fetch_jupiter(token_address)
        }
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        data = {}
        for name, res in zip(tasks.keys(), results):
            if isinstance(res, Exception):
                logger.warning(f"{name} error: {res}")
            else:
                if res: data[name] = res

        # Merge best data
        final_data = self._merge_market_data(data)
        final_data['tx_patterns'] = tx_candles
        final_data['call_time'] = call_time.isoformat()
        
        if final_data.get('price'):
            logger.info(f"=== SNAPSHOT COMPLETE | source={final_data.get('source')} | price=${final_data['price']} ===")
            return final_data
        
        logger.warning(f"No data found for {token_address[:8]}")
        return None

    def _merge_market_data(self, data: Dict) -> Dict:
        # Priority: Pumpfun > DexScreener > Gecko > Jupiter
        priority = ["pumpfun", "dexscreener", "gecko", "jupiter"]
        for source in priority:
            if source in data and data[source]:
                d = data[source]
                d['source'] = source
                return d
        return {}

    async def _fetch_pumpfun(self, ca: str) -> Optional[Dict]:
        url = f"https://frontend-api.pump.fun/coins/{ca}"
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    d = await resp.json()
                    return {
                        "price": float(d.get('usd_market_cap', 0) / d.get('virtual_token_supply', 1)) if d.get('virtual_token_supply') else 0,
                        "mcap": d.get('usd_market_cap'),
                        "liquidity": d.get('usd_market_cap'), # Approx
                        "holders": d.get('total_holders'),
                        "name": d.get('name'),
                        "symbol": d.get('symbol')
                    }
        except: pass
        
        # Fallback pumpportal
        url2 = f"https://pumpportal.fun/api/data/{ca}"
        try:
            async with self.session.get(url2, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    d = await resp.json()
                    return {
                        "price": d.get('price_usd'),
                        "mcap": d.get('market_cap_usd'),
                        "liquidity": d.get('liquidity_usd'),
                        "name": d.get('name'),
                        "symbol": d.get('symbol')
                    }
        except: pass
        return None

    async def _fetch_dexscreener(self, ca: str) -> Optional[Dict]:
        # Try v1 first
        url = f"https://api.dexscreener.com/tokens/v1/solana/{ca}"
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    d = await resp.json()
                    if d and len(d) > 0:
                        pair = d[0] # Might return multiple pairs, take first
                        return {
                            "price": float(pair.get('priceUsd', 0)),
                            "mcap": float(pair.get('fdv', 0)),
                            "liquidity": float(pair.get('liquidity', {}).get('usd', 0)),
                            "volume_24h": float(pair.get('volume', {}).get('h24', 0)),
                            "pair_age": pair.get('pairCreatedAt'),
                            "name": pair.get('baseToken', {}).get('name'),
                            "symbol": pair.get('baseToken', {}).get('symbol')
                        }
        except: pass
        
        # Fallback search
        url2 = f"https://api.dexscreener.com/latest/dex/search?q={ca[:40]}"
        try:
            async with self.session.get(url2, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    d = await resp.json()
                    if d.get('pairs'):
                        # Filter by exact CA if possible, or take top liquidity
                        pair = d['pairs'][0]
                        return {
                            "price": float(pair.get('priceUsd', 0)),
                            "mcap": float(pair.get('fdv', 0)),
                            "liquidity": float(pair.get('liquidity', {}).get('usd', 0)),
                            "name": pair.get('baseToken', {}).get('name'),
                            "symbol": pair.get('baseToken', {}).get('symbol')
                        }
        except: pass
        return None

    async def _fetch_gecko(self, ca: str) -> Optional[Dict]:
        url = f"https://api.geckoterminal.com/api/v2/solana/tokens/{ca}"
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    d = await resp.json()
                    data = d.get('data', {}).get('attributes', {})
                    return {
                        "price": float(data.get('price_usd', 0)),
                        "mcap": float(data.get('market_cap_usd', 0)),
                        "liquidity": float(data.get('liquidity_usd', 0)),
                        "name": data.get('name'),
                        "symbol": data.get('symbol')
                    }
        except: pass
        return None

    async def _fetch_jupiter(self, ca: str) -> Optional[Dict]:
        url = f"https://price.jup.ag/v6/price?ids={ca}"
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    d = await resp.json()
                    if ca in d.get('data', {}):
                        info = d['data'][ca]
                        return {
                            "price": info.get('price'),
                            "mcap": None, # Jupiter price API doesn't always give mcap
                            "liquidity": None,
                            "name": None,
                            "symbol": None
                        }
        except: pass
        return None

    async def analyze(self, token_data: Dict) -> str:
        if not self.groq_key:
            return "SKIP (No AI Key)"
        
        prompt = self._build_prompt(token_data)
        
        try:
            chat_completion = await self.groq_client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama-3.3-70b-versatile",
                temperature=0.2,
                max_tokens=200
            )
            return chat_completion.choices[0].message.content
        except Exception as e:
            logger.error(f"Groq API Error: {e}")
            return "ERROR"

    def _build_prompt(self, data: Dict) -> str:
        tx_patterns = data.get('tx_patterns', {})
        
        pattern_desc = ""
        for tf, candles in tx_patterns.items():
            if candles:
                first_c = candles[0]
                pattern_desc += f"- {tf}: Open=${first_c['open']:.6f}, Close=${first_c['close']:.6f}, Vol=${first_c['volume']:.0f}, Count={first_c['count']}\n"
        
        if not pattern_desc:
            pattern_desc = "- No transaction history available (New token or API limit)\n"

        return f"""
Analyze this Solana token called at {data.get('call_time')}.
Market Data:
- Name: {data.get('name', 'Unknown')} ({data.get('symbol', 'Unknown')})
- Price: ${data.get('price', 0)}
- Liquidity: ${data.get('liquidity', 0)}
- MCAP: ${data.get('mcap', 0)}

Early Transaction Patterns (from call time):
{pattern_desc}

Based on the early transaction velocity and price action in the first few minutes:
1. Is there immediate buy pressure? (High count/volume in 5s/15s)
2. Is the price stable or dumping immediately?
3. Predict: PUMP, DUMP, or SKIP.

Response format: 
PREDICTION: [PUMP/DUMP/SKIP]
REASON: [Short reason]
"""
