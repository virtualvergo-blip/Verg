import os
import logging
import aiohttp
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
DEXSCREENER_URL = "https://dexscreener.com/solana"
PUMP_FUN_URL = "https://pump.fun"


class Notifier:
    def __init__(self):
        self.bot_token = BOT_TOKEN
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    # ─────────────────────────────────────────────
    # FORMATTING HELPERS
    # ─────────────────────────────────────────────

    def verdict_emoji(self, verdict: str) -> str:
        return {'GO': '🟢', 'CAUTION': '🟡', 'SKIP': '🔴'}.get(verdict, '⚪')

    def score_bar(self, score: float) -> str:
        filled = int(score / 10)
        empty = 10 - filled
        return '█' * filled + '░' * empty

    def format_number(self, n: float) -> str:
        if not n:
            return 'N/A'
        if n >= 1_000_000:
            return f"${n / 1_000_000:.2f}M"
        if n >= 1_000:
            return f"${n / 1_000:.1f}K"
        return f"${n:.2f}"

    # ─────────────────────────────────────────────
    # MESSAGE SENDERS
    # ─────────────────────────────────────────────

    async def send_prediction(
        self,
        chat_id: str,
        address: str,
        snapshot: Dict[str, Any],
        prediction: Dict[str, Any],
        call_time: datetime,
    ):
        verdict = prediction.get('verdict', 'CAUTION')
        score = prediction.get('score', 50)
        reasoning = prediction.get('reasoning', '')
        red_flags = prediction.get('red_flags', [])
        green_flags = prediction.get('green_flags', [])

        symbol = snapshot.get('symbol', '???')
        name = snapshot.get('name', 'Unknown')

        age = snapshot.get('token_age_hours')
        age_str = f"{age:.1f}h" if age is not None else "?"

        top10 = snapshot.get('top10_holder_pct')
        top10_str = f"{top10:.1f}%" if top10 is not None else "?"

        bs = snapshot.get('buy_sell_ratio', 0)

        green_lines = '\n'.join(f"  ✅ {f}" for f in green_flags) if green_flags else "  —"
        red_lines   = '\n'.join(f"  ❌ {f}" for f in red_flags)   if red_flags   else "  —"

        msg = (
            f"{self.verdict_emoji(verdict)} <b>{verdict}</b> — <b>{symbol}</b> ({name})\n"
            f"\n"
            f"🎯 <b>AI Score: {score}/100</b>\n"
            f"<code>{self.score_bar(score)}</code>\n"
            f"\n"
            f"📊 <b>Snapshot at Call Time</b>\n"
            f"├ Market Cap: <b>{self.format_number(snapshot.get('market_cap', 0))}</b>\n"
            f"├ Liquidity:  <b>{self.format_number(snapshot.get('liquidity_usd', 0))}</b>\n"
            f"├ Vol 1h:     <b>{self.format_number(snapshot.get('volume_1h', 0))}</b>\n"
            f"├ Vol 24h:    <b>{self.format_number(snapshot.get('volume_24h', 0))}</b>\n"
            f"├ Age:        <b>{age_str}</b>\n"
            f"├ B/S Ratio:  <b>{bs:.2f}</b>\n"
            f"└ Top10 Hold: <b>{top10_str}</b>\n"
            f"\n"
            f"🧠 <b>Analysis</b>\n"
            f"{reasoning}\n"
            f"\n"
            f"🟢 <b>Green Flags</b>\n"
            f"{green_lines}\n"
            f"\n"
            f"🔴 <b>Red Flags</b>\n"
            f"{red_lines}\n"
            f"\n"
            f"🔗 <a href=\"{DEXSCREENER_URL}/{address}\">DexScreener</a>"
            f" | <code>{address[:16]}…</code>\n"
            f"⏰ Called: {call_time.strftime('%H:%M UTC')}"
        )

        await self._send(chat_id, msg)

    async def send_label_update(
        self,
        chat_id: str,
        address: str,
        symbol: str,
        window: str,
        pct_change: float,
        label: str,
    ):
        """Send update when token gets auto-labeled"""
        emoji = '🚀' if label == 'PUMP' else '📉'
        msg = (
            f"{emoji} <b>Result Update — {symbol}</b>\n"
            f"\n"
            f"Window: <b>{window}</b>\n"
            f"Change: <b>{pct_change:+.1f}%</b>\n"
            f"Label:  <b>{label}</b>\n"
            f"\n"
            f"<code>{address[:20]}…</code>"
        )
        await self._send(chat_id, msg)

    async def send_stats(self, chat_id: str, stats: Dict[str, Any]):
        msg = (
            f"📈 <b>Bot Stats</b>\n"
            f"\n"
            f"Total Calls: <b>{stats['total']}</b>\n"
            f"Pumps:       <b>{stats['pumps']}</b> 🟢\n"
            f"Dumps:       <b>{stats['dumps']}</b> 🔴\n"
            f"Pending:     <b>{stats['pending']}</b> ⏳\n"
            f"\n"
            f"Win Rate:    <b>{stats['winrate']}%</b>\n"
            f"Avg Pump:    <b>+{stats['avg_pump_pct']}%</b>"
        )
        await self._send(chat_id, msg)

    # ─────────────────────────────────────────────
    # INTERNAL
    # ─────────────────────────────────────────────

    async def _send(self, chat_id: str, text: str):
        if not self.bot_token:
            logger.warning("No BOT_TOKEN set, skipping send")
            logger.info(f"Would send to {chat_id}:\n{text}")
            return

        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    'chat_id': chat_id,
                    'text': text,
                    'parse_mode': 'HTML',
                    'disable_web_page_preview': True,
                }
                async with session.post(
                    f"{self.base_url}/sendMessage",
                    json=payload
                ) as resp:
                    result = await resp.json()
                    if not result.get('ok'):
                        logger.error(f"Telegram send error: {result}")
        except Exception as e:
            logger.error(f"Notifier send error: {e}")
