"""notifier.py — Telegram notification sender."""
import os
import logging
import aiohttp
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

BOT_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
DEXSCREENER_URL = "https://dexscreener.com/solana"
PUMPFUN_URL     = "https://pump.fun"


class Notifier:
    def __init__(self):
        self.bot_token = BOT_TOKEN
        if not self.bot_token:
            logger.warning(
                "⚠️  TELEGRAM_BOT_TOKEN not set! "
                "Notifications will be logged only. "
                "Set this env var to enable Telegram messages."
            )
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def verdict_emoji(self, verdict: str) -> str:
        return {"GO": "🟢", "CAUTION": "🟡", "SKIP": "🔴"}.get(verdict, "⚪")

    def score_bar(self, score: float) -> str:
        filled = max(0, min(10, int(score / 10)))
        return "█" * filled + "░" * (10 - filled)

    def format_number(self, n) -> str:
        n = n or 0
        try:
            n = float(n)
        except (TypeError, ValueError):
            return "N/A"
        if n == 0:
            return "N/A"
        if n >= 1_000_000:
            return f"${n / 1_000_000:.2f}M"
        if n >= 1_000:
            return f"${n / 1_000:.1f}K"
        return f"${n:.4f}"

    def _token_link(self, address: str, graduated: bool) -> str:
        if graduated:
            return f'<a href="{DEXSCREENER_URL}/{address}">DexScreener</a> | <a href="{PUMPFUN_URL}/coin/{address}">Pump.fun</a>'
        return f'<a href="{PUMPFUN_URL}/coin/{address}">Pump.fun</a> | <a href="{DEXSCREENER_URL}/{address}">DexScreener</a>'

    async def send_prediction(
        self,
        chat_id: str,
        address: str,
        snapshot: Dict[str, Any],
        prediction: Dict[str, Any],
        call_time: datetime,
    ):
        verdict    = prediction.get("verdict", "CAUTION")
        score      = prediction.get("score", 50)
        reasoning  = prediction.get("reasoning", "")
        red_flags  = prediction.get("red_flags", [])
        green_flags = prediction.get("green_flags", [])

        symbol   = snapshot.get("symbol", "???")
        name     = snapshot.get("name", "Unknown")
        graduated = snapshot.get("graduated", False)

        age = snapshot.get("token_age_hours")
        age_str = f"{age:.1f}h" if age is not None else "?"

        top10 = snapshot.get("top10_holder_pct")
        top10_str = f"{top10:.1f}%" if top10 is not None else "?"

        holders = snapshot.get("holder_count")
        holder_str = f"{holders:,}" if holders else "?"

        bs = snapshot.get("buy_sell_ratio", 0) or 0

        source = snapshot.get("data_source", "unknown").upper()
        source_emoji = "📡" if "pumpfun" in source.lower() else "📊"

        green_lines = "\n".join(f"  ✅ {f}" for f in green_flags) if green_flags else "  —"
        red_lines   = "\n".join(f"  ❌ {f}" for f in red_flags)   if red_flags   else "  —"

        grad_str = "✅ Graduated (DEX)" if graduated else "⏳ Bonding Curve (pump.fun)"

        vol1h  = snapshot.get("volume_1h")
        vol24h = snapshot.get("volume_24h")
        vol1h_str  = self.format_number(vol1h)  if vol1h  is not None else "N/A"
        vol24h_str = self.format_number(vol24h) if vol24h is not None else "N/A"

        msg = (
            f"{self.verdict_emoji(verdict)} <b>{verdict}</b> — <b>{symbol}</b> ({name})\n"
            f"\n"
            f"🎯 <b>AI Score: {score}/100</b>\n"
            f"<code>{self.score_bar(score)}</code>\n"
            f"\n"
            f"{source_emoji} <b>Source: {source}</b> | {grad_str}\n"
            f"\n"
            f"📊 <b>Snapshot at Call Time</b>\n"
            f"├ Market Cap: <b>{self.format_number(snapshot.get('market_cap'))}</b>\n"
            f"├ Liquidity:  <b>{self.format_number(snapshot.get('liquidity_usd'))}</b>\n"
            f"├ Vol 1h:     <b>{vol1h_str}</b>\n"
            f"├ Vol 24h:    <b>{vol24h_str}</b>\n"
            f"├ Age:        <b>{age_str}</b>\n"
            f"├ Holders:    <b>{holder_str}</b>\n"
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
            f"🔗 {self._token_link(address, graduated)}\n"
            f"<code>{address[:20]}…</code>\n"
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
        emoji = "🚀" if label == "PUMP" else "📉"
        msg = (
            f"{emoji} <b>Result — {symbol}</b>\n"
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

    async def _send(self, chat_id: str, text: str):
        if not self.bot_token:
            logger.info("[NO BOT TOKEN] Would send to %s:\n%s", chat_id, text[:200])
            return

        if not chat_id:
            logger.warning("chat_id is empty, skipping send")
            return

        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "chat_id":                  chat_id,
                    "text":                     text,
                    "parse_mode":               "HTML",
                    "disable_web_page_preview": True,
                }
                async with session.post(
                    f"{self.base_url}/sendMessage",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    result = await resp.json()
                    if not result.get("ok"):
                        err = result.get("description", "unknown error")
                        logger.error("Telegram send failed: %s | chat_id=%s", err, chat_id)
                        # Common errors guide:
                        if "chat not found" in err.lower():
                            logger.error(
                                "→ Bot not added to chat or wrong NOTIFY_CHAT_ID. "
                                "Add bot to group/channel and use /start first."
                            )
                        elif "bot was blocked" in err.lower():
                            logger.error("→ User blocked the bot.")
                        elif "unauthorized" in err.lower():
                            logger.error("→ Wrong TELEGRAM_BOT_TOKEN.")
        except Exception as e:
            logger.error("Notifier send error: %s", e)
