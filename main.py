import asyncio
import logging
import re
import os
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from datetime import datetime, timezone
from database import Database
from analyzer import TokenAnalyzer
from notifier import Notifier

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Solana address regex (base58, 32-44 chars)
SOLANA_ADDR_RE = re.compile(r'\b([1-9A-HJ-NP-Za-km-z]{32,44})\b')

# Known non-token addresses to skip
SKIP_ADDRESSES = {
    '11111111111111111111111111111111',
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
    'So11111111111111111111111111111111111111112',
}

PUMP_THRESHOLD = float(os.environ.get('PUMP_THRESHOLD', '30'))


class SignalBot:
    def __init__(self):
        self.db = Database()
        self.analyzer = TokenAnalyzer(self.db)
        self.notifier = Notifier()

        self.api_id = int(os.environ['TELEGRAM_API_ID'])
        self.api_hash = os.environ['TELEGRAM_API_HASH']
        self.session_string = os.environ['TELEGRAM_SESSION_STRING']
        self.source_channel = os.environ['SOURCE_CHANNEL']   # username or -100xxxxx id
        self.notify_chat_id = os.environ['NOTIFY_CHAT_ID']   # your personal chat id

        self.client = TelegramClient(
            StringSession(self.session_string),
            self.api_id,
            self.api_hash
        )

    async def handle_new_message(self, event):
        msg = event.message
        text = msg.text or ''

        # Extract all Solana addresses from message
        addresses = SOLANA_ADDR_RE.findall(text)
        if not addresses:
            return

        timestamp = msg.date.replace(tzinfo=timezone.utc)

        for addr in addresses:
            if addr in SKIP_ADDRESSES:
                continue

            # Skip if already tracked
            if self.db.token_exists(addr):
                logger.info(f"Token {addr[:8]}... already in DB, skipping")
                continue

            logger.info(f"New token detected: {addr[:8]}... at {timestamp}")

            # Save call to DB immediately
            self.db.save_call(addr, timestamp, text)

            # Fetch snapshot data at call time
            snapshot = await self.analyzer.fetch_snapshot(addr, timestamp)
            if not snapshot:
                logger.warning(f"Could not fetch snapshot for {addr[:8]}...")
                continue

            self.db.save_snapshot(addr, snapshot)

            # Run AI analysis
            prediction = await self.analyzer.predict(addr, snapshot)

            # Send notification
            await self.notifier.send_prediction(
                self.notify_chat_id,
                addr,
                snapshot,
                prediction,
                timestamp
            )

            # Schedule label check (1h, 6h, 24h after call)
            asyncio.create_task(
                self.schedule_label_check(addr, timestamp, snapshot)
            )

    async def schedule_label_check(self, addr: str, call_time: datetime, snapshot: dict):
        """Check price at 1h, 6h, 24h after call and auto-label result"""
        windows = [
            (3_600,  '1h'),
            (21_600, '6h'),
            (86_400, '24h'),
        ]
        entry_price = snapshot.get('price_usd', 0)
        if not entry_price:
            logger.warning(f"No entry price for {addr[:8]}..., skipping label schedule")
            return

        for delay_sec, window_label in windows:
            await asyncio.sleep(delay_sec)
            try:
                current = await self.analyzer.fetch_current_price(addr)
                if current and entry_price:
                    pct_change = ((current - entry_price) / entry_price) * 100
                    self.db.save_price_check(addr, window_label, current, pct_change)
                    logger.info(f"{addr[:8]}... {window_label} result: {pct_change:+.1f}%")

                    # Auto-label after 24h
                    if window_label == '24h':
                        outcome = 'PUMP' if pct_change >= PUMP_THRESHOLD else 'DUMP'
                        self.db.update_label(addr, outcome, pct_change)
                        logger.info(
                            f"Auto-labeled {addr[:8]}... as {outcome} ({pct_change:+.1f}%)"
                        )
            except Exception as e:
                logger.error(f"Label check error for {addr[:8]}... at {window_label}: {e}")

    async def run(self):
        await self.client.start()
        logger.info("Telethon client started")

        # Resolve source channel
        try:
            entity = await self.client.get_entity(self.source_channel)
            title = entity.title if hasattr(entity, 'title') else self.source_channel
            logger.info(f"Monitoring channel: {title}")
        except Exception as e:
            logger.error(f"Could not resolve channel '{self.source_channel}': {e}")
            return

        @self.client.on(events.NewMessage(chats=entity))
        async def handler(event):
            await self.handle_new_message(event)

        logger.info("Bot is live. Listening for signals...")
        await self.client.run_until_disconnected()


async def main():
    bot = SignalBot()
    bot.db.init()
    logger.info("Database initialized")
    await bot.run()


if __name__ == '__main__':
    asyncio.run(main())
