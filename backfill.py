"""
backfill.py — Scrape historical messages from channel and build training database.

Run once before starting the main bot:
    python backfill.py --limit 1000 --days 90

This will:
1. Fetch last N messages from the source channel
2. Extract all Solana token addresses
3. Fetch on-chain snapshot for each (using call timestamp for price reconstruction)
4. Schedule price checks and auto-label results
"""

import asyncio
import argparse
import re
import os
import logging
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession

from database import Database
from analyzer import TokenAnalyzer
from notifier import Notifier

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

SOLANA_ADDR_RE = re.compile(r'\b([1-9A-HJ-NP-Za-km-z]{32,44})\b')

# Known non-token addresses to skip
SKIP_ADDRESSES = {
    '11111111111111111111111111111111',           # System program
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  # Token program
    'So11111111111111111111111111111111111111112',    # Wrapped SOL
}


async def backfill(limit: int, days: int):
    db = Database()
    db.init()

    analyzer = TokenAnalyzer(db)
    notifier = Notifier()

    api_id = int(os.environ['TELEGRAM_API_ID'])
    api_hash = os.environ['TELEGRAM_API_HASH']
    session_string = os.environ['TELEGRAM_SESSION_STRING']
    source_channel = os.environ['SOURCE_CHANNEL']
    notify_chat_id = os.environ.get('NOTIFY_CHAT_ID', '')

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.start()
    logger.info("Telethon client started for backfill")

    entity = await client.get_entity(source_channel)
    logger.info(f"Scraping channel: {getattr(entity, 'title', source_channel)}")

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    found_addresses = []
    msg_count = 0

    async for msg in client.iter_messages(entity, limit=limit):
        if msg.date < cutoff:
            logger.info(f"Reached cutoff date {cutoff.date()}, stopping")
            break

        text = msg.text or ''
        addresses = SOLANA_ADDR_RE.findall(text)

        for addr in addresses:
            if addr in SKIP_ADDRESSES:
                continue
            if db.token_exists(addr):
                continue
            found_addresses.append((addr, msg.date.replace(tzinfo=timezone.utc), text))

        msg_count += 1
        if msg_count % 100 == 0:
            logger.info(
                f"Scanned {msg_count} messages, "
                f"found {len(found_addresses)} new tokens so far..."
            )

    await client.disconnect()
    logger.info(f"Scrape complete. Found {len(found_addresses)} unique new token addresses")

    # Process each token
    success = 0
    failed = 0
    now = datetime.now(timezone.utc)
    pump_threshold = float(os.environ.get('PUMP_THRESHOLD', '30'))

    for i, (addr, call_time, raw_msg) in enumerate(found_addresses, 1):
        logger.info(
            f"[{i}/{len(found_addresses)}] Processing {addr[:8]}... "
            f"called at {call_time.strftime('%Y-%m-%d %H:%M')}"
        )

        db.save_call(addr, call_time, raw_msg)

        try:
            snapshot = await analyzer.fetch_snapshot(addr, call_time)
            if not snapshot:
                logger.warning("  No data found, skipping")
                failed += 1
                continue

            db.save_snapshot(addr, snapshot)

            hours_since_call = (now - call_time).total_seconds() / 3600

            if hours_since_call >= 24:
                # Token old enough — fetch current price and estimate outcome
                current_price = await analyzer.fetch_current_price(addr)
                entry_price = snapshot.get('price_usd', 0)

                if current_price and entry_price and entry_price > 0:
                    pct = ((current_price - entry_price) / entry_price) * 100
                    # Note: current vs entry price — approximate for historical data.
                    # A more accurate method: use DexScreener candle max in 24h window.
                    label = 'PUMP' if pct >= pump_threshold else 'DUMP'
                    db.update_label(addr, label, pct)
                    logger.info(f"  Labeled as {label} ({pct:+.1f}%)")
                else:
                    # No price data — likely dead token
                    db.update_label(addr, 'DUMP', -100.0)
                    logger.info("  No price data, labeled as DUMP (likely dead)")
            else:
                logger.info(
                    f"  Recent token ({hours_since_call:.1f}h old), pending live monitoring"
                )

            success += 1
            await asyncio.sleep(0.5)  # Rate limiting — be gentle with APIs

        except Exception as e:
            logger.error(f"  Error processing {addr[:8]}: {e}")
            failed += 1
            await asyncio.sleep(1)

    await analyzer.close()

    stats = db.get_stats()
    logger.info(
        f"\n"
        f"═══════════════════════════════════\n"
        f"BACKFILL COMPLETE\n"
        f"═══════════════════════════════════\n"
        f"Messages scanned:       {msg_count}\n"
        f"Tokens found:           {len(found_addresses)}\n"
        f"Successfully processed: {success}\n"
        f"Failed:                 {failed}\n"
        f"\n"
        f"Database now has:\n"
        f"  Total calls:  {stats['total']}\n"
        f"  Pumps:        {stats['pumps']}\n"
        f"  Dumps:        {stats['dumps']}\n"
        f"  Pending:      {stats['pending']}\n"
        f"  Winrate:      {stats['winrate']}%\n"
        f"═══════════════════════════════════"
    )

    if notify_chat_id:
        await notifier.send_stats(notify_chat_id, stats)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill historical channel calls')
    parser.add_argument('--limit', type=int, default=2000, help='Max messages to scan')
    parser.add_argument('--days', type=int, default=90, help='How many days back to scan')
    args = parser.parse_args()

    asyncio.run(backfill(args.limit, args.days))
