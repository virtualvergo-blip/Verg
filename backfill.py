"""Backfill historical messages from a Telegram channel and build training data."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from telethon import TelegramClient
from telethon.sessions import StringSession

from analyzer import TokenAnalyzer
from database import Database
from notifier import Notifier

if load_dotenv is not None:
    load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOLANA_ADDR_RE = re.compile(r"\b([1-9A-HJ-NP-Za-km-z]{32,44})\b")

SKIP_ADDRESSES = {
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "So11111111111111111111111111111111111111112",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",   # USDT
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",   # USDC
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # mSOL
}


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"❌ Missing required environment variable: {name}")
    return value


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def backfill(limit: int, days: int, skip_recent_hours: float = 0) -> None:
    db = Database()
    db.init()

    analyzer = TokenAnalyzer(db)
    notifier = Notifier()

    api_id         = int(required_env("TELEGRAM_API_ID"))
    api_hash       = required_env("TELEGRAM_API_HASH")
    session_string = required_env("TELEGRAM_SESSION_STRING")
    source_channel = required_env("SOURCE_CHANNEL")
    notify_chat_id = os.getenv("NOTIFY_CHAT_ID", "").strip()

    pump_threshold = float(os.getenv("PUMP_THRESHOLD", "30"))
    cutoff         = datetime.now(timezone.utc) - timedelta(days=days)

    client = TelegramClient(StringSession(session_string), api_id, api_hash)

    found_addresses: Dict[str, Tuple[datetime, str]] = {}
    msg_count = 0
    success   = 0
    failed    = 0
    skipped   = 0

    try:
        await client.start()
        logger.info("✓ Telethon client started for backfill")

        entity = await client.get_entity(source_channel)
        logger.info("📡 Scraping channel: %s", getattr(entity, "title", source_channel))

        async for msg in client.iter_messages(entity, limit=limit):
            if not msg.date:
                continue

            msg_date = to_utc(msg.date)
            if msg_date < cutoff:
                logger.info("Reached cutoff date %s, stopping", cutoff.date())
                break

            text = msg.text or ""
            addresses = SOLANA_ADDR_RE.findall(text)

            for addr in addresses:
                if addr in SKIP_ADDRESSES:
                    continue
                if len(addr) < 32 or len(addr) > 44:
                    continue
                if db.token_exists(addr):
                    continue
                if addr not in found_addresses:
                    found_addresses[addr] = (msg_date, text)

            msg_count += 1
            if msg_count % 100 == 0:
                logger.info(
                    "Scanned %s messages, found %s new tokens so far...",
                    msg_count,
                    len(found_addresses),
                )

        logger.info("✓ Scrape complete. Found %s unique new token addresses", len(found_addresses))

        now = datetime.now(timezone.utc)

        for i, (addr, (call_time, raw_msg)) in enumerate(found_addresses.items(), 1):
            hours_ago = (now - call_time).total_seconds() / 3600
            logger.info(
                "[%s/%s] Processing %s... | called %s (%.1fh ago)",
                i,
                len(found_addresses),
                addr[:8],
                call_time.strftime("%Y-%m-%d %H:%M"),
                hours_ago,
            )

            db.save_call(addr, call_time, raw_msg)

            try:
                # ── Fetch snapshot (current data, not historical) ─────────────
                # NOTE: Crypto APIs don't provide historical snapshots at arbitrary times.
                # We fetch CURRENT data and use it as the learning sample.
                # Labeling (PUMP/DUMP) is done by comparing entry price vs current price later.
                snapshot = await analyzer.fetch_snapshot(addr, call_time)
                if not snapshot:
                    logger.warning("  ✗ No snapshot data — skipping %s", addr[:8])
                    failed += 1
                    await asyncio.sleep(1)
                    continue

                db.save_snapshot(addr, snapshot)
                source = snapshot.get("data_source", "unknown")
                price  = snapshot.get("price_usd", 0) or 0
                mcap   = snapshot.get("market_cap", 0) or 0

                logger.info(
                    "  ✓ Snapshot from %s | price=$%.12f | mcap=$%.0f",
                    source.upper(), price, mcap,
                )

                # ── Label: PUMP or DUMP ────────────────────────────────────
                # For tokens called >24h ago, we can label them based on current performance
                if hours_ago >= 24:
                    current_price = await analyzer.fetch_current_price(addr)
                    entry_price   = snapshot.get("price_usd", 0) or 0

                    if current_price and entry_price and entry_price > 0:
                        pct   = ((current_price - entry_price) / entry_price) * 100
                        label = "PUMP" if pct >= pump_threshold else "DUMP"
                        db.update_label(addr, label, pct)
                        logger.info("  → Labeled %s (%+.1f%%)", label, pct)
                    elif not entry_price or entry_price == 0:
                        # No entry price means token was already dead/rugged
                        db.update_label(addr, "DUMP", -100.0)
                        logger.info("  → No entry price — labeled DUMP")
                    else:
                        # Has entry price but can't get current price = dead token
                        db.update_label(addr, "DUMP", -100.0)
                        logger.info("  → No current price (likely dead) — labeled DUMP")
                else:
                    logger.info(
                        "  → Recent call (%.1fh ago) — pending label", hours_ago
                    )

                success += 1

            except Exception as exc:
                logger.exception("  Error processing %s: %s", addr[:8], exc)
                failed += 1
                await asyncio.sleep(2)
                continue

            # Rate limit: be nice to APIs
            await asyncio.sleep(0.8)

        # ── Summary ────────────────────────────────────────────────────────
        stats = db.get_stats()
        logger.info(
            "\n"
            "═══════════════════════════════════\n"
            "         BACKFILL COMPLETE\n"
            "═══════════════════════════════════\n"
            "Messages scanned:       %s\n"
            "Tokens found:           %s\n"
            "  ✓ Processed:          %s\n"
            "  ✗ Failed:             %s\n"
            "\n"
            "Database totals:\n"
            "  Total calls:  %s\n"
            "  Pumps:        %s 🟢\n"
            "  Dumps:        %s 🔴\n"
            "  Pending:      %s ⏳\n"
            "  Win rate:     %s%%\n"
            "═══════════════════════════════════",
            msg_count,
            len(found_addresses),
            success,
            failed,
            stats.get("total"),
            stats.get("pumps"),
            stats.get("dumps"),
            stats.get("pending"),
            stats.get("winrate"),
        )

        if notify_chat_id and notifier:
            try:
                await notifier.send_stats(notify_chat_id, stats)
            except Exception as e:
                logger.warning("Could not send stats notification: %s", e)

    finally:
        await analyzer.close()
        await client.disconnect()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill historical channel calls")
    parser.add_argument("--limit", type=int, default=2000, help="Max messages to scan")
    parser.add_argument("--days",  type=int, default=90,   help="How many days back to scan")
    args = parser.parse_args()

    try:
        asyncio.run(backfill(args.limit, args.days))
        return 0
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 130
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
