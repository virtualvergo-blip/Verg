import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.tl.types import Message
import re

# Import local modules
from analyzer import TokenAnalyzer
from database import DatabaseManager

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Config
API_ID = int(os.getenv("TELEGRAM_API_ID", 0))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")
CHANNEL_URL = os.getenv("SOURCE_CHANNEL", "https://t.me/pumpfunnevadie")
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 50
DAYS = int(sys.argv[2]) if len(sys.argv) > 2 else 7

async def main():
    if not API_ID or not API_HASH:
        logger.error("❌ TELEGRAM_API_ID or TELEGRAM_API_HASH missing in .env")
        return

    db = DatabaseManager()
    db.init_db()
    analyzer = TokenAnalyzer()
    
    client = TelegramClient(None, API_ID, API_HASH)
    await client.connect()
    
    if not await client.is_user_authorized():
        if not SESSION_STRING:
            logger.error("❌ Session not authorized and no SESSION_STRING provided.")
            await client.disconnect()
            return
        await client.sign_in(session_string=SESSION_STRING)
    
    logger.info("✅ Telethon client started for backfill")
    
    # Get Channel Entity
    entity = await client.get_entity(CHANNEL_URL)
    logger.info(f"📡 Scraping channel: {entity.title}")
    
    # Fetch Messages
    cutoff_date = datetime.now() - timedelta(days=DAYS)
    messages = []
    seen_addresses = set()
    
    async for message in client.iter_messages(entity, limit=1000):
        if message.date < cutoff_date:
            break
        
        # Extract CA
        text = message.message or ""
        cas = re.findall(r'[1-9A-HJ-NP-Za-km-z]{32,44}', text)
        for ca in cas:
            if ca not in seen_addresses:
                seen_addresses.add(ca)
                messages.append({'ca': ca, 'time': message.date, 'msg_id': message.id})
                
        if len(seen_addresses) >= LIMIT:
            break
            
    logger.info(f"✓ Scrape complete. Found {len(seen_addresses)} unique new token addresses")
    
    # Process Tokens
    for i, item in enumerate(messages):
        ca = item['ca']
        call_time = item['time']
        
        logger.info(f"[{i+1}/{len(messages)}] Processing {ca[:8]}... | called {call_time.strftime('%Y-%m-%d %H:%M')}")
        
        # Check if already processed
        if db.get_token(ca):
            logger.info(f"  → Already exists in DB, skipping")
            continue
            
        # Fetch Data
        snapshot = await analyzer.fetch_snapshot(ca, call_time)
        
        if snapshot:
            # Save to DB (Label pending)
            db.save_token(
                address=ca,
                name=snapshot.get('name', 'Unknown'),
                symbol=snapshot.get('symbol', 'Unknown'),
                price_entry=snapshot.get('price', 0),
                mcap_entry=snapshot.get('mcap', 0),
                liquidity_entry=snapshot.get('liquidity', 0),
                call_time=call_time,
                source=snapshot.get('source', 'unknown'),
                status='PENDING'
            )
            logger.info(f"  ✓ Saved to DB | Status=PENDING (Wait 24h for label)")
        else:
            logger.warning(f"  ✗ No data found for {ca[:8]}")
            
        # Small delay to avoid rate limits
        await asyncio.sleep(1)
        
    await analyzer.close_session()
    await client.disconnect()
    logger.info("✅ Backfill complete")

if __name__ == "__main__":
    asyncio.run(main())
