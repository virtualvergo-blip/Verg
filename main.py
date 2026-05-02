import os
import sys
import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient, events
from telethon.sessions import StringSession as TelethonStringSession
import aiohttp

# Import modules lokal
from analyzer import TokenAnalyzer
from database import DatabaseManager

# Setup Logging (Tanpa Emoji agar aman di Windows)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('bot_live.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Konfigurasi ---
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SOURCE_CHANNEL = os.getenv("SOURCE_CHANNEL", "https://t.me/pumpfunnevadie")
NOTIFY_CHAT_ID = os.getenv("NOTIFY_CHAT_ID")

if not API_ID or not API_HASH:
    logger.error("ERROR: Missing TELEGRAM_API_ID or HASH in .env")
    sys.exit(1)

# Inisialisasi
db = DatabaseManager()
db.init_db()
analyzer = TokenAnalyzer()
session_obj = TelethonStringSession(SESSION_STRING)
client = TelegramClient(session_obj, int(API_ID), API_HASH)

async def process_new_signal(message):
    """Fungsi utama saat ada sinyal baru masuk"""
    text = message.message
    if not text: return
    
    # Ekstrak CA (Contract Address)
    cas = re.findall(r'[1-9A-HJ-NP-Za-km-z]{32,44}', text)
    
    for ca in cas:
        # Cek duplikasi
        if db.get_token(ca):
            logger.info(f"Token {ca[:8]} sudah diproses sebelumnya. Skip.")
            continue
            
        call_time = message.date
        logger.info(f"🚀 NEW SIGNAL DETECTED: {ca[:8]} at {call_time.strftime('%H:%M:%S')}")
        
        # 1. Fetch Data Real-Time (Harga masih hangat!)
        logger.info(f"⏳ Fetching live data for {ca[:8]}...")
        snapshot = await analyzer.fetch_snapshot(ca, call_time)
        
        if snapshot and snapshot.get('price'):
            price = snapshot.get('price')
            mcap = snapshot.get('mcap', 0)
            liq = snapshot.get('liquidity', 0)
            source = snapshot.get('source', 'unknown')
            
            logger.info(f"✅ Data Found: Price=${price} | MCAP=${mcap} | Source={source}")
            
            # 2. Simpan ke DB dengan status PENDING
            db.save_token(
                address=ca,
                name=snapshot.get('name', 'Unknown'),
                symbol=snapshot.get('symbol', 'Unknown'),
                price_entry=price,
                mcap_entry=mcap,
                liquidity_entry=liq,
                call_time=call_time,
                source=source,
                status='PENDING'
            )
            
            # 3. Analisis AI Langsung
            prediction = await analyzer.analyze(snapshot)
            logger.info(f"🤖 AI Prediction: {prediction}")
            
            # 4. Kirim Notifikasi ke User
            if BOT_TOKEN and NOTIFY_CHAT_ID:
                await send_notification(ca, snapshot, prediction)
                
            # 5. Jadwalkan pengecekan labeling (1h, 6h, 24h)
            # Dalam implementasi sederhana, kita bisa buat task background atau cek saat bot nyala
            asyncio.create_task(schedule_labeling(ca, call_time))
            
        else:
            logger.warning(f"❌ Gagal mengambil data untuk {ca[:8]}. Mungkin token rugpull instan atau tidak terindeks.")

async def schedule_labeling(ca, call_time):
    """Task background untuk update status PUMP/DUMP"""
    intervals = [
        (1, "1H"),
        (6, "6H"),
        (24, "24H")
    ]
    
    for hours, label_name in intervals:
        wait_time = hours * 3600
        logger.info(f"⏰ Menunggu {label_name} untuk {ca[:8]} ({hours} jam)...")
        await asyncio.sleep(wait_time)
        
        # Cek harga sekarang
        current_data = await analyzer.fetch_snapshot(ca, datetime.now(timezone.utc))
        if current_data and current_data.get('price'):
            entry_price = db.get_token(ca)['price_entry']
            curr_price = current_data['price']
            
            change_pct = ((curr_price - entry_price) / entry_price) * 100
            
            status = "DUMP"
            if change_pct > 0:
                status = "PUMP"
            elif change_pct < -50: # Toleransi sedikit
                status = "DUMP"
            else:
                status = "DEAD/FLAT"
                
            db.update_status(ca, status, change_pct, label_name)
            logger.info(f"🏷️ Label Updated {ca[:8]} [{label_name}]: {status} ({change_pct:.2f}%)")

async def send_notification(ca, data, prediction):
    """Kirim pesan ke Telegram user"""
    try:
        from telethon.sync import TelegramClient as SyncClient
        # Gunakan bot token untuk kirim pesan
        async with SyncClient(BOT_TOKEN) as bot_client:
            msg = f"""
🚨 **NEW SIGNAL ANALYSIS** 🚨

💎 **Token:** {data.get('name')} ({data.get('symbol')})
📝 **CA:** `{ca}`
💰 **Price:** ${data.get('price')}
📊 **MCap:** ${data.get('mcap')}
💧 **Liq:** ${data.get('liquidity')}
📡 **Source:** {data.get('source')}

🤖 **AI Prediction:**
{prediction}

⚠️ *DYOR. Not financial advice.*
            """
            await bot_client.send_message(int(NOTIFY_CHAT_ID), msg, parse_mode='markdown')
    except Exception as e:
        logger.error(f"Gagal kirim notifikasi: {e}")

async def main():
    logger.info("🚀 Starting LIVE SIGNAL MONITORING BOT...")
    logger.info(f"Monitoring Channel: {SOURCE_CHANNEL}")
    
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Session tidak valid. Harap generate session string baru.")
        return

    @client.on(events.NewMessage(chats=[SOURCE_CHANNEL]))
    async def handler(event):
        await process_new_signal(event.message)

    logger.info("✅ Bot is LIVE! Waiting for signals...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    finally:
        asyncio.run(analyzer.close_session())
