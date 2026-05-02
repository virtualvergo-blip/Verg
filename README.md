 🤖 Solana Signal Filter Bot

Bot yang mempelajari pattern token dari signal channel Telegram, membandingkan on-chain data, dan memprediksi apakah token baru akan PUMP atau DUMP.

## Cara Kerja

```
Channel signal kirim token address
        ↓
Bot capture + catat timestamp
        ↓
Fetch data on-chain (Helius + DexScreener)
  - Snapshot SAAT token di-call (bukan sekarang)
        ↓
Groq AI analisa vs historical patterns
        ↓
Kirim prediksi: GO / CAUTION / SKIP
        ↓
Monitor harga 1h, 6h, 24h setelah call
        ↓
Auto-label: PUMP atau DUMP
        ↓
Bot makin pintar seiring waktu 🧠
```

-----

## Setup

### 1. Dapatkan API Keys

|Service     |Link                        |Catatan                      |
|------------|----------------------------|-----------------------------|
|Telegram API|https://my.telegram.org/apps|Untuk `API_ID` dan `API_HASH`|
|Groq        |https://console.groq.com    |Gratis, model Llama 3.3 70B  |
|Helius      |https://helius.dev          |Free tier: 1M credits/bulan  |
|Telegram Bot|@BotFather di Telegram      |Untuk mengirim notifikasi    |

### 2. Generate Session String (jalankan LOKAL)

```bash
pip install telethon
python generate_session.py
```

Masukkan API ID dan API Hash → copy session string yang muncul.

### 3. Dapatkan Chat ID kamu

Kirim pesan ke bot kamu, lalu buka:

```
https://api.telegram.org/bot<TOKEN>/getUpdates
```

Lihat field `chat.id` — itu adalah `NOTIFY_CHAT_ID` kamu.

-----

## Deploy ke Railway

### Step 1 — Push ke GitHub

```bash
git init
git add .
git commit -m "initial commit"
git remote add origin https://github.com/username/solana-signal-bot.git
git push -u origin main
```

### Step 2 — Buat project di Railway

1. Buka https://railway.app
1. New Project → Deploy from GitHub repo
1. Pilih repo kamu

### Step 3 — Set Environment Variables di Railway

```
TELEGRAM_API_ID         = dari my.telegram.org
TELEGRAM_API_HASH       = dari my.telegram.org
TELEGRAM_SESSION_STRING = dari generate_session.py
SOURCE_CHANNEL          = @nama_channel atau -1001234567890
TELEGRAM_BOT_TOKEN      = dari BotFather
NOTIFY_CHAT_ID          = chat id kamu
HELIUS_API_KEY          = dari helius.dev
GROQ_API_KEY            = dari console.groq.com
PUMP_THRESHOLD          = 30
DB_PATH                 = /data/signals.db
```

### Step 4 — Tambahkan Volume untuk Database

Di Railway:

1. Settings → Add Volume
1. Mount path: `/data`
1. Ini memastikan database tidak hilang saat redeploy

### Step 5 — Deploy!

Railway akan otomatis deploy dan menjalankan `python main.py`

-----

## Backfill Data Historis

Sebelum bot live, jalankan backfill untuk melatih bot dengan data April lalu:

```bash
# Install dependencies lokal
pip install -r requirements.txt

# Set env vars
cp .env.example .env
# Edit .env dengan credentials kamu

# Jalankan backfill — scan 2000 pesan terakhir, 90 hari ke belakang
python backfill.py --limit 2000 --days 90
```

Proses ini bisa makan waktu 30-60 menit tergantung jumlah token. Hasilnya:

- Database terisi dengan ratusan token berlabel PUMP/DUMP
- Bot langsung punya “sense” dari hari pertama live

-----

## Struktur File

```
solana-signal-bot/
├── main.py              # Entry point, monitor channel
├── analyzer.py          # Fetch on-chain data + Groq AI analysis
├── database.py          # SQLite database handler
├── notifier.py          # Telegram notification formatter
├── backfill.py          # Historical data scraper
├── generate_session.py  # Session string generator (jalankan lokal)
├── requirements.txt
├── Procfile             # Railway process definition
└── .env.example         # Template environment variables
```

-----

## Contoh Notifikasi

```
🟢 GO — BONK (Bonk Token)

🎯 AI Score: 78/100
████████░░

📊 Snapshot at Call Time
├ Market Cap: $2.4M
├ Liquidity:  $380K
├ Vol 1h:     $125K
├ Vol 24h:    $890K
├ Age:        4.2h
├ B/S Ratio:  2.34
└ Top10 Hold: 18.5%

🧠 Analysis
Token menunjukkan karakteristik mirip 23 token PUMP sebelumnya: liquidity sehat, usia token muda, buy/sell ratio tinggi, dan market cap masih kecil dengan ruang untuk growth.

🟢 Green Flags
  ✅ Liquidity > $100K solid
  ✅ B/S ratio > 2.0 (buying pressure)

🔴 Red Flags
  ❌ Top 10 holder agak terkonsentrasi

🔗 DexScreener | abc123def456...
⏰ Called: 14:32 UTC
```

-----

## FAQ

**Q: Bot bisa salah prediksi?**
A: Ya, selalu. Bot ini adalah filter tambahan, bukan oracle. Semakin banyak data historis, semakin akurat.

**Q: Database hilang setelah redeploy?**
A: Tidak, kalau sudah setup Railway Volume di mount `/data`.

**Q: Bisa monitor lebih dari 1 channel?**
A: Ya — modifikasi `SOURCE_CHANNEL` di `main.py` menjadi list, dan update event handler di `run()`.

**Q: Bagaimana cara reset label manual?**
A: Buka SQLite (`signals.db`) dan update kolom `label` di tabel `token_calls`.