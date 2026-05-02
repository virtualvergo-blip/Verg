"""
generate_session.py — Run this LOCALLY (not on Railway) to get your Telethon session string.

Usage:
    pip install telethon
    python generate_session.py

Then copy the printed session string into your Railway environment variables.
"""

from telethon.sync import TelegramClient
from telethon.sessions import StringSession

print("=" * 50)
print("Telethon Session String Generator")
print("=" * 50)
print()
print("Get your API credentials from: https://my.telegram.org/apps")
print()

api_id = int(input("Enter your API ID: ").strip())
api_hash = input("Enter your API Hash: ").strip()

with TelegramClient(StringSession(), api_id, api_hash) as client:
    client.start()
    session_string = client.session.save()

print()
print("=" * 50)
print("YOUR SESSION STRING (copy this to Railway env):")
print("=" * 50)
print()
print(session_string)
print()
print("⚠️  Keep this secret! It gives full access to your Telegram account.")
