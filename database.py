import sqlite3
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

DB_PATH = "signals.db"

class DatabaseManager:
    def __init__(self):
        self.db_path = DB_PATH

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        """Create tables if they don't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Tokens table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT UNIQUE NOT NULL,
            name TEXT,
            symbol TEXT,
            price_entry REAL,
            mcap_entry REAL,
            liquidity_entry REAL,
            call_time TIMESTAMP,
            source TEXT,
            status TEXT DEFAULT 'PENDING', -- PENDING, PUMP, DUMP
            label_time TIMESTAMP,
            price_current REAL,
            roi_percent REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # TX Patterns table (for storing candle data)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tx_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            timeframe TEXT NOT NULL, -- 5s, 15s, 1m, etc.
            candle_data TEXT, -- JSON string of candles
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(token_address) REFERENCES tokens(address)
        )
        """)
        
        conn.commit()
        conn.close()
        logger.info("✅ Database schema initialized")

    def save_token(self, address: str, name: str, symbol: str, price_entry: float, 
                   mcap_entry: float, liquidity_entry: float, call_time: datetime, 
                   source: str, status: str = 'PENDING'):
        """Save a new token signal."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
            INSERT OR IGNORE INTO tokens 
            (address, name, symbol, price_entry, mcap_entry, liquidity_entry, call_time, source, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (address, name, symbol, price_entry, mcap_entry, liquidity_entry, call_time, source, status))
            conn.commit()
        except Exception as e:
            logger.error(f"DB Save Error: {e}")
        finally:
            conn.close()

    def save_tx_patterns(self, token_address: str, patterns: Dict[str, List[Dict]]):
        """Save transaction pattern candles for a token."""
        import json
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Clear old patterns for this token to avoid duplicates on re-run
            cursor.execute("DELETE FROM tx_patterns WHERE token_address = ?", (token_address,))
            
            for timeframe, candles in patterns.items():
                if candles:
                    cursor.execute("""
                    INSERT INTO tx_patterns (token_address, timeframe, candle_data)
                    VALUES (?, ?, ?)
                    """, (token_address, timeframe, json.dumps(candles)))
            conn.commit()
        except Exception as e:
            logger.error(f"DB Pattern Save Error: {e}")
        finally:
            conn.close()

    def get_token(self, address: str) -> Optional[Dict]:
        """Get token by address."""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tokens WHERE address = ?", (address,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_pending_tokens(self) -> List[Dict]:
        """Get all tokens waiting for labeling."""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tokens WHERE status = 'PENDING'")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def update_token_label(self, address: str, status: str, price_current: float, roi: float):
        """Update token with final PUMP/DUMP label."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
            UPDATE tokens 
            SET status = ?, price_current = ?, roi_percent = ?, label_time = ?
            WHERE address = ?
            """, (status, price_current, roi, datetime.now(), address))
            conn.commit()
            logger.info(f"✅ Labeled {address[:8]} as {status} (ROI: {roi:.2f}%)")
        except Exception as e:
            logger.error(f"DB Update Error: {e}")
        finally:
            conn.close()

    def get_training_data(self, limit: int = 100) -> List[Dict]:
        """Get labeled tokens with their patterns for AI training context."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Get labeled tokens
        cursor.execute("""
        SELECT t.*, p.timeframe, p.candle_data 
        FROM tokens t
        JOIN tx_patterns p ON t.address = p.token_address
        WHERE t.status IN ('PUMP', 'DUMP')
        ORDER BY t.call_time DESC
        LIMIT ?
        """, (limit * 6,)) # Approximate since we have multiple rows per token
        
        rows = cursor.fetchall()
        conn.close()
        
        # Reconstruct into objects
        tokens_map = {}
        for row in rows:
            addr = row['address']
            if addr not in tokens_map:
                tokens_map[addr] = dict(row)
                tokens_map[addr]['patterns'] = {}
            
            if row['candle_data']:
                import json
                tokens_map[addr]['patterns'][row['timeframe']] = json.loads(row['candle_data'])
                
        return list(tokens_map.values())
