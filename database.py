import sqlite3
import json
import os
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

DB_PATH = 'signals.db'


class Database:
    def __init__(self):
        self.path = DB_PATH

    def get_conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init(self):
        with self.get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS token_calls (
                    address         TEXT PRIMARY KEY,
                    call_time       TEXT NOT NULL,
                    raw_message     TEXT,
                    label           TEXT,
                    max_pct_change  REAL,
                    created_at      TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS token_snapshots (
                    address             TEXT PRIMARY KEY,
                    price_usd           REAL,
                    market_cap          REAL,
                    liquidity_usd       REAL,
                    volume_1h           REAL,
                    volume_6h           REAL,
                    volume_24h          REAL,
                    price_change_1h     REAL,
                    price_change_6h     REAL,
                    price_change_24h    REAL,
                    holder_count        INTEGER,
                    top10_holder_pct    REAL,
                    token_age_hours     REAL,
                    buy_count_1h        INTEGER,
                    sell_count_1h       INTEGER,
                    buy_sell_ratio      REAL,
                    tx_count_24h        INTEGER,
                    dex_name            TEXT,
                    symbol              TEXT,
                    name                TEXT,
                    data_source         TEXT DEFAULT 'unknown',
                    raw_json            TEXT,
                    fetched_at          TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS price_checks (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    address         TEXT NOT NULL,
                    window_label    TEXT NOT NULL,
                    price           REAL,
                    pct_change      REAL,
                    checked_at      TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS ai_predictions (
                    address         TEXT PRIMARY KEY,
                    score           REAL,
                    verdict         TEXT,
                    reasoning       TEXT,
                    similar_winners INTEGER,
                    similar_losers  INTEGER,
                    predicted_at    TEXT DEFAULT (datetime('now'))
                );

                CREATE INDEX IF NOT EXISTS idx_calls_label ON token_calls(label);
                CREATE INDEX IF NOT EXISTS idx_calls_time  ON token_calls(call_time);
            """)
        logger.info("Database schema initialized")

    def token_exists(self, address: str) -> bool:
        with self.get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM token_calls WHERE address = ?", (address,)
            ).fetchone()
            return row is not None

    def save_call(self, address: str, call_time: datetime, raw_message: str):
        with self.get_conn() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO token_calls
                   (address, call_time, raw_message)
                   VALUES (?, ?, ?)""",
                (address, call_time.isoformat(), raw_message[:2000])
            )

    def save_snapshot(self, address: str, snapshot: Dict[str, Any]):
        with self.get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO token_snapshots
                   (address, price_usd, market_cap, liquidity_usd,
                    volume_1h, volume_6h, volume_24h,
                    price_change_1h, price_change_6h, price_change_24h,
                    holder_count, top10_holder_pct, token_age_hours,
                    buy_count_1h, sell_count_1h, buy_sell_ratio,
                    tx_count_24h, dex_name, symbol, name, data_source, raw_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    address,
                    snapshot.get('price_usd'),
                    snapshot.get('market_cap'),
                    snapshot.get('liquidity_usd'),
                    snapshot.get('volume_1h'),
                    snapshot.get('volume_6h'),
                    snapshot.get('volume_24h'),
                    snapshot.get('price_change_1h'),
                    snapshot.get('price_change_6h'),
                    snapshot.get('price_change_24h'),
                    snapshot.get('holder_count'),
                    snapshot.get('top10_holder_pct'),
                    snapshot.get('token_age_hours'),
                    snapshot.get('buy_count_1h'),
                    snapshot.get('sell_count_1h'),
                    snapshot.get('buy_sell_ratio'),
                    snapshot.get('tx_count_24h'),
                    snapshot.get('dex_name'),
                    snapshot.get('symbol'),
                    snapshot.get('name'),
                    snapshot.get('data_source', 'unknown'),
                    json.dumps(snapshot)
                )
            )

    def save_price_check(self, address: str, window: str, price: float, pct_change: float):
        with self.get_conn() as conn:
            conn.execute(
                """INSERT INTO price_checks (address, window_label, price, pct_change)
                   VALUES (?, ?, ?, ?)""",
                (address, window, price, pct_change)
            )

    def update_label(self, address: str, label: str, max_pct: float):
        with self.get_conn() as conn:
            conn.execute(
                """UPDATE token_calls
                   SET label = ?, max_pct_change = ?
                   WHERE address = ?""",
                (label, max_pct, address)
            )

    def save_prediction(self, address: str, prediction: Dict[str, Any]):
        with self.get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO ai_predictions
                   (address, score, verdict, reasoning, similar_winners, similar_losers)
                   VALUES (?,?,?,?,?,?)""",
                (
                    address,
                    prediction.get('score'),
                    prediction.get('verdict'),
                    prediction.get('reasoning'),
                    prediction.get('similar_winners', 0),
                    prediction.get('similar_losers', 0),
                )
            )

    def get_labeled_tokens(self, label: Optional[str] = None, limit: int = 200) -> List[Dict]:
        with self.get_conn() as conn:
            if label:
                rows = conn.execute(
                    """SELECT tc.*, ts.*
                       FROM token_calls tc
                       JOIN token_snapshots ts ON tc.address = ts.address
                       WHERE tc.label = ?
                       ORDER BY tc.call_time DESC
                       LIMIT ?""",
                    (label, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT tc.*, ts.*
                       FROM token_calls tc
                       JOIN token_snapshots ts ON tc.address = ts.address
                       WHERE tc.label IS NOT NULL
                       ORDER BY tc.call_time DESC
                       LIMIT ?""",
                    (limit,)
                ).fetchall()
            return [dict(r) for r in rows]

    def get_stats(self) -> Dict[str, Any]:
        with self.get_conn() as conn:
            total   = conn.execute("SELECT COUNT(*) FROM token_calls").fetchone()[0]
            pumps   = conn.execute("SELECT COUNT(*) FROM token_calls WHERE label='PUMP'").fetchone()[0]
            dumps   = conn.execute("SELECT COUNT(*) FROM token_calls WHERE label='DUMP'").fetchone()[0]
            pending = conn.execute("SELECT COUNT(*) FROM token_calls WHERE label IS NULL").fetchone()[0]

            avg_pump_pct = conn.execute(
                "SELECT AVG(max_pct_change) FROM token_calls WHERE label='PUMP'"
            ).fetchone()[0]

            return {
                'total':        total,
                'pumps':        pumps,
                'dumps':        dumps,
                'pending':      pending,
                'winrate':      round(pumps / max(pumps + dumps, 1) * 100, 1),
                'avg_pump_pct': round(avg_pump_pct or 0, 1),
            }

    def get_snapshot(self, address: str) -> Optional[Dict]:
        with self.get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM token_snapshots WHERE address = ?", (address,)
            ).fetchone()
            return dict(row) if row else None
