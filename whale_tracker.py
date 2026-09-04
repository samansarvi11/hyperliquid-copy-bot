import asyncio
import copy
import json
import logging
import os
import sqlite3
from collections import deque
from decimal import Decimal, InvalidOperation
from contextlib import contextmanager
from time import time

import requests
import websockets


WALLET = "0x8af700ba841f30e0a3fcb0ee4c4a9d223e1efa05".lower()
INFO_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"

RECONNECT_DELAY = 5
REQUEST_TIMEOUT = 20
RECENT_FILL_LIMIT = 10000
BACKFILL_OVERLAP_MS = 30_000
PAPER_INITIAL_CAPITAL = Decimal("200")
PAPER_WHALE_CAPITAL = Decimal("9050628")
RECOVERED = "RECOVERED"
RECOVERY_UNVERIFIED = "RECOVERY_UNVERIFIED"
RECOVERY_GAP = "RECOVERY_GAP"
PAPER_ENABLED = "PAPER_ENABLED"
PAPER_PAUSED = "PAPER_PAUSED"
BBO_MAX_AGE_MS = 5000
DEFAULT_DB_PATH = os.environ.get(
    "WHALE_TRACKER_DB_PATH",
    os.environ.get("WHALE_TRACKER_DB", "whale_tracker.sqlite3"),
)
DB_PATH = DEFAULT_DB_PATH
checkpoint_time = 0
logger = logging.getLogger("whale_tracker")


class TrackerDatabase:
    """Small SQLite journal used to make fill handling restart-safe."""

    def __init__(self, path=None):
        self.path = os.fspath(
            path if path is not None else os.environ.get(
                "WHALE_TRACKER_DB_PATH",
                os.environ.get("WHALE_TRACKER_DB", DB_PATH),
            )
        )
        parent = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(parent, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=30)
        self.conn = self.connection
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA busy_timeout = 30000")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def _commit_if_started(self, was_in_transaction):
        if not was_in_transaction:
            self.connection.commit()

    def _create_schema(self):
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS fills (
                identity TEXT PRIMARY KEY,
                dex TEXT NOT NULL,
                coin TEXT,
                fill_time INTEGER,
                tid TEXT,
                oid TEXT,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS positions (
                dex TEXT NOT NULL,
                coin TEXT NOT NULL,
                szi TEXT NOT NULL,
                entry_px TEXT,
                leverage TEXT,
                margin_used TEXT,
                PRIMARY KEY (dex, coin)
            );
            CREATE TABLE IF NOT EXISTS position_context (
                dex TEXT NOT NULL,
                coin TEXT NOT NULL,
                baseline_size TEXT NOT NULL,
                eligible_size TEXT NOT NULL,
                active_episode_id INTEGER,
                episode_state TEXT NOT NULL,
                paper_size TEXT NOT NULL,
                pending_size TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (dex, coin)
            );
            CREATE TABLE IF NOT EXISTS pending_obligations (
                dex TEXT NOT NULL,
                coin TEXT NOT NULL,
                episode_id INTEGER NOT NULL,
                direction TEXT NOT NULL,
                pending_size TEXT NOT NULL,
                reference_notional TEXT,
                reference_price TEXT,
                status TEXT NOT NULL,
                first_fill_identity TEXT,
                last_fill_identity TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (dex, coin, episode_id)
            );
            CREATE TABLE IF NOT EXISTS paper_execution_events (
                execution_id TEXT PRIMARY KEY,
                dex TEXT NOT NULL,
                coin TEXT NOT NULL,
                episode_id INTEGER NOT NULL,
                amount TEXT NOT NULL,
                price TEXT NOT NULL,
                source TEXT NOT NULL,
                bbo_time INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS episode_sequence (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                next_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS copy_start (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                started_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                initial_capital TEXT NOT NULL,
                whale_capital TEXT NOT NULL,
                realized_pnl TEXT NOT NULL,
                equity TEXT NOT NULL,
                positions_json TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS checkpoint (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cursor_time INTEGER NOT NULL,
                cursor_tid TEXT,
                cursor_oid TEXT
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                dex TEXT PRIMARY KEY,
                cursor_time INTEGER NOT NULL,
                cursor_tid TEXT,
                cursor_oid TEXT
            );
            CREATE TABLE IF NOT EXISTS tracker_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                recovery_state TEXT NOT NULL,
                paper_state TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS clearinghouse_reconciliation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dex TEXT NOT NULL,
                coin TEXT NOT NULL,
                local_szi TEXT NOT NULL,
                exchange_szi TEXT NOT NULL,
                local_entry_px TEXT,
                exchange_entry_px TEXT,
                status TEXT NOT NULL,
                reason TEXT,
                observed_at INTEGER NOT NULL DEFAULT 0,
                resolved_at INTEGER,
                UNIQUE (dex, coin)
            );
            CREATE VIEW IF NOT EXISTS reconciliation AS
                SELECT id, dex, coin, local_szi, exchange_szi,
                       local_entry_px, exchange_entry_px, status, reason,
                       observed_at, resolved_at
                FROM clearinghouse_reconciliation;
            CREATE TABLE IF NOT EXISTS quarantined_fills (
                identity TEXT PRIMARY KEY,
                dex TEXT NOT NULL,
                coin TEXT,
                reason TEXT NOT NULL,
                expected_position TEXT,
                actual_position TEXT,
                payload TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            """
        )
        paper_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(paper_state)")
        }
        if "positions_json" not in paper_columns:
            self.connection.execute(
                "ALTER TABLE paper_state ADD COLUMN positions_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "status" not in paper_columns:
            self.connection.execute(
                "ALTER TABLE paper_state ADD COLUMN status TEXT NOT NULL "
                "DEFAULT 'PAPER_ENABLED'"
            )
        # Migrate the original single global cursor without losing it.
        if self.connection.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0] == 0:
            old = self.connection.execute(
                "SELECT cursor_time, cursor_tid, cursor_oid FROM checkpoint WHERE id = 1"
            ).fetchone()
            if old is not None:
                self.connection.execute(
                    "INSERT OR IGNORE INTO checkpoints "
                    "(dex, cursor_time, cursor_tid, cursor_oid) VALUES ('', ?, ?, ?)",
                    (old["cursor_time"], old["cursor_tid"], old["cursor_oid"]),
                )
        reconciliation_columns = {
            row["name"]
            for row in self.connection.execute(
                "PRAGMA table_info(clearinghouse_reconciliation)"
            )
        }
        if "local_szi" not in reconciliation_columns:
            self.connection.execute(
                "ALTER TABLE clearinghouse_reconciliation RENAME TO "
                "clearinghouse_reconciliation_legacy"
            )
            self.connection.execute(
                """
                CREATE TABLE clearinghouse_reconciliation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dex TEXT NOT NULL,
                    coin TEXT NOT NULL,
                    local_szi TEXT NOT NULL,
                    exchange_szi TEXT NOT NULL,
                    local_entry_px TEXT,
                    exchange_entry_px TEXT,
                    status TEXT NOT NULL,
                    reason TEXT,
                    observed_at INTEGER NOT NULL DEFAULT 0,
                    resolved_at INTEGER,
                    UNIQUE (dex, coin)
                )
                """
            )
            self.connection.execute(
                """
                INSERT INTO clearinghouse_reconciliation
                    (dex, coin, local_szi, exchange_szi, status,
                     reason, observed_at)
                SELECT dex, coin, expected_szi, actual_szi, status,
                       NULL, snapshot_time
                FROM clearinghouse_reconciliation_legacy
                """
            )
            self.connection.execute(
                "DROP TABLE clearinghouse_reconciliation_legacy"
            )
            self.connection.execute("DROP VIEW IF EXISTS reconciliation")
            self.connection.execute(
                """
                CREATE VIEW reconciliation AS
                    SELECT id, dex, coin, local_szi, exchange_szi,
                           local_entry_px, exchange_entry_px, status, reason,
                           observed_at, resolved_at
                    FROM clearinghouse_reconciliation
                """
            )
        self.connection.commit()

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    @staticmethod
    def _identity(fill, dex):
        return fill_identity(fill, dex)

    def has_fill(self, fill, dex):
        identity = self._identity(fill, dex)
        return self.connection.execute(
            "SELECT 1 FROM fills WHERE identity = ?", (identity,)
        ).fetchone() is not None

    def has_quarantined_fill(self, fill, dex):
        identity = self._identity(fill, dex)
        return self.connection.execute(
            "SELECT 1 FROM quarantined_fills WHERE identity = ?", (identity,)
        ).fetchone() is not None

    def record_fill(self, fill, dex, status):
        was_in_transaction = self.connection.in_transaction
        identity = self._identity(fill, dex)
        self.connection.execute(
            """
            INSERT INTO fills
                (identity, dex, coin, fill_time, tid, oid, status, payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(identity) DO NOTHING
            """,
            (
                identity,
                dex,
                fill.get("coin"),
                int(fill.get("time", 0) or 0),
                str(fill.get("tid")) if fill.get("tid") is not None else None,
                str(fill.get("oid")) if fill.get("oid") is not None else None,
                status,
                json.dumps(fill, sort_keys=True, separators=(",", ":"), default=str),
                int(time() * 1000),
            ),
        )
        self._commit_if_started(was_in_transaction)

    def quarantine_fill(
        self, fill, dex, reason, expected_position=None, actual_position=None
    ):
        was_in_transaction = self.connection.in_transaction
        identity = self._identity(fill, dex)
        self.connection.execute(
            """
            INSERT INTO quarantined_fills
                (identity, dex, coin, reason, expected_position, actual_position,
                 payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(identity) DO NOTHING
            """,
            (
                identity,
                dex,
                fill.get("coin"),
                reason,
                str(expected_position) if expected_position is not None else None,
                str(actual_position) if actual_position is not None else None,
                json.dumps(fill, sort_keys=True, separators=(",", ":"), default=str),
                int(time() * 1000),
            ),
        )
        self._commit_if_started(was_in_transaction)
        logger.warning(
            "Fill quarantined (%s:%s): %s",
            dex or "Hyperliquid",
            fill.get("coin", "?"),
            reason,
        )

    def load_positions(self):
        positions = {}
        rows = self.connection.execute(
            "SELECT dex, coin, szi, entry_px, leverage, margin_used FROM positions"
        )
        for row in rows:
            leverage = row["leverage"]
            if isinstance(leverage, str) and leverage.startswith(("{", "[")):
                try:
                    leverage = json.loads(leverage)
                except json.JSONDecodeError:
                    pass
            positions[(row["dex"], row["coin"])] = {
                "szi": Decimal(row["szi"]),
                "entryPx": row["entry_px"],
                "leverage": leverage,
                "marginUsed": row["margin_used"],
            }
        return positions

    def initialize_copy_start(self, positions):
        """Persist the explicit Copy-Start baseline without creating exposure."""
        was_in_transaction = self.connection.in_transaction
        now = int(time() * 1000)
        self.connection.execute(
            "INSERT OR IGNORE INTO copy_start (id, started_at) VALUES (1, ?)",
            (now,),
        )
        for (dex, coin), position in positions.items():
            self.connection.execute(
                """
                INSERT OR IGNORE INTO position_context
                    (dex, coin, baseline_size, eligible_size, active_episode_id,
                     episode_state, paper_size, pending_size, version, updated_at)
                VALUES (?, ?, ?, '0', NULL, 'BASELINE_ONLY', '0', '0', 0, ?)
                """,
                (dex, coin, str(position["szi"]), now),
            )
        self._commit_if_started(was_in_transaction)

    def has_copy_start(self):
        return self.connection.execute(
            "SELECT 1 FROM copy_start WHERE id = 1"
        ).fetchone() is not None

    def load_context(self, dex, coin):
        row = self.connection.execute(
            "SELECT * FROM position_context WHERE dex = ? AND coin = ?",
            (dex, coin),
        ).fetchone()
        if row is None:
            return None
        context = dict(row)
        for key in ("baseline_size", "eligible_size", "paper_size", "pending_size"):
            context[key] = Decimal(str(context[key]))
        self._verify_context(context)
        self.validate_context(dex, coin, context)
        return context

    def validate_context(self, dex, coin, context):
        episode_id = context["active_episode_id"]
        pending = (
            self.load_pending(dex, coin, episode_id)
            if episode_id is not None else None
        )
        pending_size = Decimal(str(context["pending_size"]))
        if pending is None:
            if pending_size != 0:
                raise ValueError(
                    f"context/pending mismatch for {dex}:{coin}: "
                    f"context={pending_size}, row=missing"
                )
            return
        row_size = Decimal(str(pending["pending_size"]))
        if row_size != pending_size:
            raise ValueError(
                f"context/pending mismatch for {dex}:{coin}: "
                f"context={pending_size}, row={row_size}"
            )
        if pending_size and pending_size * context["eligible_size"] <= 0:
            raise ValueError(f"pending sign mismatch for {dex}:{coin}")
        expected_direction = "LONG" if context["eligible_size"] > 0 else "SHORT"
        if pending["direction"] != expected_direction:
            raise ValueError(f"pending direction mismatch for {dex}:{coin}")

    def load_pending(self, dex, coin, episode_id):
        return self.connection.execute(
            "SELECT * FROM pending_obligations "
            "WHERE dex = ? AND coin = ? AND episode_id = ?",
            (dex, coin, episode_id),
        ).fetchone()

    def _next_episode_id(self):
        row = self.connection.execute(
            "SELECT next_id FROM episode_sequence WHERE id = 1"
        ).fetchone()
        if row is None:
            episode_id = 1
            self.connection.execute(
                "INSERT INTO episode_sequence (id, next_id) VALUES (1, 2)"
            )
        else:
            episode_id = row["next_id"]
            self.connection.execute(
                "UPDATE episode_sequence SET next_id = ? WHERE id = 1",
                (episode_id + 1,),
            )
        return episode_id

    def _save_context(self, dex, coin, context):
        self.connection.execute(
            """
            INSERT INTO position_context
                (dex, coin, baseline_size, eligible_size, active_episode_id,
                 episode_state, paper_size, pending_size, version, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dex, coin) DO UPDATE SET
                baseline_size = excluded.baseline_size,
                eligible_size = excluded.eligible_size,
                active_episode_id = excluded.active_episode_id,
                episode_state = excluded.episode_state,
                paper_size = excluded.paper_size,
                pending_size = excluded.pending_size,
                version = excluded.version,
                updated_at = excluded.updated_at
            """,
            (
                dex, coin, str(context["baseline_size"]),
                str(context["eligible_size"]), context["active_episode_id"],
                context["episode_state"], str(context["paper_size"]),
                str(context["pending_size"]), context["version"],
                int(time() * 1000),
            ),
        )

    def save_position(self, dex, coin, position):
        was_in_transaction = self.connection.in_transaction
        leverage = position.get("leverage")
        if isinstance(leverage, (dict, list)):
            leverage = json.dumps(
                leverage, sort_keys=True, separators=(",", ":")
            )
        self.connection.execute(
            """
            INSERT INTO positions
                (dex, coin, szi, entry_px, leverage, margin_used)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(dex, coin) DO UPDATE SET
                szi = excluded.szi,
                entry_px = excluded.entry_px,
                leverage = excluded.leverage,
                margin_used = excluded.margin_used
            """,
            (
                dex,
                coin,
                str(position["szi"]),
                position.get("entryPx"),
                leverage,
                position.get("marginUsed"),
            ),
        )
        self._commit_if_started(was_in_transaction)

    def replace_positions(self, positions):
        was_in_transaction = self.connection.in_transaction
        self.connection.execute("DELETE FROM positions")
        for (dex, coin), position in positions.items():
            self.save_position(dex, coin, position)
        self._commit_if_started(was_in_transaction)

    def load_paper_state(self):
        row = self.connection.execute(
            "SELECT initial_capital, whale_capital, realized_pnl, equity, positions_json, "
            "status "
            "FROM paper_state WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        state = {key: row[key] for key in row.keys()}
        state["positions"] = json.loads(state.pop("positions_json") or "[]")
        state.setdefault("status", PAPER_ENABLED)
        return state

    def save_paper_state(self, paper_trader):
        was_in_transaction = self.connection.in_transaction
        self.connection.execute(
            """
            INSERT INTO paper_state
                (id, initial_capital, whale_capital, realized_pnl, equity, positions_json, status)
            VALUES (1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                initial_capital = excluded.initial_capital,
                whale_capital = excluded.whale_capital,
                realized_pnl = excluded.realized_pnl,
                equity = excluded.equity,
                positions_json = excluded.positions_json,
                status = excluded.status
            """,
            (
                str(paper_trader.initial_capital),
                str(paper_trader.whale_capital),
                str(paper_trader.realized_pnl),
                str(paper_trader.equity),
                json.dumps(paper_trader.state()["positions"], separators=(",", ":")),
                getattr(paper_trader, "status", PAPER_ENABLED),
            ),
        )
        self._commit_if_started(was_in_transaction)

    def load_recovery_state(self):
        row = self.connection.execute(
            "SELECT recovery_state, paper_state FROM tracker_state WHERE id = 1"
        ).fetchone()
        if row is None:
            return {"recovery_state": RECOVERY_UNVERIFIED, "paper_state": PAPER_PAUSED}
        return dict(row)

    def save_recovery_state(self, recovery_state, paper_state=None):
        if recovery_state not in {
            RECOVERED, RECOVERY_UNVERIFIED, RECOVERY_GAP
        }:
            raise ValueError(f"invalid recovery state: {recovery_state}")
        was_in_transaction = self.connection.in_transaction
        if paper_state is None:
            paper_state = self.load_recovery_state()["paper_state"]
        if paper_state not in {PAPER_ENABLED, PAPER_PAUSED}:
            raise ValueError(f"invalid paper state: {paper_state}")
        self.connection.execute(
            """
            INSERT INTO tracker_state (id, recovery_state, paper_state)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                recovery_state = excluded.recovery_state,
                paper_state = excluded.paper_state
            """,
            (recovery_state, paper_state),
        )
        self._commit_if_started(was_in_transaction)

    def set_recovery_state(self, state):
        paper_state = PAPER_ENABLED if state == RECOVERED else PAPER_PAUSED
        self.save_recovery_state(state, paper_state)
        return state

    def load_checkpoint(self, dex=None):
        if dex is None:
            # Compatibility for callers of the old global cursor API.
            row = self.connection.execute(
                "SELECT cursor_time, cursor_tid, cursor_oid FROM checkpoints "
                "ORDER BY cursor_time DESC, cursor_tid DESC, cursor_oid DESC LIMIT 1"
            ).fetchone()
        else:
            row = self.connection.execute(
                "SELECT cursor_time, cursor_tid, cursor_oid FROM checkpoints WHERE dex = ?",
                (dex or "",),
            ).fetchone()
        if row is None:
            # Also support databases created by an older version before migration.
            row = self.connection.execute(
                "SELECT cursor_time, cursor_tid, cursor_oid FROM checkpoint WHERE id = 1"
            ).fetchone() if dex in (None, "") else None
        if row is None:
            return None
        return {
            "time": row["cursor_time"],
            "tid": row["cursor_tid"],
            "oid": row["cursor_oid"],
        }

    def save_checkpoint(self, fill, dex=None):
        dex = fill.get("dex", "") if dex is None else dex
        candidate = (
            int(fill.get("time", 0) or 0),
            str(fill.get("tid")) if fill.get("tid") is not None else "",
            str(fill.get("oid")) if fill.get("oid") is not None else "",
        )
        self.save_cursor(candidate[0], candidate[1], candidate[2], dex=dex)

    def save_cursor(self, cursor_time, cursor_tid=None, cursor_oid=None, dex=""):
        was_in_transaction = self.connection.in_transaction
        candidate = (
            int(cursor_time or 0),
            str(cursor_tid) if cursor_tid is not None else "",
            str(cursor_oid) if cursor_oid is not None else "",
        )
        def order_component(value):
            try:
                return (0, int(value))
            except (TypeError, ValueError):
                return (1, value or "")

        current = self.load_checkpoint(dex)
        if current is not None:
            existing = (
                int(current["time"]),
                current["tid"],
                current["oid"],
            )
            if (
                candidate[0],
                order_component(candidate[1]),
                order_component(candidate[2]),
            ) <= (
                existing[0],
                order_component(existing[1]),
                order_component(existing[2]),
            ):
                return
        self.connection.execute(
            """
            INSERT INTO checkpoints (dex, cursor_time, cursor_tid, cursor_oid)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(dex) DO UPDATE SET
                cursor_time = excluded.cursor_time,
                cursor_tid = excluded.cursor_tid,
                cursor_oid = excluded.cursor_oid
            """,
            (dex or "", candidate[0], candidate[1] or None, candidate[2] or None),
        )
        self._commit_if_started(was_in_transaction)

    def reconcile_clearinghouse(self, expected_positions, actual_positions,
                                snapshot_time=0):
        """Persist an independent exchange-vs-journal position comparison."""
        keys = set(expected_positions) | set(actual_positions)
        rows = []
        matched = True
        for dex, coin in sorted(keys, key=lambda key: (key[0], key[1])):
            local = expected_positions.get((dex, coin), {})
            exchange = actual_positions.get((dex, coin), {})
            expected = local.get("szi", Decimal("0"))
            actual = exchange.get("szi", Decimal("0"))
            expected, actual = Decimal(str(expected)), Decimal(str(actual))
            local_entry = local.get("entryPx")
            exchange_entry = exchange.get("entryPx")
            entry_matches = True
            if local_entry is not None and exchange_entry is not None:
                try:
                    entry_matches = Decimal(str(local_entry)) == Decimal(
                        str(exchange_entry)
                    )
                except (InvalidOperation, ValueError):
                    entry_matches = False
            status = "RECONCILED" if expected == actual and entry_matches else "MISMATCH"
            matched = matched and status == "RECONCILED"
            reason = None if status == "RECONCILED" else (
                "position size mismatch" if expected != actual
                else "entry price mismatch"
            )
            rows.append((
                dex or "", coin, str(expected), str(actual),
                str(local_entry) if local_entry is not None else None,
                str(exchange_entry) if exchange_entry is not None else None,
                status, reason, int(snapshot_time or 0), None,
            ))
        if not rows:
            # An empty account is still a verified clearinghouse snapshot.
            rows.append((
                "", "__EMPTY__", "0", "0", None, None, "RECONCILED",
                None, int(snapshot_time or 0), None,
            ))
        was_in_transaction = self.connection.in_transaction
        self.connection.execute("DELETE FROM clearinghouse_reconciliation")
        self.connection.executemany(
            """
            INSERT INTO clearinghouse_reconciliation
                (dex, coin, local_szi, exchange_szi, local_entry_px,
                 exchange_entry_px, status, reason, observed_at, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self._commit_if_started(was_in_transaction)
        return matched

    # More explicit alias for integrations and older callers.
    reconcile_positions = reconcile_clearinghouse

    def reconciliation_is_safe(self):
        row = self.connection.execute(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN status = 'RECONCILED' THEN 1 ELSE 0 END) AS matches "
            "FROM clearinghouse_reconciliation"
        ).fetchone()
        return row["total"] > 0 and row["total"] == row["matches"]

    def paper_safety_gate(self):
        state = self.load_recovery_state()
        return (
            state["recovery_state"] == RECOVERED
            and state["paper_state"] == PAPER_ENABLED
            and self.reconciliation_is_safe()
        )

    def replay_pending_fills(self, paper_trader, bbo_by_market=None, now_ms=None):
        """Execute persisted obligations without replaying their source fills.

        ``bbo_by_market`` is keyed by ``(dex, coin)`` and contains the BBO
        ``data`` object.  Missing or invalid BBO data is deliberately a
        no-op; recovery must preserve the obligation rather than invent a
        price.
        """
        if not self.paper_safety_gate() or not paper_trader.enabled:
            return 0
        now_ms = int(time() * 1000) if now_ms is None else int(now_ms)
        rows = self.connection.execute(
            "SELECT dex, coin, episode_id, pending_size FROM pending_obligations "
            "WHERE status = 'ACTIVE' AND pending_size <> '0' "
            "ORDER BY dex, coin, episode_id"
        ).fetchall()
        executed_count = 0
        with self.transaction():
            for row in rows:
                market = (row["dex"], row["coin"])
                bbo = (bbo_by_market or {}).get(market)
                execution = self._validated_bbo_execution(
                    bbo, Decimal(str(row["pending_size"])), now_ms
                )
                if execution is None:
                    continue
                amount, price, bbo_time = execution
                pending_before = Decimal(str(row["pending_size"]))
                execution_id = (
                    f"recovery:{row['dex']}:{row['coin']}:"
                    f"{row['episode_id']}:{pending_before}"
                )
                inserted = self.connection.execute(
                    """
                    INSERT OR IGNORE INTO paper_execution_events
                        (execution_id, dex, coin, episode_id, amount, price,
                         source, bbo_time, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'synthetic_bbo', ?, ?)
                    """,
                    (
                        execution_id, row["dex"], row["coin"], row["episode_id"],
                        str(amount), str(price), bbo_time, now_ms,
                    ),
                ).rowcount
                if not inserted:
                    continue
                context = self.load_context(row["dex"], row["coin"])
                if context is None or context["active_episode_id"] != row["episode_id"]:
                    raise ValueError(
                        f"pending/context mismatch for {row['dex']}:{row['coin']}"
                    )
                self._validate_paper_context(
                    row["dex"], row["coin"], context, paper_trader
                )
                signed_amount = (
                    amount if pending_before > 0 else -amount
                )
                paper_trader.apply_exposure_delta(
                    row["dex"], row["coin"], signed_amount, price
                )
                context["paper_size"] += signed_amount
                context["pending_size"] -= signed_amount
                self._update_pending_after_execution(
                    row["dex"], row["coin"], row["episode_id"],
                    context["pending_size"], now_ms,
                )
                context["episode_state"] = (
                    "ACTIVE_PAPER"
                    if context["pending_size"] == 0
                    else "ACTIVE_PAPER_PENDING"
                )
                context["version"] += 1
                self._save_context(row["dex"], row["coin"], context)
                self._verify_context(context)
                self.validate_context(row["dex"], row["coin"], context)
                self._validate_paper_context(
                    row["dex"], row["coin"], context, paper_trader
                )
                executed_count += 1
            self.save_paper_state(paper_trader)
        return executed_count

    def _update_pending_after_execution(
        self, dex, coin, episode_id, pending_size, now_ms
    ):
        row = self.load_pending(dex, coin, episode_id)
        if row is None:
            raise ValueError(f"missing pending obligation for {dex}:{coin}")
        old_size = Decimal(str(row["pending_size"]))
        if old_size == 0 or pending_size == 0:
            notional, reference_price = Decimal("0"), None
            status = "EXECUTED" if pending_size == 0 else "ACTIVE"
        else:
            old_notional = Decimal(row["reference_notional"] or "0")
            old_price = Decimal(row["reference_price"] or "0")
            reduced = abs(old_size) - abs(pending_size)
            notional = max(Decimal("0"), old_notional - reduced * old_price)
            reference_price = notional / abs(pending_size)
            status = "ACTIVE"
        self.connection.execute(
            "UPDATE pending_obligations SET pending_size=?, "
            "reference_notional=?, reference_price=?, status=?, updated_at=? "
            "WHERE dex=? AND coin=? AND episode_id=?",
            (
                str(pending_size), str(notional),
                str(reference_price) if reference_price is not None else None,
                status, now_ms, dex, coin, episode_id,
            ),
        )

    @staticmethod
    def _validated_bbo_execution(bbo, pending_size, now_ms):
        if not isinstance(bbo, dict):
            return None
        try:
            bbo_time = int(bbo["time"])
            levels = bbo["bbo"]
            if now_ms - bbo_time > BBO_MAX_AGE_MS or now_ms < bbo_time:
                return None
            if not isinstance(levels, (list, tuple)) or len(levels) != 2:
                return None
            level = levels[1] if pending_size > 0 else levels[0]
            if not isinstance(level, dict):
                return None
            price = Decimal(str(level["px"]))
            size = Decimal(str(level["sz"]))
            if price <= 0 or size <= 0:
                return None
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return None
        checker = ExecutionChecker()
        amount = checker.executable_amount(pending_size, size)
        if amount <= 0:
            return None
        return amount, price, bbo_time

    @staticmethod
    def _validate_paper_context(dex, coin, context, paper_trader):
        paper = paper_trader.positions.get((dex, coin))
        actual = Decimal("0") if paper is None else Decimal(str(paper["szi"]))
        expected = Decimal(str(context["paper_size"])) * paper_trader.scale
        if actual != expected:
            raise ValueError(f"context/Paper mismatch for {dex}:{coin}")

    def process_fill(
        self,
        fill,
        dex,
        positions,
        recent_fills,
        allow_already_applied=True,
        paper_trader=None,
    ):
        """Process one fill and journal every state change in one transaction."""
        if not positions:
            positions.update(self.load_positions())
        positions_before = copy.deepcopy(positions)
        paper_before = paper_trader.state() if paper_trader is not None else None
        recent_before = list(recent_fills)
        try:
            with self.transaction():
                if self.has_fill(fill, dex) or self.has_quarantined_fill(fill, dex):
                    remember_fill(recent_fills, fill, dex)
                    return
                paper_enabled = (
                    paper_trader is not None
                    and getattr(paper_trader, "status", PAPER_ENABLED) == PAPER_ENABLED
                )
                coin = fill.get("coin")
                if coin:
                    context = self.load_context(dex, coin)
                    if context is not None:
                        self.validate_context(dex, coin, context)
                result = process_fill(
                    fill, dex, positions, recent_fills,
                    allow_already_applied=allow_already_applied,
                    paper_trader=None,
                )
                if result in {"applied", "already_applied", "unchanged"}:
                    if result == "applied" and coin:
                        self.save_position(dex, coin, positions[(dex, coin)])
                        if result == "applied":
                            self._apply_obligation_transition(
                                fill, dex, coin, paper_trader if paper_enabled else None
                            )
                    if paper_trader is not None:
                        self.save_paper_state(paper_trader)
                    self.record_fill(
                        fill, dex,
                        "paper_paused" if (
                            paper_trader is not None and not paper_enabled
                        ) else result,
                    )
                    self.save_checkpoint(fill, dex=dex)
                elif result == "mismatch":
                    try:
                        expected = Decimal(str(fill["startPosition"]))
                        actual = positions.get(
                            (dex, fill.get("coin")), {"szi": Decimal("0")}
                        )["szi"]
                    except (KeyError, InvalidOperation, ValueError):
                        expected = actual = None
                    self.quarantine_fill(
                        fill,
                        dex,
                        "position start does not match persisted state",
                        expected,
                        actual,
                    )
                return result
        except Exception:
            positions.clear()
            positions.update(positions_before)
            recent_fills.clear()
            recent_fills.extend(recent_before)
            if paper_trader is not None and paper_before is not None:
                restored = PaperTradingEngine.from_state(paper_before)
                paper_trader.initial_capital = restored.initial_capital
                paper_trader.whale_capital = restored.whale_capital
                paper_trader.scale = restored.scale
                paper_trader.positions = restored.positions
                paper_trader.realized_pnl = restored.realized_pnl
                paper_trader.equity = restored.equity
                paper_trader.status = restored.status
            raise

    def _apply_obligation_transition(self, fill, dex, coin, paper_trader):
        """Update eligible/pending/Paper accounting inside the fill transaction."""
        post = self.connection.execute(
            "SELECT szi FROM positions WHERE dex = ? AND coin = ?",
            (dex, coin),
        ).fetchone()
        whale_post = Decimal(post["szi"])
        start = Decimal(str(fill["startPosition"]))
        context = self.load_context(dex, coin)
        if context is None:
            context = {
                "baseline_size": (
                    Decimal("0") if self.has_copy_start() else start
                ),
                "eligible_size": Decimal("0"),
                "active_episode_id": None, "episode_state": "BASELINE_ONLY",
                "paper_size": Decimal("0"), "pending_size": Decimal("0"),
                "version": 0,
            }
        else:
            for key in ("baseline_size", "eligible_size", "paper_size", "pending_size"):
                context[key] = Decimal(str(context[key]))
        eligible_pre = context["eligible_size"]
        eligible_post = eligible_exposure(context["baseline_size"], whale_post)
        fill_identity_value = self._identity(fill, dex)
        px = Decimal(str(fill["px"]))
        paper_enabled = paper_trader is not None and paper_trader.enabled

        if (
            eligible_post != 0
            and eligible_pre * eligible_post > 0
            and abs(eligible_post) < abs(context["paper_size"])
            and not paper_enabled
        ):
            raise ValueError(
                f"Paper reduction deferred while paused for {dex}:{coin}"
            )
        if eligible_post == 0 and context["paper_size"] and not paper_enabled:
            raise ValueError(f"Paper closure deferred while paused for {dex}:{coin}")

        def update_pending_metadata(episode_id, pending_size, fill_identity_value):
            row = self.load_pending(dex, coin, episode_id)
            if row is None:
                if pending_size:
                    raise ValueError(f"missing pending obligation for {dex}:{coin}")
                return
            old_size = Decimal(str(row["pending_size"]))
            old_notional = Decimal(row["reference_notional"] or "0")
            old_price = Decimal(row["reference_price"]) if row["reference_price"] else None
            if pending_size == 0:
                notional, reference_price = Decimal("0"), None
            else:
                change = abs(old_size) - abs(pending_size)
                notional = max(Decimal("0"), old_notional - change * (old_price or px))
                reference_price = notional / abs(pending_size)
            self.connection.execute(
                "UPDATE pending_obligations SET pending_size=?, "
                "reference_notional=?, reference_price=?, status=?, "
                "last_fill_identity=?, updated_at=? "
                "WHERE dex=? AND coin=? AND episode_id=?",
                (
                    str(pending_size), str(notional),
                    str(reference_price) if reference_price is not None else None,
                    "OFFSET" if pending_size == 0 else "ACTIVE",
                    fill_identity_value, int(time() * 1000),
                    dex, coin, episode_id,
                ),
            )

        def close_old():
            old_episode = context["active_episode_id"]
            if context["pending_size"]:
                context["pending_size"] = Decimal("0")
            if context["paper_size"]:
                amount = -context["paper_size"]
                paper_trader.apply_exposure_delta(dex, coin, amount, px)
                context["paper_size"] = Decimal("0")
            context["active_episode_id"] = None
            context["episode_state"] = "CLOSED"
            if old_episode is not None:
                update_pending_metadata(
                    old_episode, Decimal("0"), fill_identity_value
                )

        if eligible_pre and eligible_post and eligible_pre * eligible_post < 0:
            close_old()
            eligible_pre = Decimal("0")
        if eligible_post == 0:
            close_old()
        else:
            if eligible_pre == 0:
                context["active_episode_id"] = self._next_episode_id()
                context["pending_size"] = Decimal("0")
                context["paper_size"] = Decimal("0")
                context["episode_state"] = (
                    "ACTIVE_LONG" if eligible_post > 0 else "ACTIVE_SHORT"
                )
            episode_id = context["active_episode_id"]
            delta = eligible_post - eligible_pre
            pending = context["pending_size"]
            if delta * eligible_post > 0:
                pending += delta
                reference_delta = abs(delta) * px
                row = self.load_pending(dex, coin, episode_id)
                if row is None:
                    reference_notional = reference_delta
                    reference_price = px
                    first_identity = fill_identity_value
                else:
                    reference_notional = Decimal(row["reference_notional"] or "0") + reference_delta
                    reference_price = (
                        (Decimal(row["reference_notional"] or "0") * Decimal(row["reference_price"] or px)
                         + reference_delta * px) / reference_notional
                        if reference_notional else px
                    )
                    first_identity = row["first_fill_identity"]
                context["pending_size"] = pending
                self.connection.execute(
                    """
                    INSERT INTO pending_obligations
                        (dex, coin, episode_id, direction, pending_size,
                         reference_notional, reference_price, status,
                         first_fill_identity, last_fill_identity, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?)
                    ON CONFLICT(dex, coin, episode_id) DO UPDATE SET
                        pending_size=excluded.pending_size,
                        reference_notional=excluded.reference_notional,
                        reference_price=excluded.reference_price,
                        status='ACTIVE',
                        last_fill_identity=excluded.last_fill_identity,
                        updated_at=excluded.updated_at
                    """,
                    (dex, coin, episode_id, "LONG" if pending > 0 else "SHORT",
                     str(pending), str(reference_notional), str(reference_price),
                     first_identity, fill_identity_value, int(time() * 1000),
                     int(time() * 1000)),
                )
            elif delta * eligible_post < 0:
                reduction = abs(delta)
                paper_abs = abs(context["paper_size"])
                required_close = max(Decimal("0"), paper_abs - abs(eligible_post))
                paper_close = min(paper_abs, max(required_close, reduction - abs(pending)))
                pending_cancel = reduction - paper_close
                if pending_cancel > abs(pending):
                    pending_cancel = abs(pending)
                    paper_close = reduction - pending_cancel
                sign = Decimal("1") if context["pending_size"] >= 0 else Decimal("-1")
                context["pending_size"] -= sign * pending_cancel
                if context["active_episode_id"] is not None:
                    update_pending_metadata(
                        context["active_episode_id"],
                        context["pending_size"],
                        fill_identity_value,
                    )
                if paper_close:
                    paper_delta = -(
                        Decimal("1") if context["paper_size"] > 0 else Decimal("-1")
                    ) * paper_close
                    if paper_trader is not None:
                        paper_trader.apply_exposure_delta(dex, coin, paper_delta, px)
                    context["paper_size"] += paper_delta
            checker = getattr(paper_trader, "execution_checker", ExecutionChecker())
            executable = checker.executable_amount(context["pending_size"])
            if paper_trader is not None and paper_trader.enabled and executable:
                sign = Decimal("1") if context["pending_size"] > 0 else Decimal("-1")
                executed = sign * min(executable, abs(context["pending_size"]))
                paper_trader.apply_exposure_delta(dex, coin, executed, px)
                context["paper_size"] += executed
                context["pending_size"] -= executed
                update_pending_metadata(
                    episode_id, context["pending_size"], fill_identity_value
                )
                if context["pending_size"] == 0:
                    self.connection.execute(
                        "UPDATE pending_obligations SET status='EXECUTED' "
                        "WHERE dex=? AND coin=? AND episode_id=?",
                        (dex, coin, episode_id),
                    )
            if context["pending_size"] == 0:
                context["episode_state"] = "ACTIVE_PAPER"
            elif context["paper_size"]:
                context["episode_state"] = "ACTIVE_PAPER_PENDING"
        context["eligible_size"] = eligible_post
        context["version"] += 1
        if context["active_episode_id"] is None:
            context["episode_state"] = "CLOSED" if eligible_post == 0 else context["episode_state"]
        self._save_context(dex, coin, context)
        self._verify_context(context)

    @staticmethod
    def _verify_context(context):
        eligible = Decimal(str(context["eligible_size"]))
        paper = Decimal(str(context["paper_size"]))
        pending = Decimal(str(context["pending_size"]))
        if eligible != paper + pending:
            raise ValueError("eligible exposure invariant violated")
        if eligible and (paper and paper * eligible < 0 or pending and pending * eligible < 0):
            raise ValueError("exposure direction invariant violated")
        if abs(paper) > abs(eligible) or abs(pending) > abs(eligible):
            raise ValueError("exposure bound invariant violated")

# Descriptive aliases keep the persistence API easy to discover for callers.
SQLiteStore = TrackerDatabase
PersistenceStore = TrackerDatabase


def info_request(payload):
    response = requests.post(
        INFO_URL,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def discover_dexes():
    dexes = [""]
    for row in info_request({"type": "perpDexs"}):
        if row and row.get("name"):
            dexes.append(row["name"])
    return dexes


def discover_market_dexes(dexes):
    market_dexes = {}
    for dex in dexes:
        result = info_request({"type": "meta", "dex": dex})
        for market in result.get("universe", []):
            coin = market.get("name")
            if coin:
                market_dexes.setdefault(coin, set()).add(dex)
    return market_dexes


def load_position_snapshot(dexes):
    positions = {}
    snapshot_time = 0
    for dex in dexes:
        payload = {"type": "clearinghouseState", "user": WALLET}
        if dex:
            payload["dex"] = dex

        result = info_request(payload)
        snapshot_time = max(snapshot_time, int(result.get("time", 0)))
        for item in result.get("assetPositions", []):
            position = item.get("position", {})
            coin = position.get("coin")
            szi = position.get("szi")
            if not coin or szi is None:
                continue

            try:
                size = Decimal(str(szi))
            except (InvalidOperation, ValueError):
                print(f"SNAPSHOT ERROR: invalid position size for {dex}:{coin}")
                continue

            positions[(dex, coin)] = {
                "szi": size,
                "entryPx": position.get("entryPx"),
                "leverage": position.get("leverage"),
                "marginUsed": position.get("marginUsed"),
            }

    return positions, snapshot_time


class ReconciliationResult(dict):
    def __bool__(self):
        return bool(self.get("matched"))


def reconcile_clearinghouse(expected_positions, actual_positions, db=None,
                            snapshot_time=0):
    """Compare journal positions with an exchange clearinghouse snapshot."""
    keys = set(expected_positions) | set(actual_positions)
    mismatches = []
    for key in sorted(keys, key=lambda item: (item[0], item[1])):
        local = expected_positions.get(key, {})
        exchange = actual_positions.get(key, {})
        expected = Decimal(str(local.get("szi", Decimal("0"))))
        actual = Decimal(str(exchange.get("szi", Decimal("0"))))
        entry_mismatch = False
        local_entry = local.get("entryPx")
        exchange_entry = exchange.get("entryPx")
        if local_entry is not None and exchange_entry is not None:
            try:
                entry_mismatch = Decimal(str(local_entry)) != Decimal(
                    str(exchange_entry)
                )
            except (InvalidOperation, ValueError):
                entry_mismatch = True
        if expected != actual or entry_mismatch:
            mismatches.append((key, expected, actual))
    matched = not mismatches
    if db is not None:
        db.reconcile_clearinghouse(
            expected_positions, actual_positions, snapshot_time
        )
    return ReconciliationResult({
        "matched": matched,
        "mismatches": mismatches,
        "checked": len(keys),
    })


# Keep a concise name available to callers that use the noun from the API.
reconcile_positions = reconcile_clearinghouse


def paper_safety_gate(recovery_state, reconciliation):
    """Return whether paper fills may be applied after recovery."""
    if isinstance(reconciliation, dict):
        matched = bool(reconciliation.get("matched"))
    else:
        matched = bool(reconciliation)
    return recovery_state == RECOVERED and matched


def fill_id(fill, dex):
    # tid is only globally unique together with block time and coin.
    return (
        dex,
        fill.get("coin"),
        fill.get("time"),
        fill.get("tid"),
        fill.get("oid"),
        fill.get("px"),
        fill.get("sz"),
        fill.get("side"),
    )


def fill_identity(fill, dex):
    """Return a stable, serializable identity scoped to a perp DEX."""
    return json.dumps(fill_id(fill, dex), separators=(",", ":"), default=str)


def remember_fill(recent_fills, fill, dex):
    identifier = fill_id(fill, dex)
    if identifier in recent_fills:
        return False
    recent_fills.append(identifier)
    return True


def _numeric_or_text(value):
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, str(value or ""))


def fill_sort_key(fill):
    return (
        _numeric_or_text(fill.get("time", 0)),
        _numeric_or_text(fill.get("tid", "")),
        _numeric_or_text(fill.get("oid", "")),
        fill_identity(fill, fill.get("dex", "")),
    )


def backfill_fills(start_time, end_time, dex=None, return_metadata=False):
    """Fetch pages with an inclusive timestamp overlap and stable ordering.

    The API has no cursor for ties, so pages are de-duplicated by full fill
    identity.  A repeated full page is reported as an unverified gap rather
    than silently skipping fills or looping forever.
    """
    fills = []
    page_start = start_time
    seen = set()
    complete = True
    pages = 0
    while page_start <= end_time:
        payload = {
            "type": "userFillsByTime",
            "user": WALLET,
            "startTime": page_start,
            "endTime": end_time,
            "aggregateByTime": False,
        }
        if dex:
            payload["dex"] = dex
        page = info_request(
            payload
        )
        if not isinstance(page, list):
            raise ValueError(f"unexpected userFillsByTime response: {page!r}")
        pages += 1
        page = sorted(
            (fill for fill in page if isinstance(fill, dict)),
            key=fill_sort_key,
        )
        new_count = 0
        for fill in page:
            identity = fill_identity(fill, dex or fill.get("dex", ""))
            if identity not in seen:
                seen.add(identity)
                fills.append(fill)
                new_count += 1
        if len(page) < 2000:
            break
        last_time = int(page[-1].get("time", page_start) or page_start)
        if last_time < page_start:
            raise ValueError("userFillsByTime returned non-advancing page")
        if new_count == 0:
            complete = False
            break
        # Keep the timestamp inclusive: fills sharing the boundary timestamp
        # must be seen on the next request and removed by identity de-duping.
        page_start = last_time
    fills.sort(key=fill_sort_key)
    if return_metadata:
        return fills, {"complete": complete, "pages": pages}
    return fills


def classify_event(pre, post):
    if pre == 0 and post != 0:
        return "OPEN"
    if pre != 0 and post == 0:
        return "CLOSE"
    if pre * post < 0:
        return "REVERSE"
    if abs(post) > abs(pre):
        return "ADD"
    if abs(post) < abs(pre):
        return "REDUCE"
    return "UNCHANGED"


def fill_delta(fill):
    try:
        size = Decimal(str(fill["sz"]))
    except (KeyError, InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid fill size: {error}") from error

    side = fill.get("side")
    if side == "B":
        return size
    if side == "A":
        return -size
    raise ValueError(f"invalid fill side: {side!r}")


def format_decimal(value):
    return format(value, "f")


def eligible_exposure(baseline, whale_position):
    """Return exposure outside the Copy-Start baseline deadband."""
    baseline = Decimal(str(baseline))
    whale_position = Decimal(str(whale_position))
    if baseline > 0:
        return max(Decimal("0"), whale_position - baseline) if whale_position > 0 else whale_position
    if baseline < 0:
        return min(Decimal("0"), whale_position - baseline) if whale_position < 0 else whale_position
    return whale_position


class ExecutionChecker:
    """Calculates the amount of a pending obligation that may be executed."""

    def __init__(self, minimum_amount=Decimal("0"), maximum_amount=None):
        self.minimum_amount = Decimal(str(minimum_amount))
        self.maximum_amount = (
            None if maximum_amount is None else Decimal(str(maximum_amount))
        )

    def executable_amount(self, pending_size, available_size=None):
        amount = abs(Decimal(str(pending_size)))
        if amount < self.minimum_amount:
            return Decimal("0")
        if self.maximum_amount is not None:
            amount = min(amount, self.maximum_amount)
        if available_size is not None:
            amount = min(amount, max(Decimal("0"), Decimal(str(available_size))))
        return amount


class PaperTradingEngine:
    def __init__(self, initial_capital=PAPER_INITIAL_CAPITAL,
                 whale_capital=PAPER_WHALE_CAPITAL, state=None,
                 execution_checker=None):
        if state is not None:
            initial_capital = state["initial_capital"]
            whale_capital = state["whale_capital"]
        initial_capital = Decimal(initial_capital)
        whale_capital = Decimal(whale_capital)
        if initial_capital <= 0 or whale_capital <= 0:
            raise ValueError("paper and whale capital must be positive")
        self.initial_capital = initial_capital
        self.whale_capital = whale_capital
        self.scale = self.initial_capital / self.whale_capital
        self.positions = {}
        self.realized_pnl = Decimal("0")
        self.equity = self.initial_capital
        self.status = PAPER_ENABLED
        self.execution_checker = execution_checker or ExecutionChecker()
        if state is not None:
            self.realized_pnl = Decimal(state["realized_pnl"])
            self.equity = Decimal(state["equity"])
            self.status = state.get("status", PAPER_ENABLED)
            for item in state.get("positions", []):
                self.positions[(item["dex"], item["coin"])] = {
                    "szi": Decimal(item["szi"]),
                    "entryPx": (
                        Decimal(item["entryPx"])
                        if item.get("entryPx") is not None
                        else None
                    ),
                }

    @classmethod
    def from_state(cls, state):
        return cls(state=state)

    @property
    def enabled(self):
        return self.status == PAPER_ENABLED

    def set_status(self, status):
        if status not in {PAPER_ENABLED, PAPER_PAUSED}:
            raise ValueError(f"invalid paper state: {status}")
        self.status = status

    def state(self):
        return {
            "initial_capital": str(self.initial_capital),
            "whale_capital": str(self.whale_capital),
            "realized_pnl": str(self.realized_pnl),
            "equity": str(self.equity),
            "status": self.status,
            "positions": [
                {
                    "dex": dex,
                    "coin": coin,
                    "szi": str(position["szi"]),
                    "entryPx": (
                        str(position["entryPx"])
                        if position.get("entryPx") is not None
                        else None
                    ),
                }
                for (dex, coin), position in self.positions.items()
            ],
        }

    def _update_equity(self):
        self.equity = self.initial_capital + self.realized_pnl

    def _realize(self, old_size, close_size, entry_px, exit_px):
        if entry_px is None:
            return
        if old_size > 0:
            self.realized_pnl += (exit_px - entry_px) * close_size
        else:
            self.realized_pnl += (entry_px - exit_px) * close_size

    def apply_exposure_delta(self, dex, coin, delta, price):
        """Apply an actual signed paper execution, scaled to paper capital."""
        if not self.enabled or not delta:
            return False
        paper = self.positions.setdefault(
            (dex, coin), {"szi": Decimal("0"), "entryPx": None}
        )
        delta = Decimal(str(delta)) * self.scale
        price = Decimal(str(price))
        old_size = paper["szi"]
        if old_size and old_size * delta < 0:
            close_size = min(abs(old_size), abs(delta))
            self._realize(old_size, close_size, paper["entryPx"], price)
        new_size = old_size + delta
        if old_size == 0 or old_size * delta < 0 and abs(delta) > abs(old_size):
            paper["entryPx"] = price if new_size else None
        elif old_size * delta > 0:
            old_abs = abs(old_size)
            add_abs = abs(delta)
            paper["entryPx"] = (
                (paper["entryPx"] or price) * old_abs + price * add_abs
            ) / (old_abs + add_abs)
        elif not new_size:
            paper["entryPx"] = None
        paper["szi"] = new_size
        self._update_equity()
        return True

    def apply_event(self, event, dex, coin, fill, whale_pre, whale_post):
        """Legacy direct Paper API; production fills use the obligation state machine."""
        if not self.enabled:
            return False
        key = (dex, coin)
        paper = self.positions.setdefault(
            key,
            {"szi": Decimal("0"), "entryPx": None},
        )
        price = Decimal(str(fill["px"]))
        whale_delta = whale_post - whale_pre
        paper_delta = whale_delta * self.scale
        old_size = paper["szi"]
        if old_size == 0 and event in {"REDUCE", "CLOSE"}:
            print(
                f"PAPER: {event} {dex or 'Hyperliquid'}:{coin} "
                "skipped because paper tracking starts without a "
                "historical position"
            )
            return
        if old_size == 0 and event == "REVERSE":
            paper_delta = whale_post * self.scale
        new_size = old_size + paper_delta

        if old_size != 0 and paper_delta != 0 and old_size * paper_delta < 0:
            close_size = min(abs(old_size), abs(paper_delta))
            self._realize(old_size, close_size, paper["entryPx"], price)

        if event in {"OPEN", "REVERSE"}:
            paper["entryPx"] = price
        elif event == "ADD":
            old_abs = abs(old_size)
            add_abs = abs(paper_delta)
            if old_abs + add_abs:
                old_entry = paper["entryPx"] or price
                paper["entryPx"] = (
                    old_entry * old_abs + price * add_abs
                ) / (old_abs + add_abs)
        elif event == "CLOSE":
            paper["entryPx"] = None

        paper["szi"] = new_size
        self._update_equity()
        print(
            f"PAPER: {event} {dex or 'Hyperliquid'}:{coin} "
            f"size={format_decimal(new_size)} "
            f"entry={paper['entryPx'] or '-'} "
            f"realized_pnl={format_decimal(self.realized_pnl)} "
            f"equity={format_decimal(self.equity)}"
        )
        return True


def print_position_event(event, dex, coin, fill, pre, post):
    try:
        value = Decimal(str(fill["px"])) * Decimal(str(fill["sz"]))
        value_text = f"${value:,.2f}"
    except (KeyError, InvalidOperation, ValueError):
        value_text = "?"

    print()
    print("=" * 70)
    print(f"EVENT:         {event}")
    print(f"DEX:           {dex or 'Hyperliquid'}")
    print(f"COIN:          {coin}")
    print(f"SIDE:          {fill.get('side', '?')}")
    print(f"SIZE:          {fill.get('sz', '?')}")
    print(f"PRICE:         {fill.get('px', '?')} ({value_text})")
    print(f"PRE POSITION:  {format_decimal(pre)}")
    print(f"POST POSITION: {format_decimal(post)}")
    print(f"TIME:          {fill.get('time', '?')}")
    print(f"TID:           {fill.get('tid', '?')}")
    print("=" * 70)


def report_mismatch(dex, coin, expected, actual, fill):
    print()
    print("POSITION MISMATCH")
    print(f"DEX:           {dex or 'Hyperliquid'}")
    print(f"COIN:          {coin}")
    print(f"STATE POSITION: {format_decimal(actual)}")
    print(f"FILL START:    {format_decimal(expected)}")
    print(f"TID:           {fill.get('tid', '?')}")
    print("Fill was not applied.")


def report_reconciliation(dex, coin, expected, actual, fill):
    print(
        f"RECONCILIATION: {dex or 'Hyperliquid'}:{coin} already reflects "
        f"tid {fill.get('tid', '?')} ({format_decimal(expected)} -> "
        f"{format_decimal(actual)})."
    )


def resolve_fill_dex(fill, positions, market_dexes=None):
    dex = fill.get("dex")
    if dex:
        return dex

    if market_dexes is not None:
        matches = market_dexes.get(fill.get("coin"), set())
        if len(matches) == 1:
            return next(iter(matches))

    matching_dexes = {
        position_dex
        for position_dex, position_coin in positions
        if position_coin == fill.get("coin")
    }
    if len(matching_dexes) == 1:
        return matching_dexes.pop()
    return ""


def process_fill(
    fill,
    dex,
    positions,
    recent_fills,
    allow_already_applied=True,
    paper_trader=None,
    db=None,
):
    if db is not None:
        return db.process_fill(
            fill,
            dex,
            positions,
            recent_fills,
            allow_already_applied=allow_already_applied,
            paper_trader=paper_trader,
        )
    if not remember_fill(recent_fills, fill, dex):
        return

    coin = fill.get("coin")
    if not coin:
        print("FILL ERROR: missing coin")
        return

    try:
        start_position = Decimal(str(fill["startPosition"]))
        delta = fill_delta(fill)
    except (KeyError, ValueError) as error:
        print(f"FILL ERROR: {error}")
        return

    key = (dex, coin)
    current = positions.get(key, {"szi": Decimal("0")})["szi"]
    if current != start_position:
        try:
            expected_post = start_position + delta
        except (InvalidOperation, ValueError):
            expected_post = None
        if allow_already_applied and expected_post is not None and current == expected_post:
            report_reconciliation(dex, coin, start_position, current, fill)
            return "already_applied"
        report_mismatch(dex, coin, start_position, current, fill)
        return "mismatch"

    post_position = start_position + delta
    event = classify_event(start_position, post_position)
    if event == "UNCHANGED":
        print(f"FILL NOTICE: no position change for {dex}:{coin}")
        return "unchanged"

    positions[key] = {
        **positions.get(key, {}),
        "szi": post_position,
        "entryPx": fill.get("px"),
    }
    print_position_event(
        event,
        dex,
        coin,
        fill,
        start_position,
        post_position,
    )
    if paper_trader is not None:
        paper_trader.apply_event(
            event,
            dex,
            coin,
            fill,
            start_position,
            post_position,
        )
    return "applied"


def process_user_fills_message(message):
    data = message.get("data")
    if not isinstance(data, dict):
        print(f"USERFILLS ERROR: unexpected data: {data!r}")
        return

    fills = data.get("fills")
    if not isinstance(fills, list):
        print(f"USERFILLS ERROR: missing fills: {data!r}")
        return

    if data.get("isSnapshot"):
        print(f"User fills snapshot received: {len(fills)} fills.")
        return fills

    return fills


def sort_fills(fills):
    return sorted(
        (fill for fill in (fills or []) if isinstance(fill, dict)),
        key=fill_sort_key,
    )


def apply_fills(fills, positions, recent_fills, market_dexes,
                paper_trader=None, db=None):
    latest_time = 0
    for fill in sort_fills(fills):
        dex = resolve_fill_dex(fill, positions, market_dexes)
        result = process_fill(
            fill,
            dex,
            positions,
            recent_fills,
            db=db,
            paper_trader=paper_trader,
        )
        if result == "mismatch" and db is not None:
            if paper_trader:
                paper_trader.set_status(PAPER_PAUSED)
                db.save_paper_state(paper_trader)
            db.save_recovery_state(RECOVERY_GAP, PAPER_PAUSED)
        if result in {"applied", "already_applied", "unchanged"}:
            latest_time = max(latest_time, int(fill.get("time", 0)))
    return latest_time


async def fetch_bbo_snapshots(markets):
    """Fetch one current BBO message for each persisted market."""
    markets = list(markets)
    if not markets:
        return {}
    requested_markets = set(markets)
    dexes_by_coin = {}
    for dex, coin in markets:
        dexes_by_coin.setdefault(coin, set()).add(dex)
    snapshots = {}
    async with websockets.connect(
        WS_URL, ping_interval=20, ping_timeout=20, close_timeout=10,
        max_size=None,
    ) as ws:
        for _dex, coin in markets:
            await ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "bbo", "coin": coin},
            }))
        deadline = time() + REQUEST_TIMEOUT
        while len(snapshots) < len(markets) and time() < deadline:
            raw = await asyncio.wait_for(
                ws.recv(), max(0.1, deadline - time())
            )
            message = json.loads(raw)
            if message.get("channel") != "bbo":
                continue
            data = message.get("data")
            if not isinstance(data, dict):
                continue
            coin = data.get("coin")
            if data.get("dex") is not None:
                market = (data["dex"], coin)
                if market in requested_markets:
                    snapshots[market] = data
                continue
            dexes = dexes_by_coin.get(coin, set())
            if len(dexes) == 1:
                market = (next(iter(dexes)), coin)
                snapshots[market] = data
        return snapshots


async def monitor_once(db=None):
    global checkpoint_time

    db = db or TrackerDatabase()
    persisted_positions = db.load_positions()
    persisted_checkpoint = db.load_checkpoint()
    checkpoint_time = (
        int(persisted_checkpoint["time"]) if persisted_checkpoint else 0
    )
    paper_state = db.load_paper_state()
    paper_trader = (
        PaperTradingEngine.from_state(paper_state)
        if paper_state is not None
        else PaperTradingEngine()
    )
    # A reconnect is an uncertainty boundary.  Live fills are still journaled,
    # but paper application waits for a complete snapshot/backfill check.
    paper_trader.set_status(PAPER_PAUSED)
    db.save_recovery_state(RECOVERY_UNVERIFIED, PAPER_PAUSED)
    db.save_paper_state(paper_trader)

    dexes = discover_dexes()
    market_dexes = discover_market_dexes(dexes)
    recent_fills = deque(maxlen=RECENT_FILL_LIMIT)
    payload = {
        "method": "subscribe",
        "subscription": {
            "type": "userFills",
            "user": WALLET,
            "aggregateByTime": False,
        },
    }

    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_size=None,
    ) as ws:
        await ws.send(json.dumps(payload))
        print("WebSocket connected; userFills subscription sent.")
        snapshot_fills = []
        snapshot_received = False
        while not snapshot_received:
            raw_message = await asyncio.wait_for(ws.recv(), 20)
            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                print("WEBSOCKET ERROR: received invalid JSON")
                continue

            channel = message.get("channel")
            if channel == "subscriptionResponse":
                print("userFills subscription acknowledged.")
            elif channel == "error":
                print(f"WEBSOCKET SUBSCRIPTION ERROR: {message.get('data')}")
            elif channel == "userFills":
                snapshot_fills = process_user_fills_message(
                    message
                )
                snapshot_received = bool(
                    isinstance(message.get("data"), dict)
                    and message["data"].get("isSnapshot")
                )

        snapshot_positions, snapshot_time = load_position_snapshot(dexes)
        positions = persisted_positions
        # A first run uses the exchange snapshot as a baseline. It is state,
        # not a sequence of trades, so it must never affect paper PnL.
        fresh_baseline = not db.has_copy_start()
        if fresh_baseline:
            positions = snapshot_positions
            with db.transaction():
                db.replace_positions(positions)
                db.initialize_copy_start(positions)
                db.save_paper_state(paper_trader)
        print(
            f"Snapshot loaded: {len(snapshot_positions)} positions across "
            f"{len(dexes)} perp DEX entries."
        )
        for fill in snapshot_fills:
            if isinstance(fill, dict):
                remember_fill(
                    recent_fills,
                    fill,
                    resolve_fill_dex(fill, positions, market_dexes),
                )

        backfill_end = max(int(time() * 1000), snapshot_time)
        fills = []
        backfill_complete = True
        backfill_ranges = []
        for dex in dexes:
            checkpoint = db.load_checkpoint(dex)
            if fresh_baseline:
                backfill_start = max(0, snapshot_time + 1)
            elif checkpoint:
                backfill_start = max(
                    0, int(checkpoint["time"]) - BACKFILL_OVERLAP_MS
                )
            else:
                backfill_start = max(0, snapshot_time - BACKFILL_OVERLAP_MS)
            page_fills, metadata = backfill_fills(
                backfill_start, backfill_end, dex=dex, return_metadata=True
            )
            fills.extend(page_fills)
            backfill_complete = backfill_complete and metadata["complete"]
            backfill_ranges.append((dex, backfill_start))
        print(
            f"Backfill loaded: {len(fills)} fills "
            f"({min(start for _, start in backfill_ranges)}..{backfill_end})."
        )
        watermark = max(
            snapshot_time,
            apply_fills(
                fills,
                positions,
                recent_fills,
                market_dexes,
                paper_trader,
                db,
            ),
        )
        checkpoint_time = max(checkpoint_time, watermark)
        reconciliation = reconcile_clearinghouse(
            positions, snapshot_positions, db=db, snapshot_time=snapshot_time
        )
        if not reconciliation["matched"]:
            recovery_state = RECOVERY_GAP
        elif not backfill_complete:
            # A full page or an ambiguous historical boundary proves that the
            # API response is incomplete/uncertain, not that a gap definitely
            # exists.
            recovery_state = RECOVERY_UNVERIFIED
        elif paper_safety_gate(RECOVERED, reconciliation):
            # Execute persisted obligations from this uncertain interval before
            # opening the live paper path.  The execution is transactionally journaled.
            db.save_recovery_state(RECOVERED, PAPER_ENABLED)
            pending_markets = [
                (row["dex"], row["coin"])
                for row in db.connection.execute(
                    "SELECT DISTINCT dex, coin FROM pending_obligations "
                    "WHERE status = 'ACTIVE' AND pending_size <> '0'"
                )
            ]
            bbo_snapshots = await fetch_bbo_snapshots(pending_markets)
            paper_trader.set_status(PAPER_ENABLED)
            db.replay_pending_fills(
                paper_trader, bbo_snapshots, now_ms=int(time() * 1000)
            )
            recovery_state = RECOVERED
        else:
            recovery_state = RECOVERY_GAP
        db.save_recovery_state(
            recovery_state,
            PAPER_ENABLED if paper_trader.enabled else PAPER_PAUSED,
        )
        with db.transaction():
            db.save_paper_state(paper_trader)

        async for raw_message in ws:
            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                print("WEBSOCKET ERROR: received invalid JSON")
                continue

            channel = message.get("channel")
            if channel == "subscriptionResponse":
                print("userFills subscription acknowledged.")
            elif channel == "error":
                print(f"WEBSOCKET SUBSCRIPTION ERROR: {message.get('data')}")
            elif channel == "userFills":
                latest = apply_fills(
                    process_user_fills_message(message),
                    positions,
                    recent_fills,
                    market_dexes,
                    paper_trader,
                    db,
                )
                watermark = max(watermark, latest)
                checkpoint_time = max(checkpoint_time, watermark)
async def main():
    db = TrackerDatabase()
    while True:
        try:
            await monitor_once(db)
        except KeyboardInterrupt:
            print("Tracker stopped.")
            db.close()
            return
        except Exception as error:
            # Never resume paper after a transport or recovery failure.
            db.set_recovery_state(RECOVERY_UNVERIFIED)
            state = db.load_paper_state()
            if state is not None:
                paused = PaperTradingEngine.from_state(state)
                paused.set_status(PAPER_PAUSED)
                db.save_paper_state(paused)
            print()
            print("WEBSOCKET / TRACKER ERROR")
            print(type(error).__name__)
            print(str(error))
            print(f"Reconnecting in {RECONNECT_DELAY} seconds...")
            await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    asyncio.run(main())
