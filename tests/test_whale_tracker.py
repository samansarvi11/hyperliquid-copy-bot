import asyncio
import json
import os
import tempfile
import unittest
from collections import deque
from decimal import Decimal
from unittest.mock import patch

import whale_tracker as wt


class WhaleTrackerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "tracker.sqlite3")
        self.db = wt.TrackerDatabase(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def fill(self, coin="BTC", time=1000, tid=1, oid=1, start="0",
              sz="1", side="B", px="100", dex=""):
        return {
            "coin": coin, "time": time, "tid": tid, "oid": oid,
            "startPosition": start, "sz": sz, "side": side, "px": px,
            **({"dex": dex} if dex else {}),
        }

    def apply(self, fill, positions=None, paper=None, dex=""):
        positions = {} if positions is None else positions
        recent = deque()
        result = self.db.process_fill(fill, dex, positions, recent,
                                       paper_trader=paper)
        return result, positions, recent

    def paper(self):
        return wt.PaperTradingEngine(initial_capital="200",
                                     whale_capital="1000")

    def bbo(self, bid="99", bid_size="10", ask="101", ask_size="10",
            timestamp=1000):
        return {
            "coin": "BTC",
            "time": timestamp,
            "bbo": [
                {"px": bid, "sz": bid_size, "n": 1},
                {"px": ask, "sz": ask_size, "n": 1},
            ],
        }

    def make_pending(self, size="1", side="B", dex="", coin="BTC"):
        paper = self.paper()
        paper.set_status(wt.PAPER_PAUSED)
        positions = {}
        self.db.process_fill(
            self.fill(coin=coin, dex=dex, sz=size, side=side),
            dex, positions, deque(), paper_trader=paper,
        )
        self.db.reconcile_clearinghouse(positions, positions, 10)
        self.db.save_recovery_state(wt.RECOVERED, wt.PAPER_ENABLED)
        paper.set_status(wt.PAPER_ENABLED)
        return paper

    def test_normal_restart(self):
        fill = self.fill()
        positions = {}
        self.db.process_fill(fill, "", positions, deque())
        self.db.close()
        self.db = wt.TrackerDatabase(self.path)
        self.assertEqual(self.db.load_positions()[("", "BTC")]["szi"], Decimal("1"))
        self.assertTrue(self.db.has_fill(fill, ""))

    def test_dict_leverage_persists_and_loads(self):
        position = {
            "szi": Decimal("1"),
            "entryPx": "100",
            "leverage": {"type": "cross", "value": 40},
            "marginUsed": "10",
        }
        self.db.save_position("", "BTC", position)
        stored = self.db.connection.execute(
            "SELECT leverage FROM positions WHERE dex = '' AND coin = 'BTC'"
        ).fetchone()["leverage"]
        self.assertEqual(stored, '{"type":"cross","value":40}')
        loaded = self.db.load_positions()[("", "BTC")]
        self.assertEqual(loaded["leverage"], position["leverage"])

    def test_duplicate_after_restart(self):
        fill = self.fill()
        self.apply(fill)
        self.db.close()
        self.db = wt.TrackerDatabase(self.path)
        positions = self.db.load_positions()
        result = self.db.process_fill(fill, "", positions, deque())
        self.assertIsNone(result)
        self.assertEqual(positions[("", "BTC")]["szi"], Decimal("1"))
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 1)

    def test_crash_rollback_before_commit(self):
        fill = self.fill()
        positions = {}
        original = self.db.record_fill
        def crash(*args):
            original(*args)
            raise RuntimeError("simulated crash")
        self.db.record_fill = crash
        with self.assertRaises(RuntimeError):
            self.db.process_fill(fill, "", positions, deque())
        self.assertEqual(positions, {})
        self.assertFalse(self.db.has_fill(fill, ""))
        self.assertIsNone(self.db.load_checkpoint())

    def test_open_add_reduce_close(self):
        positions, recent = {}, deque()
        for start, sz, side, expected in [
            ("0", "2", "B", Decimal("2")),
            ("2", "1", "B", Decimal("3")),
            ("3", "1", "A", Decimal("2")),
            ("2", "2", "A", Decimal("0")),
        ]:
            self.db.process_fill(self.fill(start=start, sz=sz, side=side),
                                 "", positions, recent)
            self.assertEqual(positions[("", "BTC")]["szi"], expected)

    def test_reverse(self):
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="2"), "", positions, recent)
        self.db.process_fill(self.fill(start="2", sz="3", side="A", tid=2),
                             "", positions, recent)
        self.assertEqual(positions[("", "BTC")]["szi"], Decimal("-1"))

    def test_weighted_entry(self):
        paper = self.paper()
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="2", px="100"), "", positions, recent, paper_trader=paper)
        self.db.process_fill(self.fill(start="2", sz="2", px="120", tid=2), "", positions, recent, paper_trader=paper)
        self.assertEqual(paper.positions[("", "BTC")]["entryPx"], Decimal("110"))

    def test_realized_pnl(self):
        paper = self.paper()
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="2", px="100"), "", positions, recent, paper_trader=paper)
        self.db.process_fill(self.fill(start="2", sz="2", side="A", px="125", tid=2), "", positions, recent, paper_trader=paper)
        self.assertEqual(paper.realized_pnl, Decimal("10"))

    def test_paper_scaling(self):
        paper = self.paper()
        paper.apply_event("OPEN", "", "BTC", self.fill(px="100"), Decimal("0"), Decimal("5"))
        self.assertEqual(paper.positions[("", "BTC")]["szi"], Decimal("1"))

    def test_namespace_isolation(self):
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(dex="dex-a"), "dex-a", positions, recent)
        self.db.process_fill(self.fill(dex="dex-b"), "dex-b", positions, recent)
        self.assertEqual(set(positions), {("dex-a", "BTC"), ("dex-b", "BTC")})

    def test_snapshot_isolation(self):
        positions = {("", "BTC"): {"szi": Decimal("2"), "entryPx": "100"}}
        actual = {("", "BTC"): {"szi": Decimal("2"), "entryPx": "101"}}
        result = wt.reconcile_clearinghouse(positions, actual, self.db, 55)
        self.assertFalse(result["matched"])
        self.assertEqual(positions[("", "BTC")]["szi"], Decimal("2"))
        self.assertEqual(self.db.load_positions(), {})

    def test_per_dex_checkpoint(self):
        self.db.save_cursor(10, 1, 1, "a")
        self.db.save_cursor(20, 1, 1, "b")
        self.assertEqual(self.db.load_checkpoint("a")["time"], 10)
        self.assertEqual(self.db.load_checkpoint("b")["time"], 20)

    def test_same_timestamp_fills(self):
        fills = [self.fill(tid=2), self.fill(tid=1)]
        self.assertEqual([f["tid"] for f in wt.sort_fills(fills)], [1, 2])

    def test_same_timestamp_across_multiple_dexes(self):
        positions = {}
        fills = [self.fill(dex="b"), self.fill(dex="a")]
        wt.apply_fills(fills, positions, deque(), {"BTC": {"a", "b"}}, db=self.db)
        self.assertEqual(set(positions), {("a", "BTC"), ("b", "BTC")})

    def test_recovery_below_api_limit(self):
        page = [self.fill(time=10)]
        with patch.object(wt, "info_request", return_value=page):
            fills, metadata = wt.backfill_fills(0, 20, return_metadata=True)
        self.assertEqual(len(fills), 1)
        self.assertTrue(metadata["complete"])

    def test_recovery_unverified(self):
        page = [self.fill(time=10)] * 2000
        with patch.object(wt, "info_request", return_value=page):
            _, metadata = wt.backfill_fills(0, 20, return_metadata=True)
        self.assertFalse(metadata["complete"])

    def test_recovery_gap(self):
        paper = self.paper()
        self.db.save_recovery_state(wt.RECOVERED, wt.PAPER_ENABLED)
        positions = {("", "BTC"): {"szi": Decimal("5")}}
        wt.apply_fills([self.fill(start="0")], positions, deque(), {}, paper, self.db)
        self.assertEqual(self.db.load_recovery_state()["recovery_state"], wt.RECOVERY_GAP)

    def test_recovery_status_survives_restart(self):
        self.db.save_recovery_state(wt.RECOVERY_UNVERIFIED, wt.PAPER_PAUSED)
        self.db.close()
        self.db = wt.TrackerDatabase(self.path)
        self.assertEqual(self.db.load_recovery_state(),
                         {"recovery_state": wt.RECOVERY_UNVERIFIED, "paper_state": wt.PAPER_PAUSED})

    def test_successful_recovery_resume(self):
        paper = self.paper()
        paper.set_status(wt.PAPER_PAUSED)
        fill = self.fill()
        positions = {}
        self.db.process_fill(fill, "", positions, deque(), paper_trader=paper)
        self.db.reconcile_clearinghouse(positions, positions, 10)
        self.db.save_recovery_state(wt.RECOVERED, wt.PAPER_ENABLED)
        self.assertTrue(self.db.paper_safety_gate())
        paper.set_status(wt.PAPER_ENABLED)
        self.assertEqual(
            self.db.replay_pending_fills(
                paper, {("", "BTC"): self.bbo()}, now_ms=2000
            ), 1
        )
        self.assertEqual(self.db.replay_pending_fills(paper), 0)

    def test_clearinghouse_reconciliation_match(self):
        p = {("", "BTC"): {"szi": Decimal("1"), "entryPx": "100"}}
        self.assertTrue(wt.reconcile_clearinghouse(p, p, self.db, 1)["matched"])
        self.assertTrue(self.db.reconciliation_is_safe())

    def test_szi_mismatch(self):
        p = {("", "BTC"): {"szi": Decimal("1")}}
        q = {("", "BTC"): {"szi": Decimal("2")}}
        self.assertFalse(wt.reconcile_clearinghouse(p, q)["matched"])

    def test_entry_px_mismatch(self):
        p = {("", "BTC"): {"szi": Decimal("1"), "entryPx": "100"}}
        q = {("", "BTC"): {"szi": Decimal("1"), "entryPx": "101"}}
        self.assertFalse(wt.reconcile_clearinghouse(p, q)["matched"])

    def test_exchange_only_position(self):
        result = wt.reconcile_clearinghouse({}, {("a", "BTC"): {"szi": "1"}})
        self.assertFalse(result["matched"])
        self.assertEqual(result["mismatches"][0][1:], (Decimal("0"), Decimal("1")))

    def test_local_only_position(self):
        result = wt.reconcile_clearinghouse({("a", "BTC"): {"szi": "1"}}, {})
        self.assertFalse(result["matched"])

    def test_missing_coin_on_either_side(self):
        self.db.reconcile_clearinghouse({("a", "BTC"): {"szi": "1"}}, {}, 1)
        row = self.db.connection.execute("SELECT * FROM clearinghouse_reconciliation").fetchone()
        self.assertEqual(row["coin"], "BTC")
        self.assertEqual(row["exchange_szi"], "0")

    def test_reconciliation_never_overwrites_local_state(self):
        p = {("a", "BTC"): {"szi": Decimal("1")}}
        wt.reconcile_clearinghouse(p, {("a", "BTC"): {"szi": Decimal("2")}}, self.db)
        self.assertEqual(p[("a", "BTC")]["szi"], Decimal("1"))

    def test_paper_paused_during_recovery(self):
        paper = self.paper()
        paper.set_status(wt.PAPER_PAUSED)
        self.db.save_paper_state(paper)
        self.db.save_recovery_state(wt.RECOVERY_UNVERIFIED, wt.PAPER_PAUSED)
        result, positions, _ = self.apply(self.fill(), paper=paper)
        self.assertEqual(result, "applied")
        self.assertEqual(paper.realized_pnl, Decimal("0"))
        self.assertEqual(paper.positions, {})
        self.assertEqual(self.db.connection.execute("SELECT status FROM fills").fetchone()[0], "paper_paused")

    def test_paper_paused_during_reconciliation_mismatch(self):
        self.db.save_recovery_state(wt.RECOVERY_GAP, wt.PAPER_PAUSED)
        self.assertFalse(self.db.paper_safety_gate())

    def test_paper_enabled_only_after_both_guards_pass(self):
        self.db.save_recovery_state(wt.RECOVERED, wt.PAPER_ENABLED)
        self.assertFalse(self.db.paper_safety_gate())
        self.db.reconcile_clearinghouse({}, {}, 1)
        self.assertTrue(self.db.paper_safety_gate())

    def test_paused_fill_is_replayed_once_after_enable(self):
        paper = self.paper()
        paper.set_status(wt.PAPER_PAUSED)
        fill = self.fill()
        positions = {}
        self.db.process_fill(fill, "", positions, deque(), paper_trader=paper)
        before = paper.state()
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 1)
        self.assertEqual(paper.state(), before)
        self.db.reconcile_clearinghouse(positions, positions, 10)
        self.db.save_recovery_state(wt.RECOVERED, wt.PAPER_ENABLED)
        paper.set_status(wt.PAPER_ENABLED)
        self.assertEqual(
            self.db.replay_pending_fills(
                paper, {("", "BTC"): self.bbo()}, now_ms=2000
            ), 1
        )
        after = paper.state()
        self.assertEqual(self.db.replay_pending_fills(paper), 0)
        self.assertEqual(paper.state(), after)

    def test_recovery_full_execution_uses_ask_and_records_synthetic_bbo(self):
        paper = self.make_pending()
        self.assertEqual(
            self.db.replay_pending_fills(
                paper, {("", "BTC"): self.bbo(ask="101")}, now_ms=2000
            ), 1
        )
        self.assertEqual(paper.positions[("", "BTC")]["entryPx"], Decimal("101"))
        event = self.db.connection.execute(
            "SELECT source, price FROM paper_execution_events"
        ).fetchone()
        self.assertEqual((event["source"], event["price"]), ("synthetic_bbo", "101"))
        self.assertEqual(self.db.load_context("", "BTC")["pending_size"], Decimal("0"))

    def test_recovery_bbo_size_limits_execution_and_preserves_remainder(self):
        paper = self.make_pending(size="3")
        self.assertEqual(
            self.db.replay_pending_fills(
                paper, {("", "BTC"): self.bbo(ask="101", ask_size="1.25")},
                now_ms=2000,
            ), 1
        )
        context = self.db.load_context("", "BTC")
        self.assertEqual(context["paper_size"], Decimal("1.25"))
        self.assertEqual(context["pending_size"], Decimal("1.75"))
        self.assertEqual(self.db.connection.execute(
            "SELECT reference_price FROM pending_obligations"
        ).fetchone()["reference_price"], "100")

    def test_recovery_stale_or_invalid_bbo_leaves_pending_unchanged(self):
        paper = self.make_pending()
        for value in (
            self.bbo(timestamp=0),
            {"time": 1000, "bbo": [None, self.bbo()["bbo"][1]]},
            {"time": 1000, "bbo": [{"sz": "1"}, self.bbo()["bbo"][1]]},
            {"time": 1000, "bbo": [
                {"px": "99", "sz": "1"}, {"px": "101", "sz": "0"}
            ]},
        ):
            before = self.db.load_context("", "BTC")["pending_size"]
            self.assertEqual(
                self.db.replay_pending_fills(
                    paper, {("", "BTC"): value}, now_ms=6001
                ), 0
            )
            self.assertEqual(self.db.load_context("", "BTC")["pending_size"], before)

    def test_recovery_restart_full_and_partial_are_idempotent(self):
        paper = self.make_pending(size="2")
        bbo = {("", "BTC"): self.bbo(ask_size="1", timestamp=1000)}
        self.assertEqual(self.db.replay_pending_fills(paper, bbo, 2000), 1)
        self.assertEqual(self.db.load_context("", "BTC")["pending_size"], Decimal("1"))
        self.db.close()
        self.db = wt.TrackerDatabase(self.path)
        restored = wt.PaperTradingEngine.from_state(self.db.load_paper_state())
        restored.set_status(wt.PAPER_ENABLED)
        self.assertEqual(self.db.replay_pending_fills(restored, bbo, 2000), 1)
        self.assertEqual(self.db.load_context("", "BTC")["pending_size"], Decimal("0"))
        self.assertEqual(restored.positions[("", "BTC")]["szi"], Decimal("0.4"))
        self.assertEqual(self.db.replay_pending_fills(
            restored, {("", "BTC"): self.bbo(ask_size="1", timestamp=2000)}, 3000
        ), 0)

    def test_recovery_sell_uses_bid_and_isolated_by_dex_and_episode(self):
        paper = self.make_pending(side="A", dex="xyz", coin="xyz:XYZ100")
        bbo = {("xyz", "xyz:XYZ100"): {
            "time": 1000, "bbo": [
                {"px": "29500", "sz": "1", "n": 1},
                {"px": "29501", "sz": "1", "n": 1},
            ],
        }}
        self.assertEqual(self.db.replay_pending_fills(paper, bbo, 2000), 1)
        self.assertEqual(
            paper.positions[("xyz", "xyz:XYZ100")]["entryPx"], Decimal("29500")
        )
        self.assertEqual(self.db.connection.execute(
            "SELECT COUNT(*) FROM paper_execution_events "
            "WHERE dex='xyz' AND coin='xyz:XYZ100'"
        ).fetchone()[0], 1)

    def test_bbo_snapshot_maps_unambiguous_coin_to_its_market(self):
        class FakeWebSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def send(self, _payload):
                return None

            async def recv(self):
                return json.dumps({
                    "channel": "bbo",
                    "data": self.data,
                })

        websocket = FakeWebSocket()
        websocket.data = self.bbo()
        with patch.object(wt.websockets, "connect", return_value=websocket):
            snapshots = asyncio.run(
                wt.fetch_bbo_snapshots([("", "BTC")])
            )
        self.assertEqual(snapshots[("", "BTC")]["coin"], "BTC")

    def test_bbo_snapshot_does_not_cross_ambiguous_dexes(self):
        class FakeWebSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def send(self, _payload):
                return None

        with (
            patch.object(wt.websockets, "connect", return_value=FakeWebSocket()),
            patch.object(wt, "REQUEST_TIMEOUT", 0),
        ):
            snapshots = asyncio.run(
                wt.fetch_bbo_snapshots([("", "BTC"), ("xyz", "BTC")])
            )
        self.assertEqual(snapshots, {})

    def test_reconnect_state_restore(self):
        paper = self.paper()
        self.apply(self.fill(), paper=paper)
        self.db.save_paper_state(paper)
        self.db.save_recovery_state(wt.RECOVERED, wt.PAPER_ENABLED)
        self.db.close()
        self.db = wt.TrackerDatabase(self.path)
        restored = wt.PaperTradingEngine.from_state(self.db.load_paper_state())
        self.assertEqual(restored.state(), paper.state())
        self.assertEqual(self.db.load_recovery_state()["recovery_state"], wt.RECOVERED)

    def test_fill_deduplication_after_reconnect(self):
        fill = self.fill()
        self.apply(fill)
        self.db.close()
        self.db = wt.TrackerDatabase(self.path)
        positions = self.db.load_positions()
        self.db.process_fill(fill, "", positions, deque())
        self.assertEqual(positions[("", "BTC")]["szi"], Decimal("1"))
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 1)

    def test_copy_start_baseline_and_baseline_only_reduction(self):
        positions = {("", "BTC"): {"szi": Decimal("5")}}
        self.db.initialize_copy_start(positions)
        self.db.process_fill(
            self.fill(start="5", sz="1", side="A"), "", positions, deque()
        )
        context = self.db.load_context("", "BTC")
        self.assertEqual(Decimal(context["baseline_size"]), Decimal("5"))
        self.assertEqual(Decimal(context["eligible_size"]), Decimal("0"))
        self.assertIsNone(context["active_episode_id"])

    def test_pending_partial_execution_preserves_invariant(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(maximum_amount="2.5")
        positions, recent = {}, deque()
        self.db.process_fill(
            self.fill(start="0", sz="3", tid=1), "", positions, recent,
            paper_trader=paper,
        )
        context = self.db.load_context("", "BTC")
        self.assertEqual(Decimal(context["paper_size"]), Decimal("2.5"))
        self.assertEqual(Decimal(context["pending_size"]), Decimal("0.5"))
        self.assertEqual(
            Decimal(context["eligible_size"]),
            Decimal(context["paper_size"]) + Decimal(context["pending_size"]),
        )

    def test_pending_decrease_then_paper_close(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(maximum_amount="2.5")
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="3", tid=1), "", positions, recent,
                             paper_trader=paper)
        self.db.process_fill(self.fill(start="3", sz="1", side="A", tid=2), "",
                             positions, recent, paper_trader=paper)
        context = self.db.load_context("", "BTC")
        self.assertEqual(Decimal(context["eligible_size"]), Decimal("2"))
        self.assertEqual(Decimal(context["paper_size"]), Decimal("2"))
        self.assertEqual(Decimal(context["pending_size"]), Decimal("0"))

    def test_reversal_creates_new_episode_without_pending_history(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(minimum_amount="10")
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="2", tid=1), "", positions, recent,
                             paper_trader=paper)
        first = self.db.load_context("", "BTC")["active_episode_id"]
        self.db.process_fill(self.fill(start="2", sz="4", side="A", tid=2), "",
                             positions, recent, paper_trader=paper)
        context = self.db.load_context("", "BTC")
        self.assertNotEqual(context["active_episode_id"], first)
        self.assertEqual(Decimal(context["pending_size"]), Decimal("-2"))
        self.assertEqual(
            self.db.connection.execute(
                "SELECT status FROM pending_obligations WHERE episode_id = ?", (first,)
            ).fetchone()["status"],
            "OFFSET",
        )

    def test_zero_position_copy_start_survives_restart(self):
        self.db.initialize_copy_start({})
        self.assertTrue(self.db.has_copy_start())
        self.db.close()
        self.db = wt.TrackerDatabase(self.path)
        self.assertTrue(self.db.has_copy_start())
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="2"), "", positions, recent)
        context = self.db.load_context("", "BTC")
        self.assertEqual(Decimal(context["baseline_size"]), Decimal("0"))
        self.assertIsNotNone(context["active_episode_id"])

    def test_pending_metadata_updates_on_partial_and_full_cancellation(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(minimum_amount="10")
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="3"), "", positions, recent,
                             paper_trader=paper)
        self.db.process_fill(self.fill(start="3", sz="1", side="A", tid=2), "",
                             positions, recent, paper_trader=paper)
        row = self.db.connection.execute(
            "SELECT * FROM pending_obligations"
        ).fetchone()
        self.assertEqual(row["reference_notional"], "200")
        self.assertEqual(row["reference_price"], "100")
        self.db.process_fill(self.fill(start="2", sz="2", side="A", tid=3), "",
                             positions, recent, paper_trader=paper)
        row = self.db.connection.execute(
            "SELECT * FROM pending_obligations"
        ).fetchone()
        self.assertEqual(row["pending_size"], "0")
        self.assertEqual(row["reference_notional"], "0")
        self.assertIsNone(row["reference_price"])
        self.assertEqual(row["status"], "OFFSET")

    def test_partial_execution_preserves_metadata_ratio(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(maximum_amount="2.5")
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="3"), "", positions, recent,
                             paper_trader=paper)
        row = self.db.connection.execute(
            "SELECT * FROM pending_obligations"
        ).fetchone()
        self.assertEqual(row["pending_size"], "0.5")
        self.assertEqual(row["reference_notional"], "50.0")
        self.assertEqual(row["reference_price"], "100")

    def test_zero_execution_keeps_full_pending(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(minimum_amount="10")
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="2"), "", positions, recent,
                             paper_trader=paper)
        context = self.db.load_context("", "BTC")
        self.assertEqual(Decimal(context["paper_size"]), Decimal("0"))
        self.assertEqual(Decimal(context["pending_size"]), Decimal("2"))
        self.assertEqual(paper.positions, {})

    def test_corrupt_pending_state_is_rejected(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(minimum_amount="10")
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="2"), "", positions, recent,
                             paper_trader=paper)
        self.db.connection.execute(
            "UPDATE pending_obligations SET pending_size='3', direction='SHORT'"
        )
        self.db.connection.commit()
        with self.assertRaises(ValueError):
            self.db.load_context("", "BTC")

    def test_paused_paper_reduction_does_not_fake_close(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(maximum_amount="2.5")
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="3"), "", positions, recent,
                             paper_trader=paper)
        paper.set_status(wt.PAPER_PAUSED)
        before = paper.state()
        with self.assertRaises(ValueError):
            self.db.process_fill(
                self.fill(start="3", sz="1", side="A", tid=2), "",
                positions, recent, paper_trader=paper
            )
        self.assertEqual(paper.state(), before)
        context = self.db.load_context("", "BTC")
        self.assertEqual(Decimal(context["paper_size"]), Decimal("2.5"))

    def test_paused_paper_episode_closure_is_deferred(self):
        paper = self.paper()
        paper.execution_checker = wt.ExecutionChecker(maximum_amount="2.5")
        positions, recent = {}, deque()
        self.db.process_fill(self.fill(start="0", sz="3"), "", positions, recent,
                             paper_trader=paper)
        paper.set_status(wt.PAPER_PAUSED)
        with self.assertRaises(ValueError):
            self.db.process_fill(
                self.fill(start="3", sz="3", side="A", tid=2), "",
                positions, recent, paper_trader=paper
            )
        context = self.db.load_context("", "BTC")
        self.assertIsNotNone(context["active_episode_id"])
        self.assertNotEqual(context["episode_state"], "CLOSED")

if __name__ == "__main__":
    unittest.main()
