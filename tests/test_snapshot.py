import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from streammill.server import Handler
from streammill.service import Service


class HttpTestCase(unittest.TestCase):
    def setUp(self) -> None:
        Handler.service = Service()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def request(self, method, path, body=None, raw=None):
        data = raw
        if data is None and body is not None:
            data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=data, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def create(self, name="s", window_ms=1000, allowed_lateness_ms=0, **extra):
        return self.request(
            "POST",
            "/streams",
            {
                "name": name,
                "window_ms": window_ms,
                "allowed_lateness_ms": allowed_lateness_ms,
                **extra,
            },
        )

    def event(self, ts, value, event_id=None, stream="s"):
        body = {"timestamp_ms": ts, "value": value}
        if event_id is not None:
            body["event_id"] = event_id
        return self.request("POST", f"/streams/{stream}/events", body)

    def watermark(self, wm, stream="s"):
        return self.request(
            "POST", f"/streams/{stream}/watermark", {"watermark_ms": wm}
        )

    def results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/results")

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, body=None, raw=None):
        return self.request("POST", "/snapshot/restore", body=body, raw=raw)


class SnapshotExportTest(HttpTestCase):
    def test_empty_instance_shape(self):
        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"format_version": 1, "streams": []})

    def test_document_shape_and_ordering(self):
        self.create("zeta", 1000, 100)
        self.create("alpha", 500, 0, dedup_retention_ms=5000)
        self.event(100, 5, stream="zeta")
        self.event(0, 1, event_id="b", stream="alpha")
        self.event(100, 2, event_id="a", stream="alpha")
        self.watermark(1100, stream="zeta")
        self.watermark(600, stream="alpha")  # finalizes [0,500)

        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(payload["format_version"], 1)
        names = [s["name"] for s in payload["streams"]]
        self.assertEqual(names, ["alpha", "zeta"])

        zeta = payload["streams"][1]
        self.assertEqual(zeta["window_ms"], 1000)
        self.assertEqual(zeta["allowed_lateness_ms"], 100)
        self.assertIsNone(zeta["dedup_retention_ms"])
        self.assertNotIn("dedup_records", zeta)
        self.assertEqual(zeta["watermark_ms"], 1100)
        # finalized window is not also listed as open
        self.assertEqual(
            zeta["finalized"],
            [{
                "stream": "zeta",
                "window_start_ms": 0,
                "window_end_ms": 1000,
                "count": 1,
                "sum": 5,
            }],
        )
        self.assertEqual(zeta["windows"], [])

        alpha = payload["streams"][0]
        self.assertEqual(alpha["dedup_retention_ms"], 5000)
        starts = [w["window_start_ms"] for w in alpha["windows"]]
        self.assertEqual(starts, sorted(starts))
        ids = [r["event_id"] for r in alpha["dedup_records"]]
        self.assertEqual(ids, ["a", "b"])
        for record in alpha["dedup_records"]:
            self.assertEqual(set(record), {"event_id", "timestamp_ms", "value"})

    def test_open_windows_ordered_with_finalized(self):
        self.create("s", 1000, 0)
        self.event(5000, 1)
        self.event(2000, 2)
        self.event(0, 3)
        self.watermark(1000)
        _, payload = self.snapshot()
        stream = payload["streams"][0]
        self.assertEqual(
            [w["window_start_ms"] for w in stream["finalized"]], [0]
        )
        self.assertEqual(
            [w["window_start_ms"] for w in stream["windows"]], [2000, 5000]
        )


class SnapshotRoundTripTest(HttpTestCase):
    def _build_state(self):
        self.create("plain", 500, 100)
        self.create("dedup", 1000, 100, dedup_retention_ms=5000)
        self.event(100, 5, stream="plain")
        self.event(600, 7, stream="plain")
        self.event(100, 5, event_id="e1", stream="dedup")
        self.event(1500, 9, event_id="e2", stream="dedup")
        self.watermark(600, stream="plain")    # finalizes [0,500)
        self.watermark(1100, stream="dedup")  # finalizes [0,1000)
        return {
            "plain": self.results("plain")[1],
            "dedup": self.results("dedup")[1],
        }

    def test_restore_returns_count_and_preserves_results(self):
        before = self._build_state()
        status, snap = self.snapshot()
        self.assertEqual(status, 200)

        # fresh instance: emulate by resetting the shared handler service
        Handler.service = Service()
        status, payload = self.restore(snap)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 2})

        self.assertEqual(self.results("plain")[1], before["plain"])
        self.assertEqual(self.results("dedup")[1], before["dedup"])

    def test_empty_snapshot_restores_zero(self):
        status, payload = self.restore({"format_version": 1, "streams": []})
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 0})
        status, payload = self.snapshot()
        self.assertEqual(payload, {"format_version": 1, "streams": []})

    def test_open_window_continues_and_finalizes_once(self):
        self._build_state()
        _, snap = self.snapshot()
        Handler.service = Service()
        self.restore(snap)

        # dedup stream: window [1000,2000) is open with count 1, sum 9
        status, payload = self.event(1200, 3, event_id="e3", stream="dedup")
        self.assertEqual(status, 200)
        self.assertEqual(payload["dropped"], False)
        status, payload = self.watermark(2100, stream="dedup")
        self.assertEqual(status, 200)
        self.assertEqual(
            [w["window_start_ms"] for w in payload["finalized"]], [1000]
        )
        row = payload["finalized"][0]
        self.assertEqual(row["count"], 2)
        self.assertEqual(row["sum"], 12)
        # advancing again must not re-finalize the earlier or current window
        status, payload = self.watermark(10**9, stream="dedup")
        self.assertNotIn(0, [w["window_start_ms"] for w in payload["finalized"]])
        _, results = self.results("dedup")
        starts = [r["window_start_ms"] for r in results["results"]]
        self.assertEqual(sorted(starts), starts)
        self.assertEqual(len(starts), len(set(starts)))

    def test_dedup_judgement_and_eviction_boundary_survive(self):
        self._build_state()
        _, snap = self.snapshot()
        Handler.service = Service()
        self.restore(snap)

        _, payload = self.event(100, 5, event_id="e1", stream="dedup")
        self.assertEqual(payload["duplicate"], True)
        status, payload = self.event(100, 6, event_id="e1", stream="dedup")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")

        # horizon boundary must be exactly as before: wm 5100 -> horizon 100,
        # e1 at ts 100 is on the boundary and stays retained; 5101 evicts it
        self.watermark(5100, stream="dedup")
        _, payload = self.event(100, 5, event_id="e1", stream="dedup")
        self.assertEqual(payload["duplicate"], True)
        self.watermark(5101, stream="dedup")
        _, payload = self.event(100, 5, event_id="e1", stream="dedup")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)

    def test_reexport_without_writes_is_identical(self):
        self._build_state()
        _, first = self.snapshot()
        _, snap = self.snapshot()
        Handler.service = Service()
        status, _ = self.restore(snap)
        self.assertEqual(status, 200)
        _, second = self.snapshot()
        self.assertEqual(second, first)
        # array orderings are part of the identity
        self.assertEqual(
            json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True)
        )

    def test_watermark_still_moves_forward_only(self):
        self._build_state()
        _, snap = self.snapshot()
        Handler.service = Service()
        self.restore(snap)
        status, payload = self.watermark(1099, stream="dedup")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "watermark_regression")


class RestoreFailureTest(HttpTestCase):
    def _base_stream(self, **overrides):
        entry = {
            "name": "s",
            "window_ms": 1000,
            "allowed_lateness_ms": 100,
            "dedup_retention_ms": None,
            "watermark_ms": None,
            "windows": [],
            "finalized": [],
        }
        entry.update(overrides)
        return entry

    def snap(self, *streams, version=1, **top):
        doc = {"format_version": version, "streams": list(streams)}
        doc.update(top)
        return doc

    def assert_invalid(self, doc):
        status, payload = self.restore(doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        # instance must remain completely empty
        _, current = self.snapshot()
        self.assertEqual(current, {"format_version": 1, "streams": []})

    def test_invalid_json_is_400(self):
        status, payload = self.restore(raw=b"{not json")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")

    def test_conflict_takes_priority_over_content(self):
        self.create("existing")
        # malformed JSON is a parse-level 400 even on a populated instance
        status, payload = self.restore(raw=b"{not json")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")
        # any parseable document - invalid, unknown version or valid empty -
        # is refused with 409 restore_conflict
        status, payload = self.restore([])
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")
        status, payload = self.restore({"format_version": 99, "streams": []})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")
        status, _ = self.restore({"format_version": 1, "streams": []})
        self.assertEqual(status, 409)
        good_stream = {
            "name": "other",
            "window_ms": 1000,
            "allowed_lateness_ms": 0,
            "dedup_retention_ms": None,
            "watermark_ms": None,
            "windows": [],
            "finalized": [],
        }
        status, _ = self.restore({"format_version": 1, "streams": [good_stream]})
        self.assertEqual(status, 409)
        # original stream untouched and visible
        _, payload = self.snapshot()
        self.assertEqual([s["name"] for s in payload["streams"]], ["existing"])

    def test_top_level_validation(self):
        self.assert_invalid([])
        self.assert_invalid({})
        self.assert_invalid({"format_version": "1", "streams": []})
        self.assert_invalid({"format_version": True, "streams": []})
        self.assert_invalid(self.snap(version=2))
        self.assert_invalid({"format_version": 1, "streams": {}})
        self.assert_invalid({"format_version": 1, "streams": [], "extra": 1})
        self.assert_invalid({"version": 1, "streams": []})
        self.assert_invalid({"streams": []})
        self.assert_invalid({"format_version": 1})

    def test_stream_config_validation(self):
        self.assert_invalid(self.snap({"name": "x"}))
        self.assert_invalid(self.snap(self._base_stream(name="")))
        self.assert_invalid(self.snap(self._base_stream(name=7)))
        self.assert_invalid(self.snap(self._base_stream(window_ms=0)))
        self.assert_invalid(self.snap(self._base_stream(window_ms=True)))
        self.assert_invalid(self.snap(self._base_stream(allowed_lateness_ms=-1)))
        self.assert_invalid(
            self.snap(self._base_stream(dedup_retention_ms=99))
        )
        self.assert_invalid(
            self.snap(self._base_stream(dedup_retention_ms=0))
        )
        self.assert_invalid(
            self.snap(self._base_stream(watermark_ms="x"))
        )
        self.assert_invalid(
            self.snap(self._base_stream(windows={}))
        )
        self.assert_invalid(
            self.snap(self._base_stream(finalized={}))
        )
        self.assert_invalid(
            self.snap(self._base_stream(extra=1))
        )
        # dedup_records only allowed when retention is set
        self.assert_invalid(
            self.snap(self._base_stream(dedup_records=[]))
        )
        # dedup stream requires the dedup_records key
        self.assert_invalid(
            self.snap(self._base_stream(dedup_retention_ms=5000))
        )

    def test_name_uniqueness_and_ordering(self):
        self.assert_invalid(
            self.snap(self._base_stream(name="a"), self._base_stream(name="a"))
        )
        self.assert_invalid(
            self.snap(self._base_stream(name="z"), self._base_stream(name="a"))
        )

    def test_window_validation(self):
        good = {"window_start_ms": 0, "window_end_ms": 1000, "count": 1, "sum": 1}
        next_good = {"window_start_ms": 1000, "window_end_ms": 2000, "count": 1, "sum": 1}
        self.assert_invalid(
            self.snap(self._base_stream(windows=[good, dict(good)]))
        )
        self.assert_invalid(
            self.snap(self._base_stream(windows=[next_good, good]))
        )
        self.assert_invalid(
            self.snap(
                self._base_stream(
                    windows=[{"window_start_ms": 500, "window_end_ms": 1500,
                              "count": 1, "sum": 1}]
                )
            )
        )
        self.assert_invalid(
            self.snap(
                self._base_stream(
                    windows=[{"window_start_ms": 0, "window_end_ms": 999,
                              "count": 1, "sum": 1}]
                )
            )
        )
        self.assert_invalid(
            self.snap(
                self._base_stream(
                    windows=[{"window_start_ms": 0, "window_end_ms": 1000,
                              "count": 0, "sum": 0}]
                )
            )
        )
        self.assert_invalid(
            self.snap(
                self._base_stream(
                    windows=[{"window_start_ms": 0, "window_end_ms": 1000,
                              "count": 1, "sum": "x"}]
                )
            )
        )

    def test_watermark_lateness_relations(self):
        finalized = [{
            "stream": "s",
            "window_start_ms": 0,
            "window_end_ms": 1000,
            "count": 1,
            "sum": 1,
        }]
        # finalized without a watermark
        self.assert_invalid(self.snap(self._base_stream(finalized=finalized)))
        # watermark below end + lateness
        self.assert_invalid(
            self.snap(
                self._base_stream(watermark_ms=1099, finalized=finalized)
            )
        )
        # exactly on the boundary is valid
        status, _ = self.restore(
            self.snap(self._base_stream(watermark_ms=1100, finalized=finalized))
        )
        self.assertEqual(status, 200)

        Handler.service = Service()
        # open window that should already be finalized at the watermark
        self.assert_invalid(
            self.snap(
                self._base_stream(
                    watermark_ms=1100,
                    windows=[{"window_start_ms": 0, "window_end_ms": 1000,
                              "count": 1, "sum": 1}],
                )
            )
        )
        # window present both open and finalized
        self.assert_invalid(
            self.snap(
                self._base_stream(
                    watermark_ms=1100,
                    windows=[{"window_start_ms": 0, "window_end_ms": 1000,
                              "count": 1, "sum": 1}],
                    finalized=finalized,
                )
            )
        )
        # finalized row labelled with another stream
        self.assert_invalid(
            self.snap(
                self._base_stream(
                    name="s",
                    watermark_ms=1100,
                    finalized=[dict(finalized[0], stream="other")],
                )
            )
        )

    def test_dedup_record_validation(self):
        # Consistent fixture: wm 6000, retention 5000 -> horizon 1000; retained
        # records must have ts >= 1000 and belong to finalized window [1000,2000).
        def dedup_stream(**overrides):
            base = self._base_stream(
                name="d",
                allowed_lateness_ms=0,
                dedup_retention_ms=5000,
                watermark_ms=6000,
                windows=[],
                finalized=[{
                    "stream": "d",
                    "window_start_ms": 1000,
                    "window_end_ms": 2000,
                    "count": 2,
                    "sum": 2,
                }],
                dedup_records=[
                    {"event_id": "a", "timestamp_ms": 1000, "value": 1},
                    {"event_id": "b", "timestamp_ms": 1500, "value": 1},
                ],
            )
            base.update(overrides)
            return base

        # the well-formed document restores fine
        status, _ = self.restore(self.snap(dedup_stream()))
        self.assertEqual(status, 200)
        Handler.service = Service()

        self.assert_invalid(
            self.snap(dedup_stream(dedup_records=[
                {"event_id": "a", "timestamp_ms": 1000, "value": 1},
                {"event_id": "a", "timestamp_ms": 1500, "value": 1},
            ]))
        )
        self.assert_invalid(
            self.snap(dedup_stream(dedup_records=[
                {"event_id": "b", "timestamp_ms": 1500, "value": 1},
                {"event_id": "a", "timestamp_ms": 1000, "value": 1},
            ]))
        )
        self.assert_invalid(
            self.snap(dedup_stream(dedup_records=[
                {"event_id": "", "timestamp_ms": 1000, "value": 1},
            ]))
        )
        self.assert_invalid(
            self.snap(dedup_stream(dedup_records=[
                {"event_id": "a", "timestamp_ms": "1000", "value": 1},
            ]))
        )
        self.assert_invalid(
            self.snap(dedup_stream(dedup_records=[
                {"event_id": "a", "timestamp_ms": 1000, "value": True},
            ]))
        )
        # past retention horizon (wm 6000, retention 5000 -> horizon 1000)
        self.assert_invalid(
            self.snap(dedup_stream(dedup_records=[
                {"event_id": "a", "timestamp_ms": 999, "value": 1},
            ]))
        )
        # timestamp exactly on the horizon is retained
        status, _ = self.restore(self.snap(dedup_stream(dedup_records=[
            {"event_id": "a", "timestamp_ms": 1000, "value": 1},
        ])))
        self.assertEqual(status, 200)
        Handler.service = Service()
        # record aggregates into an unknown window
        self.assert_invalid(
            self.snap(dedup_stream(dedup_records=[
                {"event_id": "a", "timestamp_ms": 5000, "value": 1},
            ]))
        )
        # more retained ids than aggregated events
        self.assert_invalid(
            self.snap(dedup_stream(dedup_records=[
                {"event_id": "a", "timestamp_ms": 1000, "value": 1},
                {"event_id": "b", "timestamp_ms": 1500, "value": 1},
                {"event_id": "c", "timestamp_ms": 1800, "value": 1},
            ]))
        )

    def test_failed_restore_allows_later_valid_restore(self):
        self.assert_invalid(self.snap(version=2))
        status, _ = self.restore({"format_version": 1, "streams": []})
        self.assertEqual(status, 200)


class SnapshotConcurrencyTest(HttpTestCase):
    def test_export_sees_consistent_point_in_time(self):
        self.create("s", 1000, 0)
        stop = threading.Event()
        payloads = []
        errors = []

        def writer():
            i = 0
            while not stop.is_set():
                try:
                    self.event(i, 1)
                    self.watermark(i + 1000)
                except Exception as exc:  # pragma: no cover - diagnostic only
                    errors.append(exc)
                i += 1000

        def exporter():
            while not stop.is_set():
                _, snap = self.snapshot()
                payloads.append(snap)

        threads = [threading.Thread(target=writer), threading.Thread(target=exporter)]
        for t in threads:
            t.start()
        # let them race briefly
        timer = threading.Timer(0.3, stop.set)
        timer.start()
        for t in threads:
            t.join(timeout=5)
        timer.join()
        self.assertFalse(errors)
        self.assertTrue(payloads)
        for snap in payloads:
            self.assertEqual(snap["format_version"], 1)
            stream = snap["streams"][0]
            # internal invariant: no window may appear both open and finalized
            open_starts = {w["window_start_ms"] for w in stream["windows"]}
            final_starts = {w["window_start_ms"] for w in stream["finalized"]}
            self.assertFalse(open_starts & final_starts)
            for row in stream["finalized"]:
                self.assertLessEqual(
                    row["window_end_ms"], stream["watermark_ms"]
                )
            starts = [w["window_start_ms"] for w in stream["windows"]] + \
                     [w["window_start_ms"] for w in stream["finalized"]]
            self.assertEqual(len(starts), len(set(starts)))
        # every observed snapshot must itself restore cleanly
        for snap in payloads[:: max(1, len(payloads) // 8)]:
            Service().restore_snapshot(json.loads(json.dumps(snap)))


if __name__ == "__main__":
    unittest.main()
