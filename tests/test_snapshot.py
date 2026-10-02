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
        body = {
            "name": name,
            "window_ms": window_ms,
            "allowed_lateness_ms": allowed_lateness_ms,
            **extra,
        }
        return self.request("POST", "/streams", body)

    def event(self, ts, value, event_id=None, stream="s"):
        body = {"timestamp_ms": ts, "value": value}
        if event_id is not None:
            body["event_id"] = event_id
        return self.request("POST", f"/streams/{stream}/events", body)

    def watermark(self, wm, stream="s"):
        return self.request("POST", f"/streams/{stream}/watermark", {"watermark_ms": wm})

    def results(self, stream="s"):
        return self.request("GET", f"/streams/{stream}/results")

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, document=None, raw=None):
        return self.request("POST", "/snapshot/restore", body=document, raw=raw)


def build_populated_service() -> Service:
    """A service with open windows, finalized results and dedup records."""
    svc = Service()
    svc.create_stream("alpha", 1000, 100, dedup_retention_ms=5000)
    svc.add_event("alpha", 100, 1.5, "e2")
    svc.add_event("alpha", 200, 2.5, "e1")
    svc.add_event("alpha", 1500, 10, "e3")
    svc.advance_watermark("alpha", 1100)  # finalizes [0, 1000)
    svc.create_stream("beta", 500, 0)
    svc.add_event("beta", 100, 7)
    svc.add_event("beta", 600, 8)
    svc.advance_watermark("beta", 1000)  # finalizes [0, 500) and [500, 1000)
    svc.create_stream("empty", 100, 0)
    return svc


class SnapshotServiceTest(unittest.TestCase):
    def test_empty_snapshot(self):
        self.assertEqual(Service().snapshot(), {"format_version": 1, "streams": []})

    def test_snapshot_layout_and_ordering(self):
        doc = build_populated_service().snapshot()
        self.assertEqual(doc["format_version"], 1)
        self.assertEqual([s["name"] for s in doc["streams"]], ["alpha", "beta", "empty"])

        alpha = doc["streams"][0]
        self.assertEqual(alpha["window_ms"], 1000)
        self.assertEqual(alpha["allowed_lateness_ms"], 100)
        self.assertEqual(alpha["dedup_retention_ms"], 5000)
        self.assertEqual(alpha["watermark_ms"], 1100)
        # only the still-open window is exported under "windows"
        self.assertEqual(
            alpha["windows"], [{"window_start_ms": 1000, "count": 1, "sum": 10}]
        )
        self.assertEqual(
            alpha["finalized"],
            [
                {
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 2,
                    "sum": 4.0,
                }
            ],
        )
        # dedup records sorted by event_id, all still within retention
        self.assertEqual(
            [r["event_id"] for r in alpha["dedup"]], ["e1", "e2", "e3"]
        )
        self.assertEqual(
            alpha["dedup"][0], {"event_id": "e1", "timestamp_ms": 200, "value": 2.5}
        )

        beta = doc["streams"][1]
        self.assertNotIn("dedup_retention_ms", beta)
        self.assertNotIn("dedup", beta)
        self.assertEqual(beta["windows"], [])
        self.assertEqual(
            [r["window_start_ms"] for r in beta["finalized"]], [0, 500]
        )

        empty = doc["streams"][2]
        self.assertIsNone(empty["watermark_ms"])
        self.assertEqual(empty["windows"], [])
        self.assertEqual(empty["finalized"], [])

    def test_restore_roundtrip_is_identical(self):
        source = build_populated_service()
        doc = source.snapshot()

        restored = Service()
        self.assertEqual(restored.restore_snapshot(doc), 3)
        # re-export without any writes yields the same document
        self.assertEqual(restored.snapshot(), doc)
        # finalized results are queryable immediately
        self.assertEqual(
            restored.results("alpha")["results"], source.results("alpha")["results"]
        )
        self.assertEqual(
            restored.results("beta")["results"], source.results("beta")["results"]
        )

    def test_restored_streams_continue_to_work(self):
        svc = Service()
        svc.restore_snapshot(build_populated_service().snapshot())

        # open window keeps accepting on-time events and finalizes later
        svc.add_event("alpha", 1800, 5, "e4")
        outcome = svc.advance_watermark("alpha", 2100)
        self.assertEqual(
            outcome["finalized"],
            [
                {
                    "stream": "alpha",
                    "window_start_ms": 1000,
                    "window_end_ms": 2000,
                    "count": 2,
                    "sum": 15,
                }
            ],
        )
        # the previously finalized window is not emitted again
        rows = svc.results("alpha")["results"]
        self.assertEqual([r["window_start_ms"] for r in rows], [0, 1000])

        # retained ids still deduplicate and conflict
        dup = svc.add_event("alpha", 200, 2.5, "e1")
        self.assertEqual(dup, {"stream": "alpha", "dropped": False, "duplicate": True})
        with self.assertRaises(Exception):
            svc.add_event("alpha", 200, 9, "e1")

        # eviction boundary is unchanged: horizon = watermark - retention
        svc.advance_watermark("alpha", 5200)  # horizon 200: e1 (ts 200) kept
        self.assertTrue(svc.add_event("alpha", 200, 2.5, "e1")["duplicate"])
        svc.advance_watermark("alpha", 5201)  # horizon 201: e1 evicted
        dropped = svc.add_event("alpha", 200, 2.5, "e1")
        self.assertEqual(
            dropped, {"stream": "alpha", "dropped": True, "duplicate": False}
        )

    def test_restore_conflict_and_empty_snapshot(self):
        svc = build_populated_service()
        with self.assertRaises(Exception):
            svc.restore_snapshot({"format_version": 1, "streams": []})
        # state untouched by the failed restore
        self.assertEqual(len(svc.snapshot()["streams"]), 3)
        # empty snapshot restores fine on an empty instance
        fresh = Service()
        self.assertEqual(fresh.restore_snapshot({"format_version": 1, "streams": []}), 0)
        self.assertEqual(fresh.snapshot()["streams"], [])


class SnapshotHttpTest(HttpTestCase):
    def populate(self):
        self.create("s", 1000, 100, dedup_retention_ms=5000)
        self.event(100, 1.5, event_id="e1")
        self.event(1500, 10, event_id="e2")
        self.watermark(1100)

    def test_get_snapshot_empty(self):
        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"format_version": 1, "streams": []})

    def test_restore_success_and_visibility(self):
        self.populate()
        status, doc = self.snapshot()
        self.assertEqual(status, 200)

        Handler.service = Service()  # simulate a fresh instance
        status, payload = self.restore(doc)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})

        # queries immediately return the pre-export results
        status, payload = self.results()
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["results"],
            [
                {
                    "stream": "s",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 1,
                    "sum": 1.5,
                }
            ],
        )
        # dedup state survived: exact retry is a duplicate
        status, payload = self.event(100, 1.5, event_id="e1")
        self.assertEqual(status, 200)
        self.assertTrue(payload["duplicate"])
        # and the open window finalizes under a later watermark
        status, payload = self.watermark(2100)
        self.assertEqual(payload["finalized"][0]["window_start_ms"], 1000)
        self.assertEqual(payload["finalized"][0]["sum"], 10)

    def test_restore_empty_snapshot(self):
        status, payload = self.restore({"format_version": 1, "streams": []})
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 0})

    def test_restore_invalid_json(self):
        status, payload = self.restore(raw=b"{not json")
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_json")

    def test_restore_invalid_snapshots(self):
        valid_stream = {
            "name": "s",
            "window_ms": 1000,
            "allowed_lateness_ms": 100,
            "watermark_ms": None,
            "windows": [],
            "finalized": [],
        }

        def doc(streams, **extra):
            return {"format_version": 1, "streams": streams, **extra}

        cases = [
            [],  # top level must be an object
            {"streams": []},  # missing format_version
            doc([], format_version=2),  # unsupported version
            doc([], format_version="1"),  # wrong type
            {**doc([]), "extra": 1},  # undeclared top-level field
            {"format_version": 1, "streams": {}},  # streams not an array
            doc([valid_stream, valid_stream]),  # duplicate stream names
            doc(  # streams not sorted by name
                [dict(valid_stream, name="b"), dict(valid_stream, name="a")]
            ),
            doc([{k: v for k, v in valid_stream.items() if k != "windows"}]),
            doc([dict(valid_stream, unknown=1)]),  # undeclared stream field
            doc([dict(valid_stream, name="")]),
            doc([dict(valid_stream, window_ms=0)]),
            doc([dict(valid_stream, allowed_lateness_ms=-1)]),
            doc([dict(valid_stream, dedup_retention_ms=50)]),  # below lateness
            doc([dict(valid_stream, watermark_ms="now")]),
            doc([dict(valid_stream, windows={})]),
            # window start not aligned to the window grid
            doc([dict(valid_stream, windows=[{"window_start_ms": 50, "count": 1, "sum": 1}])]),
            # window count must be positive
            doc([dict(valid_stream, windows=[{"window_start_ms": 0, "count": 0, "sum": 1}])]),
            # duplicate windows
            doc(
                [
                    dict(
                        valid_stream,
                        windows=[
                            {"window_start_ms": 0, "count": 1, "sum": 1},
                            {"window_start_ms": 0, "count": 2, "sum": 2},
                        ],
                    )
                ]
            ),
            # windows out of order
            doc(
                [
                    dict(
                        valid_stream,
                        windows=[
                            {"window_start_ms": 1000, "count": 1, "sum": 1},
                            {"window_start_ms": 0, "count": 1, "sum": 1},
                        ],
                    )
                ]
            ),
            # finalized end must equal start + window_ms
            doc(
                [
                    dict(
                        valid_stream,
                        watermark_ms=1100,
                        finalized=[
                            {"window_start_ms": 0, "window_end_ms": 999, "count": 1, "sum": 1}
                        ],
                    )
                ]
            ),
            # finalized results require a watermark that closed them
            doc(
                [
                    dict(
                        valid_stream,
                        finalized=[
                            {"window_start_ms": 0, "window_end_ms": 1000, "count": 1, "sum": 1}
                        ],
                    )
                ]
            ),
            doc(
                [
                    dict(
                        valid_stream,
                        watermark_ms=1099,  # needs >= 1000 + 100
                        finalized=[
                            {"window_start_ms": 0, "window_end_ms": 1000, "count": 1, "sum": 1}
                        ],
                    )
                ]
            ),
            # open window would already be final at this watermark
            doc(
                [
                    dict(
                        valid_stream,
                        watermark_ms=1100,
                        windows=[{"window_start_ms": 0, "count": 1, "sum": 1}],
                    )
                ]
            ),
            # same window both open and finalized
            doc(
                [
                    dict(
                        valid_stream,
                        watermark_ms=1100,
                        windows=[{"window_start_ms": 1000, "count": 1, "sum": 1}],
                        finalized=[
                            {"window_start_ms": 1000, "window_end_ms": 2000, "count": 1, "sum": 1}
                        ],
                    )
                ]
            ),
            # dedup records on a stream without dedup_retention_ms
            doc(
                [
                    dict(
                        valid_stream,
                        dedup=[{"event_id": "e1", "timestamp_ms": 0, "value": 1}],
                    )
                ]
            ),
            # duplicate event ids
            doc(
                [
                    dict(
                        valid_stream,
                        dedup_retention_ms=1000,
                        dedup=[
                            {"event_id": "e1", "timestamp_ms": 0, "value": 1},
                            {"event_id": "e1", "timestamp_ms": 1, "value": 1},
                        ],
                    )
                ]
            ),
            # dedup records out of order
            doc(
                [
                    dict(
                        valid_stream,
                        dedup_retention_ms=1000,
                        dedup=[
                            {"event_id": "b", "timestamp_ms": 0, "value": 1},
                            {"event_id": "a", "timestamp_ms": 0, "value": 1},
                        ],
                    )
                ]
            ),
            # dedup record older than the retention eviction horizon
            doc(
                [
                    dict(
                        valid_stream,
                        watermark_ms=5000,
                        dedup_retention_ms=1000,
                        dedup=[{"event_id": "e1", "timestamp_ms": 3999, "value": 1}],
                    )
                ]
            ),
        ]
        for i, body in enumerate(cases):
            with self.subTest(case=i):
                status, payload = self.restore(body)
                self.assertEqual(status, 422, body)
                self.assertEqual(payload["error"]["code"], "invalid_snapshot")
                # the instance stays completely empty
                _, snap = self.snapshot()
                self.assertEqual(snap, {"format_version": 1, "streams": []})

    def test_restore_conflict(self):
        self.populate()
        _, before = self.snapshot()

        # even a perfectly valid snapshot is rejected once streams exist
        status, payload = self.restore({"format_version": 1, "streams": []})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")

        # and so is an invalid one
        status, payload = self.restore({"format_version": 2, "streams": []})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "restore_conflict")

        # original state is unchanged
        _, after = self.snapshot()
        self.assertEqual(before, after)
        status, payload = self.results()
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["results"]), 1)

    def test_export_after_restore_without_writes_is_identical(self):
        self.populate()
        self.create("a", 500, 0)
        self.event(100, 3, stream="a")
        _, doc = self.snapshot()

        Handler.service = Service()
        status, _ = self.restore(doc)
        self.assertEqual(status, 200)
        _, again = self.snapshot()
        self.assertEqual(doc, again)

    def test_existing_routes_untouched(self):
        status, payload = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        status, payload = self.request("GET", "/snapshot/restore")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "not_found")
        status, payload = self.request("POST", "/snapshot", {})
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
