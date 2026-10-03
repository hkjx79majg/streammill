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


class CreateSlidingStreamTest(HttpTestCase):
    def test_slide_echoed_when_provided(self):
        status, payload = self.create("s", 1000, 100, slide_ms=250)
        self.assertEqual(status, 201)
        self.assertEqual(payload["stream"], "s")
        self.assertEqual(payload["window_ms"], 1000)
        self.assertEqual(payload["allowed_lateness_ms"], 100)
        self.assertEqual(payload["slide_ms"], 250)

    def test_slide_equal_to_window_is_valid(self):
        status, payload = self.create("s", 1000, 0, slide_ms=1000)
        self.assertEqual(status, 201)
        self.assertEqual(payload["slide_ms"], 1000)

    def test_slide_absent_by_default(self):
        status, payload = self.create("s", 1000, 100)
        self.assertEqual(status, 201)
        self.assertNotIn("slide_ms", payload)

    def test_slide_combines_with_other_options(self):
        status, payload = self.create(
            "s",
            1000,
            100,
            slide_ms=500,
            dedup_retention_ms=5000,
            auto_watermark_lag_ms=200,
        )
        self.assertEqual(status, 201)
        self.assertEqual(payload["slide_ms"], 500)
        self.assertEqual(payload["dedup_retention_ms"], 5000)
        self.assertEqual(payload["auto_watermark_lag_ms"], 200)

    def test_invalid_slide_rejected_without_partial_state(self):
        for slide in (0, -1, 1.5, "250", True, False):
            status, payload = self.create("s", 1000, 0, slide_ms=slide)
            self.assertEqual(status, 422, slide)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        # larger than the window or not dividing it evenly
        for slide in (1001, 2000, 300, 999):
            status, payload = self.create("s", 1000, 0, slide_ms=slide)
            self.assertEqual(status, 422, slide)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        # nothing was created and the name is reusable with a valid body
        status, payload = self.results("s")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")
        status, _ = self.create("s", 1000, 0, slide_ms=500)
        self.assertEqual(status, 201)


class SlidingAggregationTest(HttpTestCase):
    def test_event_counts_into_all_covering_windows(self):
        self.create("s", 1000, 0, slide_ms=250)
        status, payload = self.event(700, 5)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"stream": "s", "dropped": False})
        # 700 belongs to [-250,750), [0,1000), [250,1250), [500,1500)
        status, payload = self.watermark(749)
        self.assertEqual(payload["finalized"], [])
        status, payload = self.watermark(750)
        self.assertEqual(
            payload["finalized"],
            [{
                "stream": "s",
                "window_start_ms": -250,
                "window_end_ms": 750,
                "count": 1,
                "sum": 5,
            }],
        )
        status, payload = self.watermark(1500)
        starts = [row["window_start_ms"] for row in payload["finalized"]]
        self.assertEqual(starts, [0, 250, 500])
        for row in payload["finalized"]:
            self.assertEqual(row["count"], 1)
            self.assertEqual(row["sum"], 5)
            self.assertEqual(row["window_end_ms"], row["window_start_ms"] + 1000)
            self.assertEqual(row["stream"], "s")

    def test_negative_timestamps_follow_same_boundaries(self):
        self.create("s", 1000, 0, slide_ms=500)
        self.event(-300, 2)
        # -300 belongs to [-1000,0) and [-500,500)
        status, payload = self.watermark(0)
        self.assertEqual(
            payload["finalized"],
            [{
                "stream": "s",
                "window_start_ms": -1000,
                "window_end_ms": 0,
                "count": 1,
                "sum": 2,
            }],
        )
        status, payload = self.watermark(500)
        self.assertEqual(
            [row["window_start_ms"] for row in payload["finalized"]], [-500]
        )

    def test_boundary_timestamp_belongs_to_window_starting_at_it(self):
        self.create("s", 1000, 0, slide_ms=250)
        self.event(250, 1)
        # 250 belongs to [-500,500), [-250,750), [0,1000), [250,1250)
        status, payload = self.watermark(500)
        self.assertEqual(
            [row["window_start_ms"] for row in payload["finalized"]], [-500]
        )
        status, payload = self.watermark(1250)
        self.assertEqual(
            [row["window_start_ms"] for row in payload["finalized"]],
            [-250, 0, 250],
        )

    def test_finalization_respects_allowed_lateness_per_window(self):
        self.create("s", 1000, 100, slide_ms=500)
        self.event(100, 1)   # windows [-500,500) and [0,1000)
        self.event(600, 2)   # windows [0,1000) and [500,1500)
        status, payload = self.watermark(599)
        self.assertEqual(payload["finalized"], [])
        # [-500,500) closes at 600, [0,1000) closes at 1100
        status, payload = self.watermark(1099)
        self.assertEqual(
            payload["finalized"],
            [{
                "stream": "s",
                "window_start_ms": -500,
                "window_end_ms": 500,
                "count": 1,
                "sum": 1,
            }],
        )
        status, payload = self.watermark(1100)
        self.assertEqual(
            payload["finalized"],
            [{
                "stream": "s",
                "window_start_ms": 0,
                "window_end_ms": 1000,
                "count": 2,
                "sum": 3,
            }],
        )
        # [500,1500) closes at 1600
        status, payload = self.watermark(1600)
        self.assertEqual(
            payload["finalized"],
            [{
                "stream": "s",
                "window_start_ms": 500,
                "window_end_ms": 1500,
                "count": 1,
                "sum": 2,
            }],
        )

    def test_each_window_finalized_exactly_once(self):
        self.create("s", 1000, 0, slide_ms=500)
        self.event(100, 1)
        self.watermark(1000)
        status, payload = self.watermark(1000)
        self.assertEqual(payload["finalized"], [])
        status, payload = self.watermark(5000)
        self.assertEqual(payload["finalized"], [])
        _, payload = self.results()
        starts = [row["window_start_ms"] for row in payload["results"]]
        self.assertEqual(starts, [-500, 0])
        _, again = self.results()
        self.assertEqual(payload, again)

    def test_late_event_dropped_without_touching_windows(self):
        self.create("s", 1000, 100, slide_ms=500)
        self.event(100, 1)
        self.watermark(2000)  # lateness horizon: 1900
        status, payload = self.event(1899, 9)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"stream": "s", "dropped": True})
        status, payload = self.event(1900, 2)
        self.assertEqual(payload["dropped"], False)
        self.watermark(2600)  # finalizes every window touched above
        _, payload = self.results()
        by_start = {row["window_start_ms"]: row for row in payload["results"]}
        # the dropped 9 never landed anywhere
        self.assertEqual(by_start[-500]["sum"], 1)
        self.assertEqual(by_start[0]["sum"], 1)
        self.assertEqual(by_start[1000]["sum"], 2)
        self.assertEqual(by_start[1500]["sum"], 2)

    def test_results_only_expose_finalized_windows(self):
        self.create("s", 1000, 0, slide_ms=500)
        self.event(100, 1)
        _, payload = self.results()
        self.assertEqual(payload["results"], [])
        self.watermark(500)
        _, payload = self.results()
        self.assertEqual(
            [row["window_start_ms"] for row in payload["results"]], [-500]
        )

    def test_streams_remain_isolated(self):
        self.create("a", 1000, 0, slide_ms=500)
        self.create("b", 1000, 0)
        self.event(100, 1, stream="a")
        self.event(100, 2, stream="b")
        self.watermark(1000, stream="a")
        self.watermark(1000, stream="b")
        _, pa = self.results("a")
        _, pb = self.results("b")
        self.assertEqual(
            [row["window_start_ms"] for row in pa["results"]], [-500, 0]
        )
        self.assertEqual(len(pb["results"]), 1)
        self.assertEqual(pb["results"][0]["sum"], 2)


class SlidingDedupTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create("s", 1000, 100, slide_ms=500, dedup_retention_ms=5000)

    def test_exact_retry_is_duplicate_across_all_windows(self):
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload, {
            "stream": "s", "dropped": False, "duplicate": False,
        })
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload, {
            "stream": "s", "dropped": False, "duplicate": True,
        })
        self.watermark(1100)
        _, payload = self.results()
        by_start = {row["window_start_ms"]: row for row in payload["results"]}
        self.assertEqual(by_start[-500]["count"], 1)
        self.assertEqual(by_start[-500]["sum"], 5)
        self.assertEqual(by_start[0]["count"], 1)
        self.assertEqual(by_start[0]["sum"], 5)

    def test_conflicting_retry_rejected(self):
        self.event(100, 5, event_id="e1")
        status, payload = self.event(101, 5, event_id="e1")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")
        status, payload = self.event(100, 6, event_id="e1")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")

    def test_dedup_check_precedes_lateness_check(self):
        self.event(100, 5, event_id="e1")
        self.watermark(2000)  # lateness horizon 1900, retention horizon -3000
        # retained id: exact retry is a duplicate even past the horizon
        status, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], True)
        # unseen id too late: dropped and not remembered
        status, payload = self.event(100, 5, event_id="e2")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)
        status, payload = self.event(100, 5, event_id="e2")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)

    def test_eviction_boundary_unchanged(self):
        self.event(100, 5, event_id="e1")
        # wm 5100 -> retention horizon 100: e1 on the boundary stays
        self.watermark(5100)
        _, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload["duplicate"], True)
        # wm 5101 evicts it; reuse is a new (here: too late) event
        self.watermark(5101)
        _, payload = self.event(100, 5, event_id="e1")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)


class SlidingAutoWatermarkTest(HttpTestCase):
    def test_accepted_events_advance_and_finalize_overlapping_windows(self):
        self.create("s", 1000, 0, slide_ms=500, auto_watermark_lag_ms=0)
        status, payload = self.event(100, 1)
        self.assertEqual(payload["watermark_ms"], 100)
        self.assertEqual(payload["finalized"], [])
        self.assertEqual(
            set(payload), {"stream", "dropped", "watermark_ms", "finalized"}
        )
        status, payload = self.event(1100, 2)
        self.assertEqual(payload["watermark_ms"], 1100)
        self.assertEqual(
            [row["window_start_ms"] for row in payload["finalized"]], [-500, 0]
        )
        for row in payload["finalized"]:
            self.assertEqual(row["count"], 1)
            self.assertEqual(row["sum"], 1)

    def test_dropped_events_do_not_advance(self):
        self.create("s", 1000, 0, slide_ms=500, auto_watermark_lag_ms=0)
        self.event(1100, 1)
        status, payload = self.event(50, 1)
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["watermark_ms"], 1100)
        self.assertEqual(payload["finalized"], [])

    def test_manual_advance_still_works_and_never_regresses(self):
        self.create("s", 1000, 0, slide_ms=500, auto_watermark_lag_ms=10)
        self.event(100, 1)
        status, payload = self.watermark(5000)
        self.assertEqual(status, 200)
        self.assertEqual(payload["watermark_ms"], 5000)
        self.assertEqual(
            [row["window_start_ms"] for row in payload["finalized"]], [-500, 0]
        )
        # automatic advance never falls below the manual watermark
        status, payload = self.event(600, 1)
        self.assertEqual(payload["watermark_ms"], 5000)
        self.assertEqual(payload["finalized"], [])


class SlidingSnapshotTest(HttpTestCase):
    def _build_state(self):
        self.create(
            "slide",
            1000,
            100,
            slide_ms=500,
            dedup_retention_ms=5000,
            auto_watermark_lag_ms=200,
        )
        self.create("tumbler", 1000, 0)
        self.event(100, 1, event_id="a", stream="slide")
        self.event(1300, 2, event_id="b", stream="slide")
        self.event(100, 7, stream="tumbler")
        self.watermark(1000, stream="tumbler")

    def test_export_carries_slide_ms_only_for_sliding_streams(self):
        self._build_state()
        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        by_name = {s["name"]: s for s in payload["streams"]}
        self.assertEqual(by_name["slide"]["slide_ms"], 500)
        self.assertNotIn("slide_ms", by_name["tumbler"])
        # overlapping open windows are all exported, ordered by start
        starts = [w["window_start_ms"] for w in by_name["slide"]["windows"]]
        self.assertEqual(starts, sorted(starts))
        self.assertEqual(len(starts), len(set(starts)))

    def test_round_trip_preserves_state_and_continues(self):
        self._build_state()
        _, before_results = self.results("slide")
        _, snap = self.snapshot()

        Handler.service = Service()
        status, payload = self.restore(snap)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 2})
        self.assertEqual(self.results("slide")[1], before_results)
        # re-export without writes is identical
        _, again = self.snapshot()
        self.assertEqual(
            json.dumps(again, sort_keys=True), json.dumps(snap, sort_keys=True)
        )
        # dedup memory survived
        _, payload = self.event(100, 1, event_id="a", stream="slide")
        self.assertEqual(payload["duplicate"], True)
        # automatic advance keeps working: 1800 - 200 = 1600 closes [500,1500)
        status, payload = self.event(1800, 3, event_id="c", stream="slide")
        self.assertEqual(payload["watermark_ms"], 1600)
        self.assertEqual(
            [row["window_start_ms"] for row in payload["finalized"]], [500]
        )
        self.assertEqual(payload["finalized"][0]["count"], 1)
        self.assertEqual(payload["finalized"][0]["sum"], 2)

    def test_document_without_slide_ms_restores_as_tumbling(self):
        doc = {
            "format_version": 1,
            "streams": [{
                "name": "s",
                "window_ms": 1000,
                "allowed_lateness_ms": 0,
                "dedup_retention_ms": None,
                "watermark_ms": None,
                "windows": [
                    {"window_start_ms": 0, "window_end_ms": 1000,
                     "count": 1, "sum": 5},
                ],
                "finalized": [],
            }],
        }
        status, payload = self.restore(doc)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"restored_streams": 1})
        # tumbling: 700 lands in [0,1000) only
        status, payload = self.event(700, 1)
        self.assertEqual(payload["dropped"], False)
        _, payload = self.snapshot()
        stream = payload["streams"][0]
        self.assertNotIn("slide_ms", stream)
        self.assertEqual(len(stream["windows"]), 1)
        self.assertEqual(stream["windows"][0]["count"], 2)


class SlidingRestoreFailureTest(HttpTestCase):
    def _stream(self, **overrides):
        entry = {
            "name": "s",
            "window_ms": 1000,
            "allowed_lateness_ms": 0,
            "dedup_retention_ms": None,
            "watermark_ms": None,
            "slide_ms": 500,
            "windows": [],
            "finalized": [],
        }
        entry.update(overrides)
        return entry

    def snap(self, *streams):
        return {"format_version": 1, "streams": list(streams)}

    def assert_invalid(self, doc):
        status, payload = self.restore(doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        # instance must remain completely empty
        _, current = self.snapshot()
        self.assertEqual(current, {"format_version": 1, "streams": []})

    def test_slide_ms_constraints_validated(self):
        for slide in (0, -5, 1.5, "500", True, 1500, 300):
            self.assert_invalid(self.snap(self._stream(slide_ms=slide)))

    def test_window_alignment_and_width_validated(self):
        # start not a multiple of slide_ms
        self.assert_invalid(self.snap(self._stream(windows=[{
            "window_start_ms": 100, "window_end_ms": 1100, "count": 1, "sum": 1,
        }])))
        # width must stay window_ms even when aligned to slide_ms
        self.assert_invalid(self.snap(self._stream(windows=[{
            "window_start_ms": 500, "window_end_ms": 1400, "count": 1, "sum": 1,
        }])))
        # aligned to slide_ms but not to window_ms is fine for sliding
        status, _ = self.restore(self.snap(self._stream(windows=[{
            "window_start_ms": 500, "window_end_ms": 1500, "count": 1, "sum": 1,
        }])))
        self.assertEqual(status, 200)

    def test_window_ordering_and_duplicates_validated(self):
        w1 = {"window_start_ms": 500, "window_end_ms": 1500, "count": 1, "sum": 1}
        w2 = {"window_start_ms": 0, "window_end_ms": 1000, "count": 1, "sum": 1}
        self.assert_invalid(self.snap(self._stream(windows=[w1, w2])))
        self.assert_invalid(self.snap(self._stream(windows=[w1, dict(w1)])))
        f1 = dict(w1, stream="s")
        f2 = dict(w2, stream="s")
        self.assert_invalid(self.snap(self._stream(
            watermark_ms=10**9, finalized=[f1, f2],
        )))
        self.assert_invalid(self.snap(self._stream(
            watermark_ms=10**9, finalized=[f1, dict(f1)],
        )))

    def test_dedup_records_checked_against_sliding_windows(self):
        self.create("s", 1000, 0, slide_ms=500, dedup_retention_ms=5000)
        self.event(100, 1, event_id="a")
        _, snap = self.snapshot()
        Handler.service = Service()
        status, _ = self.restore(snap)
        self.assertEqual(status, 200)
        # a record whose timestamp falls into no known window is invalid
        snap["streams"][0]["dedup_records"][0]["timestamp_ms"] = 4600
        Handler.service = Service()
        self.assert_invalid(snap)

    def test_auto_max_timestamp_must_hit_known_sliding_windows(self):
        entry = self._stream(
            auto_watermark_lag_ms=0,
            max_event_timestamp_ms=4600,
            watermark_ms=4600,
            windows=[{
                "window_start_ms": 4500, "window_end_ms": 5500,
                "count": 1, "sum": 1,
            }],
        )
        # 4600 also belongs to [4000,5000), which is missing
        self.assert_invalid(self.snap(entry))
        entry["windows"].insert(0, {
            "window_start_ms": 4000, "window_end_ms": 5000,
            "count": 1, "sum": 1,
        })
        status, _ = self.restore(self.snap(entry))
        self.assertEqual(status, 200)


if __name__ == "__main__":
    unittest.main()
