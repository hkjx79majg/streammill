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
        status, payload = self.create("a", 1000, 100, slide_ms=250)
        self.assertEqual(status, 201)
        self.assertEqual(payload["slide_ms"], 250)
        self.assertEqual(payload["window_ms"], 1000)

    def test_slide_equal_to_window_accepted(self):
        status, payload = self.create("a", 1000, 0, slide_ms=1000)
        self.assertEqual(status, 201)
        self.assertEqual(payload["slide_ms"], 1000)

    def test_slide_absent_by_default(self):
        status, payload = self.create("plain", 1000, 100)
        self.assertEqual(status, 201)
        self.assertNotIn("slide_ms", payload)
        self.event(0, 1, stream="plain")
        _, snap = self.snapshot()
        self.assertNotIn("slide_ms", snap["streams"][0])

    def test_invalid_slide_rejected_without_partial_state(self):
        for slide in (0, -1, 1.5, "250", True, False, None):
            status, payload = self.create("a", 1000, 100, slide_ms=slide)
            self.assertEqual(status, 422, slide)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        # larger than the window
        status, payload = self.create("a", 1000, 100, slide_ms=1001)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # does not evenly divide the window
        status, payload = self.create("a", 1000, 100, slide_ms=300)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # an undeclared field is still rejected
        status, payload = self.request(
            "POST",
            "/streams",
            {
                "name": "a",
                "window_ms": 1000,
                "allowed_lateness_ms": 0,
                "slide_ms": 250,
                "bogus": 1,
            },
        )
        self.assertEqual(status, 422)
        # nothing was created and the name is reusable with a valid body
        status, _ = self.request("GET", "/streams/a/results")
        self.assertEqual(status, 404)
        status, _ = self.create("a", 1000, 100, slide_ms=250)
        self.assertEqual(status, 201)


class SlidingWindowBehaviorTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        # window 1000, step 250, allowed lateness 100
        self.create("s", 1000, 100, slide_ms=250)

    def test_event_counts_into_every_overlapping_window(self):
        # t=400 covers starts -500..250; t=600 covers starts -250..500
        self.event(400, 2)
        self.event(600, 4)
        status, payload = self.watermark(10**9)
        self.assertEqual(status, 200)
        rows = {r["window_start_ms"]: r for r in payload["finalized"]}
        self.assertEqual(set(rows), {-500, -250, 0, 250, 500})
        self.assertEqual(rows[-500]["count"], 1)
        self.assertEqual(rows[-500]["sum"], 2)
        for start in (-250, 0, 250):
            self.assertEqual(rows[start]["count"], 2, start)
            self.assertEqual(rows[start]["sum"], 6, start)
        self.assertEqual(rows[500]["count"], 1)
        self.assertEqual(rows[500]["sum"], 4)
        # shapes and ordering by window start
        starts = [r["window_start_ms"] for r in payload["finalized"]]
        self.assertEqual(starts, sorted(starts))
        for row in payload["finalized"]:
            self.assertEqual(
                set(row),
                {"stream", "window_start_ms", "window_end_ms", "count", "sum"},
            )
            self.assertEqual(row["window_end_ms"], row["window_start_ms"] + 1000)
            self.assertEqual(row["stream"], "s")

    def test_negative_timestamps_follow_the_same_grid(self):
        # t=-100 covers starts -1000, -750, -500, -250 (and NOT 0)
        self.event(-100, 3)
        status, payload = self.watermark(10**9)
        self.assertEqual(status, 200)
        starts = [r["window_start_ms"] for r in payload["finalized"]]
        self.assertEqual(starts, [-1000, -750, -500, -250])
        for row in payload["finalized"]:
            self.assertEqual(row["count"], 1)
            self.assertEqual(row["sum"], 3)

    def test_left_closed_right_open_boundaries(self):
        # t=0 belongs to windows starting -750..0; t=250 to -500..250
        self.event(0, 1)
        self.event(250, 2)
        self.watermark(10**9)
        _, payload = self.results()
        rows = {r["window_start_ms"]: r for r in payload["results"]}
        self.assertEqual(set(rows), {-750, -500, -250, 0, 250})
        self.assertEqual(rows[-750]["count"], 1)
        self.assertEqual(rows[-750]["sum"], 1)
        self.assertEqual(rows[0]["count"], 2)
        self.assertEqual(rows[0]["sum"], 3)
        self.assertEqual(rows[250]["count"], 1)
        self.assertEqual(rows[250]["sum"], 2)

    def test_windows_finalize_independently_at_their_own_boundary(self):
        # event at 400 -> windows starting -500..250, ending 500..1250
        self.event(400, 1)
        # lateness 100: the window ending 500 finalizes at wm 600
        status, payload = self.watermark(599)
        self.assertEqual(payload["finalized"], [])
        status, payload = self.watermark(600)
        self.assertEqual([r["window_start_ms"] for r in payload["finalized"]], [-500])

        # each further step finalizes exactly one more window
        for wm, start in ((850, -250), (1100, 0), (1350, 250)):
            status, payload = self.watermark(wm)
            self.assertEqual(
                [r["window_start_ms"] for r in payload["finalized"]], [start], wm
            )
        # idempotent repeat finalizes nothing
        status, payload = self.watermark(1350)
        self.assertEqual(payload["finalized"], [])
        _, payload = self.results()
        self.assertEqual(
            [r["window_start_ms"] for r in payload["results"]], [-500, -250, 0, 250]
        )

    def test_late_event_touches_no_overlapping_window(self):
        self.event(5000, 1)
        self.watermark(6000)  # lateness horizon: 5900
        status, payload = self.event(5899, 7)
        self.assertEqual(status, 200)
        self.assertEqual(payload["dropped"], True)
        # the boundary event is accepted and fans out over overlapping windows
        status, payload = self.event(5900, 7)
        self.assertEqual(payload["dropped"], False)
        self.watermark(10**9)
        _, payload = self.results()
        rows = {r["window_start_ms"]: r for r in payload["results"]}
        # t=5000 alone covers 4250..5000; t=5900 alone covers 5250..5750
        for start in (4250, 4500, 4750):
            self.assertEqual(rows[start]["count"], 1)
            self.assertEqual(rows[start]["sum"], 1)
        self.assertEqual(rows[5000]["count"], 2)
        self.assertEqual(rows[5000]["sum"], 8)
        for start in (5250, 5500, 5750):
            self.assertEqual(rows[start]["count"], 1)
            self.assertEqual(rows[start]["sum"], 7)

    def test_slide_equal_window_matches_tumbling(self):
        self.create("t", 1000, 0, slide_ms=1000)
        self.event(0, 1, stream="t")
        self.event(999, 2, stream="t")
        self.event(1000, 5, stream="t")
        self.watermark(10**9, stream="t")
        _, payload = self.results("t")
        self.assertEqual(
            payload["results"],
            [
                {"stream": "t", "window_start_ms": 0, "window_end_ms": 1000,
                 "count": 2, "sum": 3},
                {"stream": "t", "window_start_ms": 1000, "window_end_ms": 2000,
                 "count": 1, "sum": 5},
            ],
        )


class SlidingDedupTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create("s", 1000, 0, slide_ms=250, dedup_retention_ms=5000)

    def event(self, ts, value, event_id):  # type: ignore[override]
        return super().event(ts, value, event_id=event_id)

    def test_duplicate_counts_in_no_window_again(self):
        self.event(500, 2, "e1")
        status, payload = self.event(500, 2, "e1")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], True)
        self.watermark(10**9)
        _, payload = self.results()
        for row in payload["results"]:
            self.assertEqual(row["count"], 1)
            self.assertEqual(row["sum"], 2)

    def test_conflict_leaves_every_window_untouched(self):
        self.event(500, 2, "e1")
        status, payload = self.event(500, 3, "e1")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")
        self.watermark(10**9)
        _, payload = self.results()
        self.assertTrue(all(r["sum"] == 2 for r in payload["results"]))

    def test_unseen_too_late_event_is_not_remembered(self):
        self.event(9000, 1, "e0")
        self.watermark(9000)
        # horizon 4000 (retention 5000): ts 3999 is too late
        status, payload = self.event(3999, 1, "late")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)
        status, payload = self.event(3999, 1, "late")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)
        _, snap = self.snapshot()
        ids = [r["event_id"] for r in snap["streams"][0]["dedup_records"]]
        self.assertNotIn("late", ids)

    def test_retention_eviction_boundary(self):
        self.event(100, 1, "e1")
        # wm 5100 -> horizon 100, e1 on the boundary stays
        self.watermark(5100)
        status, payload = self.event(100, 1, "e1")
        self.assertEqual(payload["duplicate"], True)
        self.watermark(5101)
        # evicted; the old timestamp is now far too late
        status, payload = self.event(100, 1, "e1")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)


class SlidingAutoWatermarkTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create("s", 1000, 100, slide_ms=250, auto_watermark_lag_ms=200)

    def test_first_accepted_event_advances_and_finalizes_per_window(self):
        # first event covers windows starting -500..250 (ending 500..1250);
        # 400 - 200 = 200 reaches none of them yet
        status, payload = self.event(400, 1)
        self.assertEqual(payload["watermark_ms"], 200)
        self.assertEqual(payload["finalized"], [])
        # a later event pushes the watermark to 650: only the oldest window
        # (start -500, end 500, threshold 600) finalizes
        status, payload = self.event(850, 1)
        self.assertEqual(payload["watermark_ms"], 650)
        self.assertEqual(
            [r["window_start_ms"] for r in payload["finalized"]], [-500]
        )

    def test_duplicates_and_drops_never_advance(self):
        self.create(
            "d", 1000, 0, slide_ms=250,
            dedup_retention_ms=9000, auto_watermark_lag_ms=0,
        )
        self.event(500, 1, event_id="e1", stream="d")
        status, payload = self.event(500, 1, event_id="e1", stream="d")
        self.assertEqual(payload["duplicate"], True)
        self.assertEqual(payload["watermark_ms"], 500)
        self.assertEqual(payload["finalized"], [])
        # horizon 500 - 0 = 500; an unseen id at 499 is dropped
        status, payload = self.event(499, 1, event_id="e2", stream="d")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["watermark_ms"], 500)
        self.assertEqual(payload["finalized"], [])
        # conflict changes nothing either
        status, payload = self.event(600, 2, event_id="e1", stream="d")
        self.assertEqual(status, 409)
        _, snap = self.snapshot()
        dedup_stream = next(s for s in snap["streams"] if s["name"] == "d")
        self.assertEqual(dedup_stream["max_event_timestamp_ms"], 500)

    def test_round_trip_matches_uninterrupted(self):
        script = [(500, 1), (850, 2), (600, 3), (2000, 4), (1500, 5)]
        for ts, value in script:
            self.event(ts, value)
        _, snap = self.snapshot()
        Handler.service = Service()
        self.assertEqual(self.restore(snap)[0], 200)
        continuation = [(2100, 6), (5000, 7)]
        for ts, value in continuation:
            self.event(ts, value)
        _, results_after = self.results()
        _, snap_after = self.snapshot()

        Handler.service = Service()
        self.create("s", 1000, 100, slide_ms=250, auto_watermark_lag_ms=200)
        for ts, value in [*script, *continuation]:
            self.event(ts, value)
        _, results_ref = self.results()
        _, snap_ref = self.snapshot()

        self.assertEqual(results_after, results_ref)
        self.assertEqual(snap_after, snap_ref)


class SlidingSnapshotTest(HttpTestCase):
    def test_export_shape(self):
        self.create("slide", 1000, 100, slide_ms=250)
        self.create("tumble", 1000, 100)
        self.event(500, 1, stream="slide")
        _, payload = self.snapshot()
        by_name = {s["name"]: s for s in payload["streams"]}
        self.assertEqual(by_name["slide"]["slide_ms"], 250)
        self.assertNotIn("slide_ms", by_name["tumble"])
        starts = [w["window_start_ms"] for w in by_name["slide"]["windows"]]
        self.assertEqual(starts, [-250, 0, 250, 500])
        self.assertTrue(all(s % 250 == 0 for s in starts))

    def test_reexport_identical(self):
        self.create("s", 1000, 100, slide_ms=250,
                    dedup_retention_ms=5000, auto_watermark_lag_ms=500)
        self.event(500, 2, event_id="e1", stream="s")
        self.event(900, 3, event_id="e2", stream="s")
        self.watermark(700, stream="s")
        _, first = self.snapshot()
        Handler.service = Service()
        status, _ = self.restore(first)
        self.assertEqual(status, 200)
        _, second = self.snapshot()
        self.assertEqual(second, first)
        self.assertEqual(
            json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True)
        )

    def test_old_document_without_slide_restores_as_tumbling(self):
        doc = {
            "format_version": 1,
            "streams": [{
                "name": "s",
                "window_ms": 1000,
                "allowed_lateness_ms": 0,
                "dedup_retention_ms": None,
                "watermark_ms": None,
                "windows": [],
                "finalized": [],
            }],
        }
        status, _ = self.restore(doc)
        self.assertEqual(status, 200)
        status, payload = self.event(250, 1)
        self.assertEqual(status, 200)
        self.assertEqual(set(payload), {"stream", "dropped"})
        _, snap = self.snapshot()
        self.assertNotIn("slide_ms", snap["streams"][0])
        # tumbling: the event aggregates into exactly one window
        self.assertEqual(
            [w["window_start_ms"] for w in snap["streams"][0]["windows"]], [0]
        )

    def test_continue_after_restore(self):
        self.create("s", 1000, 0, slide_ms=250, dedup_retention_ms=9000)
        self.event(500, 2, event_id="e1", stream="s")
        self.watermark(750, stream="s")  # finalizes start -250 (end 750)
        _, snap = self.snapshot()
        Handler.service = Service()
        self.restore(snap)

        # retained id still deduped across every overlapping window
        status, payload = self.event(500, 2, event_id="e1", stream="s")
        self.assertEqual(payload["duplicate"], True)
        status, payload = self.event(750, 4, event_id="e2", stream="s")
        self.assertEqual(payload["dropped"], False)
        self.watermark(10**9, stream="s")
        _, payload = self.results("s")
        rows = {r["window_start_ms"]: r for r in payload["results"]}
        # start -250 was finalized before restore holding just e1 (sum 2)
        self.assertEqual(rows[-250]["sum"], 2)
        self.assertEqual(rows[-250]["count"], 1)
        # windows 0/250/500 hold both events; 750 only e2
        for start in (0, 250, 500):
            self.assertEqual(rows[start]["count"], 2, start)
            self.assertEqual(rows[start]["sum"], 6, start)
        self.assertEqual(rows[750]["count"], 1)
        self.assertEqual(rows[750]["sum"], 4)


class SlidingSnapshotValidationTest(HttpTestCase):
    def base_sliding(self, **overrides):
        entry = {
            "name": "s",
            "window_ms": 1000,
            "slide_ms": 250,
            "allowed_lateness_ms": 0,
            "dedup_retention_ms": None,
            "watermark_ms": None,
            "windows": [],
            "finalized": [],
        }
        entry.update(overrides)
        return entry

    def snap(self, entry):
        return {"format_version": 1, "streams": [entry]}

    def assert_invalid(self, doc):
        status, payload = self.restore(doc)
        self.assertEqual(status, 422, doc)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")
        _, current = self.snapshot()
        self.assertEqual(current, {"format_version": 1, "streams": []})

    def test_slide_config_validation(self):
        for slide in (0, -250, 1.5, "250", True, None, 1001, 300):
            self.assert_invalid(self.snap(self.base_sliding(slide_ms=slide)))

    def test_window_alignment_and_shape_validation(self):
        # start off the 250 grid
        self.assert_invalid(self.snap(self.base_sliding(
            windows=[{"window_start_ms": 100, "window_end_ms": 1100,
                      "count": 1, "sum": 1}],
        )))
        # wrong width
        self.assert_invalid(self.snap(self.base_sliding(
            windows=[{"window_start_ms": 0, "window_end_ms": 999,
                      "count": 1, "sum": 1}],
        )))
        # duplicate windows
        row = {"window_start_ms": 0, "window_end_ms": 1000, "count": 1, "sum": 1}
        self.assert_invalid(self.snap(self.base_sliding(
            windows=[row, dict(row)],
        )))
        # wrong ordering
        self.assert_invalid(self.snap(self.base_sliding(
            windows=[
                {"window_start_ms": 250, "window_end_ms": 1250, "count": 1, "sum": 1},
                {"window_start_ms": 0, "window_end_ms": 1000, "count": 1, "sum": 1},
            ],
        )))
        # aligned 250-grid window restores fine
        status, _ = self.restore(self.snap(self.base_sliding(
            windows=[{"window_start_ms": -250, "window_end_ms": 750,
                      "count": 1, "sum": 1}],
        )))
        self.assertEqual(status, 200)

    def test_finalized_window_on_slide_grid(self):
        finalized = [{
            "stream": "s",
            "window_start_ms": 250,
            "window_end_ms": 1250,
            "count": 1,
            "sum": 1,
        }]
        status, _ = self.restore(self.snap(self.base_sliding(
            watermark_ms=1250, finalized=finalized,
        )))
        self.assertEqual(status, 200)
        Handler.service = Service()
        # because slide_ms divides window_ms every window-aligned start is
        # also slide-aligned; an arbitrary off-grid start is still rejected
        bad = [dict(finalized[0], window_start_ms=100, window_end_ms=1100)]
        self.assert_invalid(self.snap(self.base_sliding(
            watermark_ms=1100, finalized=bad,
        )))

    def test_dedup_record_must_cover_only_known_windows(self):
        # One retained id at 500 covers starts -250..500; all must exist.
        windows = [
            {"window_start_ms": start, "window_end_ms": start + 1000,
             "count": 1, "sum": 1}
            for start in (-250, 0, 250, 500)
        ]
        # wm 400 keeps every window open (the earliest ends at 750)
        good = self.base_sliding(
            dedup_retention_ms=9000,
            watermark_ms=400,
            windows=windows,
            finalized=[],
            dedup_records=[{"event_id": "e1", "timestamp_ms": 500, "value": 1}],
        )
        status, _ = self.restore(self.snap(good))
        self.assertEqual(status, 200)
        Handler.service = Service()
        # drop one of the overlapping windows: record aggregates into it
        self.assert_invalid(self.snap(self.base_sliding(
            dedup_retention_ms=9000,
            watermark_ms=400,
            windows=[w for w in windows if w["window_start_ms"] != 0],
            finalized=[],
            dedup_records=[{"event_id": "e1", "timestamp_ms": 500, "value": 1}],
        )))
        # two ids at 500 but each window count is 1: over-count
        self.assert_invalid(self.snap(self.base_sliding(
            dedup_retention_ms=9000,
            watermark_ms=400,
            windows=windows,
            finalized=[],
            dedup_records=[
                {"event_id": "e1", "timestamp_ms": 500, "value": 1},
                {"event_id": "e2", "timestamp_ms": 500, "value": 1},
            ],
        )))

    def test_auto_max_event_timestamp_covers_known_windows(self):
        # automatic sliding stream: max ts 500 covers four windows
        windows = [
            {"window_start_ms": start, "window_end_ms": start + 1000,
             "count": 1, "sum": 1}
            for start in (-250, 0, 250, 500)
        ]
        good = self.base_sliding(
            watermark_ms=300,
            windows=windows,
            auto_watermark_lag_ms=200,
            max_event_timestamp_ms=500,
        )
        status, _ = self.restore(self.snap(good))
        self.assertEqual(status, 200)
        Handler.service = Service()
        self.assert_invalid(self.snap(self.base_sliding(
            watermark_ms=300,
            windows=[w for w in windows if w["window_start_ms"] != 0],
            auto_watermark_lag_ms=200,
            max_event_timestamp_ms=500,
        )))


class SlidingConcurrencyTest(HttpTestCase):
    def test_concurrent_events_keep_windows_consistent(self):
        self.create("s", 200, 50, slide_ms=50, auto_watermark_lag_ms=0)
        threads_count = 8
        per_thread = 30

        def submit(worker):
            for i in range(per_thread):
                self.event(worker * 37 + i * 13, 1)

        threads = [
            threading.Thread(target=submit, args=(w,)) for w in range(threads_count)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        _, snap = self.snapshot()
        stream = snap["streams"][0]
        open_starts = [w["window_start_ms"] for w in stream["windows"]]
        final_starts = [w["window_start_ms"] for w in stream["finalized"]]
        self.assertEqual(open_starts, sorted(open_starts))
        self.assertEqual(final_starts, sorted(final_starts))
        self.assertFalse(set(open_starts) & set(final_starts))
        self.assertTrue(all(s % 50 == 0 for s in open_starts + final_starts))
        # the raced-upon snapshot restores cleanly
        Service().restore_snapshot(json.loads(json.dumps(snap)))


if __name__ == "__main__":
    unittest.main()
