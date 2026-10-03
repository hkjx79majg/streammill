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


class CreateAutoStreamTest(HttpTestCase):
    def test_lag_echoed_when_provided(self):
        status, payload = self.create("a", 1000, 100, auto_watermark_lag_ms=500)
        self.assertEqual(status, 201)
        self.assertEqual(payload["stream"], "a")
        self.assertEqual(payload["auto_watermark_lag_ms"], 500)
        # zero is a valid lag
        status, payload = self.create("z", 1000, 0, auto_watermark_lag_ms=0)
        self.assertEqual(status, 201)
        self.assertEqual(payload["auto_watermark_lag_ms"], 0)

    def test_lag_absent_on_manual_stream(self):
        status, payload = self.create("m", 1000, 100)
        self.assertEqual(status, 201)
        self.assertNotIn("auto_watermark_lag_ms", payload)

    def test_invalid_lag_rejected_without_partial_state(self):
        for lag in (-1, 1.5, "500", True, False):
            status, payload = self.create("a", 1000, 100, auto_watermark_lag_ms=lag)
            self.assertEqual(status, 422, lag)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        # an undeclared field is still rejected
        status, payload = self.request(
            "POST",
            "/streams",
            {
                "name": "a",
                "window_ms": 1000,
                "allowed_lateness_ms": 0,
                "auto_watermark_lag_ms": 1,
                "bogus": 1,
            },
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # nothing was created and the name is reusable with a valid body
        status, payload = self.request("GET", "/streams/a/results")
        self.assertEqual(status, 404)
        status, _ = self.create("a", 1000, 100, auto_watermark_lag_ms=0)
        self.assertEqual(status, 201)

    def test_manual_event_response_shape_unchanged(self):
        self.create("m", 1000, 100)
        status, payload = self.event(100, 1, stream="m")
        self.assertEqual(status, 200)
        self.assertEqual(set(payload), {"stream", "dropped"})


class AutoWatermarkBehaviorTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        # window 1000, allowed lateness 100, lag 200
        self.create("s", 1000, 100, auto_watermark_lag_ms=200)

    def test_watermark_tracks_max_event_timestamp_minus_lag(self):
        status, payload = self.event(500, 1)
        self.assertEqual(status, 200)
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["watermark_ms"], 300)
        self.assertEqual(payload["finalized"], [])
        self.assertEqual(
            set(payload), {"stream", "dropped", "watermark_ms", "finalized"}
        )

        # 1200 - 200 = 1000: window [0,1000) needs wm >= 1100, still open
        status, payload = self.event(1200, 2)
        self.assertEqual(payload["watermark_ms"], 1000)
        self.assertEqual(payload["finalized"], [])

        # 1300 - 200 = 1100: the first window finalizes exactly once here
        status, payload = self.event(1300, 3)
        self.assertEqual(payload["watermark_ms"], 1100)
        self.assertEqual(
            payload["finalized"],
            [{
                "stream": "s",
                "window_start_ms": 0,
                "window_end_ms": 1000,
                "count": 1,
                "sum": 1,
            }],
        )

        # another event at the same max timestamp: no regression, no repeat
        status, payload = self.event(1300, 4)
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["watermark_ms"], 1100)
        self.assertEqual(payload["finalized"], [])

        _, payload = self.results()
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(payload["results"][0]["count"], 1)
        self.assertEqual(payload["results"][0]["sum"], 1)

    def test_older_acceptable_event_does_not_regress_watermark(self):
        self.event(5000, 1)          # wm 4800
        self.event(4800, 2)         # older acceptable event, wm unchanged
        status, payload = self.event(4750, 3)  # >= horizon 4700: accepted
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["watermark_ms"], 4800)
        self.assertEqual(payload["finalized"], [])

    def test_late_event_uses_pre_event_watermark_and_changes_nothing(self):
        self.event(5000, 1)          # wm 4800; lateness horizon 4700
        status, payload = self.event(4699, 9)
        self.assertEqual(status, 200)
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["watermark_ms"], 4800)
        self.assertEqual(payload["finalized"], [])
        # boundary event is still accepted
        status, payload = self.event(4700, 9)
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["watermark_ms"], 4800)

        # climbing to wm 5700 finalizes the only populated eligible window
        # [4000,5000), holding just the boundary event at 4700; empty
        # windows never appear and the dropped 4699 never aggregated. The
        # events at 5000/5900 keep [5000,6000) open.
        status, payload = self.event(5900, 0)
        self.assertEqual(payload["watermark_ms"], 5700)
        self.assertEqual(
            [w["window_start_ms"] for w in payload["finalized"]], [4000]
        )
        row_4000 = payload["finalized"][0]
        self.assertEqual(row_4000["count"], 1)
        self.assertEqual(row_4000["sum"], 9)
        # a repeat of the late event is dropped again and finalizes nothing
        status, payload = self.event(4699, 9)
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["watermark_ms"], 5700)
        self.assertEqual(payload["finalized"], [])

    def test_fresh_stream_fields_before_first_event(self):
        # Before any accepted event the snapshot carries null watermark and
        # null max event timestamp; the first accepted event then publishes
        # the post-processing watermark in its response.
        Handler.service = Service()  # discard the stream created by setUp
        fresh = {
            "format_version": 1,
            "streams": [{
                "name": "fresh",
                "window_ms": 1000,
                "allowed_lateness_ms": 100,
                "dedup_retention_ms": None,
                "watermark_ms": None,
                "windows": [],
                "finalized": [],
                "auto_watermark_lag_ms": 200,
                "max_event_timestamp_ms": None,
            }],
        }
        status, _ = self.restore(fresh)
        self.assertEqual(status, 200)
        _, snap = self.snapshot()
        stream = snap["streams"][0]
        self.assertIsNone(stream["watermark_ms"])
        self.assertIsNone(stream["max_event_timestamp_ms"])
        status, payload = self.event(500, 1, stream="fresh")
        self.assertEqual(status, 200)
        self.assertIn("watermark_ms", payload)
        self.assertEqual(payload["watermark_ms"], 300)
        self.assertEqual(payload["finalized"], [])


class ManualWatermarkOnAutoStreamTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create("s", 1000, 100, auto_watermark_lag_ms=200)

    def test_manual_advance_and_later_auto_progress(self):
        self.event(500, 1)           # auto wm 300
        # manual push finalizes [0,1000) at wm 1100
        status, payload = self.watermark(1100)
        self.assertEqual(status, 200)
        self.assertEqual(
            [w["window_start_ms"] for w in payload["finalized"]], [0]
        )

        # an event whose auto target is below the manual wm cannot regress it
        status, payload = self.event(1200, 2)  # target 1000 < 1100
        self.assertEqual(payload["watermark_ms"], 1100)
        self.assertEqual(payload["finalized"], [])

        # regression and idempotency rules are unchanged
        status, payload = self.watermark(1099)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "watermark_regression")
        status, payload = self.watermark(1100)
        self.assertEqual(status, 200)
        self.assertEqual(payload["finalized"], [])

        # a newer event eventually pushes the automatic watermark forward
        status, payload = self.event(1500, 3)  # target 1300 > 1100
        self.assertEqual(payload["watermark_ms"], 1300)
        self.assertEqual(payload["finalized"], [])

        # manual advances keep working afterwards as well
        status, payload = self.watermark(2100)
        self.assertEqual(payload["watermark_ms"], 2100)
        rows = payload["finalized"]
        self.assertEqual([w["window_start_ms"] for w in rows], [1000])
        self.assertEqual(rows[0]["count"], 2)
        self.assertEqual(rows[0]["sum"], 5)


class AutoDedupInteractionTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create(
            "s", 1000, 100,
            dedup_retention_ms=5000, auto_watermark_lag_ms=200,
        )

    def event(self, ts, value, event_id):  # type: ignore[override]
        return super().event(ts, value, event_id=event_id)

    def test_duplicate_conflict_and_drop_change_nothing(self):
        status, payload = self.event(1000, 5, "e1")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], False)
        self.assertEqual(payload["watermark_ms"], 800)

        status, payload = self.event(1000, 5, "e1")
        self.assertEqual(payload["duplicate"], True)
        self.assertEqual(payload["watermark_ms"], 800)
        self.assertEqual(payload["finalized"], [])

        # conflict is rejected and leaves max timestamp / watermark untouched
        status, payload = self.event(1000, 6, "e1")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")
        _, snap = self.snapshot()
        stream = snap["streams"][0]
        self.assertEqual(stream["max_event_timestamp_ms"], 1000)
        self.assertEqual(stream["watermark_ms"], 800)

        # push the watermark far ahead, then an unseen id arrives too late
        self.event(6000, 1, "e2")     # wm 5800, lateness horizon 5700
        status, payload = self.event(5699, 1, "e3")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)
        self.assertEqual(payload["watermark_ms"], 5800)
        self.assertEqual(payload["finalized"], [])
        # e3 was never registered: resubmitting is a drop, not a duplicate
        status, payload = self.event(5699, 1, "e3")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)
        _, snap = self.snapshot()
        self.assertNotIn("e3", [r["event_id"] for r in snap["streams"][0]["dedup_records"]])

    def test_window_finalized_once_through_retries(self):
        self.event(500, 1, "e1")     # wm 300
        self.event(1300, 1, "e2")    # wm 1100 -> finalizes [0,1000)
        self.event(500, 1, "e1")     # exact retry: no repeat finalization
        self.event(1050, 2, "e3")    # older acceptable event; wm unchanged
        _, payload = self.results()
        self.assertEqual(
            [r["window_start_ms"] for r in payload["results"]], [0]
        )
        self.assertEqual(payload["results"][0]["count"], 1)
        self.assertEqual(payload["results"][0]["sum"], 1)


class AutoDedupEvictionTest(HttpTestCase):
    def test_auto_advance_evicts_retained_ids_at_retention_boundary(self):
        # lag 0 drives the watermark purely through accepted events;
        # retention 5000 evicts retained ids once ts < wm - 5000, and that
        # eviction must run on the automatic advance path as well.
        self.create(
            "s", 1000, 0,
            dedup_retention_ms=5000, auto_watermark_lag_ms=0,
        )

        def submit(ts, value, event_id):
            return self.event(ts, value, event_id=event_id, stream="s")

        submit(100, 5, "e1")          # wm 100
        submit(5100, 1, "e2")         # wm 5100, retention horizon 100: kept
        status, payload = submit(100, 5, "e1")
        self.assertEqual(payload["duplicate"], True)
        submit(5101, 1, "e3")         # wm 5101, horizon 101: e1 evicted
        # evicted id is treated as new; the old timestamp is now too late,
        # so it is dropped rather than answered as a duplicate
        status, payload = submit(100, 5, "e1")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)
        self.assertEqual(payload["watermark_ms"], 5101)
        self.assertEqual(payload["finalized"], [])


class AutoSnapshotExportTest(HttpTestCase):
    def test_fields_present_for_auto_and_absent_for_manual(self):
        self.create("a", 1000, 100, auto_watermark_lag_ms=200)
        self.create("m", 1000, 100)
        self.event(1000, 1, stream="a")
        _, payload = self.snapshot()
        by_name = {s["name"]: s for s in payload["streams"]}
        self.assertEqual(by_name["a"]["auto_watermark_lag_ms"], 200)
        self.assertEqual(by_name["a"]["max_event_timestamp_ms"], 1000)
        self.assertNotIn("auto_watermark_lag_ms", by_name["m"])
        self.assertNotIn("max_event_timestamp_ms", by_name["m"])

    def test_max_event_timestamp_null_before_accepted_events(self):
        self.create("a", 1000, 100, auto_watermark_lag_ms=200)
        _, payload = self.snapshot()
        stream = payload["streams"][0]
        self.assertEqual(stream["auto_watermark_lag_ms"], 200)
        self.assertIsNone(stream["max_event_timestamp_ms"])
        self.assertIsNone(stream["watermark_ms"])


class AutoSnapshotRoundTripTest(HttpTestCase):
    SCRIPT = [
        (1000, 1), (1200, 2), (1300, 3),   # finalizes [0,1000)
        (4750, 4),                          # older acceptable event
        (5000, 5),
    ]
    CONTINUATION = [(5300, 6), (2000, 7), (6300, 8)]

    def _play(self, events):
        for ts, value in events:
            self.event(ts, value, stream="s")

    def test_continue_after_restore_matches_uninterrupted_instance(self):
        # interrupted instance: build, export, restore, continue
        self.create("s", 1000, 100, auto_watermark_lag_ms=200)
        self._play(self.SCRIPT)
        _, snap = self.snapshot()
        Handler.service = Service()
        self.assertEqual(self.restore(snap)[0], 200)
        self._play(self.CONTINUATION)
        _, results_after = self.results()
        _, snap_after = self.snapshot()

        # uninterrupted reference instance replays the whole history
        Handler.service = Service()
        self.create("s", 1000, 100, auto_watermark_lag_ms=200)
        self._play([*self.SCRIPT, *self.CONTINUATION])
        _, results_ref = self.results()
        _, snap_ref = self.snapshot()

        self.assertEqual(results_after, results_ref)
        self.assertEqual(snap_after, snap_ref)

    def test_reexport_without_writes_is_identical(self):
        self.create("s", 1000, 100, auto_watermark_lag_ms=200)
        self.event(1300, 1)
        self.event(4000, 2)
        _, first = self.snapshot()
        Handler.service = Service()
        status, _ = self.restore(first)
        self.assertEqual(status, 200)
        _, second = self.snapshot()
        self.assertEqual(second, first)
        self.assertEqual(
            json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True)
        )

    def test_manual_watermark_with_null_max_round_trips(self):
        # A manual push before any accepted event leaves max null while the
        # watermark is non-null; export, restore and later auto progress must
        # all keep that state consistent.
        self.create("s", 1000, 100, auto_watermark_lag_ms=200)
        status, payload = self.watermark(900)
        self.assertEqual(status, 200)
        self.assertEqual(payload["finalized"], [])
        _, first = self.snapshot()
        stream = first["streams"][0]
        self.assertEqual(stream["watermark_ms"], 900)
        self.assertIsNone(stream["max_event_timestamp_ms"])
        self.assertEqual(stream["windows"], [])

        Handler.service = Service()
        self.assertEqual(self.restore(first)[0], 200)
        _, second = self.snapshot()
        self.assertEqual(second, first)

        # an event whose auto target is below the manual wm does not move it
        status, payload = self.event(900, 1)  # bucket [0,1000), target 700 < 900
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["watermark_ms"], 900)
        self.assertEqual(payload["finalized"], [])
        _, snap = self.snapshot()
        self.assertEqual(snap["streams"][0]["max_event_timestamp_ms"], 900)
        self.assertEqual(snap["streams"][0]["watermark_ms"], 900)
        # a newer event pushes the watermark and finalizes [0,1000)
        status, payload = self.event(1300, 2)  # target 1100 > 900
        self.assertEqual(payload["watermark_ms"], 1100)
        self.assertEqual(
            [w["window_start_ms"] for w in payload["finalized"]], [0]
        )

    def test_old_manual_document_restores_as_manual(self):
        manual_doc = {
            "format_version": 1,
            "streams": [{
                "name": "s",
                "window_ms": 1000,
                "allowed_lateness_ms": 100,
                "dedup_retention_ms": None,
                "watermark_ms": None,
                "windows": [],
                "finalized": [],
            }],
        }
        status, _ = self.restore(manual_doc)
        self.assertEqual(status, 200)
        status, payload = self.event(100, 1)
        self.assertEqual(status, 200)
        self.assertEqual(set(payload), {"stream", "dropped"})
        _, snap = self.snapshot()
        self.assertNotIn("auto_watermark_lag_ms", snap["streams"][0])
        self.assertNotIn("max_event_timestamp_ms", snap["streams"][0])


class AutoSnapshotValidationTest(HttpTestCase):
    def base_auto(self, **overrides):
        entry = {
            "name": "s",
            "window_ms": 1000,
            "allowed_lateness_ms": 100,
            "dedup_retention_ms": None,
            "watermark_ms": 1100,
            "windows": [{
                "window_start_ms": 1000,
                "window_end_ms": 2000,
                "count": 1,
                "sum": 1,
            }],
            "finalized": [{
                "stream": "s",
                "window_start_ms": 0,
                "window_end_ms": 1000,
                "count": 1,
                "sum": 1,
            }],
            "auto_watermark_lag_ms": 200,
            # 1300 - 200 = 1100 == watermark: valid boundary, bucket open
            "max_event_timestamp_ms": 1300,
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

    def test_valid_boundaries_restore(self):
        status, _ = self.restore(self.snap(self.base_auto()))
        self.assertEqual(status, 200)
        Handler.service = Service()
        # max timestamp inside a finalized window is also fine
        status, _ = self.restore(self.snap(self.base_auto(
            watermark_ms=2200,
            windows=[],
            finalized=[{
                "stream": "s",
                "window_start_ms": 0,
                "window_end_ms": 1000,
                "count": 1,
                "sum": 1,
            }, {
                "stream": "s",
                "window_start_ms": 1000,
                "window_end_ms": 2000,
                "count": 1,
                "sum": 1,
            }],
            max_event_timestamp_ms=1300,
        )))
        self.assertEqual(status, 200)
        Handler.service = Service()
        # null max with null watermark is a fresh automatic stream
        status, _ = self.restore(self.snap(self.base_auto(
            watermark_ms=None, windows=[], finalized=[],
            max_event_timestamp_ms=None,
        )))
        self.assertEqual(status, 200)

    def test_pair_must_appear_together(self):
        without_max = self.base_auto()
        del without_max["max_event_timestamp_ms"]
        self.assert_invalid(self.snap(without_max))
        without_lag = self.base_auto()
        del without_lag["auto_watermark_lag_ms"]
        self.assert_invalid(self.snap(without_lag))

    def test_lag_type_and_range(self):
        for lag in (-1, 1.5, "200", True):
            self.assert_invalid(self.snap(self.base_auto(auto_watermark_lag_ms=lag)))
        # a lone lag field on a manual document is rejected as well
        lone = {
            "name": "s",
            "window_ms": 1000,
            "allowed_lateness_ms": 100,
            "dedup_retention_ms": None,
            "watermark_ms": None,
            "windows": [],
            "finalized": [],
            "auto_watermark_lag_ms": 200,
        }
        self.assert_invalid(self.snap(lone))

    def test_max_type(self):
        for value in (1300.0, "1300", True):
            self.assert_invalid(
                self.snap(self.base_auto(max_event_timestamp_ms=value))
            )

    def test_max_requires_watermark_and_known_window(self):
        self.assert_invalid(self.snap(self.base_auto(
            watermark_ms=None,
            windows=[],
            finalized=[],
            max_event_timestamp_ms=1300,
        )))
        # bucket 3000 is neither open nor finalized
        self.assert_invalid(self.snap(self.base_auto(max_event_timestamp_ms=3000)))

    def test_null_max_forbids_aggregated_windows(self):
        self.assert_invalid(self.snap(self.base_auto(
            watermark_ms=1100, max_event_timestamp_ms=None
        )))
        self.assert_invalid(self.snap(self.base_auto(
            watermark_ms=1100,
            windows=[],
            max_event_timestamp_ms=None,
        )))

    def test_watermark_below_max_minus_lag(self):
        # finalized removed so the dedicated automatic invariant is the
        # violated rule: 1300 - 200 = 1100 is required, 1099 is too low
        self.assert_invalid(self.snap(self.base_auto(
            watermark_ms=1099, finalized=[]
        )))
        # exactly on the boundary is valid with the window still open
        status, _ = self.restore(self.snap(self.base_auto(finalized=[])))
        self.assertEqual(status, 200)


class AutoWatermarkConcurrencyTest(HttpTestCase):
    def test_concurrent_events_serialize_monotonically(self):
        # window 100, lag 0: the watermark tracks the largest processed
        # timestamp and early windows finalize as it climbs.
        self.create("s", 100, 0, auto_watermark_lag_ms=0)
        observed_watermarks: list[int] = []
        finalized_starts: list[int] = []
        recorder = threading.Lock()
        threads_count = 12
        per_thread = 25

        def submit(worker):
            local = []
            for i in range(per_thread):
                ts = worker * 1000 + i * 7  # interleaved, out of order
                _, payload = self.event(ts, 1)
                local.append((
                    payload["watermark_ms"],
                    [w["window_start_ms"] for w in payload["finalized"]],
                ))
            with recorder:
                for wm, starts in local:
                    observed_watermarks.append(wm)
                    finalized_starts.extend(starts)

        threads = [
            threading.Thread(target=submit, args=(w,))
            for w in range(threads_count)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # every response reports an integer, non-null watermark
        self.assertTrue(all(isinstance(w, int) for w in observed_watermarks))
        _, snap = self.snapshot()
        final_wm = snap["streams"][0]["watermark_ms"]
        max_ts = (threads_count - 1) * 1000 + (per_thread - 1) * 7
        # lag 0 means the final watermark is the largest accepted timestamp
        self.assertEqual(final_wm, max_ts)
        self.assertEqual(max(observed_watermarks), final_wm)
        # no response reports a watermark above the final one
        self.assertLessEqual(max(observed_watermarks), final_wm)
        # each window is finalized in at most one response (the recording
        # groups lists per worker, so global order is not asserted here)
        self.assertEqual(len(finalized_starts), len(set(finalized_starts)))
        # every finalized start is aligned and lies below the final watermark
        self.assertTrue(all(s % 100 == 0 for s in finalized_starts))
        self.assertTrue(all(s < final_wm for s in finalized_starts))
        # the snapshot of the raced-upon state restores cleanly
        Service().restore_snapshot(json.loads(json.dumps(snap)))


if __name__ == "__main__":
    unittest.main()
