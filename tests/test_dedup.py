import json
import threading
import unittest

from test_windows import HttpTestCase


class DedupStreamTest(HttpTestCase):
    def create_dedup(self, name="d", window_ms=1000, allowed_lateness_ms=100,
                     dedup_retention_ms=10000):
        return self.request(
            "POST",
            "/streams",
            {
                "name": name,
                "window_ms": window_ms,
                "allowed_lateness_ms": allowed_lateness_ms,
                "dedup_retention_ms": dedup_retention_ms,
            },
        )

    def event(self, stream="d", **body):
        return self.request("POST", f"/streams/{stream}/events", body)

    def watermark(self, wm, stream="d"):
        return self.request("POST", f"/streams/{stream}/watermark", {"watermark_ms": wm})

    def results(self, stream="d"):
        return self.request("GET", f"/streams/{stream}/results")


class CreateDedupTest(DedupStreamTest):
    def test_create_echoes_retention(self):
        status, payload = self.create_dedup("orders", 5000, 250, 1000)
        self.assertEqual(status, 201)
        self.assertEqual(payload["stream"], "orders")
        self.assertEqual(payload["window_ms"], 5000)
        self.assertEqual(payload["allowed_lateness_ms"], 250)
        self.assertEqual(payload["dedup_retention_ms"], 1000)

    def test_plain_create_response_omits_retention(self):
        status, payload = self.create("plain", 1000, 100)
        self.assertEqual(status, 201)
        self.assertNotIn("dedup_retention_ms", payload)

    def test_retention_equal_to_lateness_allowed(self):
        status, _ = self.create_dedup("eq", 1000, 100, 100)
        self.assertEqual(status, 201)

    def test_invalid_retention_rejected_without_partial_stream(self):
        for bad in (0, -1, 1.5, True, "100", None):
            body = {
                "name": "x",
                "window_ms": 1000,
                "allowed_lateness_ms": 100,
                "dedup_retention_ms": bad,
            }
            status, payload = self.request("POST", "/streams", body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        # below allowed_lateness_ms is also invalid
        status, payload = self.request(
            "POST",
            "/streams",
            {"name": "x", "window_ms": 1000, "allowed_lateness_ms": 100,
             "dedup_retention_ms": 99},
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # undeclared field
        status, payload = self.request(
            "POST",
            "/streams",
            {"name": "x", "window_ms": 1000, "allowed_lateness_ms": 100,
             "dedup_retention_ms": 100, "extra": 1},
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        # no partial state: the stream must not exist
        status, payload = self.request("GET", "/streams/x/results")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "stream_not_found")


class DedupEventValidationTest(DedupStreamTest):
    def setUp(self):
        super().setUp()
        self.create_dedup()

    def test_event_id_required_nonempty_string(self):
        ok = {"timestamp_ms": 0, "value": 1}
        for body in (
            {**ok},                              # missing event_id
            {**ok, "event_id": 7},               # wrong type
            {**ok, "event_id": True},            # bool is not a string
            {**ok, "event_id": None},            # null type
            {**ok, "event_id": ""},              # empty string
            {**ok, "event_id": "a", "x": 1},     # extra field
        ):
            status, payload = self.event(**body)
            self.assertEqual(status, 422, body)
            self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_plain_streams_still_reject_event_id(self):
        self.create("plain", 1000, 100)
        status, payload = self.event(
            stream="plain", timestamp_ms=0, value=1, event_id="a"
        )
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")


class DedupSemanticsTest(DedupStreamTest):
    def setUp(self):
        super().setUp()
        self.create_dedup("d", 1000, 100, 10000)

    def test_new_event_then_exact_retry(self):
        status, payload = self.event(timestamp_ms=0, value=1.5, event_id="a")
        self.assertEqual(status, 200)
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], False)

        # same window, different id, aggregates normally
        self.event(timestamp_ms=999, value=2.5, event_id="b")

        status, payload = self.event(timestamp_ms=0, value=1.5, event_id="a")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], True)

        # numerically equal value (1 == 1.0) is an exact retry
        status, payload = self.event(timestamp_ms=0, value=1.5, event_id="a")
        self.assertEqual(payload["duplicate"], True)

        self.watermark(1100)
        _, payload = self.results()
        self.assertEqual(
            payload["results"],
            [{"stream": "d", "window_start_ms": 0, "window_end_ms": 1000,
              "count": 2, "sum": 4.0}],
        )

    def test_conflicting_id_returns_409_and_changes_nothing(self):
        self.event(timestamp_ms=0, value=1, event_id="a")
        status, payload = self.event(timestamp_ms=1, value=1, event_id="a")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")
        status, payload = self.event(timestamp_ms=0, value=2, event_id="a")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "event_id_conflict")

        # original binding still recognized, aggregation only happened once
        status, payload = self.event(timestamp_ms=0, value=1, event_id="a")
        self.assertEqual(payload["duplicate"], True)

        # the conflicting timestamp never entered its window either
        self.event(timestamp_ms=1, value=10, event_id="c")
        self.watermark(1100)
        _, payload = self.results()
        self.assertEqual(payload["results"][0]["count"], 2)  # a + c only
        self.assertEqual(payload["results"][0]["sum"], 11)

    def test_dedup_precedes_lateness(self):
        self.event(timestamp_ms=100, value=5, event_id="x")
        self.watermark(10000)  # late horizon 9900

        # exact retry is far past lateness but still a duplicate
        status, payload = self.event(timestamp_ms=100, value=5, event_id="x")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], True)

        # unseen late id: dropped and not recorded
        status, payload = self.event(timestamp_ms=100, value=9, event_id="y")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)

        # proof y was not recorded: same id with an on-time event is "new"
        status, payload = self.event(timestamp_ms=9900, value=7, event_id="y")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], False)

        self.watermark(20000)
        _, payload = self.results()
        by_start = {r["window_start_ms"]: r for r in payload["results"]}
        self.assertEqual(by_start[0]["count"], 1)
        self.assertEqual(by_start[0]["sum"], 5)
        self.assertEqual(by_start[9000]["count"], 1)
        self.assertEqual(by_start[9000]["sum"], 7)

    def test_watermark_regression_keeps_dedup_state(self):
        self.event(timestamp_ms=0, value=1, event_id="a")
        self.watermark(500)
        status, _ = self.watermark(499)
        self.assertEqual(status, 409)
        status, payload = self.event(timestamp_ms=0, value=1, event_id="a")
        self.assertEqual(payload["duplicate"], True)


class DedupRetentionTest(DedupStreamTest):
    def setUp(self):
        super().setUp()
        # small retention (>= lateness) so eviction is observable
        self.create_dedup("d", 1000, 100, 500)

    def test_boundary_retained_then_evicted_and_reused(self):
        self.event(timestamp_ms=0, value=1, event_id="a")

        # wm 500 -> retention horizon 0; ts 0 is on the boundary, retained
        self.watermark(500)
        status, payload = self.event(timestamp_ms=0, value=1, event_id="a")
        self.assertEqual(payload["duplicate"], True)

        # wm 501 -> horizon 1; ts 0 < 1, evicted
        self.watermark(501)

        # reused id with a late event: unseen => dropped, not recorded
        status, payload = self.event(timestamp_ms=0, value=1, event_id="a")
        self.assertEqual(payload["dropped"], True)
        self.assertEqual(payload["duplicate"], False)

        # the late reuse did not bind the id: on-time reuse is new
        # (late horizon is 501 - 100 = 401; 450 is on time)
        status, payload = self.event(timestamp_ms=450, value=2, event_id="a")
        self.assertEqual(payload["dropped"], False)
        self.assertEqual(payload["duplicate"], False)

        self.watermark(1500)
        _, payload = self.results()
        self.assertEqual(payload["results"][0]["count"], 2)
        self.assertEqual(payload["results"][0]["sum"], 3)

    def test_no_watermark_means_no_eviction(self):
        for i in range(5):
            self.event(timestamp_ms=i, value=1, event_id=f"e{i}")
        for i in range(5):
            _, payload = self.event(timestamp_ms=i, value=1, event_id=f"e{i}")
            self.assertEqual(payload["duplicate"], True)


class DedupIsolationTest(DedupStreamTest):
    def test_ids_and_config_isolated_between_streams(self):
        self.create_dedup("d", 1000, 100, 10000)
        self.create("p", 1000, 0)  # plain stream

        self.event(stream="d", timestamp_ms=0, value=5, event_id="a")
        # plain stream has no dedup: resubmitting aggregates twice
        self.event(stream="p", timestamp_ms=0, value=7)
        self.event(stream="p", timestamp_ms=0, value=7)

        self.watermark(1100, stream="d")
        self.watermark(1000, stream="p")
        _, dedup = self.results("d")
        _, plain = self.results("p")
        self.assertEqual(dedup["results"][0]["count"], 1)
        self.assertEqual(plain["results"][0]["count"], 2)

        # duplicate the id in d, then use it independently in a 2nd dedup stream
        _, payload = self.event(stream="d", timestamp_ms=0, value=5, event_id="a")
        self.assertEqual(payload["duplicate"], True)
        self.create_dedup("d2", 1000, 100, 10000)
        status, payload = self.event(stream="d2", timestamp_ms=0, value=9, event_id="a")
        self.assertEqual(status, 200)
        self.assertEqual(payload["duplicate"], False)


class DedupConcurrencyTest(DedupStreamTest):
    def test_concurrent_same_id_aggregates_once(self):
        self.create_dedup("d", 1000, 100, 10000)
        n = 24
        barrier = threading.Barrier(n)
        outcomes = [None] * n

        def submit(i):
            barrier.wait()
            outcomes[i] = self.event(timestamp_ms=0, value=3, event_id="same")

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        statuses = [o[0] for o in outcomes]
        payloads = [o[1] for o in outcomes]
        self.assertTrue(all(s == 200 for s in statuses), statuses)
        fresh = [p for p in payloads if p["duplicate"] is False]
        dups = [p for p in payloads if p["duplicate"] is True]
        self.assertEqual(len(fresh), 1, payloads)
        self.assertEqual(len(dups), n - 1)

        self.watermark(1100)
        _, payload = self.results()
        self.assertEqual(payload["results"][0]["count"], 1)
        self.assertEqual(payload["results"][0]["sum"], 3)


if __name__ == "__main__":
    unittest.main()
