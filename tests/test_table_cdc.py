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

    def request(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def create_table(self, name="t", **extra):
        return self.request("POST", "/tables", {"name": name, **extra})

    def put_row(self, key, label, table="t", **extra):
        return self.request(
            "POST",
            f"/tables/{table}/rows",
            {"key": key, "label": label, **extra},
        )

    def changes(self, after_seq=0, limit=1000, table="t"):
        return self.request(
            "GET", f"/tables/{table}/changes?after_seq={after_seq}&limit={limit}"
        )

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restore(self, document):
        return self.request("POST", "/snapshot/restore", document)


class TableCdcCreateTest(HttpTestCase):
    def test_create_echoes_cdc_retention(self):
        status, payload = self.create_table(cdc_retention=50)
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t", "cdc_retention": 50})

    def test_create_without_cdc_retention_keeps_shape(self):
        status, payload = self.create_table()
        self.assertEqual(status, 201)
        self.assertEqual(payload, {"table": "t"})

    def test_create_versioned_with_cdc(self):
        status, payload = self.create_table(
            event_time_versioned=True, cdc_retention=5
        )
        self.assertEqual(status, 201)
        self.assertEqual(
            payload,
            {"table": "t", "event_time_versioned": True, "cdc_retention": 5},
        )

    def test_invalid_cdc_retention_rejected(self):
        for bad in (0, -3, 1.5, "10", True, None):
            status, payload = self.create_table(cdc_retention=bad)
            self.assertEqual(status, 422, bad)
            self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.changes(table="t")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")

    def test_cdc_not_enabled(self):
        self.create_table()
        status, payload = self.changes()
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "cdc_not_enabled")

    def test_unknown_table(self):
        status, payload = self.changes()
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")

    def test_initial_cursor_is_zero(self):
        self.create_table(cdc_retention=10)
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"table": "t", "latest_seq": 0, "changes": []})


class TableCdcQueryParamTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(cdc_retention=10)

    def assert_invalid(self, query):
        status, payload = self.request("GET", f"/tables/t/changes{query}")
        self.assertEqual(status, 422, query)
        self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_missing_params(self):
        self.assert_invalid("")
        self.assert_invalid("?after_seq=0")
        self.assert_invalid("?limit=10")

    def test_duplicate_params(self):
        self.assert_invalid("?after_seq=0&after_seq=1&limit=10")
        self.assert_invalid("?after_seq=0&limit=10&limit=20")

    def test_unknown_params(self):
        self.assert_invalid("?after_seq=0&limit=10&cursor=3")
        self.assert_invalid("?after_seq=0&limit=10&limit=10&x=1")

    def test_invalid_values(self):
        self.assert_invalid("?after_seq=-1&limit=10")
        self.assert_invalid("?after_seq=1.5&limit=10")
        self.assert_invalid("?after_seq=x&limit=10")
        self.assert_invalid("?after_seq=0&limit=0")
        self.assert_invalid("?after_seq=0&limit=1001")
        self.assert_invalid("?after_seq=0&limit=-2")
        self.assert_invalid("?after_seq=0&limit=y")

    def test_validation_precedes_table_existence(self):
        status, payload = self.request("GET", "/tables/nope/changes?after_seq=x")
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_request")
        status, payload = self.request(
            "GET", "/tables/nope/changes?after_seq=0&limit=1"
        )
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "table_not_found")


class TableCdcCurrentValueTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(cdc_retention=3)

    def test_upsert_records(self):
        self.put_row("a", "x")
        self.put_row("b", "y")
        self.put_row("a", "z")
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(payload["latest_seq"], 3)
        self.assertEqual(
            payload["changes"],
            [
                {"seq": 1, "kind": "upsert", "key": "a", "label": "x"},
                {"seq": 2, "kind": "upsert", "key": "b", "label": "y"},
                {"seq": 3, "kind": "upsert", "key": "a", "label": "z"},
            ],
        )

    def test_identical_retry_consumes_no_seq(self):
        self.put_row("a", "x")
        status, payload = self.put_row("a", "x")
        self.assertEqual(status, 200)
        self.assertFalse(payload["changed"])
        _, feed = self.changes()
        self.assertEqual(feed["latest_seq"], 1)
        self.assertEqual(len(feed["changes"]), 1)

    def test_table_without_cdc_write_shape_unchanged(self):
        self.create_table("plain")
        status, payload = self.put_row("a", "x", table="plain")
        self.assertEqual(status, 200)
        self.assertEqual(
            payload, {"table": "plain", "key": "a", "label": "x", "changed": True}
        )

    def test_trimming_keeps_latest_and_cursor_rules(self):
        for index in range(5):
            self.put_row("k", f"v{index}")
        # The oldest retained seq minus one is still valid.
        status, payload = self.changes(after_seq=2)
        self.assertEqual(status, 200)
        self.assertEqual(payload["latest_seq"], 5)
        self.assertEqual([r["seq"] for r in payload["changes"]], [3, 4, 5])
        # Cursor ahead of the latest sequence.
        status, payload = self.changes(after_seq=6)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "cdc_cursor_ahead")
        # Cursor behind the oldest retained record minus one.
        status, payload = self.changes(after_seq=1)
        self.assertEqual(status, 410)
        self.assertEqual(payload["error"]["code"], "cdc_cursor_expired")
        status, payload = self.changes(after_seq=0)
        self.assertEqual(status, 410)

    def test_after_seq_and_limit(self):
        for index in range(4):
            self.put_row("k", f"v{index}")
        status, payload = self.changes(after_seq=1, limit=2)
        self.assertEqual(status, 200)
        self.assertEqual([r["seq"] for r in payload["changes"]], [2, 3])
        self.assertEqual(payload["latest_seq"], 4)


class TableCdcVersionedTest(HttpTestCase):
    def setUp(self):
        super().setUp()
        self.create_table(event_time_versioned=True, cdc_retention=10)

    def test_version_records_carry_effective_from_ms(self):
        self.put_row("a", "x", effective_from_ms=100)
        self.put_row("a", "y", effective_from_ms=50)
        status, payload = self.changes()
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["changes"],
            [
                {
                    "seq": 1,
                    "kind": "upsert",
                    "key": "a",
                    "label": "x",
                    "effective_from_ms": 100,
                },
                {
                    "seq": 2,
                    "kind": "upsert",
                    "key": "a",
                    "label": "y",
                    "effective_from_ms": 50,
                },
            ],
        )

    def test_identical_retry_consumes_no_seq(self):
        self.put_row("a", "x", effective_from_ms=100)
        status, payload = self.put_row("a", "x", effective_from_ms=100)
        self.assertEqual(status, 200)
        self.assertFalse(payload["changed"])
        _, feed = self.changes()
        self.assertEqual(feed["latest_seq"], 1)

    def test_conflict_produces_no_record(self):
        self.put_row("a", "x", effective_from_ms=100)
        status, payload = self.put_row("a", "y", effective_from_ms=100)
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "dimension_version_conflict")
        _, feed = self.changes()
        self.assertEqual(feed["latest_seq"], 1)
        self.assertEqual(len(feed["changes"]), 1)


class TableCdcSnapshotTest(HttpTestCase):
    def test_snapshot_version_7_shape(self):
        self.create_table("cdc", cdc_retention=5)
        self.create_table("plain")
        self.put_row("a", "x", table="cdc")
        self.put_row("b", "y", table="plain")
        status, payload = self.snapshot()
        self.assertEqual(status, 200)
        self.assertEqual(payload["format_version"], 7)
        tables = {table["name"]: table for table in payload["tables"]}
        self.assertEqual(tables["cdc"]["cdc_retention"], 5)
        self.assertEqual(tables["cdc"]["latest_cdc_seq"], 1)
        self.assertEqual(
            tables["cdc"]["cdc_changes"],
            [{"seq": 1, "kind": "upsert", "key": "a", "label": "x"}],
        )
        self.assertNotIn("cdc_retention", tables["plain"])
        self.assertNotIn("latest_cdc_seq", tables["plain"])
        self.assertNotIn("cdc_changes", tables["plain"])

    def test_snapshot_without_cdc_keeps_version(self):
        self.create_table("plain")
        _, payload = self.snapshot()
        self.assertEqual(payload["format_version"], 2)

    def test_versioned_cdc_snapshot(self):
        self.create_table("v", event_time_versioned=True, cdc_retention=5)
        self.put_row("a", "x", table="v", effective_from_ms=100)
        _, payload = self.snapshot()
        self.assertEqual(payload["format_version"], 7)
        table = payload["tables"][0]
        self.assertEqual(
            table["cdc_changes"],
            [
                {
                    "seq": 1,
                    "kind": "upsert",
                    "key": "a",
                    "label": "x",
                    "effective_from_ms": 100,
                }
            ],
        )

    def test_restore_round_trip_continues_sequence(self):
        self.create_table(cdc_retention=2)
        for index in range(3):
            self.put_row("k", f"v{index}")
        _, document = self.snapshot()
        self.assertEqual(document["format_version"], 7)
        Handler.service = Service()
        status, payload = self.restore(document)
        self.assertEqual(status, 200)
        # The feed, cursor and trimming continue exactly as before.
        status, payload = self.changes(after_seq=1)
        self.assertEqual(status, 200)
        self.assertEqual(payload["latest_seq"], 3)
        self.assertEqual([r["seq"] for r in payload["changes"]], [2, 3])
        status, payload = self.changes(after_seq=0)
        self.assertEqual(status, 410)
        self.put_row("k", "v3")
        _, payload = self.changes(after_seq=2)
        self.assertEqual(payload["latest_seq"], 4)
        self.assertEqual([r["seq"] for r in payload["changes"]], [3, 4])
        # Re-export is identical to the pre-restore export shape.
        _, again = self.snapshot()
        self.assertEqual(again["format_version"], 7)
        self.assertEqual(again["tables"][0]["cdc_retention"], 2)

    def test_restore_version_7_requires_enabled_table(self):
        document = {
            "format_version": 7,
            "tables": [{"name": "t", "rows": []}],
            "streams": [],
        }
        status, payload = self.restore(document)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")

    def test_restore_rejects_bad_cdc_documents(self):
        self.create_table(cdc_retention=2)
        self.put_row("a", "x")
        _, good = self.snapshot()
        table = good["tables"][0]

        def mutated(**changes):
            doc = json.loads(json.dumps(good))
            doc["tables"][0].update(changes)
            return doc

        bad_documents = [
            mutated(cdc_retention=0),
            mutated(cdc_retention=-1),
            mutated(latest_cdc_seq=-1),
            mutated(latest_cdc_seq=2),  # last record seq must match
            mutated(cdc_changes=[]),  # retention/continuity mismatch
            mutated(
                cdc_changes=[
                    {"seq": 1, "kind": "upsert", "key": "a", "label": "x"},
                    {"seq": 1, "kind": "upsert", "key": "a", "label": "x"},
                ],
                latest_cdc_seq=2,
            ),
            mutated(
                cdc_changes=[
                    {"seq": 1, "kind": "upsert", "key": "a", "label": "y"}
                ]
            ),  # contradicts the current row
            mutated(
                cdc_changes=[
                    {"seq": 1, "kind": "upsert", "key": "ghost", "label": "x"}
                ]
            ),  # unknown key
            mutated(
                cdc_changes=[
                    {"seq": 1, "kind": "delete", "key": "a", "label": "x"}
                ]
            ),
            mutated(
                cdc_changes=[
                    {"seq": 1, "kind": "upsert", "key": "a", "label": "x", "n": 1}
                ]
            ),
        ]
        for document in bad_documents:
            Handler.service = Service()
            status, payload = self.restore(document)
            self.assertEqual(status, 422, document)
            self.assertEqual(payload["error"]["code"], "invalid_snapshot")
            # Nothing was published.
            status, _ = self.snapshot()
            self.assertEqual(status, 200)
            _, empty = self.snapshot()
            self.assertEqual(empty, {"format_version": 1, "streams": []})

    def test_restore_rejects_cdc_fields_on_older_versions(self):
        document = {
            "format_version": 2,
            "tables": [
                {
                    "name": "t",
                    "rows": [],
                    "cdc_retention": 1,
                    "latest_cdc_seq": 0,
                    "cdc_changes": [],
                }
            ],
            "streams": [],
        }
        status, payload = self.restore(document)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")

    def test_restore_rejects_orphan_cdc_fields(self):
        for field in ("latest_cdc_seq", "cdc_changes"):
            document = {
                "format_version": 7,
                "tables": [{"name": "t", "rows": [], field: 0 if field.endswith("seq") else []}],
                "streams": [],
            }
            status, payload = self.restore(document)
            self.assertEqual(status, 422, field)
            self.assertEqual(payload["error"]["code"], "invalid_snapshot")

    def test_restore_versioned_cdc_contradiction(self):
        document = {
            "format_version": 7,
            "tables": [
                {
                    "name": "v",
                    "event_time_versioned": True,
                    "versions": [
                        {"key": "a", "label": "x", "effective_from_ms": 100}
                    ],
                    "cdc_retention": 5,
                    "latest_cdc_seq": 1,
                    "cdc_changes": [
                        {
                            "seq": 1,
                            "kind": "upsert",
                            "key": "a",
                            "label": "y",
                            "effective_from_ms": 100,
                        }
                    ],
                }
            ],
            "streams": [],
        }
        status, payload = self.restore(document)
        self.assertEqual(status, 422)
        self.assertEqual(payload["error"]["code"], "invalid_snapshot")

    def test_restore_older_versions_still_work(self):
        for version, tables in (
            (1, None),
            (2, []),
            (5, [{"name": "v", "event_time_versioned": True, "versions": []}]),
        ):
            Handler.service = Service()
            document = {"format_version": version, "streams": []}
            if tables is not None:
                document["tables"] = tables
            status, payload = self.restore(document)
            self.assertEqual(status, 200, version)


class TableCdcPersistenceTest(HttpTestCase):
    def test_persist_and_reload(self):
        import os
        import tempfile

        directory = tempfile.mkdtemp()
        path = os.path.join(directory, "state.json")
        try:
            Handler.service = Service.from_state_file(path)
            self.create_table(cdc_retention=2)
            self.put_row("a", "x")
            self.put_row("a", "y")
            with open(path, encoding="utf-8") as handle:
                document = json.load(handle)
            self.assertEqual(document["format_version"], 7)
            self.assertEqual(document["tables"][0]["latest_cdc_seq"], 2)
            Handler.service = Service.from_state_file(path)
            status, payload = self.changes()
            self.assertEqual(status, 200)
            self.assertEqual(payload["latest_seq"], 2)
            self.assertEqual(
                payload["changes"],
                [
                    {"seq": 1, "kind": "upsert", "key": "a", "label": "x"},
                    {"seq": 2, "kind": "upsert", "key": "a", "label": "y"},
                ],
            )
            self.put_row("a", "z")
            _, payload = self.changes(after_seq=1)
            self.assertEqual(payload["latest_seq"], 3)
            self.assertEqual([r["seq"] for r in payload["changes"]], [2, 3])
        finally:
            Handler.service = Service()


if __name__ == "__main__":
    unittest.main()
