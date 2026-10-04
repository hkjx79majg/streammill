import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from streammill.persistence import StateStore, StateStoreError
from streammill.server import Handler
from streammill.service import Service


class FailingStore(StateStore):
    """A store whose writes always fail, to exercise the 503 path."""

    def persist(self, document):
        raise StateStoreError("injected persist failure")


class PersistentHttpTestCase(unittest.TestCase):
    """HTTP-level tests with a state store attached to the service."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "state.json")
        store, document = StateStore.open(self.path)
        self.assertIsNone(document)
        Handler.service = Service(state_store=store)
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

    def snapshot(self):
        return self.request("GET", "/snapshot")

    def restart(self):
        """Simulate a process restart: fresh service from the state file."""
        store, document = StateStore.open(self.path)
        service = Service()
        if document is not None:
            service.restore_snapshot(document)
        service.attach_state_store(store)
        Handler.service = service

    def file_bytes(self):
        with open(self.path, "rb") as handle:
            return handle.read()


class CommitPersistenceTest(PersistentHttpTestCase):
    def test_every_successful_mutation_is_persisted(self):
        self.assertFalse(os.path.exists(self.path))
        status, _ = self.create("s", 1000, 100, dedup_retention_ms=5000)
        self.assertEqual(status, 201)
        with open(self.path, encoding="utf-8") as handle:
            document = json.load(handle)
        self.assertEqual(document, self.snapshot()[1])

        status, _ = self.event(100, 5, event_id="a")
        self.assertEqual(status, 200)
        status, _ = self.watermark(1200)
        self.assertEqual(status, 200)
        with open(self.path, encoding="utf-8") as handle:
            document = json.load(handle)
        self.assertEqual(document, self.snapshot()[1])
        self.assertEqual(document["streams"][0]["watermark_ms"], 1200)
        self.assertEqual(len(document["streams"][0]["finalized"]), 1)

    def test_reads_do_not_write(self):
        self.create("s")
        before = self.file_bytes()
        self.snapshot()
        self.request("GET", "/healthz")
        self.request("GET", "/streams/s/results")
        self.request("GET", "/streams/s/changes?after_seq=0&limit=10")
        self.assertEqual(self.file_bytes(), before)

    def test_noop_successes_do_not_rewrite(self):
        self.create("s", 1000, 100, dedup_retention_ms=5000)
        self.event(100, 5, event_id="a")
        self.watermark(500)
        before = self.file_bytes()

        # Exact duplicate, too-late drop, same watermark: no rewrite.
        status, payload = self.event(100, 5, event_id="a")
        self.assertEqual((status, payload["duplicate"]), (200, True))
        status, payload = self.event(10, 1, event_id="late")
        self.assertEqual((status, payload["dropped"]), (200, True))
        status, _ = self.watermark(500)
        self.assertEqual(status, 200)
        self.assertEqual(self.file_bytes(), before)

        # Same row value: no rewrite either; real writes do persist.
        status, _ = self.request("POST", "/tables", {"name": "t"})
        self.assertEqual(status, 201)
        self.request("POST", "/tables/t/rows", {"key": "k", "label": "v"})
        mid = self.file_bytes()
        self.assertNotEqual(before, mid)  # table creation did persist
        status, payload = self.request(
            "POST", "/tables/t/rows", {"key": "k", "label": "v"}
        )
        self.assertEqual((status, payload["changed"]), (200, False))
        self.assertEqual(self.file_bytes(), mid)

    def test_4xx_failures_do_not_rewrite(self):
        self.create("s", 1000, 0, dedup_retention_ms=1000)
        self.event(100, 5, event_id="a")
        before = self.file_bytes()
        # Conflict, regression, validation failure, unknown stream.
        self.assertEqual(self.event(200, 5, event_id="a")[0], 409)
        self.assertEqual(self.watermark(50)[0], 200)  # first watermark, persists
        mid = self.file_bytes()
        self.assertEqual(self.watermark(49)[0], 409)
        self.assertEqual(self.event(100, 5)[0], 422)
        self.assertEqual(self.event(100, 5, stream="nope")[0], 404)
        self.assertEqual(self.create("s")[0], 409)
        self.assertEqual(self.file_bytes(), mid)

    def test_restart_recovers_full_state(self):
        self.create(
            "s",
            1000,
            100,
            dedup_retention_ms=5000,
            change_retention=10,
            batch_retention=5,
        )
        self.event(100, 5, event_id="a")
        self.event(200, 7, event_id="b")
        self.request(
            "POST",
            "/streams/s/batches",
            {"batch_id": "b1", "events": [{"timestamp_ms": 300, "value": 2, "event_id": "c"}]},
        )
        self.watermark(1200)
        expected = self.snapshot()[1]

        self.restart()
        self.assertEqual(self.snapshot()[1], expected)

        # Dedup memory, change cursor, batch replay and next writes
        # continue exactly as on the uninterrupted instance.
        status, payload = self.event(100, 5, event_id="a")
        self.assertEqual((status, payload["duplicate"]), (200, True))
        status, payload = self.request(
            "POST",
            "/streams/s/batches",
            {"batch_id": "b1", "events": [{"timestamp_ms": 300, "value": 2, "event_id": "c"}]},
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["batch_id"], "b1")
        status, payload = self.request("GET", "/streams/s/changes?after_seq=0&limit=100")
        self.assertEqual(status, 200)
        latest = payload["latest_seq"]
        self.assertGreater(latest, 0)
        self.event(1300, 1, event_id="d")
        status, payload = self.request(
            "GET", f"/streams/s/changes?after_seq={latest}&limit=100"
        )
        self.assertEqual(status, 200)
        self.assertEqual([row["seq"] for row in payload["changes"]], [latest + 1])

    def test_restore_endpoint_is_a_commit(self):
        self.create("s")
        self.event(100, 5)
        document = self.snapshot()[1]

        # Fresh empty persistent instance; restore through HTTP.
        self.tmp2 = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp2.cleanup)
        self.path = os.path.join(self.tmp2.name, "state.json")
        store, _ = StateStore.open(self.path)
        Handler.service = Service(state_store=store)
        status, payload = self.request("POST", "/snapshot/restore", body=document)
        self.assertEqual((status, payload["restored_streams"]), (200, 1))
        with open(self.path, encoding="utf-8") as handle:
            self.assertEqual(json.load(handle), document)


class PersistFailureTest(PersistentHttpTestCase):
    def test_failed_commit_returns_503_and_rolls_back(self):
        self.create("s", 1000, 0)
        self.event(100, 5)
        before_file = self.file_bytes()
        before_snapshot = self.snapshot()[1]

        Handler.service.attach_state_store(FailingStore(self.path))
        status, payload = self.event(200, 7)
        self.assertEqual(status, 503)
        self.assertEqual(payload["error"]["code"], "state_persist_failed")

        # Memory and file both hold the pre-commit state.
        self.assertEqual(self.snapshot()[1], before_snapshot)
        self.assertEqual(self.file_bytes(), before_file)

        # A retry after recovery is processed as if nothing happened.
        store, _ = StateStore.open(self.path)
        Handler.service.attach_state_store(store)
        status, _ = self.event(200, 7)
        self.assertEqual(status, 200)
        self.assertEqual(self.snapshot()[1]["streams"][0]["windows"][0]["count"], 2)

    def test_failed_create_leaves_no_stream(self):
        Handler.service = Service(state_store=FailingStore(self.path))
        status, payload = self.create("s")
        self.assertEqual(status, 503)
        self.assertEqual(payload["error"]["code"], "state_persist_failed")
        self.assertFalse(os.path.exists(self.path))
        status, payload = self.snapshot()
        self.assertEqual(payload, {"format_version": 1, "streams": []})


class StartupValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def path(self, name="state.json"):
        return os.path.join(self.tmp.name, name)

    def test_empty_path_rejected(self):
        with self.assertRaises(StateStoreError):
            StateStore.open("")

    def test_directory_path_rejected(self):
        with self.assertRaises(StateStoreError):
            StateStore.open(self.tmp.name)

    def test_missing_parent_rejected(self):
        with self.assertRaises(StateStoreError):
            StateStore.open(self.path("missing/state.json"))

    def test_unwritable_parent_rejected(self):
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            self.skipTest("root bypasses permission checks")
        readonly = os.path.join(self.tmp.name, "readonly")
        os.mkdir(readonly, 0o555)
        try:
            with self.assertRaises(StateStoreError):
                StateStore.open(os.path.join(readonly, "state.json"))
        finally:
            os.chmod(readonly, 0o755)

    def test_invalid_utf8_rejected(self):
        path = self.path()
        with open(path, "wb") as handle:
            handle.write(b"\xff\xfe{}")
        with self.assertRaises(StateStoreError):
            StateStore.open(path)

    def test_invalid_json_rejected(self):
        path = self.path()
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("not json")
        with self.assertRaises(StateStoreError):
            StateStore.open(path)

    def test_invalid_snapshot_document_rejected_on_restore(self):
        path = self.path()
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"format_version": 99, "streams": []}, handle)
        _, document = StateStore.open(path)
        service = Service()
        with self.assertRaises(Exception):
            service.restore_snapshot(document)

    def test_missing_file_starts_empty(self):
        store, document = StateStore.open(self.path())
        self.assertIsNone(document)
        self.assertFalse(os.path.exists(self.path()))


class ServerProcessTest(unittest.TestCase):
    """End-to-end: the server binary honors STREAMMILL_STATE_FILE."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "state.json")
        self.env = {
            **os.environ,
            "PYTHONPATH": os.path.join(
                os.path.dirname(__file__), "..", "src"
            ),
            "STREAMMILL_STATE_FILE": self.state,
        }

    def free_port(self):
        import socket

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    def start(self, port):
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streammill.server",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def request(self, port, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}", data=data, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                return exc.code, json.loads(raw)
            except ValueError:
                return exc.code, {}

    def wait_ready(self, proc, port):
        import time

        for _ in range(100):
            if proc.poll() is not None:
                self.fail(
                    f"server exited early: {proc.stderr.read()}"
                )
            try:
                status, _ = self.request(port, "GET", "/healthz")
                if status == 200:
                    return
            except Exception:
                pass
            time.sleep(0.05)
        self.fail("server did not start listening")

    def test_restart_recovers_commits(self):
        port = self.free_port()
        proc = self.start(port)
        try:
            self.wait_ready(proc, port)
            self.assertEqual(
                self.request(
                    port,
                    "POST",
                    "/streams",
                    {"name": "s", "window_ms": 1000, "allowed_lateness_ms": 0},
                )[0],
                201,
            )
            self.assertEqual(
                self.request(
                    port, "POST", "/streams/s/events",
                    {"timestamp_ms": 100, "value": 5},
                )[0],
                200,
            )
            self.assertEqual(
                self.request(
                    port, "POST", "/streams/s/watermark", {"watermark_ms": 1000}
                )[0],
                200,
            )
        finally:
            proc.kill()
            proc.wait()

        proc = self.start(port)
        try:
            self.wait_ready(proc, port)
            status, payload = self.request(port, "GET", "/streams/s/results")
            self.assertEqual(status, 200)
            self.assertEqual(
                payload["results"],
                [
                    {
                        "stream": "s",
                        "window_start_ms": 0,
                        "window_end_ms": 1000,
                        "count": 1,
                        "sum": 5,
                    }
                ],
            )
        finally:
            proc.kill()
            proc.wait()

    def test_invalid_state_file_exits_nonzero(self):
        with open(self.state, "w", encoding="utf-8") as handle:
            handle.write("not json")
        proc = self.start(self.free_port())
        _, stderr = proc.communicate(timeout=30)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("STREAMMILL_STATE_FILE", stderr)

    def test_directory_state_path_exits_nonzero(self):
        self.env["STREAMMILL_STATE_FILE"] = self.tmp.name
        proc = self.start(self.free_port())
        proc.communicate(timeout=30)
        self.assertNotEqual(proc.returncode, 0)


if __name__ == "__main__":
    unittest.main()
