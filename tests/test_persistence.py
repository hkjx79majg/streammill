import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from streammill.server import Handler
from streammill.service import Service, StateFileError

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class HttpTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="streammill-state-")
        self.state_path = os.path.join(self.dir, "state.json")
        Handler.service = Service.from_state_file(self.state_path)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)
        Handler.service = Service()
        shutil.rmtree(self.dir, ignore_errors=True)

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

    def file_state(self):
        """(exists, parsed_document, mtime_ns) of the state file."""
        if not os.path.exists(self.state_path):
            return False, None, None
        with open(self.state_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        return True, document, os.stat(self.state_path).st_mtime_ns


class CommitPersistenceTest(HttpTestCase):
    def test_missing_file_starts_empty(self):
        status, payload = self.request("GET", "/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"format_version": 1, "streams": []})
        self.assertFalse(os.path.exists(self.state_path))

    def test_commits_persist_complete_snapshots(self):
        status, _ = self.request(
            "POST",
            "/streams",
            {"name": "s", "window_ms": 1000, "allowed_lateness_ms": 0},
        )
        self.assertEqual(status, 201)
        exists, document, _ = self.file_state()
        self.assertTrue(exists)
        status, snapshot = self.request("GET", "/snapshot")
        self.assertEqual(document, snapshot)

        self.request(
            "POST", "/streams/s/events", {"timestamp_ms": 10, "value": 2.5}
        )
        self.request(
            "POST", "/streams/s/watermark", {"watermark_ms": 2000}
        )
        exists, document, _ = self.file_state()
        self.assertTrue(exists)
        status, snapshot = self.request("GET", "/snapshot")
        self.assertEqual(document, snapshot)
        self.assertEqual(
            document["streams"][0]["finalized"],
            [
                {
                    "stream": "s",
                    "window_start_ms": 0,
                    "window_end_ms": 1000,
                    "count": 1,
                    "sum": 2.5,
                }
            ],
        )

    def test_restart_recovers_committed_state(self):
        self.request(
            "POST",
            "/streams",
            {
                "name": "s",
                "window_ms": 1000,
                "allowed_lateness_ms": 100,
                "dedup_retention_ms": 5000,
                "change_retention": 10,
                "batch_retention": 5,
            },
        )
        self.request(
            "POST",
            "/streams/s/events",
            {"timestamp_ms": 10, "value": 1, "event_id": "e1"},
        )
        self.request(
            "POST",
            "/streams/s/batches",
            {
                "batch_id": "b1",
                "events": [{"timestamp_ms": 20, "value": 2, "event_id": "e2"}],
            },
        )
        self.request("POST", "/streams/s/watermark", {"watermark_ms": 500})
        _, before, _ = self.file_state()

        Handler.service = Service.from_state_file(self.state_path)
        status, snapshot = self.request("GET", "/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(snapshot, before)
        # The change cursor continues exactly where it left off.
        status, changes = self.request(
            "GET", "/streams/s/changes?after_seq=0&limit=100"
        )
        self.assertEqual(status, 200)
        self.assertEqual(changes["latest_seq"], before["streams"][0]["latest_seq"])
        # A retained batch replays its stored response without a new commit.
        _, _, mtime = self.file_state()
        status, replay = self.request(
            "POST",
            "/streams/s/batches",
            {
                "batch_id": "b1",
                "events": [{"timestamp_ms": 20, "value": 2, "event_id": "e2"}],
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(replay["batch_id"], "b1")
        self.assertEqual(os.stat(self.state_path).st_mtime_ns, mtime)
        # A retained event id is still a duplicate after the restart.
        status, duplicate = self.request(
            "POST",
            "/streams/s/events",
            {"timestamp_ms": 10, "value": 1, "event_id": "e1"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(duplicate["duplicate"])

    def test_noop_successes_do_not_touch_the_file(self):
        self.request("POST", "/tables", {"name": "t"})
        self.request(
            "POST", "/tables/t/rows", {"key": "k", "label": "v"}
        )
        self.request(
            "POST",
            "/streams",
            {
                "name": "s",
                "window_ms": 1000,
                "allowed_lateness_ms": 0,
                "dedup_retention_ms": 100000,
                "batch_retention": 5,
            },
        )
        self.request(
            "POST",
            "/streams/s/events",
            {"timestamp_ms": 10, "value": 1, "event_id": "e1"},
        )
        self.request("POST", "/streams/s/watermark", {"watermark_ms": 5000})
        exists, document, mtime = self.file_state()
        self.assertTrue(exists)

        # Exact duplicate, too-late unseen id, identical row value,
        # identical watermark: all successful, none a commit.
        status, outcome = self.request(
            "POST",
            "/streams/s/events",
            {"timestamp_ms": 10, "value": 1, "event_id": "e1"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(outcome["duplicate"])
        status, outcome = self.request(
            "POST",
            "/streams/s/events",
            {"timestamp_ms": 1, "value": 9, "event_id": "late"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(outcome["dropped"])
        status, row = self.request(
            "POST", "/tables/t/rows", {"key": "k", "label": "v"}
        )
        self.assertEqual(status, 200)
        self.assertFalse(row["changed"])
        status, _ = self.request(
            "POST", "/streams/s/watermark", {"watermark_ms": 5000}
        )
        self.assertEqual(status, 200)

        exists, after, after_mtime = self.file_state()
        self.assertEqual(after, document)
        self.assertEqual(after_mtime, mtime)

    def test_failed_requests_do_not_touch_the_file(self):
        # Before any commit, failures must not create the file.
        status, _ = self.request("POST", "/streams", {"name": "s"})
        self.assertEqual(status, 422)
        status, _ = self.request(
            "POST", "/streams/x/events", {"timestamp_ms": 1, "value": 1}
        )
        self.assertEqual(status, 404)
        self.assertFalse(os.path.exists(self.state_path))

        self.request(
            "POST",
            "/streams",
            {"name": "s", "window_ms": 1000, "allowed_lateness_ms": 0},
        )
        self.request("POST", "/streams/s/watermark", {"watermark_ms": 100})
        exists, document, mtime = self.file_state()
        self.assertTrue(exists)

        status, _ = self.request(
            "POST",
            "/streams",
            {"name": "s", "window_ms": 1000, "allowed_lateness_ms": 0},
        )
        self.assertEqual(status, 409)
        status, _ = self.request(
            "POST", "/streams/s/watermark", {"watermark_ms": 50}
        )
        self.assertEqual(status, 409)
        status, _ = self.request(
            "POST", "/snapshot/restore", {"format_version": 1, "streams": []}
        )
        self.assertEqual(status, 409)

        exists, after, after_mtime = self.file_state()
        self.assertEqual(after, document)
        self.assertEqual(after_mtime, mtime)

    def test_persist_failure_returns_503_and_rolls_back(self):
        self.request(
            "POST",
            "/streams",
            {"name": "s", "window_ms": 1000, "allowed_lateness_ms": 0},
        )
        exists, document, _ = self.file_state()
        self.assertTrue(exists)

        # Make the state path a directory so the atomic replace fails.
        os.unlink(self.state_path)
        os.mkdir(self.state_path)
        status, error = self.request(
            "POST", "/streams/s/events", {"timestamp_ms": 10, "value": 1}
        )
        self.assertEqual(status, 503)
        self.assertEqual(error["error"]["code"], "state_persist_failed")

        # In-memory state is rolled back to the pre-commit version.
        status, snapshot = self.request("GET", "/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(snapshot, document)

        # Once the path is usable again the retry succeeds as if the
        # failed request had never happened.
        os.rmdir(self.state_path)
        status, outcome = self.request(
            "POST", "/streams/s/events", {"timestamp_ms": 10, "value": 1}
        )
        self.assertEqual(status, 200)
        self.assertFalse(outcome["dropped"])
        exists, persisted, _ = self.file_state()
        self.assertTrue(exists)
        status, snapshot = self.request("GET", "/snapshot")
        self.assertEqual(persisted, snapshot)
        self.assertEqual(persisted["streams"][0]["windows"][0]["count"], 1)


class StartupContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="streammill-startup-")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def path(self, name="state.json"):
        return os.path.join(self.dir, name)

    def write(self, data, name="state.json"):
        path = self.path(name)
        with open(path, "wb") as handle:
            handle.write(data)
        return path

    def test_empty_path_is_rejected(self):
        with self.assertRaises(StateFileError):
            Service.from_state_file("")

    def test_directory_path_is_rejected(self):
        with self.assertRaises(StateFileError):
            Service.from_state_file(self.dir)

    def test_missing_parent_directory_is_rejected(self):
        with self.assertRaises(StateFileError):
            Service.from_state_file(os.path.join(self.dir, "missing", "s.json"))

    @unittest.skipIf(os.geteuid() == 0, "root can write anywhere")
    def test_unwritable_parent_directory_is_rejected(self):
        locked = os.path.join(self.dir, "locked")
        os.mkdir(locked)
        os.chmod(locked, 0o555)
        try:
            with self.assertRaises(StateFileError):
                Service.from_state_file(os.path.join(locked, "s.json"))
        finally:
            os.chmod(locked, 0o755)

    def test_invalid_utf8_is_rejected(self):
        path = self.write(b"\xff\xfe\x00")
        with self.assertRaises(StateFileError):
            Service.from_state_file(path)

    def test_invalid_json_is_rejected(self):
        path = self.write(b"not json")
        with self.assertRaises(StateFileError):
            Service.from_state_file(path)

    def test_unaccepted_snapshot_is_rejected(self):
        path = self.write(json.dumps({"format_version": 99, "streams": []}).encode())
        with self.assertRaises(StateFileError):
            Service.from_state_file(path)
        path = self.write(
            json.dumps(
                {
                    "format_version": 1,
                    "streams": [
                        {
                            "name": "s",
                            "window_ms": 1000,
                            "allowed_lateness_ms": 0,
                            "dedup_retention_ms": None,
                            "watermark_ms": None,
                            "windows": [
                                {
                                    "window_start_ms": 100,
                                    "window_end_ms": 1100,
                                    "count": 1,
                                    "sum": 1,
                                }
                            ],
                            "finalized": [],
                        }
                    ],
                }
            ).encode()
        )
        with self.assertRaises(StateFileError):
            Service.from_state_file(path)

    def test_valid_snapshot_file_loads(self):
        document = {"format_version": 1, "streams": []}
        path = self.write(json.dumps(document).encode())
        service = Service.from_state_file(path)
        self.assertEqual(service.snapshot(), document)

    def test_round_trip_through_the_file(self):
        path = self.path()
        service = Service.from_state_file(path)
        service.create_table("t", event_time_versioned=True)
        service.put_version_row("t", "k", "v1", 100)
        service.create_stream(
            "s",
            1000,
            50,
            dedup_retention_ms=500,
            auto_watermark_lag_ms=10,
            slide_ms=250,
            lookup_table="t",
            change_retention=20,
            batch_retention=3,
        )
        service.add_event("s", 100, 1.5, "e1", "k")
        service.add_batch(
            "s", "b1", [{"timestamp_ms": 120, "value": 2, "event_id": "e2", "lookup_key": "k"}]
        )
        service.advance_watermark("s", 5000)
        expected = service.snapshot()

        reloaded = Service.from_state_file(path)
        self.assertEqual(reloaded.snapshot(), expected)
        # The change cursor and batch replay continue uninterrupted.
        self.assertEqual(
            reloaded.changes("s", 0, 100)["latest_seq"],
            service.changes("s", 0, 100)["latest_seq"],
        )
        replay = reloaded.add_batch(
            "s", "b1", [{"timestamp_ms": 120, "value": 2, "event_id": "e2", "lookup_key": "k"}]
        )
        self.assertEqual(replay["batch_id"], "b1")
        self.assertEqual(reloaded.snapshot(), expected)


class ServerProcessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="streammill-server-")
        self.state_path = os.path.join(self.dir, "state.json")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def env(self, port):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(REPO_ROOT, "src")
        env["STREAMMILL_ADDR"] = f"127.0.0.1:{port}"
        env["STREAMMILL_STATE_FILE"] = self.state_path
        return env

    def free_port(self):
        import socket

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    def start_server(self, port):
        process = subprocess.Popen(
            [sys.executable, "-m", "streammill.server"],
            env=self.env(port),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        line = process.stdout.readline()
        self.assertIn("listening", line)
        return process

    def request(self, port, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}", data=data, method=method
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_restart_recovers_commits(self):
        port = self.free_port()
        server = self.start_server(port)
        try:
            status, _ = self.request(
                port,
                "POST",
                "/streams",
                {"name": "s", "window_ms": 1000, "allowed_lateness_ms": 0},
            )
            self.assertEqual(status, 201)
            status, _ = self.request(
                port, "POST", "/streams/s/events", {"timestamp_ms": 10, "value": 3}
            )
            self.assertEqual(status, 200)
            status, _ = self.request(
                port, "POST", "/streams/s/watermark", {"watermark_ms": 2000}
            )
            self.assertEqual(status, 200)
        finally:
            server.kill()
            server.wait()

        server = self.start_server(port)
        try:
            status, results = self.request(port, "GET", "/streams/s/results")
            self.assertEqual(status, 200)
            self.assertEqual(
                results["results"],
                [
                    {
                        "stream": "s",
                        "window_start_ms": 0,
                        "window_end_ms": 1000,
                        "count": 1,
                        "sum": 3,
                    }
                ],
            )
        finally:
            server.kill()
            server.wait()

    def test_invalid_state_file_exits_nonzero(self):
        for value in ("", os.path.join(self.dir, "missing", "s.json"), self.dir):
            env = self.env(self.free_port())
            env["STREAMMILL_STATE_FILE"] = value
            process = subprocess.run(
                [sys.executable, "-m", "streammill.server"],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertNotEqual(process.returncode, 0, value)
            self.assertNotIn("listening", process.stdout)

        self.write_state(b"not json")
        env = self.env(self.free_port())
        process = subprocess.run(
            [sys.executable, "-m", "streammill.server"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertNotEqual(process.returncode, 0)
        self.assertNotIn("listening", process.stdout)

    def write_state(self, data):
        with open(self.state_path, "wb") as handle:
            handle.write(data)


if __name__ == "__main__":
    unittest.main()
