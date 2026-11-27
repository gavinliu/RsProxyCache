"""End-to-end checks using a real proxy process and a controlled HTTP origin."""

import concurrent.futures
import contextlib
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
import threading
import unittest


BINARY = Path(os.environ.get("RPC_TEST_BINARY", "target/debug/rs-proxy-cache")).resolve()
FFMPEG = None if os.environ.get("RPC_TEST_SKIP_FFMPEG") == "1" else shutil.which("ffmpeg")
SIGNED_PATH = "/video%2Emp4?token=a%2Fb&expires=123"


class Origin(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_HEAD(self):
        self.reply()

    def do_GET(self):
        self.reply()

    def reply(self):
        state = self.server
        requested = self.headers.get("Range")
        state.requests.append((self.command, self.path, requested))
        data, etag = state.data, state.etag
        if self.path == "/playable.mp4?sig=a%2Fb&z=1":
            data, etag = state.mp4, '"mp4"'
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", SIGNED_PATH)
            self.end_headers()
            return
        if self.path == "/denied-redirect":
            self.send_response(302)
            self.send_header("Location", f"http://localhost:{state.server_port}{SIGNED_PATH}")
            self.end_headers()
            return
        if requested and requested == state.fail_range:
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if state.expired:
            self.send_error(403)
            return

        if self.path in state.redirects:
            self.send_response(302)
            self.send_header("Location", state.redirects[self.path])
            self.end_headers()
            return
        content_type, data = state.resources.get(self.path, ("video/mp4", data))
        total = len(data)
        status, start, end = 200, 0, total
        if requested and not state.ignore_range:
            left, right = re.fullmatch(r"bytes=(\d*)-(\d*)", requested).groups()
            start = int(left) if left else max(0, total - int(right))
            end = min(total, int(right) + 1) if left and right else total
            if start >= end:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{total}")
                self.end_headers()
                return
            status = 206

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        if self.path not in state.unsized:
            self.send_header("Content-Length", str(end - start))
        if etag is not None:
            self.send_header("ETag", etag)
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end - 1}/{total}")
        self.end_headers()
        if self.command == "GET":
            if state.delay:
                time.sleep(state.delay)
            try:
                if self.path == state.block_path or (state.block_range and requested == state.block_range):
                    state.release_range.wait(5)
                if requested and requested == state.pause_body_range:
                    middle = (start + end) // 2
                    self.wfile.write(data[start:middle])
                    self.wfile.flush()
                    state.release_range.wait(state.body_pause_seconds)
                    start = middle
                if requested and requested == state.hold_body_range:
                    # Send the whole range, then keep the response open so the
                    # proxy cannot learn that the origin body has ended.
                    written = self.wfile.write(data[start:end])
                    self.wfile.flush()
                    with state.transfer_lock:
                        state.transfers.append((self.path, requested, written))
                    state.release_range.wait(5)
                    return
                stop = (start + end) // 2 if state.truncate else end
                written = self.wfile.write(data[start:stop])
                with state.transfer_lock:
                    state.transfers.append((self.path, requested, written))
                if state.truncate:
                    self.close_connection = True
            except (BrokenPipeError, ConnectionResetError):
                pass



class PlaybackTests(unittest.TestCase):
    origin_handler = Origin

    def setUp(self):
        self.root = self.enterContext(tempfile.TemporaryDirectory())
        self.cache = Path(self.root) / "cache"
        origin = http.server.ThreadingHTTPServer(("127.0.0.1", 0), self.origin_handler)
        self.origin = origin
        self.addCleanup(origin.server_close)
        threading.Thread(target=origin.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
        self.addCleanup(origin.shutdown)
        origin.unsized = set()
        origin.resources = {}
        origin.redirects = {}
        origin.requests = []
        origin.transfers = []
        origin.transfer_lock = threading.Lock()
        origin.fail_range = None
        origin.data = bytes(range(200))
        origin.etag = '"one"'
        origin.expired = origin.ignore_range = origin.truncate = False
        origin.delay = 0
        origin.block_range = origin.block_path = None
        origin.pause_body_range = None
        origin.hold_body_range = None
        origin.body_pause_seconds = 5
        origin.release_range = threading.Event()
        self.source = f"http://127.0.0.1:{origin.server_port}"
        self.proxy = None
        self.addCleanup(self.stop)

    def start(self, capacity=1024, private=True, log_level="info", parallelism=2, queue_capacity=16):
        self.proxy_log = Path(self.root) / "proxy.log"
        with self.proxy_log.open("wb") as log:
            self.proxy = subprocess.Popen(
                [BINARY],
                env={**os.environ, "RPC_LISTEN": "127.0.0.1:0",
                     "RPC_ORIGIN_HOSTS": "127.0.0.1",
                     "RPC_ALLOW_PRIVATE_ORIGINS": str(private).lower(),
                     "RPC_CACHE_DIR": str(self.cache), "RPC_CACHE_BYTES": str(capacity),
                     "RPC_LOG_LEVEL": log_level,
                     "RPC_DOWNLOAD_PARALLELISM": str(parallelism),
                     "RPC_DOWNLOAD_QUEUE_CAPACITY": str(queue_capacity)},
                stderr=log,
            )
        deadline = time.monotonic() + 5
        while True:
            logs = self.proxy_log.read_text(encoding="utf-8", errors="replace")
            ready = re.search(r"listening on configured address listen=127\.0\.0\.1:(\d+)", logs)
            if ready:
                self.port = int(ready[1])
                return
            if self.proxy.poll() is not None or time.monotonic() >= deadline:
                self.fail(f"Proxy failed to start (exit={self.proxy.poll()}):\n{logs}")
            time.sleep(0.01)

    def stop(self):
        if self.proxy is not None:
            if self.proxy.poll() is None:
                self.proxy.terminate()
            try:
                self.proxy.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proxy.kill()
                self.proxy.wait(timeout=5)
            self.proxy = None
            return self.proxy_log.read_text(encoding="utf-8", errors="replace")

    def get(self, spec=None, path=SIGNED_PATH, method="GET", headers_received=None, proxy_path=None):
        headers = {"Range": f"bytes={spec}"} if spec is not None else {}
        with contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)) as conn:
            conn.request(method, proxy_path or "/v1/" + self.source + path, headers=headers)
            response = conn.getresponse()
            if headers_received is not None:
                headers_received.set()
            return response.status, response.headers, response.read()

    def transfers(self):
        with self.origin.transfer_lock:
            return self.origin.transfers.copy()

    def test_mp4_published_at_source_path_only_after_both_ranges(self):
        self.start()
        path = "/movies/demo.mp4?token=secret"
        published = self.cache / "127.0.0.1" / "movies" / "demo.mp4"
        status, _, body = self.get("0-99", path=path)
        self.assertEqual((status, body), (206, self.origin.data[:100]))
        self.assertFalse(published.exists())

        status, _, body = self.get("100-199", path=path)
        self.assertEqual((status, body), (206, self.origin.data[100:]))
        deadline = time.monotonic() + 3
        while not published.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(published.is_file())
        self.assertEqual(published.read_bytes(), self.origin.data)

    def test_mp4_publishes_after_player_closes_first_range_early(self):
        self.start()
        path = "/movies/seek.mp4"
        published = self.cache / "127.0.0.1" / "movies" / "seek.mp4"
        self.origin.pause_body_range = "bytes=0-199"
        with contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)) as conn:
            conn.request("GET", "/v1/" + self.source + path, headers={"Range": "bytes=0-199"})
            with conn.getresponse() as response:
                self.assertEqual(response.status, 206)
                self.assertEqual(response.read(100), self.origin.data[:100])
        try:
            deadline = time.monotonic() + 2
            while list((self.cache / ".rpc").glob(".rpc-stage-*")) and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertFalse(published.exists())
            self.assertEqual(self.get("100-199", path=path)[2], self.origin.data[100:])
            self.assertTrue(published.is_file())
            self.assertEqual(published.read_bytes(), self.origin.data)
        finally:
            self.origin.release_range.set()

    def leave_before_final_bytes(self, path):
        """Receives the response head of a range whose body the origin sends in full
        and then keeps open, and closes the connection without reading the body."""
        self.origin.unsized = {path}
        self.origin.hold_body_range = "bytes=0-99"
        with contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)) as conn:
            conn.request("GET", "/v1/" + self.source + path, headers={"Range": "bytes=0-99"})
            response = conn.getresponse()
            self.assertEqual(response.status, 206)
            # The proxy has received the whole range and is holding its final bytes.
            deadline = time.monotonic() + 3
            while not self.transfers() and time.monotonic() < deadline:
                time.sleep(0.01)
            time.sleep(0.1)
        self.origin.hold_body_range = None

    def await_no_stage(self, timeout=8):
        deadline = time.monotonic() + timeout
        while list((self.cache / ".rpc").glob(".rpc-stage-*")) and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(list((self.cache / ".rpc").glob(".rpc-stage-*")), [])

    def test_fully_received_range_survives_player_leaving_before_final_bytes(self):
        self.start()
        path = "/movies/leave.mp4"
        self.leave_before_final_bytes(path)
        try:
            # Let the proxy observe the disconnect, then let the origin end its response.
            time.sleep(0.3)
            self.origin.release_range.set()
            self.await_no_stage()
        finally:
            self.origin.release_range.set()
        self.origin.requests.clear()
        status, _, body = self.get("0-99", path=path)
        self.assertEqual((status, body), (206, self.origin.data[:100]))
        self.assertEqual(self.origin.requests, [])

    def test_unended_origin_response_is_not_cached_after_player_leaves(self):
        self.start()
        path = "/movies/stalled.mp4"
        self.leave_before_final_bytes(path)
        try:
            # The origin keeps the response open, so its end cannot be confirmed.
            self.await_no_stage()
        finally:
            self.origin.release_range.set()
        self.origin.requests.clear()
        status, _, body = self.get("0-99", path=path)
        self.assertEqual((status, body), (206, self.origin.data[:100]))
        self.assertEqual([span for _, _, span in self.origin.requests], ["bytes=0-99"])

    def test_mp4_query_collision_preserves_both_files_and_evicts_whole_video(self):
        self.start(capacity=400)
        first = "/films/movie.mp4?token=first"
        second = "/films/movie.mp4?token=second"
        self.origin.resources[second] = ("video/mp4", b"B" * 200)
        directory = self.cache / "127.0.0.1" / "films"
        self.assertEqual(self.get(path=first)[2], self.origin.data)
        self.assertEqual(self.get(path=second)[2], b"B" * 200)
        original = directory / "movie.mp4"
        alternate, = directory.glob("movie~*.mp4")
        self.assertEqual(original.read_bytes(), self.origin.data)
        self.assertEqual(alternate.read_bytes(), b"B" * 200)
        self.stop()
        self.start(capacity=400)
        self.origin.expired = True
        self.assertEqual(self.get("10-19", path=second)[2], b"B" * 10)
        self.origin.expired = False
        self.assertEqual(self.get(path="/films/third.mp4")[2], self.origin.data)
        self.assertFalse(original.exists())
        self.assertEqual(alternate.read_bytes(), b"B" * 200)

    def test_whole_mp4_publishes_at_exact_capacity(self):
        self.start(capacity=len(self.origin.data))
        self.assertEqual(self.get(path="/quota/whole.mp4")[2], self.origin.data)
        published = self.cache / "127.0.0.1" / "quota" / "whole.mp4"
        self.assertEqual(published.read_bytes(), self.origin.data)

    def test_fragmented_mp4_publishes_at_exact_publication_capacity(self):
        self.start(capacity=2 * len(self.origin.data))
        path = "/quota/exact.mp4"
        self.assertEqual(self.get("0-99", path=path)[2], self.origin.data[:100])
        self.assertEqual(self.get("100-199", path=path)[2], self.origin.data[100:])
        published = self.cache / "127.0.0.1" / "quota" / "exact.mp4"
        self.assertEqual(published.read_bytes(), self.origin.data)
        self.assertEqual(list((self.cache / ".rpc").glob("*.part")), [])

    def test_fragmented_mp4_preserves_ranges_without_publication_headroom(self):
        self.start(capacity=2 * len(self.origin.data) - 1)
        path = "/quota/fragmented.mp4"
        self.assertEqual(self.get("0-99", path=path)[2], self.origin.data[:100])
        self.assertEqual(self.get("100-199", path=path)[2], self.origin.data[100:])
        self.assertFalse((self.cache / "127.0.0.1" / "quota" / "fragmented.mp4").exists())
        self.origin.expired = True
        self.assertEqual(self.get(path=path)[2], self.origin.data)

    def test_offline_queue_is_bounded_and_cancelled_work_never_fetches(self):
        self.start(parallelism=1, queue_capacity=1)
        self.origin.block_path = "/active.mp4"

        def task_request(source, method="GET"):
            status, _, body = self.get(method=method, proxy_path="/v1/offline/" + self.source + source)
            return status, json.loads(body)

        def await_state(source, state):
            deadline = time.monotonic() + 3
            while True:
                _, progress = task_request(source)
                if progress["state"] == state:
                    return
                self.assertLess(time.monotonic(), deadline, progress)
                time.sleep(0.01)

        self.assertFalse(task_request("/active.mp4", "POST")[1]["queued"])
        self.assertTrue(task_request("/cancelled.mp4", "POST")[1]["queued"])
        self.assertTrue(task_request("/cancelled.mp4", "POST")[1]["queued"])
        status, progress = task_request("/next.mp4", "POST")
        self.assertEqual((status, progress["error_code"]), (429, "queue_full"))
        task_request("/cancelled.mp4", "DELETE")
        await_state("/cancelled.mp4", "cancelled")
        self.assertTrue(task_request("/next.mp4", "POST")[1]["queued"])
        self.assertEqual(self.get(path="/playing.mp4")[2], self.origin.data)
        untouched = {"/cancelled.mp4", "/next.mp4"}
        self.assertFalse(any(path in untouched for _, path, _ in self.origin.requests))
        self.origin.release_range.set()
        await_state("/active.mp4", "completed")
        await_state("/next.mp4", "completed")
        self.assertFalse(any(path == "/cancelled.mp4" for _, path, _ in self.origin.requests))

    def test_offline_task_publishes_mp4_and_reports_completion(self):
        self.start()
        path = "/v1/offline/" + self.source + "/download/movie.mp4"
        status, _, body = self.get(method="POST", proxy_path=path)
        self.assertEqual(status, 202)
        self.assertEqual(json.loads(body)["state"], "running")
        deadline = time.monotonic() + 5
        while True:
            status, _, body = self.get(proxy_path=path)
            self.assertEqual(status, 200)
            task = json.loads(body)
            if task["state"] != "running" or time.monotonic() >= deadline:
                break
            time.sleep(0.01)
        self.assertEqual(task["state"], "completed")
        self.assertEqual(task["processed_bytes"], len(self.origin.data))
        published = self.cache / "127.0.0.1" / "download" / "movie.mp4"
        self.assertEqual(published.read_bytes(), self.origin.data)

    def test_offline_reports_capacity_write_and_publication_failures(self):
        self.start(capacity=400)

        self.origin.resources["/oversized.mp4"] = ("video/mp4", b"x" * 401)
        self.assertEqual(self.await_offline_task("/oversized.mp4")["state"], "insufficient_capacity")

        self.stop()
        self.cache = Path(self.root) / "write-failure-cache"
        self.start(capacity=400)
        store = self.cache / ".rpc"
        store.rmdir()
        store.write_text("blocks staging", encoding="utf-8")
        try:
            task = self.await_offline_task("/write.mp4")
            self.assertEqual((task["state"], task["error_code"]), ("failed", "cache_write_failed"))
        finally:
            store.unlink()
            store.mkdir()
        self.assertEqual(self.await_offline_task("/write.mp4")["state"], "completed")

        self.stop()
        self.cache = Path(self.root) / "fragmented-cache"
        self.start(capacity=400)
        self.assertEqual(self.get("0-99", path="/blocked/movie.mp4")[2], self.origin.data[:100])
        folder = self.cache / "127.0.0.1" / "blocked"
        folder.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256((self.source + "/blocked/movie.mp4").encode()).hexdigest()[:12]
        blocker = folder / "movie.mp4"
        alternate = folder / f"movie~{digest}.mp4"
        for path in (blocker, alternate):
            path.write_text("blocks publishing", encoding="utf-8")
        task = self.await_offline_task("/blocked/movie.mp4")
        self.assertEqual((task["state"], task["error_code"]), ("failed", "publish_failed"))
        self.assertEqual(self.get(path="/blocked/movie.mp4")[2], self.origin.data)
        blocker.unlink()
        alternate.unlink()
        previous = len(self.origin.requests)
        self.assertEqual(self.await_offline_task("/blocked/movie.mp4")["state"], "completed")
        self.assertEqual(len(self.origin.requests), previous)

    @unittest.skipUnless(os.environ.get("RPC_TEST_DISK_FULL_ROOT"), "requires a small disposable test volume")
    def test_offline_reports_real_disk_full_and_retries(self):
        import errno

        volume = Path(os.environ["RPC_TEST_DISK_FULL_ROOT"]).resolve()
        self.assertTrue(volume.is_mount(), "use a dedicated mounted test volume")
        total = shutil.disk_usage(volume).total
        self.assertLessEqual(total, 48 * 1024 * 1024, "refusing to fill a large volume")
        folder = self.enterContext(tempfile.TemporaryDirectory(dir=volume))
        self.cache = Path(folder) / "cache"
        self.start(capacity=16 * 1024 * 1024)
        self.addCleanup(self.stop)
        source = "/disk-full.mp4"
        data = b"x" * (4 * 1024 * 1024)
        self.origin.resources[source] = ("video/mp4", data)

        filler = Path(folder) / "filler"
        try:
            with filler.open("wb", buffering=0) as stream:
                with self.assertRaises(OSError) as error:
                    block = bytes(64 * 1024)
                    while True:
                        stream.write(block)
                self.assertEqual(error.exception.errno, errno.ENOSPC)
                stream.truncate(max(0, stream.tell() - 256 * 1024))
            result = self.await_offline_task(source)
            self.assertEqual((result["state"], result["error_code"]), ("failed", "disk_full"))
            self.assertEqual(self.get(path=source)[2], data)
        finally:
            filler.unlink(missing_ok=True)
        self.assertEqual(self.await_offline_task(source)["state"], "completed")
        self.assertEqual((self.cache / "127.0.0.1" / "disk-full.mp4").read_bytes(), data)
        self.stop()

    def test_offline_reports_disallowed_origin_address(self):
        self.start(private=False)
        task = self.await_offline_task("/blocked-address.mp4")
        self.assertEqual((task["state"], task["error_code"]), ("failed", "origin_address_denied"))
        self.assertEqual(self.origin.requests, [])

    def test_offline_truncated_body_reports_failure_and_can_retry(self):
        self.start()
        source = "/offline-truncated.mp4"
        self.origin.truncate = True
        self.assertEqual(self.await_offline_task(source)["error_code"], "origin_body_failed")
        self.assertFalse((self.cache / "127.0.0.1" / "offline-truncated.mp4").exists())
        self.origin.truncate = False
        self.assertEqual(self.await_offline_task(source)["state"], "completed")

    @unittest.skipUnless(os.environ.get("RPC_TEST_FULL") == "1", "network timeout is checked in full verification")
    def test_offline_stalled_body_reports_origin_timeout(self):
        self.start()
        source = "/offline-timeout.mp4"
        self.origin.pause_body_range = "bytes=0-"
        self.origin.body_pause_seconds = 40
        try:
            result = self.await_offline_task(source, timeout=35)
            self.assertEqual((result["state"], result["error_code"]), ("failed", "origin_timeout"))
        finally:
            self.origin.release_range.set()
        self.assertFalse((self.cache / "127.0.0.1" / "offline-timeout.mp4").exists())
        self.assertEqual(self.await_offline_task(source)["state"], "completed")

    def await_offline_task(self, source, timeout=5):
        path = "/v1/offline/" + self.source + source
        self.assertEqual(self.get(method="POST", proxy_path=path)[0], 202)
        deadline = time.monotonic() + timeout
        while True:
            status, _, body = self.get(proxy_path=path)
            self.assertEqual(status, 200)
            task = json.loads(body)
            if task["state"] != "running":
                return task
            self.assertLess(time.monotonic(), deadline, task)
            time.sleep(0.05)

    def test_failed_offline_download_can_retry_and_publish(self):
        self.start()
        source = "/retry/movie.mp4"
        path = "/v1/offline/" + self.source + source
        published = self.cache / "127.0.0.1" / "retry" / "movie.mp4"
        self.assertEqual(self.get("0-99", path=source)[2], self.origin.data[:100])
        self.origin.fail_range = "bytes=100-199"

        def run_download():
            self.assertEqual(self.get(method="POST", proxy_path=path)[0], 202)
            deadline = time.monotonic() + 5
            while True:
                status, _, body = self.get(proxy_path=path)
                self.assertEqual(status, 200)
                task = json.loads(body)
                if task["state"] != "running":
                    return task
                self.assertLess(time.monotonic(), deadline, task)
                time.sleep(0.01)

        failure = run_download()
        self.assertEqual((failure["state"], failure["error_code"]), ("failed", "origin_response"))
        self.assertFalse(published.exists())
        self.origin.fail_range = None
        self.assertEqual(run_download()["state"], "completed")
        self.assertEqual(published.read_bytes(), self.origin.data)
        self.assertEqual([span for method, _, span in self.origin.requests if method == "GET"],
                         ["bytes=0-99", "bytes=100-199", "bytes=100-199"])

    def test_offline_task_adds_requested_hls_variant_to_existing_master(self):
        self.start(capacity=8192)
        self.origin.resources["/master.m3u8"] = ("application/vnd.apple.mpegurl", b"""#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=100
low.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=500
high.m3u8
""")
        for variant in ("low", "high"):
            self.origin.resources[f"/{variant}.m3u8"] = (
                "application/vnd.apple.mpegurl",
                f"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\n{variant}.ts\n#EXT-X-ENDLIST\n".encode(),
            )
            self.origin.resources[f"/{variant}.ts"] = ("video/mp2t", variant.encode())
        path = "/v1/offline/" + self.source + "/master.m3u8"
        published = self.cache / "127.0.0.1" / "master.m3u8"
        for bandwidth, expected in [(None, ["low.m3u8"]), (500, ["low.m3u8", "high.m3u8"])]:
            with contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)) as conn:
                headers = {"X-RPC-Bandwidth": str(bandwidth)} if bandwidth else {}
                conn.request("POST", path, headers=headers)
                response = conn.getresponse()
                self.assertEqual(response.status, 202)
                response.read()
            deadline = time.monotonic() + 5
            while True:
                _, _, body = self.get(proxy_path=path)
                progress = json.loads(body)
                if progress["state"] != "running" or time.monotonic() >= deadline:
                    break
                time.sleep(0.01)
            self.assertEqual(progress["state"], "completed")
            self.assertEqual([line for line in published.read_text().splitlines()
                              if line and not line.startswith("#")], expected)

    def test_offline_cancel_preserves_previously_cached_range(self):
        self.start()
        source = "/cancel/movie.mp4"
        path = "/v1/offline/" + self.source + source
        self.assertEqual(self.get("0-99", path=source)[2], self.origin.data[:100])
        self.origin.block_range = "bytes=100-199"
        try:
            self.assertEqual(self.get(method="POST", proxy_path=path)[0], 202)
            deadline = time.monotonic() + 5
            while not any(request[2] == "bytes=100-199" for request in self.origin.requests):
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.01)
            self.assertEqual(self.get(method="POST", proxy_path=path)[0], 202)
            self.assertEqual(self.get(method="DELETE", proxy_path=path)[0], 200)
            while json.loads(self.get(proxy_path=path)[2])["state"] != "cancelled":
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.01)
        finally:
            self.origin.release_range.set()
        self.origin.expired = True
        self.assertEqual(self.get("0-99", path=source)[2], self.origin.data[:100])
        self.assertFalse((self.cache / "127.0.0.1" / "cancel" / "movie.mp4").exists())

    def test_hls_rewrites_redirected_playlist_using_final_url(self):
        self.start()
        self.origin.redirects["/stream"] = "/media/entry"
        self.origin.resources["/media/entry"] = (
            "application/vnd.apple.mpegurl",
            b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nclip.ts?sig=a%2Fb&z=2\n#EXT-X-ENDLIST\n",
        )
        self.origin.resources["/media/clip.ts?sig=a%2Fb&z=2"] = ("video/mp2t", b"segment")
        status, headers, body = self.get(path="/stream")
        self.assertEqual((status, headers["Content-Type"]), (200, "application/vnd.apple.mpegurl"))
        segment = body.decode().splitlines()[3]
        self.assertTrue(segment.startswith("/"))
        self.assertEqual(self.get(proxy_path=segment)[2], b"segment")

    def test_hls_publishes_local_playlist_and_plays_after_restart(self):
        self.start()
        self.origin.resources["/show/index.m3u8"] = (
            "application/vnd.apple.mpegurl",
            b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nclip.ts\n#EXT-X-ENDLIST\n",
        )
        self.origin.resources["/show/clip.ts"] = ("video/mp2t", b"segment")
        published = self.cache / "127.0.0.1" / "show" / "index.m3u8"
        _, _, playlist = self.get(path="/show/index.m3u8")
        self.assertFalse(published.exists())
        self.assertEqual(self.get(proxy_path=playlist.decode().splitlines()[3])[2], b"segment")
        deadline = time.monotonic() + 3
        while not published.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(published.is_file())
        self.assertEqual((published.parent / "clip.ts").read_bytes(), b"segment")
        self.assertIn("\nclip.ts\n", published.read_text())
        self.stop()
        self.origin.expired = True
        self.origin.requests.clear()
        self.start()
        status, _, playlist = self.get(path="/show/index.m3u8")
        self.assertEqual(status, 200)
        self.assertEqual(self.get(proxy_path=playlist.decode().splitlines()[3])[2], b"segment")
        self.assertEqual(self.origin.requests, [])

    def test_hls_master_routes_variants_audio_and_subtitles_through_proxy(self):
        self.start()
        self.origin.resources["/master.m3u8"] = ("text/plain", b"""#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="English, stereo",URI="audio"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="English",URI="subs"
#EXT-X-STREAM-INF:BANDWIDTH=100000,AUDIO="audio",SUBTITLES="subs"
low
#EXT-X-STREAM-INF:BANDWIDTH=500000,AUDIO="audio",SUBTITLES="subs"
high
""")
        for rendition in ("audio", "subs", "low", "high"):
            self.origin.resources[f"/{rendition}"] = (
                "text/plain",
                f"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\n{rendition}.bin\n#EXT-X-ENDLIST\n".encode(),
            )
        status, _, body = self.get(path="/master.m3u8")
        self.assertEqual(status, 200)
        lines = body.decode().splitlines()
        self.assertIn('NAME="English, stereo"', lines[1])
        urls = re.findall(r'URI="([^"]+)"', body.decode()) + [lines[4], lines[6]]
        for url, rendition in zip(urls, ("audio", "subs", "low", "high"), strict=True):
            status, _, playlist = self.get(proxy_path=url)
            self.assertEqual(status, 200)
            self.assertIn(f"/{rendition}.bin".encode(), playlist)

    def test_hls_publishes_only_referenced_bytes_of_incomplete_resource(self):
        self.start()
        self.origin.resources["/show/range.m3u8"] = ("application/vnd.apple.mpegurl", b"""#EXTM3U
#EXT-X-VERSION:4
#EXT-X-TARGETDURATION:4
#EXTINF:4,
#EXT-X-BYTERANGE:4@2
shared.ts
#EXT-X-ENDLIST
""")
        self.origin.resources["/show/shared.ts"] = ("video/mp2t", b"12abcd78")
        _, _, playlist = self.get(path="/show/range.m3u8")
        self.assertEqual(self.get("2-5", proxy_path=playlist.decode().splitlines()[-2])[2], b"abcd")
        published = self.cache / "127.0.0.1" / "show" / "range.m3u8"
        deadline = time.monotonic() + 3
        while not published.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(published.is_file())
        self.assertFalse((published.parent / "shared.ts").exists())
        segment = published.read_text().splitlines()[-2]
        self.assertEqual((published.parent / segment).read_bytes(), b"abcd")
        self.origin.expired = True
        _, _, offline = self.get(path="/show/range.m3u8")
        self.assertEqual(self.get("2-5", proxy_path=offline.decode().splitlines()[-2])[2], b"abcd")

    def test_clear_hls_releases_exclusive_segments_and_preserves_shared_segments(self):
        self.start(capacity=4096)
        directory = self.cache / "127.0.0.1"
        for name in ("one", "two"):
            self.origin.resources[f"/{name}.m3u8"] = (
                "application/vnd.apple.mpegurl",
                f"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nshared.ts\n"
                f"#EXTINF:4,\n{name}.ts\n#EXT-X-ENDLIST\n".encode(),
            )
            _, _, playlist = self.get(path=f"/{name}.m3u8")
            for line in playlist.decode().splitlines():
                if line.startswith("/"):
                    self.assertEqual(self.get(proxy_path=line)[0], 200)
            self.assertTrue((directory / f"{name}.m3u8").is_file())
        self.stop()
        subprocess.run([BINARY, "clear", self.source + "/one.m3u8"], check=True,
                       env={**os.environ, "RPC_CACHE_DIR": str(self.cache)})
        self.assertFalse((directory / "one.m3u8").exists())
        self.assertFalse((directory / "one.ts").exists())
        self.assertTrue((directory / "two.m3u8").is_file())
        self.assertTrue((directory / "shared.ts").is_file())

    def test_hls_fmp4_ranges_and_aes_key_use_their_own_cache_policies(self):
        self.start()
        self.origin.resources["/movie.m3u8"] = ("text/plain", b"""#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXT-X-MAP:URI="shared.mp4",BYTERANGE="4@0"
#EXT-X-KEY:METHOD=AES-128,URI="key?sig=x%2Fy",IV=0x00000000000000000000000000000001
#EXTINF:4,
#EXT-X-BYTERANGE:6@4
shared.mp4
#EXT-X-ENDLIST
""")
        self.origin.resources["/shared.mp4"] = ("video/mp4", b"initabcdef")
        self.origin.resources["/key?sig=x%2Fy"] = ("application/octet-stream", b"0123456789abcdef")

        def play():
            status, _, body = self.get(path="/movie.m3u8")
            self.assertEqual(status, 200)
            init, key = re.findall(r'URI="([^"]+)"', body.decode())
            segment = body.decode().splitlines()[-2]
            self.assertEqual(init, segment)
            self.assertEqual(self.get("0-3", proxy_path=init)[2], b"init")
            self.assertEqual(self.get("4-9", proxy_path=segment)[2], b"abcdef")
            return self.get(proxy_path=key)[2]

        self.assertEqual(play(), b"0123456789abcdef")
        self.stop()
        self.start()
        self.origin.resources["/key?sig=x%2Fy"] = ("application/octet-stream", b"fedcba9876543210")
        self.origin.requests.clear()
        self.assertEqual(play(), b"fedcba9876543210")
        self.assertEqual(self.origin.requests, [
            ("GET", "/movie.m3u8", None), ("GET", "/key?sig=x%2Fy", None),
        ])

    def test_hls_whole_segments_are_reused_when_origin_ignores_ranges(self):
        self.start()
        self.origin.ignore_range = True
        self.origin.unsized.add("/unsized.ts")
        self.origin.resources["/movie.m3u8"] = ("text/plain", b"""#EXTM3U
#EXT-X-TARGETDURATION:4
#EXTINF:4,
clip.ts
#EXTINF:4,
unsized.ts
#EXT-X-ENDLIST
""")
        self.origin.resources["/clip.ts"] = ("video/mp2t", b"known-length")
        self.origin.resources["/unsized.ts"] = ("video/mp2t", b"unknown-length")
        _, _, playlist = self.get(path="/movie.m3u8")
        sized, unsized = (line for line in playlist.decode().splitlines() if line.startswith("/"))
        self.assertEqual(self.get(proxy_path=sized)[2], b"known-length")
        self.assertEqual(self.get(proxy_path=unsized)[2], b"unknown-length")
        self.stop()
        self.start()
        self.origin.expired = True
        self.assertEqual(self.get(proxy_path=sized)[2], b"known-length")
        self.assertEqual(self.get(proxy_path=unsized)[2], b"unknown-length")

    def test_hls_truncated_whole_segment_is_not_reused(self):
        self.start()
        self.origin.ignore_range = True
        self.origin.resources["/movie.m3u8"] = (
            "text/plain",
            b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nclip.ts\n#EXT-X-ENDLIST\n",
        )
        self.origin.resources["/clip.ts"] = ("video/mp2t", b"old-segment")
        _, _, playlist = self.get(path="/movie.m3u8")
        segment = playlist.decode().splitlines()[-2]
        self.origin.truncate = True
        with self.assertRaises(http.client.IncompleteRead):
            self.get(proxy_path=segment)
        self.stop()
        self.start()
        self.origin.truncate = False
        self.origin.resources["/clip.ts"] = ("video/mp2t", b"new-segment")
        self.assertEqual(self.get(proxy_path=segment)[2], b"new-segment")

    def test_hls_rejects_live_drm_and_disallowed_resource_urls(self):
        self.start()
        for name, tags, reference, end in (
            ("live", b"", b"clip.ts", b""),
            ("drm", b'#EXT-X-KEY:METHOD=SAMPLE-AES,URI="key"\n', b"clip.ts", b"#EXT-X-ENDLIST\n"),
            ("host", b"", b"https://localhost/clip.ts", b"#EXT-X-ENDLIST\n"),
            ("scheme", b"", b"file:///clip.ts", b"#EXT-X-ENDLIST\n"),
        ):
            with self.subTest(name=name):
                path = f"/{name}.m3u8"
                self.origin.resources[path] = (
                    "application/vnd.apple.mpegurl",
                    b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n" + tags + b"#EXTINF:4,\n" + reference + b"\n" + end,
                )
                self.assertEqual(self.get(path=path)[0], 502)

    @unittest.skipUnless(FFMPEG, "requires FFmpeg")
    def test_ffmpeg_decodes_and_seeks_hls_ts_and_fmp4(self):
        self.start(capacity=8 * 1024 * 1024)
        for segment_type in ("mpegts", "fmp4"):
            with self.subTest(segment_type=segment_type):
                directory = Path(self.root) / segment_type
                directory.mkdir()
                subprocess.run([
                    FFMPEG, "-v", "error", "-f", "lavfi", "-i",
                    "testsrc2=size=160x90:rate=10", "-t", "6", "-c:v", "libx264",
                    "-g", "10", "-sc_threshold", "0", "-hls_time", "1",
                    "-hls_playlist_type", "vod", "-hls_segment_type", segment_type,
                    str(directory / "movie.m3u8"),
                ], check=True, timeout=20)
                for asset in directory.iterdir():
                    self.origin.resources[f"/{segment_type}/{asset.name}"] = (
                        "application/vnd.apple.mpegurl" if asset.suffix == ".m3u8"
                        else "video/mp2t" if asset.suffix == ".ts" else "video/mp4",
                        asset.read_bytes(),
                    )
                def decode(address, input_seek=True):
                    seek = ["-ss", "4"]
                    source_args = ["-i", str(address)]
                    args = seek + source_args if input_seek else source_args + seek
                    result = subprocess.run([
                        FFMPEG, "-v", "error", *args, "-frames:v", "4",
                        "-f", "framehash", "-",
                    ], check=True, timeout=20, capture_output=True)
                    return re.findall(rb"(?m)^0,.*", result.stdout)

                fixture = directory / "movie.m3u8"
                expected = decode(fixture)
                input_seek = True
                if segment_type == "fmp4" and not expected:
                    # FFmpeg 5.1 can fail to seek even in its own local fMP4 HLS output.
                    # Select the decode mode with the original files before using the proxy.
                    input_seek = False
                    expected = decode(fixture, input_seek)
                self.assertEqual(len(expected), 4)
                url = f"http://127.0.0.1:{self.port}/v1/{self.source}/{segment_type}/movie.m3u8"
                decoded = decode(url, input_seek)
                self.assertEqual(decoded, expected)
                self.assertTrue(any(item[1].startswith(f"/{segment_type}/movie4")
                                    for item in self.origin.requests))
                offline = f"/v1/offline/{self.source}/{segment_type}/movie.m3u8"
                self.assertEqual(self.get(method="POST", proxy_path=offline)[0], 202)
                deadline = time.monotonic() + 5
                while True:
                    progress = json.loads(self.get(proxy_path=offline)[2])
                    if progress["state"] != "running" or time.monotonic() >= deadline:
                        break
                    time.sleep(0.01)
                self.assertEqual(progress["state"], "completed")
                published = self.cache / "127.0.0.1" / segment_type / "movie.m3u8"
                self.origin.expired = True
                try:
                    local = decode(published, input_seek)
                finally:
                    self.origin.expired = False
                self.assertEqual(local, decoded)

    def test_ranges_hits_restart_expiry_and_clear(self):
        self.start()
        status, headers, body = self.get("80-99")
        self.assertEqual((status, headers["Content-Range"], body),
                         (206, "bytes 80-99/200", self.origin.data[80:100]))
        self.assertEqual(self.origin.requests, [("GET", SIGNED_PATH, "bytes=80-99")])
        self.assertEqual(self.transfers(), [(SIGNED_PATH, "bytes=80-99", 20)])
        self.assertEqual(self.get("90-94")[2], self.origin.data[90:95])
        self.assertEqual(self.transfers(), [(SIGNED_PATH, "bytes=80-99", 20)])
        self.assertEqual(self.get(method="HEAD")[1]["Content-Length"], "200")
        self.stop()
        self.origin.expired = True
        self.start()
        self.assertEqual(self.get("80-99")[2], self.origin.data[80:100])
        self.assertEqual(len(self.origin.requests), 1)
        self.assertEqual(self.get("75-90")[0], 403)
        self.assertEqual(self.origin.requests[-1][2], "bytes=75-79")
        self.stop()
        subprocess.run([BINARY, "clear", self.source + SIGNED_PATH], check=True,
                       env={**os.environ, "RPC_CACHE_DIR": str(self.cache)})
        self.start()
        self.assertEqual(self.get("80-99")[0], 403)

    def test_deleted_interval_is_refilled_and_hits_again(self):
        self.start(log_level="debug")
        self.get("40-59")
        self.get("60-79")
        next(path for path in (self.cache / ".rpc").glob("*.part")
             if path.read_bytes() == self.origin.data[40:60]).unlink()
        self.assertEqual(self.get("40-79")[2], self.origin.data[40:80])
        self.assertEqual(self.get("40-79")[2], self.origin.data[40:80])
        self.assertEqual([item[2] for item in self.origin.requests],
                         ["bytes=40-59", "bytes=60-79", "bytes=40-59"])
        logs = self.stop()
        for field in ("read_interval", "request_id=", "cache_key=", "cache hit", "elapsed_ms="):
            self.assertIn(field, logs)
        self.assertNotIn(SIGNED_PATH, logs)
        self.assertNotIn("token=a%2Fb", logs)

    def test_directory_recovery_discards_inflight_old_writes(self):
        self.start()
        self.get("0-9")
        self.origin.block_range = "bytes=20-39"
        headers_received = threading.Event()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            inflight = executor.submit(self.get, "20-39", SIGNED_PATH, "GET", headers_received)
            try:
                self.assertTrue(headers_received.wait(5))
                shutil.rmtree(self.cache)
                self.assertEqual(self.get("0-9")[2], self.origin.data[:10])
                self.assertEqual(self.get("0-9")[2], self.origin.data[:10])
            finally:
                self.origin.release_range.set()
            self.assertEqual(inflight.result()[2], self.origin.data[20:40])
        self.assertEqual(self.get("20-39")[2], self.origin.data[20:40])
        self.assertEqual(self.get("20-39")[2], self.origin.data[20:40])
        self.assertEqual([item[2] for item in self.origin.requests],
                         ["bytes=0-9", "bytes=20-39", "bytes=0-9", "bytes=20-39"])
        self.assertIn("cache directory recovered", self.stop())

    @unittest.skipUnless(os.name == "posix", "requires POSIX flock")
    def test_recovery_waits_for_exclusive_directory_lock(self):
        import fcntl

        self.start()
        self.get("0-9")
        shutil.rmtree(self.cache)
        self.cache.mkdir()
        with (self.cache / ".lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.assertEqual(self.get("0-9")[2], self.origin.data[:10])
            self.assertEqual(self.get("0-9")[2], self.origin.data[:10])
            self.assertEqual(list((self.cache / ".rpc").glob("*.part")), [])
        self.assertEqual(self.get("0-9")[2], self.origin.data[:10])
        self.assertEqual(self.get("0-9")[2], self.origin.data[:10])
        self.assertEqual(len(self.origin.requests), 4)
        logs = self.stop()
        self.assertEqual(logs.count('operation="recover_directory"'), 1)
        self.assertIn("cache directory recovered", logs)

    def test_staging_failure_keeps_playing_and_later_caches(self):
        self.start()
        store = self.cache / ".rpc"
        store.rmdir()
        store.write_text("blocks staging", encoding="utf-8")
        try:
            for _ in range(2):
                status, _, body = self.get("20-39")
                self.assertEqual((status, body), (206, self.origin.data[20:40]))
        finally:
            store.unlink()
            store.mkdir()

        self.assertEqual(self.get("20-39")[2], self.origin.data[20:40])
        self.assertEqual(self.get("20-39")[2], self.origin.data[20:40])
        self.assertEqual([span for _, _, span in self.origin.requests], ["bytes=20-39"] * 3)
        self.assertIn("stage_interval", self.stop())

    def test_restart_cleans_up_interrupted_write_and_caches_on_retry(self):
        self.start()
        self.origin.pause_body_range = "bytes=20-39"
        store = self.cache / ".rpc"
        with contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)) as conn:
            conn.request("GET", "/v1/" + self.source + SIGNED_PATH, headers={"Range": "bytes=20-39"})
            response = conn.getresponse()
            self.assertEqual(response.status, 206)
            self.assertEqual(response.read(10), self.origin.data[20:30])
            self.assertTrue(list(store.glob(".rpc-stage-*")))
            self.proxy.kill()
            self.proxy.wait(timeout=5)
        self.stop()
        self.origin.release_range.set()
        self.origin.pause_body_range = None

        self.start()
        self.assertFalse(list(store.glob(".rpc-stage-*")))
        self.assertEqual(self.get("20-39")[2], self.origin.data[20:40])
        self.assertEqual(self.get("20-39")[2], self.origin.data[20:40])
        self.assertEqual([span for _, _, span in self.origin.requests], ["bytes=20-39"] * 2)

    def test_deleted_interval_after_headers_requires_same_version(self):
        self.start()
        for expected_same_version in (True, False):
            with self.subTest(same_version=expected_same_version):
                self.get("0-9")
                self.get("20-29")
                self.origin.block_range = "bytes=10-19"
                self.origin.release_range.clear()
                headers_received = threading.Event()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    inflight = executor.submit(self.get, "0-29", SIGNED_PATH, "GET", headers_received)
                    try:
                        self.assertTrue(headers_received.wait(5))
                        next(path for path in (self.cache / ".rpc").glob("*.part")
                             if path.read_bytes() == self.origin.data[20:30]).unlink()
                        if not expected_same_version:
                            self.origin.etag = '"replacement"'
                    finally:
                        self.origin.release_range.set()
                    if expected_same_version:
                        self.assertEqual(inflight.result()[2], self.origin.data[:30])
                        self.assertEqual(self.origin.requests[-1][2], "bytes=20-29")
                        self.origin.block_range = None
                        self.stop()
                        shutil.rmtree(self.cache)
                        self.start()
                    else:
                        with self.assertRaises(http.client.IncompleteRead) as error:
                            inflight.result()
                        self.assertEqual(error.exception.partial, self.origin.data[:20])

    def test_fill_only_gaps_then_replace_representation(self):
        self.start()
        self.get("40-59")
        self.assertEqual(self.get("30-69")[2], self.origin.data[30:70])
        self.assertEqual([item[2] for item in self.origin.requests],
                         ["bytes=40-59", "bytes=30-39", "bytes=60-69"])
        self.assertCountEqual(self.transfers(),
                              [(SIGNED_PATH, "bytes=40-59", 20),
                               (SIGNED_PATH, "bytes=30-39", 10),
                               (SIGNED_PATH, "bytes=60-69", 10)])
        self.origin.etag = '"two"'
        self.origin.data = bytes(reversed(range(150)))
        self.assertEqual(self.get("40-199")[2], self.origin.data[40:])
        self.assertEqual(self.get("-10")[2], self.origin.data[-10:])
        self.assertEqual(len(self.origin.requests), 5)

    def test_detected_replacement_invalidates_old_ranges_even_if_retry_fails(self):
        self.start()
        self.assertEqual(self.get("40-59")[0], 206)
        self.origin.etag = '"two"'
        self.origin.data = bytes(reversed(range(200)))
        self.origin.fail_range = "bytes=40-69"
        self.assertEqual(self.get("40-69")[0], 503)
        self.assertEqual(self.get("40-59")[2], self.origin.data[40:60])
        self.assertEqual([item[2] for item in self.origin.requests],
                         ["bytes=40-59", "bytes=60-69", "bytes=40-69", "bytes=40-59"])

    def test_persistent_lru(self):
        self.start(capacity=40)
        self.get("0-19")
        self.get("20-39")
        self.get("0-19")
        self.stop()
        self.start(capacity=40)
        self.get("40-59")
        self.get("0-19")
        self.assertEqual(len(self.origin.requests), 3)
        self.get("20-39")
        self.assertEqual(len(self.origin.requests), 4)

    def test_concurrent_identical_ranges(self):
        self.start()
        self.origin.delay = 0.2
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(self.get, ["40-59", "40-59"]))
        self.assertEqual([result[2] for result in results], [self.origin.data[40:60]] * 2)
        self.assertEqual(len(self.origin.requests), 1)

    def test_reopened_range_does_not_wait_for_previous_response_to_drain(self):
        self.start(capacity=1024)
        self.origin.block_range = "bytes=0-"
        # The first response has headers, but no bytes and no completed cache reservation.
        with contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)) as conn:
            conn.request("GET", "/v1/" + self.source + SIGNED_PATH, headers={"Range": "bytes=0-"})
            self.assertEqual(conn.getresponse().status, 206)
            with contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)) as reopened:
                reopened.request("GET", "/v1/" + self.source + SIGNED_PATH, headers={"Range": "bytes=0-"})
                try:
                    self.assertEqual(reopened.getresponse().status, 206)
                    self.assertEqual(len(self.origin.requests), 2)
                finally:
                    self.origin.release_range.set()

    def test_different_playback_ranges_share_the_same_gap(self):
        self.start()
        self.get("0-39")
        self.get("60-99")
        self.origin.block_range = "bytes=40-59"
        headers_received = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.get, "20-79", headers_received=headers_received)
            try:
                self.assertTrue(headers_received.wait(3))
                second = pool.submit(self.get, "30-89")
                with self.assertRaises(concurrent.futures.TimeoutError):
                    second.result(timeout=0.2)
                self.assertEqual([line[2] for line in self.origin.requests].count("bytes=40-59"), 1)
            finally:
                self.origin.release_range.set()
            self.assertEqual(first.result(timeout=3)[2], self.origin.data[20:80])
            self.assertEqual(second.result(timeout=3)[2], self.origin.data[30:90])
        self.assertEqual([line for line in self.transfers() if line[1] == "bytes=40-59"],
                         [(SIGNED_PATH, "bytes=40-59", 20)])

    def test_failed_shared_gap_can_be_retried(self):
        self.start()
        self.get("0-39")
        self.get("60-99")
        self.origin.block_range = "bytes=40-59"
        self.origin.truncate = True
        headers_received = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.get, "20-79", headers_received=headers_received)
            try:
                self.assertTrue(headers_received.wait(3))
                second = pool.submit(self.get, "30-89")
                with self.assertRaises(concurrent.futures.TimeoutError):
                    second.result(timeout=0.2)
            finally:
                self.origin.release_range.set()
            with self.assertRaises(http.client.IncompleteRead):
                first.result(timeout=3)
            self.assertEqual(second.result(timeout=3)[0], 502)
        self.assertEqual([line[2] for line in self.origin.requests].count("bytes=40-59"), 1)
        self.origin.truncate = False
        self.assertEqual(self.get("30-89")[2], self.origin.data[30:90])

    def test_seek_does_not_wait_for_existing_download(self):
        self.start()
        self.origin.block_range = "bytes=0-99"
        headers_received = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.get, "0-99", headers_received=headers_received)
            try:
                self.assertTrue(headers_received.wait(3))
                second = pool.submit(self.get, "150-159")
                self.assertEqual(second.result(timeout=3)[2], self.origin.data[150:160])
            finally:
                self.origin.release_range.set()
            self.assertEqual(first.result(timeout=3)[2], self.origin.data[:100])

    def test_inflight_old_version_cannot_repopulate_cache(self):
        self.start()
        self.origin.block_range = "bytes=0-99"
        original = self.origin.data
        headers_received = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(self.get, "0-99", headers_received=headers_received)
            try:
                self.assertTrue(headers_received.wait(3))
                self.origin.etag = '"two"'
                self.origin.data = bytes(reversed(range(200)))
                self.assertEqual(self.get("150-159")[2], self.origin.data[150:160])
            finally:
                self.origin.release_range.set()
            self.assertEqual(first.result(timeout=3)[2], original[:100])
        self.assertEqual(self.get("0-99")[2], self.origin.data[:100])
        self.assertEqual(len(self.origin.requests), 3)

    def test_head_whole_and_ignored_ranges(self):
        self.start()
        self.assertEqual(self.get(method="HEAD")[1]["Content-Length"], "200")
        status, _, body = self.get()
        self.assertEqual((status, body), (200, self.origin.data))
        self.assertEqual(self.get("199-")[2], self.origin.data[-1:])
        self.assertEqual(self.get("200-")[0], 416)
        self.assertEqual(self.get("1-2,4-5")[0], 400)
        self.origin.ignore_range = True
        self.assertEqual(self.get("1-2", path="/other")[0], 502)
        self.assertEqual(self.get(path="/other")[2], self.origin.data)

    def test_no_strong_etag_fetches_whole_request_on_partial_hit(self):
        self.start()
        self.origin.etag = 'W/"one"'
        self.get("40-59")
        self.get("45-54")
        self.assertEqual(len(self.origin.requests), 1)
        self.get("30-49")
        self.assertEqual(self.origin.requests[-1][2], "bytes=30-49")

    def test_truncated_body_is_not_published(self):
        self.start()
        self.origin.truncate = True
        with self.assertRaises(http.client.IncompleteRead):
            self.get("40-59")
        self.origin.truncate = False
        self.assertEqual(self.get("40-59")[2], self.origin.data[40:60])
        self.assertEqual(len(self.origin.requests), 2)

    def test_redirect_is_checked_and_private_addresses_are_blocked_by_default(self):
        self.start(private=False)
        self.assertEqual(self.get("0-9")[0], 502)
        self.assertEqual(self.origin.requests, [])
        self.stop()
        self.start()
        self.assertEqual(self.get("0-9", path="/redirect")[2], self.origin.data[:10])
        self.assertEqual(self.get("0-9", path="/denied-redirect")[0], 502)
        self.assertEqual([item[1] for item in self.origin.requests],
                         ["/redirect", SIGNED_PATH, "/denied-redirect"])

    @unittest.skipUnless(FFMPEG, "requires FFmpeg")
    def test_ffmpeg_decodes_and_seeks_a_real_mp4(self):
        self.start(capacity=2 * 1024 * 1024)
        mp4 = Path(self.root) / "generated.mp4"
        subprocess.run([FFMPEG, "-v", "error", "-f", "lavfi", "-i",
                        "testsrc2=size=640x360:rate=24", "-t", "12", "-c:v", "mpeg4",
                        "-g", "12", "-q:v", "3", "-movflags", "+faststart", str(mp4)], check=True, timeout=20)
        self.origin.mp4 = mp4.read_bytes()
        path = "/playable.mp4?sig=a%2Fb&z=1"
        url = f"http://127.0.0.1:{self.port}/v1/{self.source}{path}"
        subprocess.run([FFMPEG, "-v", "error", "-ss", "9", "-short_seek_size", "0", "-i", url,
                        "-frames:v", "4", "-f", "null", "-"], check=True, timeout=20)
        self.assertTrue(all(item[1] == path for item in self.origin.requests))
        self.assertTrue(any(item[2] and item[2] != "bytes=0-" for item in self.origin.requests))


if __name__ == "__main__":
    unittest.main()
