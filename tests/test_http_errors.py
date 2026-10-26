import copy
import http.client
import json
import io
import threading
import unittest
from unittest.mock import Mock, patch
from http.server import ThreadingHTTPServer

from sview.http.errors import localize_errors
from sview.http.server import SviewCacheHandler


class ErrorTranslationTests(unittest.TestCase):
    def test_documented_errors_keep_original(self):
        cases = [
            ("source not found on local mount", "数据文件不存在。"),
            ("Invalid remote MCAP URL: https://example.test/a", "数据文件不存在。"),
            ("Head-cam MCAP has no aligned video frames: /a.mcap", "数据加载失败。"),
            ("MCAP has no ChunkIndex; use progressive mode or reindex the source", "数据文件已损坏。"),
            ("no supported sparse MCAP video streams", "数据文件已损坏。"),
            ("no indexed video streams", "数据文件已损坏。"),
            ("probe failed: arbitrary exception", "数据文件已损坏。"),
            ("PAGE_TIMEOUT", "数据源打开耗时较长，尚未完成加载，请稍后重试。"),
        ]
        for english, chinese in cases:
            with self.subTest(english=english):
                result = localize_errors({"error": english, "status": "failed"})
                self.assertEqual(result, {"error": chinese, "error-en": english, "status": "failed"})

    def test_nested_jobs_no_mutation(self):
        original = {"jobs": [{"error": "unexpected codec error", "encode_status": "failed", "status": "processing"}, {"error": None}, {"error": ""}, {"status": "ready"}]}
        snapshot = copy.deepcopy(original)
        result = localize_errors(original)
        self.assertEqual(original, snapshot)
        self.assertEqual(result["jobs"][0]["error"], "数据文件已损坏。")
        self.assertEqual(result["jobs"][0]["status"], "processing")
        self.assertIsNone(result["jobs"][1]["error-en"])
        self.assertEqual(result["jobs"][2]["error-en"], "")
        self.assertNotIn("error-en", result["jobs"][3])

    def test_unknown_exception_has_chinese_fallback(self):
        self.assertEqual(localize_errors({"error": "unexpected failure"}), {"error": "数据加载失败。", "error-en": "unexpected failure"})

    def test_opening_without_error_does_not_invent_timeout(self):
        payload = {"status": "opening", "available_ranges": [], "error": None}
        self.assertEqual(localize_errors(payload), dict(payload, **{"error-en": None}))


class ResponseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(SviewCacheHandler):
            def do_GET(self):
                if self.path == "/retry":
                    self._send_retry(425, "segment not ready yet", retry_after=3)
                elif self.path == "/jobs":
                    self._send_json(200, {"jobs": [{"error": "probe failed: EOF", "status": "failed"}]})
                else:
                    self._send_err(404, "file not found")

            do_HEAD = do_GET

            def log_message(self, *args):
                pass

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def request(self, method, path):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=5)
        try:
            connection.request(method, path)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def test_media_retry_preserves_headers_and_status(self):
        status, headers, body = self.request("GET", "/retry")
        self.assertEqual(status, 425)
        self.assertEqual(headers["Retry-After"], "3")
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
        self.assertEqual(int(headers["Content-Length"]), len(body))
        self.assertEqual(headers["Access-Control-Allow-Origin"], "*")
        self.assertEqual(json.loads(body)["error-en"], "segment not ready yet")
        self.assertEqual(json.loads(body)["error"], "分段尚未生成，请稍后重试。")

    def test_not_found_and_head(self):
        status, headers, body = self.request("GET", "/missing")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"], "数据文件不存在。")
        head_status, head_headers, head_body = self.request("HEAD", "/missing")
        self.assertEqual(head_status, status)
        self.assertEqual(head_headers["Content-Length"], headers["Content-Length"])
        self.assertEqual(head_body, b"")

    def test_success_status_can_include_failed_jobs(self):
        status, _, body = self.request("GET", "/jobs")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["jobs"][0]["error-en"], "probe failed: EOF")


class EventTests(unittest.TestCase):
    def events(self, failure=False):
        handler = object.__new__(SviewCacheHandler)
        handler.wfile = io.BytesIO()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        jobs = Mock()
        jobs.get_job_fields.return_value = {"status": "failed", "error": "probe failed: EOF"}
        jobs.job_progress_percent.return_value = 0
        if failure:
            jobs.get_job_fields.side_effect = RuntimeError("connection lost")
        with patch.dict("sys.modules", {"sview.jobs.job_status": jobs}), patch("sview.http.server._get_redis", return_value=Mock()):
            handler._handle_job_events("example")
        return handler.wfile.getvalue().decode("utf-8")

    def test_progress_and_done(self):
        events = self.events()
        self.assertIn("event: progress", events)
        self.assertIn("event: done", events)
        for line in events.splitlines():
            if line.startswith("data: "):
                payload = json.loads(line[6:])
                self.assertEqual(payload["error"], "数据文件已损坏。")
                self.assertEqual(payload["error-en"], "probe failed: EOF")

    def test_stream_error(self):
        events = self.events(failure=True)
        self.assertIn("event: error", events)
        payload = json.loads(next(line[6:] for line in events.splitlines() if line.startswith("data: ")))
        self.assertEqual(payload, {"error": "数据加载失败。", "error-en": "connection lost"})


if __name__ == "__main__":
    unittest.main()
