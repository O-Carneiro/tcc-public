import gzip
import http.server
import json
import threading


class Request:
    def __init__(self, index, path, headers, body):
        self.index, self.path, self.headers, self.body = index, path, headers, body

    def json(self):
        raw = self.body
        if self.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))


class Stub:
    """Local OTLP receiver on 127.0.0.1.

    `codes` is a per-request list whose last entry repeats; an entry is an int,
    "hang", "trickle" or "redirect" (302 to /ok, which answers 200). `before(req)`
    runs after the body is read and before the reply; exceptions land in `errors`.
    `respond(req)` returns (code, headers, body) and overrides `codes`.
    """

    def __init__(self, codes=None, respond=None, before=None):
        self.codes = list(codes) if codes else [200]
        self._respond = respond
        self.before = before
        self.requests = []
        self.errors = []
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        stub = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _handle(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                with stub._lock:
                    req = Request(len(stub.requests), self.path, self.headers, body)
                    stub.requests.append(req)
                if stub.before is not None:
                    try:
                        stub.before(req)
                    except Exception as exc:
                        stub.errors.append(exc)
                code, headers, payload = stub._answer(req)
                if code == "hang":
                    stub.stop_event.wait(20)
                    self.close_connection = True
                    return
                if code == "trickle":
                    # One byte every 5 s keeps each recv under the client's socket
                    # timeout, so only a total deadline can end the request.
                    try:
                        for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}":
                            self.wfile.write(bytes([byte]))
                            self.wfile.flush()
                            if stub.stop_event.wait(5):
                                break
                    except OSError:
                        pass
                    self.close_connection = True
                    return
                self.send_response(code)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_POST = _handle
            do_GET = _handle

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def _answer(self, req):
        if self._respond is not None:
            return self._respond(req)
        if req.path == "/ok":
            return 200, {}, b"{}"
        code = self.codes[min(req.index, len(self.codes) - 1)]
        if code in ("hang", "trickle"):
            return code, {}, b""
        if code == "redirect":
            return 302, {"Location": "/ok"}, b""
        return code, {}, b"{}"

    @property
    def url(self):
        return "http://127.0.0.1:%d/v1/logs" % self.server.server_address[1]

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.stop_event.set()
        self.server.shutdown()
        self.server.server_close()

    def posts(self):
        return [r for r in self.requests if r.path == "/v1/logs"]
