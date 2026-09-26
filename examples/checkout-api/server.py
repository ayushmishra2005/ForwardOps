"""Minimal checkout-api that emits one OTLP trace per failed checkout.

Spans carry correlation ids only. They do not carry SQL, credentials, or
request bodies.
"""

import json
import os
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OTLP = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318").rstrip("/")


def _export(trace_id: str, root_id: str, child_id: str, request_id: str) -> None:
    end = time.time_ns()
    start = end - 20_000_000
    child_end = end - 5_000_000
    body = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "checkout-api"}}
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "checkout-api"},
                        "spans": [
                            {
                                "traceId": trace_id,
                                "spanId": root_id,
                                "name": "POST /checkout",
                                "kind": 2,
                                "startTimeUnixNano": str(start),
                                "endTimeUnixNano": str(end),
                                "attributes": [
                                    {"key": "request.id", "value": {"stringValue": request_id}},
                                    {"key": "http.route", "value": {"stringValue": "/checkout"}},
                                ],
                                "status": {"code": 2},
                            },
                            {
                                "traceId": trace_id,
                                "spanId": child_id,
                                "parentSpanId": root_id,
                                "name": "db.pool.acquire",
                                "kind": 3,
                                "startTimeUnixNano": str(start),
                                "endTimeUnixNano": str(child_end),
                                "attributes": [
                                    {"key": "request.id", "value": {"stringValue": request_id}},
                                    {
                                        "key": "db.operation",
                                        "value": {"stringValue": "pool_acquire"},
                                    },
                                    {
                                        "key": "error.classification",
                                        "value": {"stringValue": "db_acquisition_timeout"},
                                    },
                                ],
                                "status": {"code": 2},
                            },
                        ],
                    }
                ],
            }
        ]
    }
    request = urllib.request.Request(
        f"{OTLP}/v1/traces",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        response.read()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send(200, {"status": "ok"})
            return
        self._send(404, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/checkout":
            self._send(404, {"error": "not_found"})
            return
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length:
            self.rfile.read(length)
        trace_id = uuid.uuid4().hex
        root_id = uuid.uuid4().hex[:16]
        child_id = uuid.uuid4().hex[:16]
        request_id = f"req-{uuid.uuid4().hex[:12]}"
        try:
            _export(trace_id, root_id, child_id, request_id)
        except OSError:
            self._send(503, {"error": "trace_export_failed", "request_id": request_id})
            return
        self._send(
            503,
            {
                "error": "db_acquisition_timeout",
                "request_id": request_id,
                "trace_id": trace_id,
            },
            headers={"X-Trace-Id": trace_id, "X-Request-Id": request_id},
        )

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send(self, status: int, payload: dict, headers: dict | None = None) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(encoded)


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    ThreadingHTTPServer(("0.0.0.0", port), _Handler).serve_forever()


if __name__ == "__main__":
    main()
