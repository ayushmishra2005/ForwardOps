"""Compare one bounded Solana backfill against a Python reference.

The RPC server is local and delays every response. The numbers describe that
run. They are not a claim about mainnet or about Rust in general.
"""

import argparse
import hashlib
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CRATE = ROOT / "ingestion" / "solana"
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
TX_COUNT = 60
DELAY_SECONDS = 0.015
CONCURRENCY = 4
RUNS = 3


def b58encode(data: bytes) -> str:
    if data == bytes(len(data)):
        return "1" * len(data)
    number = int.from_bytes(data, "big")
    characters = []
    while number:
        number, remainder = divmod(number, 58)
        characters.append(ALPHABET[remainder])
    pad = 0
    for byte in data:
        if byte == 0:
            pad += 1
        else:
            break
    return ("1" * pad) + "".join(reversed(characters))


ADDRESS = b58encode(bytes([4]) * 32)
SIGNATURES = [b58encode(bytes([index]) * 64) for index in range(1, TX_COUNT + 1)]
SLOTS = [5_000 - index for index in range(TX_COUNT)]


class RpcState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.inflight = 0
        self.peak = 0
        self.calls = 0

    def enter(self) -> None:
        with self.lock:
            self.inflight += 1
            self.peak = max(self.peak, self.inflight)
            self.calls += 1

    def leave(self) -> None:
        with self.lock:
            self.inflight -= 1

    def reset(self) -> None:
        with self.lock:
            self.inflight = 0
            self.peak = 0
            self.calls = 0


STATE = RpcState()
CONNECTIONS = threading.local()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length))
        method = request.get("method")
        STATE.enter()
        try:
            time.sleep(DELAY_SECONDS)
            if method == "getSignaturesForAddress":
                options = request["params"][1]
                result = (
                    []
                    if options.get("before")
                    else [
                        {"signature": signature, "slot": slot, "blockTime": None}
                        for signature, slot in zip(SIGNATURES, SLOTS, strict=True)
                    ]
                )
            elif method == "getTransaction":
                signature = request["params"][0]
                slot = SLOTS[SIGNATURES.index(signature)]
                result = {
                    "slot": slot,
                    "blockTime": None,
                    "transaction": {
                        "message": {
                            "accountKeys": [ADDRESS],
                            "instructions": [{"programIdIndex": 0}],
                        }
                    },
                    "meta": {"err": None, "logMessages": ["ok"]},
                }
            else:
                self._send(
                    400,
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "error": {"code": -32601, "message": "method not allowed"},
                    },
                )
                return
            self._send(200, {"jsonrpc": "2.0", "id": request.get("id", 1), "result": result})
        finally:
            STATE.leave()

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


def post(host: str, port: int, payload: dict) -> dict:
    connection = getattr(CONNECTIONS, "connection", None)
    if connection is None:
        connection = HTTPConnection(host, port, timeout=5)
        CONNECTIONS.connection = connection
    connection.request("POST", "/", json.dumps(payload), {"Content-Type": "application/json"})
    response = connection.getresponse()
    body = json.loads(response.read())
    if response.status != 200 or "error" in body:
        raise RuntimeError(f"rpc failed: status {response.status}")
    return body["result"]


def python_reference(host: str, port: int, dsn: str, source_id: str) -> None:
    import asyncio

    page = post(
        host,
        port,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [ADDRESS, {"commitment": "finalized", "limit": 100}],
        },
    )
    if page:
        post(
            host,
            port,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    ADDRESS,
                    {"commitment": "finalized", "limit": 100, "before": page[-1]["signature"]},
                ],
            },
        )

    async def one(signature: str, slot: int) -> tuple[str, int, dict]:
        body = await asyncio.to_thread(
            post,
            host,
            port,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "json",
                        "commitment": "finalized",
                        "maxSupportedTransactionVersion": 0,
                    },
                ],
            },
        )
        return signature, slot, body

    async def fetch_in_chunks() -> None:
        pairs = list(zip(SIGNATURES, SLOTS, strict=True))
        for offset in range(0, TX_COUNT, CONCURRENCY):
            chunk = pairs[offset : offset + CONCURRENCY]
            fetched = await asyncio.gather(*(one(signature, slot) for signature, slot in chunk))
            done = offset + len(chunk) == TX_COUNT
            await asyncio.to_thread(insert_rows, dsn, source_id, fetched, done)

    asyncio.run(fetch_in_chunks())


def insert_rows(
    dsn: str, source_id: str, rows: list[tuple[str, int, dict]], finished: bool
) -> None:
    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            for signature, slot, body in rows:
                digest = hashlib.sha256(
                    json.dumps(body, separators=(",", ":")).encode()
                ).hexdigest()
                cursor.execute(
                    """
                    INSERT INTO source_records (
                      source_id, cluster_id, signature, record_identity, decoder_version,
                      address, slot, block_time, commitment, outcome, program_ids,
                      instruction_errors, log_messages, logs_truncated, payload_sha256, gap_reason
                    ) VALUES (
                      %s, 'mainnet-beta', %s, 'transaction', 'generic-v1',
                      %s, %s, NULL, 'finalized', 'stored', %s::jsonb, '[]'::jsonb,
                      '[]'::jsonb, false, %s, NULL
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (source_id, signature, ADDRESS, slot, json.dumps([ADDRESS]), digest),
                )
            if finished:
                cursor.execute(
                    """
                    INSERT INTO ingestion_checkpoints (
                      source_id, address, commitment, from_slot, to_slot,
                      enumeration_complete, high_water_slot, status
                    ) VALUES (%s, %s, 'finalized', 1, 100000, true, 100000, 'complete')
                    ON CONFLICT DO NOTHING
                    """,
                    (source_id, ADDRESS),
                )
        connection.commit()


def timed(command: list[str], env: dict[str, str]) -> tuple[int, str, float | None, int | None]:
    completed = subprocess.run(
        ["/usr/bin/time", "-l", *command],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    seconds = None
    rss = None
    for line in completed.stderr.splitlines():
        stripped = line.strip()
        parts = stripped.split()
        if "real" in parts:
            seconds = float(parts[parts.index("real") - 1])
        if "maximum resident set size" in stripped:
            rss = int(stripped.split()[0])
    return completed.returncode, completed.stdout + completed.stderr, seconds, rss


def write_config(path: Path, port: int, source_id: str) -> None:
    path.write_text(
        "sources:\n"
        f"  {source_id}:\n"
        "    cluster: mainnet-beta\n"
        f"    rpc_url: http://127.0.0.1:{port}\n"
        "    commitment: finalized\n"
        "    addresses:\n"
        f'      - "{ADDRESS}"\n',
        encoding="utf-8",
    )


def median(values: list[float]) -> float:
    return statistics.median(values)


def summarize(label: str, samples: list[dict]) -> dict:
    durations = [sample["seconds"] for sample in samples]
    throughput = [TX_COUNT / sample["seconds"] for sample in samples]
    rss = [sample["rss"] for sample in samples if sample["rss"] is not None]
    return {
        "implementation": label,
        "runs": len(samples),
        "transactions": TX_COUNT,
        "median_seconds": round(median(durations), 3),
        "median_throughput_tx_per_s": round(median(throughput), 1),
        "median_max_rss_bytes": int(median(rss)) if rss else None,
        "peak_concurrency": max(sample["peak"] for sample in samples),
        "retries": sum(sample["retries"] for sample in samples),
        "failures": sum(sample["failures"] for sample in samples),
    }


def compare(port: int, dsn: str) -> None:
    env = os.environ.copy()
    env["FORWARDOPS_INGEST_DATABASE_URL"] = dsn
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(CRATE / "Cargo.toml")],
        check=True,
        cwd=ROOT,
    )
    metadata = subprocess.run(
        [
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--manifest-path",
            str(CRATE / "Cargo.toml"),
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    target_dir = Path(json.loads(metadata.stdout)["target_directory"])
    binary = target_dir / "release" / "forwardops-solana-ingest"
    config = CRATE / "bench" / "bench.config.yaml"
    rust_samples: list[dict] = []
    python_samples: list[dict] = []
    try:
        for index in range(RUNS):
            for label, samples in (("rust", rust_samples), ("python", python_samples)):
                source_id = f"bench-{label}-{index}"
                STATE.reset()
                if label == "rust":
                    write_config(config, port, source_id)
                    code, output, seconds, rss = timed(
                        [
                            str(binary),
                            "--config",
                            str(config),
                            "--source",
                            source_id,
                            "--address",
                            ADDRESS,
                            "--from-slot",
                            "1",
                            "--to-slot",
                            "100000",
                            "--max-transactions",
                            str(TX_COUNT + 1),
                            "--concurrency",
                            str(CONCURRENCY),
                            "--timeout-seconds",
                            "5",
                            "--max-retries",
                            "0",
                            "--deadline-seconds",
                            "60",
                        ],
                        env,
                    )
                    retries = 0
                    for token in output.split():
                        if token.startswith("retries="):
                            retries = int(token.split("=", 1)[1])
                else:
                    code, output, seconds, rss = timed(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "--mode",
                            "python",
                            "--host",
                            "127.0.0.1",
                            "--port",
                            str(port),
                            "--dsn",
                            dsn,
                            "--source",
                            source_id,
                        ],
                        env,
                    )
                    retries = 0
                if code != 0 or seconds is None:
                    detail = output[-2000:]
                    raise SystemExit(f"{label} run {index} failed with status {code}\n{detail}")
                samples.append(
                    {
                        "seconds": seconds,
                        "rss": rss,
                        "peak": STATE.peak,
                        "retries": retries,
                        "failures": 0,
                    }
                )
    finally:
        config.unlink(missing_ok=True)
    result = {
        "workload": {
            "transactions": TX_COUNT,
            "rpc_delay_ms": int(DELAY_SECONDS * 1000),
            "concurrency": CONCURRENCY,
            "runs": RUNS,
            "rpc": "local mock",
        },
        "rust": summarize("rust", rust_samples),
        "python": summarize("python", python_samples),
    }
    print(json.dumps(result, indent=2))


def main() -> None:
    if b58encode(bytes(32)) != "1" * 32:
        raise SystemExit("base58 self-check failed")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("compare", "python"), default="compare")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--dsn", default="")
    parser.add_argument("--source", default="")
    args = parser.parse_args()
    if args.mode == "python":
        python_reference(args.host, args.port, args.dsn, args.source)
        return
    sys.path.insert(0, str(ROOT))
    from evals.database import prepare_database

    database_url, _app = prepare_database("forwardops_ingest_bench")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        compare(server.server_address[1], database_url)
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
