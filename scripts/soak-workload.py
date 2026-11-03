#!/usr/bin/env python3
"""Generate continuous Redis-protocol load and retain raw latency/availability samples."""

from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
import socket
import statistics
import threading
import time
from pathlib import Path


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def resp(*parts: bytes) -> bytes:
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n" % len(part) + part + b"\r\n" for part in parts)


def read_response(stream) -> bytes:
    marker = stream.read(1)
    if marker in (b"+", b"-", b":"):
        return marker + stream.readline()
    if marker == b"$":
        length = int(stream.readline().strip())
        if length < 0:
            return marker + b"-1\r\n"
        value = stream.read(length)
        if len(value) != length or stream.read(2) != b"\r\n":
            raise OSError("truncated RESP bulk response")
        return marker + str(length).encode("ascii") + b"\r\n" + value
    raise OSError(f"unexpected RESP response marker: {marker!r}")


def request(sock: socket.socket, *parts: bytes) -> bytes:
    sock.sendall(resp(*parts))
    with sock.makefile("rb", buffering=0) as stream:
        return read_response(stream)


def connect(host: str, port: int) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=3.0)
    sock.settimeout(3.0)
    return sock


def worker(index: int, host: str, port: int, stop: threading.Event, out: Path,
           totals: dict, totals_lock: threading.Lock, interval: float) -> None:
    path = out / f"operations-worker-{index:02d}.csv"
    key = f"soak:worker:{index}".encode()
    value = (f"distributed-kv-soak-{index}-" + "x" * 96).encode()
    operations = ((b"PUT", b"SET", key, value), (b"GET", b"GET", key), (b"DELETE", b"DEL", key))
    op_index = index % len(operations)
    sock = None
    with path.open("w", newline="", encoding="utf-8") as raw:
        writer = csv.writer(raw)
        writer.writerow(("timestampUtc", "perfCounterNs", "operation", "latencyMs", "success", "error"))
        next_operation = time.monotonic()
        while not stop.is_set():
            delay = next_operation - time.monotonic()
            if delay > 0 and stop.wait(delay):
                break
            kind, command, op_key, *tail = operations[op_index]
            started_ns = time.perf_counter_ns()
            error = ""
            success = False
            try:
                if sock is None:
                    sock = connect(host, port)
                response = request(sock, command, op_key, *tail)
                if response.startswith(b"-"):
                    raise OSError(response.decode("utf-8", "replace").strip())
                success = True
            except (OSError, ValueError) as exc:
                error = f"{type(exc).__name__}:{exc}"[:240]
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass
                    sock = None
            ended_ns = time.perf_counter_ns()
            latency_ms = (ended_ns - started_ns) / 1_000_000
            writer.writerow((utc_now(), ended_ns, kind.decode(), f"{latency_ms:.6f}", int(success), error))
            if totals_lock.acquire(timeout=1):
                try:
                    slot = totals.setdefault(kind.decode(), {"success": 0, "failure": 0})
                    slot["success" if success else "failure"] += 1
                finally:
                    totals_lock.release()
            if totals["_rows"] % 100 == 0:
                raw.flush()
            totals["_rows"] += 1
            next_operation = max(next_operation + interval, time.monotonic())
            op_index = (op_index + 1) % len(operations)
        raw.flush()
    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass


def probe(host: str, port: int, stop: threading.Event, out: Path) -> None:
    with (out / "availability-probes.csv").open("w", newline="", encoding="utf-8") as raw:
        writer = csv.writer(raw)
        writer.writerow(("timestampUtc", "perfCounterNs", "latencyMs", "success", "error"))
        sock = None
        while not stop.is_set():
            scheduled = time.monotonic()
            started = time.perf_counter_ns()
            error = ""
            success = False
            try:
                if sock is None:
                    sock = connect(host, port)
                probe_key = f"soak:availability:{time.perf_counter_ns()}".encode()
                probe_value = b"available"
                if not request(sock, b"SET", probe_key, probe_value).startswith(b"+"):
                    raise OSError("availability SET failed")
                if request(sock, b"GET", probe_key) != b"$9\r\navailable":
                    raise OSError("availability GET returned an unexpected value")
                if not request(sock, b"DEL", probe_key).startswith(b":1"):
                    raise OSError("availability DELETE failed")
                success = True
            except (OSError, ValueError) as exc:
                error = f"{type(exc).__name__}:{exc}"[:240]
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass
                    sock = None
            ended = time.perf_counter_ns()
            writer.writerow((utc_now(), ended, f"{(ended-started)/1_000_000:.6f}", int(success), error))
            raw.flush()
            stop.wait(max(0.0, 1.0 - (time.monotonic() - scheduled)))
        if sock is not None:
            sock.close()


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(p * len(ordered)) - 1)]


def summarize_operations(out: Path, started_mono: int, ended_mono: int, cycles_path: Path | None) -> dict:
    elapsed = max(0.0, (ended_mono - started_mono) / 1_000_000_000)
    per_op: dict[str, list[tuple[float, int, str]]] = {"PUT": [], "GET": [], "DELETE": []}
    cycles = []
    if cycles_path is not None and cycles_path.is_file():
        with cycles_path.open(newline="", encoding="utf-8-sig") as raw:
            for row in csv.DictReader(raw):
                cycles.append({
                    "cycle": int(row["cycle"]), "started": dt.datetime.fromisoformat(row["startedAtLocal"]).astimezone(dt.timezone.utc),
                    "finished": dt.datetime.fromisoformat(row["finishedAtLocal"]).astimezone(dt.timezone.utc),
                    "elapsedSeconds": float(row["elapsedSeconds"]), "status": row["status"],
                    "operations": {"PUT": [], "GET": [], "DELETE": []},
                })
    cycle_starts = [row["started"] for row in cycles]
    for path in sorted(out.glob("operations-worker-*.csv")):
        with path.open(newline="", encoding="utf-8") as raw:
            for row in csv.DictReader(raw):
                op = row["operation"]
                timestamp = row["timestampUtc"]
                record = (float(row["latencyMs"]), int(row["success"]), timestamp)
                per_op[op].append(record)
                if cycles:
                    instant = dt.datetime.fromisoformat(timestamp).astimezone(dt.timezone.utc)
                    cycle_index = bisect.bisect_right(cycle_starts, instant) - 1
                    if cycle_index >= 0 and instant <= cycles[cycle_index]["finished"]:
                        cycles[cycle_index]["operations"][op].append(record)

    def summarize_op(rows: list[tuple[float, int, str]], duration: float) -> dict:
        latencies = [row[0] for row in rows]
        successful = sum(row[1] for row in rows)
        failed = len(rows) - successful
        return {
            "total": len(rows), "successful": successful, "failed": failed,
            "opsPerSecond": successful / duration if duration else 0.0,
            "latencyMs": {
                "mean": statistics.fmean(latencies) if latencies else None,
                "p50": percentile(latencies, .50), "p90": percentile(latencies, .90),
                "p95": percentile(latencies, .95), "p99": percentile(latencies, .99),
                "max": max(latencies) if latencies else None,
            },
        }

    operations = {op: summarize_op(rows, elapsed) for op, rows in per_op.items()}
    per_cycle = []
    for cycle in cycles:
        by_operation = {op: summarize_op(rows, cycle["elapsedSeconds"]) for op, rows in cycle["operations"].items()}
        successful = sum(item["successful"] for item in by_operation.values())
        total = sum(item["total"] for item in by_operation.values())
        per_cycle.append({
            "cycle": cycle["cycle"], "status": cycle["status"], "elapsedSeconds": cycle["elapsedSeconds"],
            "totalOps": total, "successfulOps": successful,
            "successfulOpsPerSecond": successful / cycle["elapsedSeconds"] if cycle["elapsedSeconds"] else 0.0,
            "operations": by_operation,
        })

    probes = []
    probe_file = out / "availability-probes.csv"
    with probe_file.open(newline="", encoding="utf-8") as raw:
        probes = list(csv.DictReader(raw))
    downtime = 0.0
    in_outage = False
    outage_start = None
    for row in probes:
        stamp = int(row["perfCounterNs"])
        if row["success"] == "0" and not in_outage:
            in_outage, outage_start = True, stamp
        elif row["success"] == "1" and in_outage:
            downtime += max(0, stamp - outage_start) / 1_000_000_000
            in_outage, outage_start = False, None
    if in_outage and outage_start is not None:
        downtime += max(0, ended_mono - outage_start) / 1_000_000_000
    success_probes = sum(p["success"] == "1" for p in probes)
    total_ops = sum(v["total"] for v in operations.values())
    successful_ops = sum(v["successful"] for v in operations.values())
    return {
        "startedAtUtc": (out / "workload-started-utc.txt").read_text(encoding="utf-8").strip(),
        "finishedAtUtc": utc_now(), "elapsedSeconds": elapsed, "concurrency": len(list(out.glob("operations-worker-*.csv"))),
        "methodology": {
            "workload": "Equal round-robin PUT/GET/DELETE on persistent RESP connections; fixed 100-byte values; worker cadence rate-limits the aggregate request rate.",
            "availability": "One end-to-end SET/GET/DELETE transaction per second over a persistent connection. A failed probe begins an outage interval ending at the next successful probe; a trailing outage ends at test completion. This is sampled service-time availability; operation request errors are reported separately.",
            "latency": "Per-operation client wall time from request send through complete RESP response read; raw samples are retained per worker.",
        },
        "throughput": {"successfulOpsPerSecond": successful_ops / elapsed if elapsed else 0.0,
                       "totalOps": total_ops, "successfulOps": successful_ops, "failedOps": total_ops - successful_ops},
        "operations": operations,
        "perCyclePerformance": per_cycle,
        "availability": {"percent": (max(0.0, elapsed - downtime) / elapsed * 100.0) if elapsed else None,
                         "measuredServiceSeconds": elapsed, "estimatedDowntimeSeconds": downtime,
                         "probeCount": len(probes), "successfulProbes": success_probes,
                         "failedProbes": len(probes) - success_probes,
                         "requestFailureRatePercent": ((total_ops-successful_ops)/total_ops*100.0) if total_ops else None},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--target-ops-per-second", type=float, default=1.0,
                        help="aggregate request rate; default 1 avoids overwhelming a local test host")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stop-file", type=Path, required=True)
    parser.add_argument("--cycles-file", type=Path)
    parser.add_argument("--check", action="store_true", help="verify end-to-end SET/GET/DELETE availability and exit")
    args = parser.parse_args()
    if args.concurrency < 1 or args.concurrency > 128:
        parser.error("concurrency must be between 1 and 128")
    if args.check:
        key = b"soak:readiness-check"
        sock = connect(args.host, args.port)
        try:
            if request(sock, b"PING") != b"+PONG\r\n":
                raise OSError("PING failed")
            if not request(sock, b"SET", key, b"ready").startswith(b"+"):
                raise OSError("SET readiness failed")
            if request(sock, b"GET", key) != b"$5\r\nready":
                raise OSError("GET readiness failed")
            if not request(sock, b"DEL", key).startswith(b":1"):
                raise OSError("DELETE readiness failed")
        finally:
            sock.close()
        return 0
    args.output.mkdir(parents=True, exist_ok=True)
    started_mono = time.perf_counter_ns()
    (args.output / "workload-started-utc.txt").write_text(utc_now() + "\n", encoding="utf-8")
    stop = threading.Event()
    totals = {"_rows": 0}
    totals_lock = threading.Lock()
    interval = args.concurrency / args.target_ops_per_second
    workers = [threading.Thread(target=worker, args=(i, args.host, args.port, stop, args.output, totals, totals_lock, interval), daemon=True)
               for i in range(args.concurrency)]
    health = threading.Thread(target=probe, args=(args.host, args.port, stop, args.output), daemon=True)
    for thread in workers + [health]:
        thread.start()
    try:
        while not args.stop_file.exists():
            time.sleep(.5)
    except KeyboardInterrupt:
        pass
    stop.set()
    for thread in workers + [health]:
        thread.join(timeout=10)
    ended_mono = time.perf_counter_ns()
    report = summarize_operations(args.output, started_mono, ended_mono, args.cycles_file)
    report["targetOpsPerSecond"] = args.target_ops_per_second
    (args.output / "performance-summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"elapsedSeconds": report["elapsedSeconds"], "totalOps": report["throughput"]["totalOps"],
                      "availabilityPercent": report["availability"]["percent"]}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
