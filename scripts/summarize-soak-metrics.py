#!/usr/bin/env python3
"""Consolidate the raw six-hour workload and Windows resource samples."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path


def numbers(rows, field):
    result = []
    for row in rows:
        try:
            value = float(row[field])
            if math.isfinite(value):
                result.append(value)
        except (KeyError, TypeError, ValueError):
            continue
    return result


def distribution(values):
    if not values:
        return None
    ordered = sorted(values)
    def pct(p):
        return ordered[min(len(ordered) - 1, max(0, math.ceil(p * len(ordered)) - 1))]
    return {"count": len(values), "mean": statistics.fmean(values), "min": ordered[0],
            "p50": pct(.50), "p95": pct(.95), "p99": pct(.99), "max": ordered[-1]}


def parse_pid_cpu(value):
    result = {}
    for item in (value or "").split(";"):
        if ":" not in item:
            continue
        pid, seconds = item.split(":", 1)
        try:
            result[int(pid)] = float(seconds)
        except ValueError:
            pass
    return result


def parse_gc(value):
    result = {}
    for node in (value or "").split("|"):
        if ":" not in node:
            continue
        name, rest = node.split(":", 1)
        values = {}
        for item in rest.split(";"):
            if "=" in item:
                key, val = item.split("=", 1)
                try:
                    values[key] = float(val)
                except ValueError:
                    pass
        result[name] = values
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--processor-count", type=int, required=True)
    args = parser.parse_args()
    perf_path = args.run_dir / "workload" / "performance-summary.json"
    resource_path = args.run_dir / "resource-samples.csv"
    if not perf_path.is_file() or not resource_path.is_file():
        raise SystemExit("Missing performance summary or resource samples")
    performance = json.loads(perf_path.read_text(encoding="utf-8"))
    with resource_path.open(newline="", encoding="utf-8-sig") as raw:
        samples = list(csv.DictReader(raw))

    cpu_util = []
    gc_start = {}
    gc_end = {}
    previous_time = None
    previous_cpu = {}
    for row in samples:
        try:
            timestamp = __import__("datetime").datetime.fromisoformat(row["timestampLocal"]).timestamp()
        except (ValueError, KeyError):
            continue
        current_cpu = parse_pid_cpu(row.get("javaCpuByPid", ""))
        if previous_time is not None and timestamp > previous_time:
            common = set(previous_cpu) & set(current_cpu)
            cpu_delta = sum(max(0.0, current_cpu[pid] - previous_cpu[pid]) for pid in common)
            cpu_util.append(cpu_delta / (timestamp - previous_time) / max(1, args.processor_count) * 100.0)
        previous_time, previous_cpu = timestamp, current_cpu
        gc_current = parse_gc(row.get("gcCounters", ""))
        if gc_current:
            if not gc_start:
                gc_start = gc_current
            gc_end = gc_current

    gc_delta = {}
    for node, ending in gc_end.items():
        beginning = gc_start.get(node, {})
        gc_delta[node] = {key: max(0.0, ending.get(key, 0.0) - beginning.get(key, 0.0))
                          for key in ("YGC", "YGCT", "FGC", "FGCT", "GCT")}

    report = {
        "performance": performance,
        "resources": {
            "sampleCount": len(samples),
            "methodology": "CPU utilization is derived from per-PID Java process CPU-time deltas, normalized by logical processor count. Memory/heap and host disk/network rates use raw 10-second samples. Disk and network counters are host-wide Windows performance counters.",
            "javaCpuUtilizationPercent": distribution(cpu_util),
            "javaPrivateBytes": distribution(numbers(samples, "javaPrivateBytes")),
            "javaWorkingSetBytes": distribution(numbers(samples, "javaWorkingSetBytes")),
            "raftNodesHeapUsedKb": distribution(numbers(samples, "jvmHeapUsedKb")),
            "hostDiskReadBytesPerSecond": distribution(numbers(samples, "hostDiskReadBytesPerSecond")),
            "hostDiskWriteBytesPerSecond": distribution(numbers(samples, "hostDiskWriteBytesPerSecond")),
            "hostNetworkBytesPerSecond": distribution(numbers(samples, "hostNetworkBytesPerSecond")),
            "freeDiskBytesAtStart": numbers(samples, "hostFreeDiskBytes")[0] if numbers(samples, "hostFreeDiskBytes") else None,
            "freeDiskBytesAtEnd": numbers(samples, "hostFreeDiskBytes")[-1] if numbers(samples, "hostFreeDiskBytes") else None,
            "gcDeltaByNode": gc_delta,
            "jstatErrorSamples": sum(bool(row.get("jstatError")) for row in samples),
            "hostCounterErrorSamples": sum(bool(row.get("counterError")) for row in samples),
        },
    }
    output = args.run_dir / "final-metrics-report.json"
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
