#!/usr/bin/env python3
"""Run a disposable local Prometheus -> Alertmanager firing/resolution check."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


SERVER = r'''import http.server, json, os, time
role = os.environ.get("ROLE", "target")
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics" and role == "target":
            body = b'kv_process_up 1\nkv_raft_group_is_leader{group="alert-test"} 1\n'
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        payload["receivedAtUtc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open("/data/notifications.jsonl", "a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok\n")
    def log_message(self, fmt, *args):
        print(fmt % args, flush=True)
http.server.ThreadingHTTPServer(("0.0.0.0", 8080 if role == "receiver" else 9109), Handler).serve_forever()
'''


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, default=Path("artifacts"))
    parser.add_argument("--prometheus-image", default="prom/prometheus:v3.5.0")
    parser.add_argument("--alertmanager-image", default="prom/alertmanager:v0.28.1")
    parser.add_argument("--python-image", default="python:3.12-alpine")
    args = parser.parse_args()

    run_id = "alertmanager-validation-" + dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    out = args.artifacts.resolve() / run_id
    out.mkdir(parents=True, exist_ok=False)
    project = "kvam" + dt.datetime.now().strftime("%H%M%S")
    compose_file = out / "compose.yml"
    events_file = out / "notifications.jsonl"
    events_file.write_text("", encoding="utf-8")
    start = utc_now()
    result = "FAIL"
    failure = None
    commands_log = out / "docker-commands.log"
    compose_base = ["compose", "-p", project, "-f", str(compose_file)]

    def docker(command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run(["docker", *command], text=True, capture_output=True)
        with commands_log.open("a", encoding="utf-8") as stream:
            stream.write("$ docker " + " ".join(command) + "\n")
            stream.write(proc.stdout)
            stream.write(proc.stderr)
        if check and proc.returncode:
            raise RuntimeError(f"docker {' '.join(command)} exited {proc.returncode}: {proc.stderr.strip()}")
        return proc

    def notifications() -> list[dict]:
        found = []
        for line in events_file.read_text(encoding="utf-8").splitlines():
            try:
                found.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return found

    def wait_notification(status: str, timeout: int = 90) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for item in notifications():
                if item.get("status") == status:
                    return item
            time.sleep(1)
        raise RuntimeError(f"No Alertmanager {status!r} webhook arrived within {timeout}s")

    prom_port_sock = socket.socket()
    prom_port_sock.bind(("127.0.0.1", 0))
    prom_port = prom_port_sock.getsockname()[1]
    prom_port_sock.close()

    (out / "webhook-test-server.py").write_text(SERVER, encoding="utf-8")
    (out / "Dockerfile").write_text(
        f"FROM {args.python_image}\nCOPY webhook-test-server.py /app/server.py\n"
        'CMD ["python", "-u", "/app/server.py"]\n', encoding="utf-8")
    (out / "test-rules.yml").write_text('''groups:
  - name: alert-delivery-test
    rules:
      - alert: KVAlertDeliveryTest
        expr: up{job="kvstore-validation-target"} == 0
        for: 5s
        labels: {severity: warning}
        annotations: {summary: "Synthetic target unavailable"}
''', encoding="utf-8")
    (out / "prometheus.yml").write_text('''global:
  scrape_interval: 2s
  evaluation_interval: 2s
rule_files: [/etc/prometheus/test-rules.yml]
alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]
scrape_configs:
  - job_name: kvstore-validation-target
    static_configs:
      - targets: ["target:9109"]
''', encoding="utf-8")
    (out / "alertmanager.yml").write_text('''global: {resolve_timeout: 15s}
route:
  receiver: local-webhook
  group_by: [alertname]
  group_wait: 1s
  group_interval: 1s
  repeat_interval: 1h
receivers:
  - name: local-webhook
    webhook_configs:
      - url: http://receiver:8080/notify
        send_resolved: true
''', encoding="utf-8")
    (out / "compose.yml").write_text(f'''services:
  receiver:
    build: .
    image: kvstore-alert-test:{project}
    pull_policy: build
    environment: {{ROLE: receiver}}
    volumes: ["./notifications.jsonl:/data/notifications.jsonl"]
  target:
    image: kvstore-alert-test:{project}
    pull_policy: build
    environment: {{ROLE: target}}
  alertmanager:
    image: {args.alertmanager_image}
    command: ["--config.file=/etc/alertmanager/alertmanager.yml"]
    volumes: ["./alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro"]
  prometheus:
    image: {args.prometheus_image}
    command: ["--config.file=/etc/prometheus/prometheus.yml"]
    ports: ["127.0.0.1:{prom_port}:9090"]
    volumes:
      - "./prometheus.yml:/etc/prometheus/prometheus.yml:ro"
      - "./test-rules.yml:/etc/prometheus/test-rules.yml:ro"
''', encoding="utf-8")

    try:
        docker(compose_base + ["config", "--quiet"])
        docker(compose_base + ["up", "-d", "--build"])
        docker(compose_base + ["exec", "-T", "prometheus", "promtool", "check", "config", "/etc/prometheus/prometheus.yml"])
        docker(compose_base + ["exec", "-T", "prometheus", "promtool", "check", "rules", "/etc/prometheus/test-rules.yml"])
        docker(compose_base + ["exec", "-T", "alertmanager", "amtool", "check-config", "/etc/alertmanager/alertmanager.yml"])

        query = urllib.parse.urlencode({"query": 'up{job="kvstore-validation-target"}'})
        url = f"http://127.0.0.1:{prom_port}/api/v1/query?{query}"
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        deadline = time.monotonic() + 60
        target_up = False
        while time.monotonic() < deadline:
            try:
                with opener.open(url, timeout=3) as response:
                    data = json.load(response)
                series = data.get("data", {}).get("result", [])
                if data.get("status") == "success" and series and series[0]["value"][1] == "1":
                    target_up = True
                    break
            except (OSError, urllib.error.URLError, json.JSONDecodeError, KeyError, IndexError):
                pass
            time.sleep(2)
        if not target_up:
            raise RuntimeError("Synthetic target did not report UP before fault injection")

        docker(compose_base + ["stop", "target"])
        firing = wait_notification("firing")
        docker(compose_base + ["start", "target"])
        resolved = wait_notification("resolved")
        if not firing.get("alerts") or not resolved.get("alerts"):
            raise RuntimeError("Webhook status received without alert details")
        result = "PASS"
    except Exception as exc:  # record every failure before cleanup
        failure = str(exc)
    finally:
        logs = docker(compose_base + ["logs", "--no-color"], check=False)
        (out / "container-logs.txt").write_text(logs.stdout + logs.stderr, encoding="utf-8")
        cleanup = docker(compose_base + ["down", "--volumes", "--remove-orphans"], check=False)
        (out / "cleanup.log").write_text(cleanup.stdout + cleanup.stderr, encoding="utf-8")

    events = notifications()
    summary = {
        "result": result,
        "testType": "Synthetic local Prometheus-to-Alertmanager webhook",
        "startedAtUtc": start,
        "finishedAtUtc": utc_now(),
        "prometheusImage": args.prometheus_image,
        "alertmanagerImage": args.alertmanager_image,
        "pythonImage": args.python_image,
        "firingReceived": any(event.get("status") == "firing" for event in events),
        "resolvedReceived": any(event.get("status") == "resolved" for event in events),
        "notifications": events,
        "productionNotificationValidated": False,
        "error": failure,
        "limitation": "Synthetic local webhook only; production route, credentials, application metrics and on-call acknowledgement are untested.",
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"{result}: alerting evidence at {out}")
    return 0 if result == "PASS" and summary["firingReceived"] and summary["resolvedReceived"] else 1


if __name__ == "__main__":
    sys.exit(main())
