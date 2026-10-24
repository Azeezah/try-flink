"""Local control panel for the Flink/Kafka click-stream playground."""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, request, send_file


ROOT = Path(__file__).resolve().parent
CONTROL_FILE = ROOT / ".simulation-control.json"
METRICS_FILE = ROOT / ".traffic-metrics.jsonl"
SQL_FILE = "/opt/flink/sql/clicks-per-minute.sql"
app = Flask(__name__)

lock = threading.RLock()
simulator: subprocess.Popen | None = None
sql_client: subprocess.Popen | None = None
state = {"running": False, "paused": False, "speed": 120, "message": "Ready"}


def save_control():
    CONTROL_FILE.write_text(json.dumps({"paused": state["paused"], "speed": state["speed"]}))


def alive(process):
    return process is not None and process.poll() is None


def run_command(args, **kwargs):
    return subprocess.Popen(args, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, **kwargs)


def wait_for_flink(timeout=45):
    import urllib.request

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen("http://127.0.0.1:8081/overview", timeout=1):
                return True
        except Exception:
            time.sleep(1)
    return False


def stop_process(process):
    if alive(process):
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


def start_stack():
    global simulator, sql_client
    with lock:
        if state["running"]:
            return
        state.update(running=True, paused=False, message="Starting Docker services…")
        save_control()
        subprocess.run(["docker", "compose", "up", "-d"], cwd=ROOT, check=True)
        if not wait_for_flink():
            state.update(running=False, message="Flink did not become ready")
            return
        state["message"] = "Submitting Flink SQL…"
        sql_client = run_command(["docker", "compose", "exec", "-T", "jobmanager", "./bin/sql-client.sh", "-f", SQL_FILE])
        # This simulation may start before the above FlinkSQL is submitted and that's fine because the clicks will be durably stored in Kafka either way.
        simulator = run_command(
            ["python3", "run-sim.py"],
            env={**os.environ, "SIM_CONTROL_FILE": str(CONTROL_FILE), "SIM_METRICS_FILE": str(METRICS_FILE)},
        )
        state["message"] = "Traffic is running"


def stop_stack():
    global simulator, sql_client
    with lock:
        stop_process(simulator)
        stop_process(sql_client)
        simulator = sql_client = None
        subprocess.run(["docker", "compose", "down", "--remove-orphans"], cwd=ROOT, check=False)
        state.update(running=False, paused=False, message="Stopped")
        save_control()


@app.get("/")
def index():
    return send_file(ROOT / "control-panel.html")


@app.get("/api/status")
def status():
    with lock:
        if state["running"] and not alive(simulator):
            state["running"] = False
            state["message"] = "Simulation finished"
        return jsonify(state)


@app.get("/api/traffic")
def traffic_metrics():
    samples = []
    try:
        with METRICS_FILE.open() as metrics_file:
            for line in metrics_file:
                try:
                    samples.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return jsonify(samples[-900:])


@app.post("/api/start")
def start():
    try:
        start_stack()
        return status()
    except (OSError, subprocess.CalledProcessError) as error:
        state.update(running=False, message=f"Could not start: {error}")
        return jsonify(state), 500


@app.post("/api/stop")
def stop():
    stop_stack()
    return status()


@app.post("/api/restart")
def restart():
    stop_stack()
    return start()


@app.post("/api/traffic")
def traffic():
    with lock:
        state["paused"] = bool(request.json.get("paused", False))
        state["message"] = "Traffic is paused" if state["paused"] else "Traffic is running"
        save_control()
    return status()


@app.post("/api/speed")
def speed():
    value = float(request.json.get("speed", 120))
    if value not in (1, 60, 120):
        return jsonify({"error": "Speed must be 1, 60, or 120"}), 400
    with lock:
        state["speed"] = int(value)
        save_control()
    return status()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
