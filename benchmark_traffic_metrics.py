"""Compare traffic-history formats without starting Kafka, Docker, or a web server.

Run: python3 benchmark_traffic_metrics.py
The only non-stdlib dependency is Flask, which the control panel already uses.
"""

from __future__ import annotations

import argparse
import gc
import importlib.metadata
import json
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, Response, jsonify

from traffic_metrics import HEADER, RECORD, pack_sample, read_recent


ROOT = Path(__file__).resolve().parent
VARIANTS = ("jsonl_original", "jsonl_bounded_tail", "binary_seek")
LABELS = {
    "jsonl_original": "Original JSONL",
    "jsonl_bounded_tail": "JSONL with bounded tail",
    "binary_seek": "Binary with seek",
}
LIMIT = 900


def build_app(variant, fixture):
    """Use the original route, a minimal JSONL alternative, or the new route."""
    app = Flask(__name__)

    @app.get("/api/traffic")
    def traffic():
        if variant == "binary_seek":
            return Response(
                read_recent(fixture, limit=LIMIT),
                mimetype="application/octet-stream",
                headers={"Cache-Control": "no-store"},
            )

        samples = []
        try:
            with fixture.open() as metrics_file:
                lines = (
                    deque(metrics_file, maxlen=LIMIT)
                    if variant == "jsonl_bounded_tail"
                    else metrics_file
                )
                for line in lines:
                    try:
                        samples.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except FileNotFoundError:
            pass
        return jsonify(samples[-LIMIT:])

    return app


def rss_bytes():
    # macOS reports bytes; Linux and most other Unix systems report KiB.
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def decode_payload(variant, payload):
    if variant != "binary_seek":
        return json.loads(payload)
    if payload[: len(HEADER)] != HEADER:
        raise AssertionError("Unexpected binary metrics header")
    return [
        {
            "wall_time": datetime.fromtimestamp(values[0], timezone.utc).isoformat(),
            "expected": values[1],
            "attempted": values[2],
            "acked": values[3],
            "errors": values[4],
        }
        for values in RECORD.iter_unpack(payload[len(HEADER) :])
    ]


def worker(args):
    fixture = Path(args.fixture)
    app = build_app(args.variant, fixture)
    client = app.test_client()
    gc.collect()
    initial_peak_rss = rss_bytes()

    # Warm the page cache and Flask machinery, then time complete in-process
    # requests including route handling, serialization, and response consumption.
    for _ in range(args.warmups):
        response = client.get("/api/traffic")
        assert response.status_code == 200
        response.get_data()
        response.close()
    durations_ms = []
    for _ in range(args.requests):
        start = time.perf_counter_ns()
        response = client.get("/api/traffic")
        payload = response.get_data()
        durations_ms.append((time.perf_counter_ns() - start) / 1_000_000)
        assert response.status_code == 200
        response.close()
    process_peak_rss = rss_bytes()
    wire_bytes = len(payload)

    # Validate outside the timed/memory measurements. All three responses must
    # contain the same last 900 points and preserve all counter/timestamp values.
    with fixture.with_suffix(".expected.json").open() as expected_file:
        expected = json.load(expected_file)
    actual = decode_payload(args.variant, payload)
    assert len(actual) == len(expected)
    for left, right in zip(actual, expected):
        assert {k: v for k, v in left.items() if k != "wall_time"} == {
            k: v for k, v in right.items() if k != "wall_time"
        }
        assert abs(
            (
                datetime.fromisoformat(left["wall_time"])
                - datetime.fromisoformat(right["wall_time"])
            ).total_seconds()
        ) <= 0.000001
    del expected, actual, payload, response
    gc.collect()

    # Trace one separate request so allocation tracking cannot affect latency or
    # the previously captured process RSS high-water mark.
    tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    response = client.get("/api/traffic")
    response.get_data()
    _, peak = tracemalloc.get_traced_memory()
    traced_request_peak = peak - before
    response.close()
    tracemalloc.stop()

    print(json.dumps({
        "durations_ms": durations_ms,
        "initial_peak_rss_bytes": initial_peak_rss,
        "process_peak_rss_bytes": process_peak_rss,
        "rss_high_water_increase_bytes": max(0, process_peak_rss - initial_peak_rss),
        "request_python_peak_allocation_bytes": traced_request_peak,
        "response_body_bytes": wire_bytes,
        "validated_samples": min(args.samples, LIMIT),
    }))


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def sample_for(index):
    # Typical small counter values, occasional delivery lag/errors, and explicit
    # UTC microsecond timestamps, matching the original writer's JSONL shape.
    expected = 120 + index % 91
    attempted = max(0, expected - (index % 13))
    return {
        "expected": expected,
        "attempted": attempted,
        "acked": max(0, attempted - (index % 3)),
        "errors": int(index % 97 == 0),
        "wall_time": (
            datetime(2026, 10, 24, tzinfo=timezone.utc)
            + timedelta(seconds=index, microseconds=130417)
        ).isoformat(),
    }


def write_fixtures(directory, name, count, source_samples=None):
    jsonl_path = directory / (name + ".jsonl")
    binary_path = directory / (name + ".bin")
    tail = deque(maxlen=LIMIT)
    with jsonl_path.open("w") as jsonl_file, binary_path.open("wb") as binary_file:
        binary_file.write(HEADER)
        for index in range(count):
            sample = source_samples[index] if source_samples is not None else sample_for(index)
            jsonl_file.write(json.dumps(sample) + "\n")
            binary_file.write(pack_sample(
                sample, datetime.fromisoformat(sample["wall_time"]).timestamp()
            ))
            tail.append(sample)
    expected_text = json.dumps(list(tail))
    # One common expected file per fixture stem, shared by both extensions.
    jsonl_path.with_suffix(".expected.json").write_text(expected_text)
    return {"jsonl": jsonl_path, "binary": binary_path}


def make_report(result):
    lines = [
        "# Traffic metrics format benchmark",
        "",
        "This compares the original JSONL endpoint with the fixed-width binary "
        "endpoint and a smaller JSONL optimization that retains only the last "
        "900 lines while scanning. It does not start Docker, Kafka, Flink, "
        "the simulator, or a network server.",
        "",
        "Reproduce from the project directory:",
        "",
        "```sh",
        "python3 benchmark_traffic_metrics.py",
        "```",
        "",
        "## Method",
        "",
        f"- Run: {result['generated_at']}; Python {result['environment']['python']}, "
        f"Flask {result['environment']['flask']}, {result['environment']['system']}.",
        f"- {result['config']['rounds']} independent fresh processes per format/history; "
        f"{result['config']['warmups']} warm-up requests followed by "
        f"{result['config']['requests']} measured requests per process. "
        "Formats run sequentially in rotating order, so benchmark workers do not "
        "compete with one another for CPU.",
        "- Latency includes Flask test-client dispatch, file reading, JSON parsing "
        "and serialization where applicable, and consuming the complete response "
        "body. Warm page cache; no network round trip or browser rendering.",
        "- Process RAM is the median fresh worker's peak resident set (RSS), "
        "including Python and Flask. The RSS increase is a high-water-mark "
        "difference from immediately before the first request, not a precise "
        "measurement of current resident memory.",
        "- Python allocation peaks use one additional request with tracemalloc "
        "after timed requests and RSS capture, so tracing does not distort those "
        "measurements. They exclude the browser and OS filesystem cache.",
        "- All response variants were decoded and checked against identical last "
        "900 samples: exact counters and timestamps within one microsecond.",
        "- Requests return at most 900 points. A 24-hour event-time simulation "
        "at 120× speed creates about 720 wall-clock samples; 86,400 samples "
        "means 24 real hours of retained measurements.",
        "",
        "## Endpoint latency and RAM",
        "",
        "| History | Variant | p50 ms | p95 ms | Process peak MiB | RSS increase MiB | Python request peak KiB |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for scenario in result["scenarios"]:
        for variant in VARIANTS:
            values = scenario["variants"][variant]
            lines.append(
                f"| {scenario['label']} ({scenario['samples']:,}) | {LABELS[variant]} "
                f"| {values['latency_p50_ms']:.3f} | {values['latency_p95_ms']:.3f} "
                f"| {values['process_peak_rss_bytes'] / 2**20:.2f} "
                f"| {values['rss_high_water_increase_bytes'] / 2**20:.2f} "
                f"| {values['request_python_peak_allocation_bytes'] / 1024:.1f} |"
            )
    lines.extend([
        "",
        "## File and response sizes",
        "",
        "Uncompressed body sizes exclude HTTP headers. Bytes read are derived "
        "from the implemented read paths: JSONL scans the entire fixture each "
        "request, while binary reads its header and at most 900 complete records.",
        "",
        "| History | JSONL file/read bytes | Binary file bytes | Binary read bytes | JSON body bytes | Binary body bytes |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for scenario in result["scenarios"]:
        lines.append(
            f"| {scenario['label']} ({scenario['samples']:,}) "
            f"| {scenario['file_bytes']['jsonl']:,} | {scenario['file_bytes']['binary']:,} "
            f"| {scenario['variants']['binary_seek']['bytes_read_per_request']:,} "
            f"| {scenario['variants']['jsonl_original']['response_body_bytes']:,} "
            f"| {scenario['variants']['binary_seek']['response_body_bytes']:,} |"
        )
    lines.extend([
        "",
        "## Interpretation limits",
        "",
        "This measures the traffic dashboard endpoint, not click delivery latency "
        "to Kafka, simulation pacing, checkpoint timing, or total Docker RAM. "
        "It also does not measure browser heap usage. The new browser decoder "
        "still creates one ordinary object per displayed point, so a smaller "
        "transfer does not imply that its chart heap shrinks proportionally.",
        "",
        "Small-history total RSS differences around a MiB can be comparable "
        "to fresh-process variation. The traced per-request allocation "
        "reduction is clearer evidence there; large-history RSS differences "
        "are much larger. The optimization does not remove Python/Flask's "
        "baseline memory footprint.",
        "",
        "The bounded-tail JSONL variant separates avoiding retained Python "
        "objects from avoiding the full file scan. It is a simpler alternative "
        "when keeping a human-readable metrics file matters more than binary "
        "disk/network savings.",
        "",
    ])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=30)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", type=Path, default=ROOT / "benchmarks" / "traffic-metrics-results.json")
    parser.add_argument("--report", type=Path, default=ROOT / "benchmarks" / "traffic-metrics.md")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--variant", choices=VARIANTS, help=argparse.SUPPRESS)
    parser.add_argument("--fixture", help=argparse.SUPPRESS)
    parser.add_argument("--samples", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args)
        return
    if args.requests < 1 or args.rounds < 1 or args.warmups < 0:
        parser.error("requests and rounds must be positive; warmups cannot be negative")

    # Preserve the old runtime metrics file as an actual small-history example.
    legacy_file = ROOT / ".traffic-metrics.jsonl"
    source_samples = []
    if legacy_file.exists():
        with legacy_file.open() as source:
            for line in source:
                try:
                    source_samples.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    if not source_samples:
        source_samples = [sample_for(index) for index in range(721)]
    specs = [
        ("current", "Current saved run", len(source_samples), source_samples),
        ("minutes15", "15 real minutes", 900, None),
        ("hours2", "2 real hours", 7200, None),
        ("hours24", "24 real hours", 86400, None),
    ]
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "flask": importlib.metadata.version("flask"),
            "system": platform.platform(),
            "machine": platform.machine(),
        },
        "config": {"requests": args.requests, "warmups": args.warmups, "rounds": args.rounds},
        "current_history_source": "legacy runtime JSONL" if legacy_file.exists() else "synthetic 721-point fixture",
        "scenarios": [],
    }
    with tempfile.TemporaryDirectory(prefix="traffic-metrics-benchmark-") as temporary:
        directory = Path(temporary)
        for name, label, count, existing in specs:
            paths = write_fixtures(directory, name, count, existing)
            results = {variant: [] for variant in VARIANTS}
            for round_index in range(args.rounds):
                order = VARIANTS[round_index % len(VARIANTS) :] + VARIANTS[: round_index % len(VARIANTS)]
                for variant in order:
                    fixture = paths["binary" if variant == "binary_seek" else "jsonl"]
                    command = [
                        sys.executable, str(Path(__file__).resolve()), "--worker",
                        "--variant", variant, "--fixture", str(fixture), "--samples", str(count),
                        "--requests", str(args.requests), "--warmups", str(args.warmups),
                    ]
                    completed = subprocess.run(command, check=True, capture_output=True, text=True)
                    results[variant].append(json.loads(completed.stdout))
                print(f"{label}: round {round_index + 1}/{args.rounds} complete", flush=True)
            scenario = {
                "name": name, "label": label, "samples": count,
                "file_bytes": {key: path.stat().st_size for key, path in paths.items()},
                "variants": {},
            }
            for variant, workers in results.items():
                durations = [value for run in workers for value in run["durations_ms"]]
                summary = {
                    "latency_p50_ms": statistics.median(durations),
                    "latency_p95_ms": percentile(durations, 0.95),
                    "response_body_bytes": workers[0]["response_body_bytes"],
                    "bytes_read_per_request": (
                        len(HEADER) + min(count, LIMIT) * RECORD.size
                        if variant == "binary_seek" else scenario["file_bytes"]["jsonl"]
                    ),
                    "workers": workers,
                }
                for key in (
                    "initial_peak_rss_bytes", "process_peak_rss_bytes",
                    "rss_high_water_increase_bytes", "request_python_peak_allocation_bytes",
                ):
                    summary[key] = statistics.median(run[key] for run in workers)
                scenario["variants"][variant] = summary
            result["scenarios"].append(scenario)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(make_report(result))
    print(f"Results: {args.output}")
    print(f"Report: {args.report}")


if __name__ == "__main__":
    main()
