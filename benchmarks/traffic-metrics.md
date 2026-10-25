# Traffic metrics format benchmark

This compares the original JSONL endpoint with the fixed-width binary endpoint and a smaller JSONL optimization that retains only the last 900 lines while scanning. It does not start Docker, Kafka, Flink, the simulator, or a network server.

Reproduce from the project directory:

```sh
python3 benchmark_traffic_metrics.py
```

## Method

- Run: 2026-10-24T22:11:20.131173+00:00; Python 3.9.6, Flask 3.1.3, macOS-15.7.2-x86_64-i386-64bit.
- 3 independent fresh processes per format/history; 3 warm-up requests followed by 30 measured requests per process. Formats run sequentially in rotating order, so benchmark workers do not compete with one another for CPU.
- Latency includes Flask test-client dispatch, file reading, JSON parsing and serialization where applicable, and consuming the complete response body. Warm page cache; no network round trip or browser rendering.
- Process RAM is the median fresh worker's peak resident set (RSS), including Python and Flask. The RSS increase is a high-water-mark difference from immediately before the first request, not a precise measurement of current resident memory.
- Python allocation peaks use one additional request with tracemalloc after timed requests and RSS capture, so tracing does not distort those measurements. They exclude the browser and OS filesystem cache.
- All response variants were decoded and checked against identical last 900 samples: exact counters and timestamps within one microsecond.
- Requests return at most 900 points. A 24-hour event-time simulation at 120× speed creates about 720 wall-clock samples; 86,400 samples means 24 real hours of retained measurements.

## Endpoint latency and RAM

| History | Variant | p50 ms | p95 ms | Process peak MiB | RSS increase MiB | Python request peak KiB |
|---|---|---:|---:|---:|---:|---:|
| Current saved run (721) | Original JSONL | 6.219 | 6.787 | 31.34 | 1.29 | 1058.7 |
| Current saved run (721) | JSONL with bounded tail | 6.227 | 7.097 | 31.63 | 1.48 | 1171.2 |
| Current saved run (721) | Binary with seek | 0.576 | 0.717 | 30.89 | 0.22 | 45.2 |
| 15 real minutes (900) | Original JSONL | 8.856 | 19.678 | 32.16 | 1.70 | 1322.0 |
| 15 real minutes (900) | JSONL with bounded tail | 7.786 | 9.152 | 32.09 | 1.85 | 1463.4 |
| 15 real minutes (900) | Binary with seek | 0.581 | 0.726 | 30.90 | 0.32 | 53.6 |
| 2 real hours (7,200) | Original JSONL | 42.687 | 47.644 | 36.00 | 5.75 | 5033.3 |
| 2 real hours (7,200) | JSONL with bounded tail | 9.507 | 10.852 | 32.11 | 1.91 | 1463.4 |
| 2 real hours (7,200) | Binary with seek | 0.586 | 0.741 | 30.26 | 0.25 | 53.6 |
| 24 real hours (86,400) | Original JSONL | 512.129 | 626.240 | 92.37 | 62.06 | 51689.8 |
| 24 real hours (86,400) | JSONL with bounded tail | 24.963 | 27.493 | 32.18 | 1.84 | 1463.4 |
| 24 real hours (86,400) | Binary with seek | 0.581 | 0.753 | 30.89 | 0.31 | 53.6 |

## File and response sizes

Uncompressed body sizes exclude HTTP headers. Bytes read are derived from the implemented read paths: JSONL scans the entire fixture each request, while binary reads its header and at most 900 complete records.

| History | JSONL file/read bytes | Binary file bytes | Binary read bytes | JSON body bytes | Binary body bytes |
|---|---:|---:|---:|---:|---:|
| Current saved run (721) | 79,964 | 17,312 | 17,312 | 73,477 | 17,312 |
| 15 real minutes (900) | 100,800 | 21,608 | 21,608 | 92,702 | 21,608 |
| 2 real hours (7,200) | 806,400 | 172,808 | 21,608 | 92,702 | 21,608 |
| 24 real hours (86,400) | 9,676,800 | 2,073,608 | 21,608 | 92,702 | 21,608 |

## Interpretation limits

This measures the traffic dashboard endpoint, not click delivery latency to Kafka, simulation pacing, checkpoint timing, or total Docker RAM. It also does not measure browser heap usage. The new browser decoder still creates one ordinary object per displayed point, so a smaller transfer does not imply that its chart heap shrinks proportionally.

Small-history total RSS differences around a MiB can be comparable to fresh-process variation. The traced per-request allocation reduction is clearer evidence there; large-history RSS differences are much larger. The optimization does not remove Python/Flask's baseline memory footprint.

The bounded-tail JSONL variant separates avoiding retained Python objects from avoiding the full file scan. It is a simpler alternative when keeping a human-readable metrics file matters more than binary disk/network savings.
