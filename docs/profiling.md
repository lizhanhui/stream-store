## In-Memory Replication

```text
═══════════════════════════════════════════════════════════════
  Pipeline Append Benchmark Results
═══════════════════════════════════════════════════════════════
  Duration:        5.05s
  Senders:         4 (single stream)
  Payload size:    1024 bytes
  Arena capacity:  64 MiB
  RF:              2
  Pipeline depth:  16
───────────────────────────────────────────────────────────────
  Total appends:   652858
  Total bytes:     668526592 (668.53 MB)
  Throughput:      129167 ops/sec
  Throughput:      126.14 MB/sec
  Total seals:     371
───────────────────────────────────────────────────────────────
  Latency p50:     404.398µs
  Latency p99:     602.697µs
  Latency max:     82.860706ms
═══════════════════════════════════════════════════════════════
```