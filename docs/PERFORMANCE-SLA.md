# bus-foundation 性能 SLA (0.3.0)

> 基于 R87 benchmark + R89 metrics 的官方性能目标。
> 更新频率: 每次 benchmark 基线变更时更新。

## Throughput

| Backend | 操作 | P50 | P99 | 测试条件 |
|---------|------|-----|-----|----------|
| EventBus.publish | 10K events | < 5µs | < 20µs | in-process dict fanout |
| EventBus.subscribe | 10K events | < 5µs | < 20µs | pattern match + callback |
| Asyncio.publish | 10K events | < 20µs | < 50µs | queue-based |
| Asyncio.deliver | 1K events | < 20ms | < 50ms | queue + drain task |
| PersistentBus.write | 500 events | < 200µs | < 500µs | SQLite INSERT + WAL |
| PersistentBus.recent | 100 ops | < 2ms | < 5ms | SQLite get_recent |
| Redis.publish (R93) | 1K events | < 1ms | < 5ms | Redis PUBLISH |

## R89 Metrics Overhead

| 模式 | Publish throughput | 相对基线 |
|------|-------------------|----------|
| Metrics DISABLED (default) | 基准 | 100% |
| Metrics ENABLED (no HTTP) | 基准 + < 1µs | > 95% |
| Metrics ENABLED (HTTP :8745) | 基准 + < 5µs | > 90% |

## Observability

- `/metrics` 请求延迟: < 10ms (1000 concurrent metrics)
- OTel span 创建开销: < 5µs (default pure-stdlib fallback)
- OTel span (opentelemetry-api): < 50µs

## 验证方法

```bash
cd projects/bus-foundation
pytest benchmarks/ --benchmark-only  # R87 baseline
pytest tests/test_metrics.py         # R89 overhead validation
```

CI 中 benchmark 结果自动追踪到 `.benchmarks/` 目录, 与 `0001_baseline.json` 对比。
