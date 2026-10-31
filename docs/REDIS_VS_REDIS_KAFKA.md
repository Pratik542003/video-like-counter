# Redis-only vs Redis+Kafka: Architecture Comparison

This document explains the difference between the Phase 2 architecture (Redis + PostgreSQL) and the Phase 3 architecture (Redis + Kafka + PostgreSQL) for the video like-counter service.

## Side-by-side

| Aspect | Phase 2: Redis + PostgreSQL | Phase 3: Redis + Kafka + PostgreSQL |
|---|---|---|
| Write path (`POST`/`DELETE`) | Redis op **and** a synchronous PostgreSQL write, both inside the same HTTP request | Redis op only; PostgreSQL is updated later, asynchronously, by a Kafka consumer |
| What the client waits for | Redis **and** PostgreSQL (whichever finishes last, same request) | Redis only |
| PostgreSQL write frequency | Once per request | Once per consumer batch (many requests' deltas aggregated into one write per video) |
| Durability timing | Immediate (request can't complete until PostgreSQL commits) | A few hundred ms to a couple of seconds after the response, depending on load |
| Failure mode: Redis down | Falls back to synchronous PostgreSQL-only (same as having no cache) | Falls back to synchronous PostgreSQL-only **and** skips publishing (avoids double-counting when Redis comes back) |
| Failure mode: PostgreSQL down | Request fails (its synchronous write has nowhere to go) | Request still succeeds (Redis already answered); durability catches up once PostgreSQL recovers |
| Failure mode: broker down | N/A (no broker in this phase) | Request still succeeds (Redis already answered); publish failure is logged; reconciliation job self-heals later |
| Idempotency / duplicate handling | Not applicable — there's no redelivery concept without a broker | Required and implemented — events carry a UUID, deduplicated via `processed_like_events` before being aggregated |
| Consistency model | Strong — both stores reflect the request before it returns | Eventual — Redis is authoritative immediately; PostgreSQL converges shortly after |
| Throughput ceiling | PostgreSQL's per-row lock (every request still serializes on it) | Redis's single in-memory op (PostgreSQL no longer gates the response) |
| Measured throughput (5,000-like burst, this project, same machine) | ~165–170 req/s | ~270–300 req/s |
| Added moving parts | None beyond Redis itself | Kafka broker, topic/partitions, consumer group, dead-letter topic, batch persistence service, reconciliation job |
| Operational complexity | Low | Higher — more infrastructure to run, monitor (consumer lag), and reason about (eventual consistency, DLT) |

## Why the write path changes

**Phase 2** treats Redis purely as an accelerator sitting *beside* PostgreSQL: every single write still does both operations, in the same request, so the request can only be as fast as the slower of the two — and PostgreSQL's row-level lock means concurrent likes on the same video still queue up one at a time. Redis mainly helps with **reads**: `GET` can be served from Redis after the first write without touching PostgreSQL at all.

**Phase 3** removes PostgreSQL from the request's critical path entirely. The request's job becomes "update Redis, tell someone else to persist this" — and "telling someone else" (publishing to Kafka) is fire-and-forget, so it doesn't block the response even if it's slow or fails. PostgreSQL durability becomes the Kafka consumer's job, running independently and in batches, which is both faster per-write (one aggregated write instead of N) and decoupled from request latency.

## Why idempotency only matters in Phase 3

Kafka guarantees **at-least-once** delivery: a consumer crash, rebalance, or manual retry can cause the same event to be delivered and processed more than once. Phase 2 has no such redelivery mechanism — each request performs its write exactly once, synchronously, so there's nothing to deduplicate. Phase 3 has to defend against double-processing, which is why `processed_like_events` (keyed by each event's UUID) exists: only the first delivery of a given event is ever allowed to change the count.

## When to prefer each approach

- **Redis + PostgreSQL (Phase 2)** is simpler to run and reason about, and is a reasonable stopping point if write throughput on a single hot video isn't a bottleneck, or if you need every write to be immediately durable (e.g. you can't tolerate even a few seconds of PostgreSQL lag).
- **Redis + Kafka + PostgreSQL (Phase 3)** is worth the added complexity when write throughput matters (viral videos getting thousands of likes/sec) and a short window of eventual consistency between Redis and PostgreSQL is acceptable — which it typically is for a "like count".
