# Video Like-Counter Service (Phase 3: Redis + Kafka + PostgreSQL)

A minimal, production-style REST API that maintains an atomic like counter per video. No authentication, no users — just a counter. Phase 3 moves PostgreSQL off the synchronous request path: writes land on Redis instantly and are durably persisted to PostgreSQL asynchronously via Kafka, **without changing the public REST API**.

## Architecture

```
Client
  |
  v
Spring Boot API  (VideoLikeController -> VideoLikeService)
  |
  v
Redis atomic counter  (RedisLikeCounterGateway: INCR / Lua-guarded DECR)
  |
  v
Kafka event ("video-like-events", fire-and-forget, non-blocking)
  |
  v
Background consumer/worker  (VideoLikeEventConsumer, batch @KafkaListener)
  |
  v
PostgreSQL durable persistence  (LikeEventPersistenceService: idempotent, batched)
```

```
Degraded path when Redis is down (automatic, logged):
Client -> Spring Boot API -> VideoLikeRepository -> PostgreSQL (synchronous, no Kafka publish)
```

- **`VideoLikeController`** — exposes `POST / GET / DELETE /api/videos/{videoId}/likes`. Unchanged since Phase 1.
- **`VideoLikeService`** — validates `videoId`. For writes: calls `RedisLikeCounterGateway` first (fast, authoritative response), then fires a Kafka event asynchronously via `VideoLikeEventPublisher` — the HTTP response never waits on Kafka. If Redis is unavailable, it falls back to writing `VideoLikeRepository` directly and synchronously, and deliberately **skips** publishing (that row is already durable; publishing too would double-count once Redis recovers).
- **`RedisLikeCounterGateway`** — the only class that talks to Redis for live counters. Unchanged from Phase 2.
- **`VideoLikeEventPublisher`** — fire-and-forget Kafka producer. Logs failures (sync or async) but never throws back to the caller.
- **`VideoLikeEventConsumer`** — a *batch* `@KafkaListener` (up to `KAFKA_MAX_POLL_RECORDS` events per invocation) with manual acknowledgment; only acks after the batch is durably persisted.
- **`LikeEventPersistenceService`** — applies a batch in one transaction: a single batched dedup-insert into `processed_like_events` (idempotency via the event's UUID primary key), then one aggregated upsert per distinct video touched in that batch — so PostgreSQL is written once per batch per video, not once per event.
- **`RedisToPostgresReconciliationJob`** — a scheduled safety net (`RECONCILIATION_INTERVAL_MS`, default 30s) that copies Redis's current counts into PostgreSQL via `SCAN`, self-healing any gap left by a failed publish or a crash between consuming and committing.
- **`VideoLikeRepository`** — atomic `INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING` statements for increment/decrement/apply-delta/set-absolute. No read-then-write round trip anywhere.
- **`GlobalExceptionHandler`** — converts invalid input into `400` and unexpected errors into `500`, with a consistent JSON error body.
- **Flyway** — owns the schema (`video_like_counter`, `processed_like_events` + indexes); Hibernate is `ddl-auto: validate` only.
- **Actuator health** (`/actuator/health`) — PostgreSQL, Redis, and Kafka health indicators are all auto-configured for free once their respective starters/beans are present (`db`, `redis`, `kafka` components in the health response).
- **Graceful shutdown** — `server.shutdown=graceful` with a 20s grace period, so in-flight requests and the Kafka consumer finish cleanly on `SIGTERM`/Ctrl+C instead of being cut off mid-batch.

### Phase 1 -> Phase 2 -> Phase 3

| | Phase 1 | Phase 2 | Phase 3 |
|---|---|---|---|
| Read path (`GET`) | Always hits PostgreSQL | Reads Redis first; PostgreSQL only on cache miss | Same as Phase 2 |
| Write path (`POST`/`DELETE`) | Single atomic PostgreSQL statement | Atomic Redis op **and** a synchronous atomic PostgreSQL statement, every request | Atomic Redis op **returned immediately**; PostgreSQL updated **asynchronously** via a Kafka event + batch consumer |
| Hot-video contention | Every request serializes on the PostgreSQL row lock | Redis absorbs most traffic, but PostgreSQL's row lock is still in the synchronous critical path | PostgreSQL is off the request's critical path entirely; only the background consumer serializes on it, in batches |
| Source of truth | PostgreSQL | PostgreSQL | PostgreSQL remains the durable system of record, but Redis is now the moment-to-moment authoritative value for clients |
| Consistency | Strong (single store) | Strong (both stores updated synchronously, same request) | **Eventual** — PostgreSQL catches up within one consumer batch interval (typically well under a second; worst case bounded by the reconciliation job's interval) |
| Redis down | N/A | Falls back to PostgreSQL-only (sync) | Falls back to PostgreSQL-only (sync) and **skips** the Kafka publish for that request |
| Kafka down | N/A | N/A | Publish failure is logged; Redis's value is still returned to the client; the reconciliation job eventually syncs PostgreSQL from Redis regardless |

### Redis key design & consistency model

- Key: `video:likes:{videoId}`, a plain string integer.
- **No expiration.** Like counts are small, cheap, long-lived reference data. Expiring them would force a PostgreSQL read on every cache miss for popular videos — defeating the purpose of the cache — for no staleness benefit, since PostgreSQL is written synchronously on every request anyway.
- **Cache miss / cold start**: if the Redis key doesn't exist (first request, or after a Redis restart/flush), the gateway reads the current count from PostgreSQL and seeds Redis via `SETNX` before applying the operation. `SETNX` makes concurrent seeding race-safe — whichever request seeds first wins, and every request still applies its own atomic `+1`/`-1` afterward, so totals converge correctly regardless of interleaving.
- **Decrement floor**: `DECR` alone can go negative, so the bounded decrement is a small Lua script (`GET` then conditional `DECR`) executed atomically by Redis — matching the PostgreSQL `GREATEST(like_count - 1, 0)` behavior.
- **Redis vs PostgreSQL agreement**: in the normal (Kafka) path, PostgreSQL is only ever updated by applying the *same relative delta* the event carries, aggregated per batch — never by copying Redis's absolute value — so ordering/batching never corrupts the total. The reconciliation job is the one exception: it does write Redis's absolute value into PostgreSQL, by design, purely as a self-healing safety net.

### Consistency model: Redis <-> PostgreSQL

This system is **eventually consistent** by design, not strongly consistent, and that's an explicit trade-off for throughput:

1. A `POST`/`DELETE` request only waits on Redis. It returns in the time it takes to do one `INCR` (or the bounded-decrement Lua script) — PostgreSQL is never on this critical path.
2. The same request publishes a `VideoLikeEvent` to Kafka asynchronously, after the response has already been (or is about to be) sent. A publish failure is caught, logged, and does not fail the request.
3. The `VideoLikeEventConsumer` batches events (up to `KAFKA_MAX_POLL_RECORDS` per poll) and persists them to PostgreSQL in one transaction per batch — typically within milliseconds to a couple of seconds of the original request, depending on load.
4. **Idempotency**: each event carries a unique `eventId` (UUID). The consumer's dedup-insert into `processed_like_events` uses that UUID as the primary key with `ON CONFLICT (event_id) DO NOTHING`; only events that were *newly* inserted (not duplicates) contribute to the aggregated delta applied to `video_like_counter`. This makes Kafka's at-least-once delivery (redeliveries after a consumer crash, rebalance, or manual retry) safe — a duplicate event changes PostgreSQL's count by exactly zero.
5. **Failure modes and recovery**:
   - *Kafka publish fails* (broker down, timeout): logged; Redis already has the correct value; PostgreSQL will catch up via the next successful publish for that video, or via reconciliation.
   - *Consumer processing fails* (e.g. a transient PostgreSQL error): Spring Kafka's `DefaultErrorHandler` retries the whole batch up to 3 times with a 1s backoff; if it keeps failing, every record in the batch is published to `video-like-events.DLT` (dead-letter topic) instead of blocking the partition forever, and consumption continues.
   - *Consumer crashes after persisting but before acking* (or anywhere mid-batch): on restart, the same offsets are redelivered; thanks to the dedup table, replaying them is a no-op.
   - *Any other gap* (e.g. an event was silently lost): `RedisToPostgresReconciliationJob` runs every `RECONCILIATION_INTERVAL_MS` (default 30s) and copies Redis's current value for every known key straight into PostgreSQL, closing any remaining gap within one interval.
6. **What "eventual" means in practice**: if you `GET` immediately after a `POST`, you'll see the fresh Redis value (correct, since `GET` also reads Redis first). If you instead queried PostgreSQL directly, you might see a slightly stale value for up to a few seconds — acceptable for a "like count", not acceptable for something like a financial ledger, which is why this architecture is explicitly scoped to this kind of counter.

### Kafka event schema

Topic: `video-like-events` (configurable via `KAFKA_TOPIC`), partitioned by `videoId` (used as the Kafka message key) so all events for a given video are processed in order by the same partition/consumer.

```json
{
  "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "eventType": "VIDEO_LIKE_INCREMENTED",
  "videoId": 101,
  "amount": 1,
  "createdAt": "2026-10-31T10:15:30Z"
}
```

`eventType` is `VIDEO_LIKE_INCREMENTED` (amount `+1`) or `VIDEO_LIKE_DECREMENTED` (amount `-1`). Failed-and-exhausted records are republished unchanged to `video-like-events.DLT`.

### Table

```sql
CREATE TABLE video_like_counter (
    video_id    BIGINT PRIMARY KEY,
    like_count  BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);
```

Indexes on `like_count` and `updated_at` are included for future sorting/analytics queries.

### Why the counter is atomic

Increment/decrement never do a `SELECT` followed by a `SAVE`. Instead:

```sql
INSERT INTO video_like_counter (video_id, like_count, updated_at)
VALUES (:videoId, 1, now())
ON CONFLICT (video_id)
DO UPDATE SET like_count = video_like_counter.like_count + 1, updated_at = now()
RETURNING like_count;
```

This is one statement, so Postgres guarantees the read-modify-write is atomic even under heavy concurrent load. The decrement variant uses `GREATEST(like_count - 1, 0)` so the counter can never go negative.

## Prerequisites

- Java 21
- Maven 3.9+ (or use the included wrapper if present)
- Docker + Docker Compose (for PostgreSQL, Redis, and Kafka)

## Starting PostgreSQL, Redis, and Kafka

```bash
docker compose up -d
```

This starts:
- Postgres on `localhost:5433` (host port, to avoid clashing with a native Postgres install) with database/user/password `likecounter` (overridable via `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`, `DB_PORT`).
- Redis on `localhost:6379` (overridable via `REDIS_PORT`), no password by default.
- Kafka (KRaft mode, no ZooKeeper) on `localhost:9094` (overridable via `KAFKA_PORT`). `video-like-events` and `video-like-events.DLT` are created automatically on first app startup (`KafkaTopicConfig` + the broker's `auto.create.topics.enable=true`) — no manual topic creation needed.

Wait for all three to report healthy before starting the app:

```bash
docker compose ps
```

## Running the Spring Boot application

### From the command line

```bash
mvn spring-boot:run
```

### From IntelliJ IDEA

1. Open the project folder (`pom.xml` will be auto-detected as a Maven project).
2. Let IntelliJ import dependencies.
3. Run the `LikeCounterApplication` main class directly (right-click → Run).
4. Ensure Postgres is running first (`docker compose up -d`).

### Configuration (environment variables)

| Variable          | Default                                          | Purpose                                   |
|-------------------|---------------------------------------------------|--------------------------------------------|
| `DB_URL`          | `jdbc:postgresql://localhost:5433/likecounter`    | JDBC connection string                    |
| `DB_USERNAME`     | `likecounter`                                     | Database user                             |
| `DB_PASSWORD`     | `likecounter`                                     | Database password                         |
| `SERVER_PORT`     | `8080`                                            | HTTP port                                 |
| `LOG_LEVEL`       | `INFO`                                            | App log level                             |
| `REDIS_HOST`      | `localhost`                                       | Redis host                                |
| `REDIS_PORT`      | `6379`                                            | Redis port                                |
| `REDIS_PASSWORD`  | *(empty)*                                         | Redis password, if your instance requires one |
| `REDIS_ENABLED`   | `true`                                            | Set to `false` to fully bypass Redis and run PostgreSQL-only (same behavior as Phase 1) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094`                          | Kafka broker address the app connects to |
| `KAFKA_TOPIC`     | `video-like-events`                               | Main topic name (the `.DLT` suffix is derived automatically) |
| `KAFKA_CONSUMER_GROUP` | `video-like-counter-group`                   | Consumer group id |
| `KAFKA_MAX_POLL_RECORDS` | `500`                                       | Max events per consumer batch/poll (the "batching" knob) |
| `RECONCILIATION_ENABLED` | `true`                                      | Enable/disable the Redis -> PostgreSQL safety-net job |
| `RECONCILIATION_INTERVAL_MS` | `30000`                                 | How often the reconciliation job runs |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173`                      | Comma-separated origins allowed to call the API |

The app will apply Flyway migrations automatically on startup.

### Health checks

```bash
curl http://localhost:8080/actuator/health
```

Returns aggregated status plus per-dependency components (`db`, `redis`, `kafka`) once the app is up — useful for confirming all three dependencies are reachable before load testing.

### How to inspect Redis keys

```bash
# Open a redis-cli shell inside the container
docker exec -it like-counter-redis redis-cli

# Then, inside redis-cli:
KEYS video:likes:*
GET video:likes:42

# Or in one line from your shell:
docker exec -it like-counter-redis redis-cli GET video:likes:42
```

### How to inspect Kafka messages

```bash
# List topics (should show video-like-events and video-like-events.DLT)
docker exec -it like-counter-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Tail messages on the main topic as you like/unlike videos
docker exec -it like-counter-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic video-like-events --from-beginning

# Check the dead-letter topic (should normally be empty)
docker exec -it like-counter-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic video-like-events.DLT --from-beginning

# Inspect consumer group lag (how far behind PostgreSQL persistence currently is)
docker exec -it like-counter-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group video-like-counter-group
```

### API documentation (Swagger UI)

Once the app is running:

- Swagger UI: http://localhost:8080/swagger-ui.html
- OpenAPI JSON: http://localhost:8080/v3/api-docs

## Testing the API with curl

```bash
# Like a video (creates the counter row if needed, returns new count)
curl -X POST http://localhost:8080/api/videos/42/likes

# Get current like count (returns 0 if the video has never been liked)
curl http://localhost:8080/api/videos/42/likes

# Unlike a video (never goes below 0)
curl -X DELETE http://localhost:8080/api/videos/42/likes

# Invalid videoId -> 400 with a JSON error body
curl http://localhost:8080/api/videos/-1/likes
```

## Running tests

```bash
mvn test
```

- `VideoLikeServiceTest` — unit tests with Mockito, including Redis cache-hit/cache-miss, Redis-unavailable fallback, and Kafka publish-on-success vs skip-on-fallback.
- `RedisLikeCounterGatewayTest` — unit tests for seeding, bounded decrement, and graceful handling when Redis throws (simulated unavailability).
- `VideoLikeEventPublisherTest` — unit tests proving a Kafka publish failure (sync or async) never throws back to the caller.
- `LikeEventPersistenceServiceTest` — unit tests for batched aggregation and duplicate-event skipping (simulated via the dedup-insert's per-row update count).
- `VideoLikeControllerTest` — `@WebMvcTest` slice tests with MockMvc.
- `VideoLikeCounterIntegrationTest` — full-stack integration tests using Testcontainers (real throwaway Postgres + Redis + Kafka containers), covering:
  - cache-hit reads mirrored correctly in Redis,
  - cache miss on a cold/flushed Redis key correctly falls back to and reseeds from PostgreSQL,
  - a concurrency test that fires 20 parallel `POST` requests and asserts both Redis and PostgreSQL report exactly 20,
  - a `POST` request eventually persisting to PostgreSQL via the real Kafka pipeline (`likeRequest_eventuallyPersistsToPostgresViaKafkaConsumer`),
  - publishing the exact same event twice and asserting PostgreSQL only counts it once (`duplicateKafkaEvent_isAppliedExactlyOnce`).

Testcontainers requires Docker to be running locally.

## Load testing

The quickest way to generate load is the bundled frontend (below) — its burst buttons fire hundreds/thousands of concurrent requests at a single video and report requests/sec.

For command-line load testing, e.g. with [hey](https://github.com/rakyll/hey):

```bash
hey -n 5000 -c 50 -m POST http://localhost:8080/api/videos/42/likes
```

While it runs, watch:
- `docker exec -it like-counter-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group video-like-counter-group` — consumer lag should rise then drain back to ~0 shortly after the load stops.
- `docker exec like-counter-redis redis-cli GET video:likes:42` vs `docker exec -i like-counter-postgres psql -U likecounter -d likecounter -c "SELECT like_count FROM video_like_counter WHERE video_id=42;"` — Redis updates instantly; PostgreSQL should converge to the same number within a few seconds after the load stops.

## Frontend (React + TypeScript)

A small demo UI lives in [src/main/resources/frontend](src/main/resources/frontend). It lets you pick a video ID and fire a burst of likes (100 / 500 / 1000 / 2000 / 5000) at the backend to visually exercise the counter under concurrent load, and shows requests/sec so you can feel the throughput difference Kafka makes versus a synchronous write path.

```bash
cd src/main/resources/frontend
npm install
npm run dev
```

Open http://localhost:5173. It calls the backend at `VITE_API_BASE_URL` (defaults to `http://localhost:8080`, see `.env.example`).

The backend's `CorsConfig` allows `http://localhost:5173` by default; override with the `CORS_ALLOWED_ORIGINS` env var (comma-separated) if you serve the frontend elsewhere.

Since Phase 3 makes PostgreSQL persistence asynchronous, the UI shows a small note that the on-screen count (from Redis, via the API) is the live value, while PostgreSQL durability lags slightly behind via the Kafka consumer.

## Stopping and resetting the environment

Stop the app (Ctrl+C or your IDE's stop button) — it shuts down gracefully (`server.shutdown=graceful`), finishing in-flight requests and the current Kafka batch before exiting.

Stop the containers, keeping data:

```bash
docker compose down
```

Stop the containers and wipe all data (Postgres rows, Redis keys, Kafka topics/offsets) for a completely clean slate:

```bash
docker compose down -v
```
