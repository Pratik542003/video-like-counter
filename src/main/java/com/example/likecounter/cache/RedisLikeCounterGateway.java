package com.example.likecounter.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Fast per-video like counter backed by Redis, with PostgreSQL as the source of truth for
 * seeding cold keys. Every public method returns {@link Optional#empty()} instead of throwing
 * when Redis is unavailable, so callers can transparently fall back to PostgreSQL.
 *
 * Keys never expire: a like count is cheap, small, long-lived reference data, and expiring it
 * would just force avoidable PostgreSQL reads on every cache miss for popular videos, defeating
 * the purpose of the cache. PostgreSQL stays the durable copy, so there's no staleness risk from
 * keeping the Redis key around forever.
 */
@Component
public class RedisLikeCounterGateway {

    private static final Logger log = LoggerFactory.getLogger(RedisLikeCounterGateway.class);
    private static final String KEY_PREFIX = "video:likes:";

    private final StringRedisTemplate redisTemplate;
    private final boolean enabled;
    private final RedisScript<Long> boundedDecrementScript = RedisScript.of("""
            local v = tonumber(redis.call('GET', KEYS[1]) or '0')
            if v > 0 then
              v = redis.call('DECR', KEYS[1])
              if v < 0 then
                redis.call('SET', KEYS[1], 0)
                v = 0
              end
            end
            return v
            """, Long.class);

    public RedisLikeCounterGateway(StringRedisTemplate redisTemplate,
                                    @Value("${REDIS_ENABLED:true}") boolean enabled) {
        this.redisTemplate = redisTemplate;
        this.enabled = enabled;
        log.info("Redis like-counter cache enabled={}", enabled);
    }

    public Optional<Long> get(Long videoId, Supplier<Long> postgresFallback) {
        return execute(videoId, postgresFallback, key -> {
            String value = redisTemplate.opsForValue().get(key);
            return value == null ? 0L : Long.parseLong(value);
        });
    }

    public Optional<Long> increment(Long videoId, Supplier<Long> postgresFallback) {
        return execute(videoId, postgresFallback, key -> redisTemplate.opsForValue().increment(key));
    }

    public Optional<Long> decrementBoundedAtZero(Long videoId, Supplier<Long> postgresFallback) {
        return execute(videoId, postgresFallback,
                key -> redisTemplate.execute(boundedDecrementScript, List.of(key)));
    }

    private Optional<Long> execute(Long videoId, Supplier<Long> postgresFallback,
                                    java.util.function.Function<String, Long> operation) {
        if (!enabled) {
            log.debug("Redis cache disabled, using PostgreSQL for videoId={}", videoId);
            return Optional.empty();
        }
        String key = key(videoId);
        try {
            ensureSeeded(key, postgresFallback);
            Long result = operation.apply(key);
            log.debug("Redis operation succeeded for key={} result={}", key, result);
            return Optional.ofNullable(result);
        } catch (RedisSystemException ex) {
            log.warn("Redis unavailable for videoId={}, falling back to PostgreSQL: {}", videoId, ex.getMessage());
            return Optional.empty();
        } catch (RuntimeException ex) {
            log.error("Unexpected Redis failure for videoId={}, falling back to PostgreSQL", videoId, ex);
            return Optional.empty();
        }
    }

    // SETNX makes concurrent seeding race-safe: whichever request seeds first wins, and every
    // request still applies its own +1/-1 afterwards, so totals stay correct either way.
    private void ensureSeeded(String key, Supplier<Long> postgresFallback) {
        if (Boolean.FALSE.equals(redisTemplate.hasKey(key))) {
            Long seed = postgresFallback.get();
            Boolean seeded = redisTemplate.opsForValue().setIfAbsent(key, String.valueOf(seed));
            log.info("Cache miss for key={}, seeded from PostgreSQL with value={} won={}", key, seed, seeded);
        }
    }

    private String key(Long videoId) {
        return KEY_PREFIX + videoId;
    }
}
