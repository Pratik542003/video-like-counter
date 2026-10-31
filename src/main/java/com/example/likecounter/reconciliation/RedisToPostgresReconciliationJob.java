package com.example.likecounter.reconciliation;

import com.example.likecounter.repository.VideoLikeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;
import java.util.TreeSet;

/**
 * Safety net on top of the Kafka pipeline: periodically copies Redis's current (authoritative,
 * fast) counts into PostgreSQL. This self-heals any gap left by a failed Kafka publish, a consumer
 * crash before a batch was persisted, or any other dropped event - Redis is never a source of
 * truth loss as long as this job keeps running. Uses SCAN (not KEYS) so it never blocks Redis.
 */
@Component
public class RedisToPostgresReconciliationJob {

    private static final Logger log = LoggerFactory.getLogger(RedisToPostgresReconciliationJob.class);
    private static final String KEY_PATTERN = "video:likes:*";
    private static final String KEY_PREFIX = "video:likes:";

    private final StringRedisTemplate redisTemplate;
    private final VideoLikeRepository repository;
    private final boolean enabled;

    public RedisToPostgresReconciliationJob(StringRedisTemplate redisTemplate,
                                             VideoLikeRepository repository,
                                             @Value("${RECONCILIATION_ENABLED:true}") boolean enabled) {
        this.redisTemplate = redisTemplate;
        this.repository = repository;
        this.enabled = enabled;
    }

    @Scheduled(fixedDelayString = "${RECONCILIATION_INTERVAL_MS:30000}")
    @Transactional
    public void reconcile() {
        if (!enabled) {
            return;
        }
        Set<String> keys = scanKeys();
        if (keys.isEmpty()) {
            return;
        }

        int reconciled = 0;
        for (String key : keys) {
            try {
                String value = redisTemplate.opsForValue().get(key);
                if (value == null) {
                    continue;
                }
                Long videoId = Long.valueOf(key.substring(KEY_PREFIX.length()));
                repository.setAbsoluteCount(videoId, Long.valueOf(value));
                reconciled++;
            } catch (RuntimeException ex) {
                log.warn("Failed to reconcile key={}: {}", key, ex.getMessage());
            }
        }
        log.info("Reconciliation pass complete: {} Redis keys synced to PostgreSQL", reconciled);
    }

    private Set<String> scanKeys() {
        Set<String> keys = new TreeSet<>();
        try (var cursor = redisTemplate.scan(ScanOptions.scanOptions().match(KEY_PATTERN).count(500).build())) {
            cursor.forEachRemaining(keys::add);
        } catch (RuntimeException ex) {
            log.warn("Redis unavailable during reconciliation scan: {}", ex.getMessage());
        }
        return keys;
    }
}
