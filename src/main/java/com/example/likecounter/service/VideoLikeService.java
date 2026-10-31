package com.example.likecounter.service;

import com.example.likecounter.cache.RedisLikeCounterGateway;
import com.example.likecounter.dto.LikeCountResponse;
import com.example.likecounter.exception.InvalidVideoIdException;
import com.example.likecounter.kafka.VideoLikeEventPublisher;
import com.example.likecounter.repository.VideoLikeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class VideoLikeService {

    private static final Logger log = LoggerFactory.getLogger(VideoLikeService.class);

    private final VideoLikeRepository repository;
    private final RedisLikeCounterGateway redisGateway;
    private final VideoLikeEventPublisher eventPublisher;

    public VideoLikeService(VideoLikeRepository repository, RedisLikeCounterGateway redisGateway,
                             VideoLikeEventPublisher eventPublisher) {
        this.repository = repository;
        this.redisGateway = redisGateway;
        this.eventPublisher = eventPublisher;
    }

    // Normal path: Redis INCR (fast, authoritative response) + async Kafka event for durable
    // PostgreSQL persistence by the consumer. If Redis is down, we degrade to writing PostgreSQL
    // directly and synchronously, and deliberately skip publishing - that write already made the
    // row durable, so publishing the same delta too would double-count it once Redis (and the
    // normal path) comes back.
    @Transactional
    public LikeCountResponse incrementLikes(Long videoId) {
        validateVideoId(videoId);
        log.debug("Incrementing like count for videoId={}", videoId);
        try {
            Optional<Long> redisCount = redisGateway.increment(videoId, () -> repository.findLikeCount(videoId).orElse(0L));
            Long newCount;
            if (redisCount.isPresent()) {
                newCount = redisCount.get();
                eventPublisher.publishIncrement(videoId);
            } else {
                log.warn("Redis unavailable, writing PostgreSQL directly for videoId={}", videoId);
                newCount = repository.incrementAndGet(videoId);
            }
            log.info("Incremented like count for videoId={} newCount={}", videoId, newCount);
            return new LikeCountResponse(videoId, newCount);
        } catch (RuntimeException ex) {
            log.error("Failed to increment like count for videoId={}", videoId, ex);
            throw ex;
        }
    }

    @Transactional(readOnly = true)
    public LikeCountResponse getLikes(Long videoId) {
        validateVideoId(videoId);
        log.debug("Fetching like count for videoId={}", videoId);
        try {
            Long count = redisGateway.get(videoId, () -> repository.findLikeCount(videoId).orElse(0L))
                    .orElseGet(() -> repository.findLikeCount(videoId).orElse(0L));
            log.debug("Fetched like count for videoId={} count={}", videoId, count);
            return new LikeCountResponse(videoId, count);
        } catch (RuntimeException ex) {
            log.error("Failed to fetch like count for videoId={}", videoId, ex);
            throw ex;
        }
    }

    @Transactional
    public LikeCountResponse decrementLikes(Long videoId) {
        validateVideoId(videoId);
        log.debug("Decrementing like count for videoId={}", videoId);
        try {
            Optional<Long> redisCount = redisGateway.decrementBoundedAtZero(videoId,
                    () -> repository.findLikeCount(videoId).orElse(0L));
            Long newCount;
            if (redisCount.isPresent()) {
                newCount = redisCount.get();
                eventPublisher.publishDecrement(videoId);
            } else {
                log.warn("Redis unavailable, writing PostgreSQL directly for videoId={}", videoId);
                newCount = repository.decrementAndGet(videoId);
            }
            log.info("Decremented like count for videoId={} newCount={}", videoId, newCount);
            return new LikeCountResponse(videoId, newCount);
        } catch (RuntimeException ex) {
            log.error("Failed to decrement like count for videoId={}", videoId, ex);
            throw ex;
        }
    }

    private void validateVideoId(Long videoId) {
        if (videoId == null || videoId <= 0) {
            log.warn("Rejected request with invalid videoId={}", videoId);
            throw new InvalidVideoIdException("videoId must be a positive number");
        }
    }
}
