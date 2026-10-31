package com.example.likecounter.service;

import com.example.likecounter.event.VideoLikeEvent;
import com.example.likecounter.repository.VideoLikeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies a batch of Kafka events to PostgreSQL in one transaction: a single batched dedup-insert
 * (idempotency, via the UUID primary key) followed by one upsert per distinct video touched in
 * the batch (aggregated delta), instead of one write per event.
 */
@Service
public class LikeEventPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(LikeEventPersistenceService.class);

    private static final String DEDUP_INSERT_SQL = """
            INSERT INTO processed_like_events (event_id, video_id, amount)
            VALUES (?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;
    private final VideoLikeRepository repository;

    public LikeEventPersistenceService(JdbcTemplate jdbcTemplate, VideoLikeRepository repository) {
        this.jdbcTemplate = jdbcTemplate;
        this.repository = repository;
    }

    @Transactional
    public void applyBatch(List<VideoLikeEvent> events) {
        if (events.isEmpty()) {
            return;
        }

        // A JDBC batch update is a single network round trip; Postgres reports update count 1
        // for a newly inserted row and 0 when ON CONFLICT DO NOTHING skipped a duplicate.
        // batchSize == events.size() guarantees exactly one chunk, i.e. one row in the result.
        int[][] chunkedResults = jdbcTemplate.batchUpdate(DEDUP_INSERT_SQL, events, events.size(),
                (ps, event) -> {
                    ps.setObject(1, event.eventId());
                    ps.setLong(2, event.videoId());
                    ps.setLong(3, event.amount());
                });
        int[] insertResults = chunkedResults[0];

        Map<Long, Long> deltaByVideoId = new LinkedHashMap<>();
        int duplicates = 0;
        for (int i = 0; i < events.size(); i++) {
            VideoLikeEvent event = events.get(i);
            if (insertResults[i] == 1) {
                deltaByVideoId.merge(event.videoId(), event.amount(), Long::sum);
            } else {
                duplicates++;
                log.warn("Skipped duplicate event eventId={} videoId={} (already processed)",
                        event.eventId(), event.videoId());
            }
        }

        deltaByVideoId.forEach((videoId, delta) -> {
            Long newCount = repository.applyDelta(videoId, delta);
            log.debug("Applied aggregated delta={} for videoId={} newCount={}", delta, videoId, newCount);
        });

        log.info("Persisted batch of {} events ({} videos touched, {} duplicates skipped)",
                events.size(), deltaByVideoId.size(), duplicates);
    }
}
