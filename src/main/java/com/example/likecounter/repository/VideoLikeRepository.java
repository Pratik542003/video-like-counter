package com.example.likecounter.repository;

import com.example.likecounter.model.VideoLikeCounter;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface VideoLikeRepository extends JpaRepository<VideoLikeCounter, Long> {

    @Query(value = """
            SELECT like_count FROM video_like_counter WHERE video_id = :videoId
            """, nativeQuery = true)
    Optional<Long> findLikeCount(@Param("videoId") Long videoId);

    // Atomic upsert + increment, single round-trip, race-safe via Postgres row lock.
    @Query(value = """
            INSERT INTO video_like_counter (video_id, like_count, updated_at)
            VALUES (:videoId, 1, now())
            ON CONFLICT (video_id)
            DO UPDATE SET like_count = video_like_counter.like_count + 1, updated_at = now()
            RETURNING like_count
            """, nativeQuery = true)
    Long incrementAndGet(@Param("videoId") Long videoId);

    // Atomic upsert + decrement clamped at zero, single round-trip, race-safe.
    @Query(value = """
            INSERT INTO video_like_counter (video_id, like_count, updated_at)
            VALUES (:videoId, 0, now())
            ON CONFLICT (video_id)
            DO UPDATE SET like_count = GREATEST(video_like_counter.like_count - 1, 0), updated_at = now()
            RETURNING like_count
            """, nativeQuery = true)
    Long decrementAndGet(@Param("videoId") Long videoId);

    // Applies an aggregated signed delta (from a batch of Kafka events) atomically, clamped at zero.
    @Query(value = """
            INSERT INTO video_like_counter (video_id, like_count, updated_at)
            VALUES (:videoId, GREATEST(:delta, 0), now())
            ON CONFLICT (video_id)
            DO UPDATE SET like_count = GREATEST(video_like_counter.like_count + :delta, 0), updated_at = now()
            RETURNING like_count
            """, nativeQuery = true)
    Long applyDelta(@Param("videoId") Long videoId, @Param("delta") Long delta);

    // Reconciliation sets the absolute value (Redis is authoritative), regardless of current row state.
    @Query(value = """
            INSERT INTO video_like_counter (video_id, like_count, updated_at)
            VALUES (:videoId, :count, now())
            ON CONFLICT (video_id)
            DO UPDATE SET like_count = :count, updated_at = now()
            """, nativeQuery = true)
    @org.springframework.data.jpa.repository.Modifying
    void setAbsoluteCount(@Param("videoId") Long videoId, @Param("count") Long count);
}
