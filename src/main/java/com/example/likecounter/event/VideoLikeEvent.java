package com.example.likecounter.event;

import java.time.Instant;
import java.util.UUID;

public record VideoLikeEvent(
        UUID eventId,
        String eventType,
        Long videoId,
        long amount,
        Instant createdAt) {

    public static final String TYPE_INCREMENTED = "VIDEO_LIKE_INCREMENTED";
    public static final String TYPE_DECREMENTED = "VIDEO_LIKE_DECREMENTED";

    public static VideoLikeEvent incremented(Long videoId) {
        return new VideoLikeEvent(UUID.randomUUID(), TYPE_INCREMENTED, videoId, 1L, Instant.now());
    }

    public static VideoLikeEvent decremented(Long videoId) {
        return new VideoLikeEvent(UUID.randomUUID(), TYPE_DECREMENTED, videoId, -1L, Instant.now());
    }
}
