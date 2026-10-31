package com.example.likecounter.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "video_like_counter")
public class VideoLikeCounter {

    @Id
    @Column(name = "video_id")
    private Long videoId;

    @Column(name = "like_count", nullable = false)
    private Long likeCount;

    @Column(name = "updated_at")
    private Instant updatedAt;

    protected VideoLikeCounter() {
        // JPA
    }

    public VideoLikeCounter(Long videoId, Long likeCount, Instant updatedAt) {
        this.videoId = videoId;
        this.likeCount = likeCount;
        this.updatedAt = updatedAt;
    }

    public Long getVideoId() {
        return videoId;
    }

    public Long getLikeCount() {
        return likeCount;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
