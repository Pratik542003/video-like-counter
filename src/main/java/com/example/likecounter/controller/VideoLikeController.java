package com.example.likecounter.controller;

import com.example.likecounter.dto.LikeCountResponse;
import com.example.likecounter.service.VideoLikeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/videos/{videoId}/likes")
@Tag(name = "Video Likes", description = "Per-video like counter operations")
public class VideoLikeController {

    private static final Logger log = LoggerFactory.getLogger(VideoLikeController.class);

    private final VideoLikeService service;

    public VideoLikeController(VideoLikeService service) {
        this.service = service;
    }

    @PostMapping
    @Operation(summary = "Like a video", description = "Atomically increments the like counter and returns the new count.")
    public ResponseEntity<LikeCountResponse> like(
            @Parameter(description = "Positive video identifier", example = "42") @PathVariable Long videoId) {
        log.info("Received like request for videoId={}", videoId);
        LikeCountResponse response = service.incrementLikes(videoId);
        log.debug("Like request completed for videoId={} likeCount={}", videoId, response.likeCount());
        return ResponseEntity.status(HttpStatus.OK).body(response);
    }

    @GetMapping
    @Operation(summary = "Get like count", description = "Returns the current like count, or 0 if the video has never been liked.")
    public ResponseEntity<LikeCountResponse> getLikes(
            @Parameter(description = "Positive video identifier", example = "42") @PathVariable Long videoId) {
        log.debug("Received like count request for videoId={}", videoId);
        LikeCountResponse response = service.getLikes(videoId);
        log.debug("Like count request completed for videoId={} likeCount={}", videoId, response.likeCount());
        return ResponseEntity.ok(response);
    }

    @DeleteMapping
    @Operation(summary = "Unlike a video", description = "Atomically decrements the like counter, never going below 0.")
    public ResponseEntity<LikeCountResponse> unlike(
            @Parameter(description = "Positive video identifier", example = "42") @PathVariable Long videoId) {
        log.info("Received unlike request for videoId={}", videoId);
        LikeCountResponse response = service.decrementLikes(videoId);
        log.debug("Unlike request completed for videoId={} likeCount={}", videoId, response.likeCount());
        return ResponseEntity.ok(response);
    }
}
