package com.example.likecounter.service;

import com.example.likecounter.cache.RedisLikeCounterGateway;
import com.example.likecounter.dto.LikeCountResponse;
import com.example.likecounter.exception.InvalidVideoIdException;
import com.example.likecounter.kafka.VideoLikeEventPublisher;
import com.example.likecounter.repository.VideoLikeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VideoLikeServiceTest {

    @Mock
    private VideoLikeRepository repository;

    @Mock
    private RedisLikeCounterGateway redisGateway;

    @Mock
    private VideoLikeEventPublisher eventPublisher;

    @InjectMocks
    private VideoLikeService service;

    @BeforeEach
    void defaultRedisUnavailable() {
        // Default: Redis gateway reports "unavailable" so every test falls back to PostgreSQL
        // unless a test explicitly stubs a cache hit.
        lenient().when(redisGateway.increment(anyLong(), any())).thenReturn(Optional.empty());
        lenient().when(redisGateway.decrementBoundedAtZero(anyLong(), any())).thenReturn(Optional.empty());
        lenient().when(redisGateway.get(anyLong(), any())).thenReturn(Optional.empty());
    }

    @Test
    void incrementLikes_returnsNewCount() {
        when(repository.incrementAndGet(1L)).thenReturn(5L);

        LikeCountResponse response = service.incrementLikes(1L);

        assertThat(response.videoId()).isEqualTo(1L);
        assertThat(response.likeCount()).isEqualTo(5L);
        verify(repository).incrementAndGet(1L);
    }

    @Test
    void incrementLikes_prefersRedisCount_andPublishesEvent_whenCacheAvailable() {
        when(redisGateway.increment(anyLong(), any())).thenReturn(Optional.of(99L));

        LikeCountResponse response = service.incrementLikes(1L);

        assertThat(response.likeCount()).isEqualTo(99L);
        verify(eventPublisher).publishIncrement(1L);
        verify(repository, org.mockito.Mockito.never()).incrementAndGet(anyLong());
    }

    @Test
    void incrementLikes_fallsBackToPostgres_andSkipsPublish_whenRedisUnavailable() {
        when(repository.incrementAndGet(1L)).thenReturn(7L);

        LikeCountResponse response = service.incrementLikes(1L);

        assertThat(response.likeCount()).isEqualTo(7L);
        verify(eventPublisher, org.mockito.Mockito.never()).publishIncrement(anyLong());
    }

    @Test
    void getLikes_returnsZero_whenNoRowExists() {
        when(repository.findLikeCount(1L)).thenReturn(java.util.Optional.empty());

        LikeCountResponse response = service.getLikes(1L);

        assertThat(response.likeCount()).isEqualTo(0L);
    }

    @Test
    void getLikes_returnsStoredCount() {
        when(repository.findLikeCount(1L)).thenReturn(java.util.Optional.of(42L));

        LikeCountResponse response = service.getLikes(1L);

        assertThat(response.likeCount()).isEqualTo(42L);
    }

    @Test
    void getLikes_returnsRedisValue_onCacheHit_withoutRequiringPostgres() {
        when(redisGateway.get(anyLong(), any())).thenReturn(Optional.of(123L));

        LikeCountResponse response = service.getLikes(1L);

        assertThat(response.likeCount()).isEqualTo(123L);
    }

    @Test
    void decrementLikes_returnsNewCount() {
        when(repository.decrementAndGet(1L)).thenReturn(0L);

        LikeCountResponse response = service.decrementLikes(1L);

        assertThat(response.likeCount()).isEqualTo(0L);
    }

    @Test
    void decrementLikes_prefersRedisCount_andPublishesEvent_whenCacheAvailable() {
        when(redisGateway.decrementBoundedAtZero(anyLong(), any())).thenReturn(Optional.of(4L));

        LikeCountResponse response = service.decrementLikes(1L);

        assertThat(response.likeCount()).isEqualTo(4L);
        verify(eventPublisher).publishDecrement(1L);
        verify(repository, org.mockito.Mockito.never()).decrementAndGet(anyLong());
    }

    @Test
    void incrementLikes_rejectsNonPositiveVideoId() {
        assertThatThrownBy(() -> service.incrementLikes(0L))
                .isInstanceOf(InvalidVideoIdException.class);
        assertThatThrownBy(() -> service.incrementLikes(-5L))
                .isInstanceOf(InvalidVideoIdException.class);
    }

    @Test
    void getLikes_rejectsNullVideoId() {
        assertThatThrownBy(() -> service.getLikes(null))
                .isInstanceOf(InvalidVideoIdException.class);
    }

    @Test
    void repositoryNeverCalled_whenVideoIdInvalid() {
        assertThatThrownBy(() -> service.decrementLikes(-1L))
                .isInstanceOf(InvalidVideoIdException.class);
        verify(repository, org.mockito.Mockito.never()).decrementAndGet(anyLong());
    }
}
