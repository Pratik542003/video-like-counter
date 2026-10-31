package com.example.likecounter.service;

import com.example.likecounter.event.VideoLikeEvent;
import com.example.likecounter.repository.VideoLikeRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LikeEventPersistenceServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private VideoLikeRepository repository;

    @InjectMocks
    private LikeEventPersistenceService persistenceService;

    private VideoLikeEvent event(long videoId, long amount) {
        return new VideoLikeEvent(UUID.randomUUID(), "VIDEO_LIKE_INCREMENTED", videoId, amount, Instant.now());
    }

    @Test
    void applyBatch_aggregatesDeltaPerVideo_inOneUpsertEach() {
        VideoLikeEvent e1 = event(1L, 1);
        VideoLikeEvent e2 = event(1L, 1);
        VideoLikeEvent e3 = event(2L, -1);
        List<VideoLikeEvent> batch = List.of(e1, e2, e3);

        when(jdbcTemplate.batchUpdate(anyString(), anyList(), anyInt(), any())).thenReturn(new int[][] {{1, 1, 1}});

        persistenceService.applyBatch(batch);

        verify(repository).applyDelta(eq(1L), eq(2L));
        verify(repository).applyDelta(eq(2L), eq(-1L));
    }

    @Test
    void applyBatch_skipsDuplicateEvents_detectedByDedupInsertConflict() {
        VideoLikeEvent e1 = event(1L, 1);
        VideoLikeEvent duplicate = event(1L, 1);
        List<VideoLikeEvent> batch = List.of(e1, duplicate);

        // Simulate: first event newly inserted (1), second is a retry/duplicate (0 = ON CONFLICT DO NOTHING).
        when(jdbcTemplate.batchUpdate(anyString(), anyList(), anyInt(), any())).thenReturn(new int[][] {{1, 0}});

        persistenceService.applyBatch(batch);

        verify(repository).applyDelta(eq(1L), eq(1L));
    }

    @Test
    void applyBatch_doesNothing_forEmptyBatch() {
        persistenceService.applyBatch(List.of());

        verify(jdbcTemplate, never()).batchUpdate(anyString(), anyList(), anyInt(), any());
        verify(repository, never()).applyDelta(any(), any());
    }
}
