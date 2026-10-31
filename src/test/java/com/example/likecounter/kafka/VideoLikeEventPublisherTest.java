package com.example.likecounter.kafka;

import com.example.likecounter.event.VideoLikeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VideoLikeEventPublisherTest {

    @Mock
    private KafkaTemplate<String, VideoLikeEvent> kafkaTemplate;

    @Test
    void publishIncrement_doesNotThrow_whenKafkaSendFailsAsynchronously() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker unavailable")));
        VideoLikeEventPublisher publisher = new VideoLikeEventPublisher(kafkaTemplate, "video-like-events");

        assertThatCode(() -> publisher.publishIncrement(1L)).doesNotThrowAnyException();
    }

    @Test
    void publishDecrement_doesNotThrow_whenKafkaTemplateThrowsSynchronously() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new org.apache.kafka.common.errors.TimeoutException("no brokers available"));
        VideoLikeEventPublisher publisher = new VideoLikeEventPublisher(kafkaTemplate, "video-like-events");

        assertThatCode(() -> publisher.publishDecrement(1L)).doesNotThrowAnyException();
    }
}
