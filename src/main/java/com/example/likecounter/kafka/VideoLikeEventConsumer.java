package com.example.likecounter.kafka;

import com.example.likecounter.event.VideoLikeEvent;
import com.example.likecounter.service.LikeEventPersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class VideoLikeEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(VideoLikeEventConsumer.class);

    private final LikeEventPersistenceService persistenceService;

    public VideoLikeEventConsumer(LikeEventPersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }

    @KafkaListener(
            topics = "${KAFKA_TOPIC:video-like-events}",
            groupId = "${KAFKA_CONSUMER_GROUP:video-like-counter-group}",
            containerFactory = "videoLikeBatchListenerContainerFactory")
    public void onBatch(List<VideoLikeEvent> events, Acknowledgment acknowledgment) {
        log.debug("Received batch of {} video-like events", events.size());
        // Any exception here is retried (with backoff) and eventually dead-lettered by the
        // container's DefaultErrorHandler; we only ack once the batch is durably persisted.
        persistenceService.applyBatch(events);
        acknowledgment.acknowledge();
    }
}
