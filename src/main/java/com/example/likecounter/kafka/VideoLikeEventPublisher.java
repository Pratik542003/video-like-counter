package com.example.likecounter.kafka;

import com.example.likecounter.event.VideoLikeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Fire-and-forget publisher: never blocks or fails the HTTP request. Redis already holds the
 * authoritative fast count by the time this is called, so a publish failure only delays
 * PostgreSQL durability (self-healed later by {@link com.example.likecounter.reconciliation.RedisToPostgresReconciliationJob})
 * rather than losing the like itself.
 */
@Component
public class VideoLikeEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(VideoLikeEventPublisher.class);

    private final KafkaTemplate<String, VideoLikeEvent> kafkaTemplate;
    private final String topic;

    public VideoLikeEventPublisher(KafkaTemplate<String, VideoLikeEvent> kafkaTemplate,
                                    @Value("${KAFKA_TOPIC:video-like-events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publishIncrement(Long videoId) {
        publish(VideoLikeEvent.incremented(videoId));
    }

    public void publishDecrement(Long videoId) {
        publish(VideoLikeEvent.decremented(videoId));
    }

    private void publish(VideoLikeEvent event) {
        try {
            kafkaTemplate.send(topic, String.valueOf(event.videoId()), event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish {} for videoId={} eventId={}: {}",
                                    event.eventType(), event.videoId(), event.eventId(), ex.getMessage());
                        } else {
                            log.debug("Published {} for videoId={} eventId={} offset={}",
                                    event.eventType(), event.videoId(), event.eventId(),
                                    result.getRecordMetadata().offset());
                        }
                    });
        } catch (RuntimeException ex) {
            // Covers synchronous failures (e.g. serialization, producer in a fatal state).
            log.error("Could not submit Kafka publish for videoId={} eventId={}: {}",
                    event.videoId(), event.eventId(), ex.getMessage());
        }
    }
}
