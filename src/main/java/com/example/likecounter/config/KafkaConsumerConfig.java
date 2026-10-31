package com.example.likecounter.config;

import com.example.likecounter.event.VideoLikeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, VideoLikeEvent> videoLikeEventConsumerFactory(
            @Value("${KAFKA_BOOTSTRAP_SERVERS:localhost:9094}") String bootstrapServers,
            @Value("${KAFKA_CONSUMER_GROUP:video-like-counter-group}") String groupId,
            @Value("${KAFKA_MAX_POLL_RECORDS:500}") int maxPollRecords) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.likecounter.event");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.likecounter.event.VideoLikeEvent");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Batches up to max.poll.records events per listener invocation so PostgreSQL is written
    // once per batch (aggregated delta per video), not once per request. After retries are
    // exhausted, failed records are routed to "<topic>.DLT" instead of blocking the partition.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VideoLikeEvent> videoLikeBatchListenerContainerFactory(
            ConsumerFactory<String, VideoLikeEvent> videoLikeEventConsumerFactory,
            KafkaTemplate<String, VideoLikeEvent> videoLikeEventKafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, VideoLikeEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(videoLikeEventConsumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(videoLikeEventKafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        // 3 retries, 1s apart, before giving up and sending to the dead-letter topic.
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3)));
        return factory;
    }
}
