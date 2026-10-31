package com.example.likecounter.config;

import com.example.likecounter.event.VideoLikeEvent;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    // Defining this bean ourselves (keyed off KAFKA_BOOTSTRAP_SERVERS) stops Spring Boot's
    // autoconfigured KafkaAdmin from defaulting to localhost:9092 - it's used both for the
    // NewTopic auto-creation beans and the actuator "kafka" health indicator.
    @Bean
    public KafkaAdmin kafkaAdmin(@Value("${KAFKA_BOOTSTRAP_SERVERS:localhost:9094}") String bootstrapServers) {
        return new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    @Bean
    public ProducerFactory<String, VideoLikeEvent> videoLikeEventProducerFactory(
            @Value("${KAFKA_BOOTSTRAP_SERVERS:localhost:9094}") String bootstrapServers) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.RETRIES_CONFIG, 3,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, VideoLikeEvent> videoLikeEventKafkaTemplate(
            ProducerFactory<String, VideoLikeEvent> videoLikeEventProducerFactory) {
        return new KafkaTemplate<>(videoLikeEventProducerFactory);
    }
}
