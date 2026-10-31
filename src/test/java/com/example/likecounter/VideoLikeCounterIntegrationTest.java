package com.example.likecounter;

import com.example.likecounter.event.VideoLikeEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class VideoLikeCounterIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("likecounter")
            .withUsername("likecounter")
            .withPassword("likecounter");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.1"));

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("REDIS_HOST", redis::getHost);
        registry.add("REDIS_PORT", () -> redis.getMappedPort(6379));
        registry.add("KAFKA_BOOTSTRAP_SERVERS", kafka::getBootstrapServers);
        // Keep the consumer batch flush fast so tests don't need long waits.
        registry.add("KAFKA_MAX_POLL_RECORDS", () -> 50);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private KafkaTemplate<String, VideoLikeEvent> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private String likesUrl(long videoId) {
        return "http://localhost:" + port + "/api/videos/" + videoId + "/likes";
    }

    private String redisKey(long videoId) {
        return "video:likes:" + videoId;
    }

    private Long postgresCount(long videoId) {
        List<Long> rows = jdbcTemplate.query(
                "SELECT like_count FROM video_like_counter WHERE video_id = ?",
                (rs, rowNum) -> rs.getLong(1), videoId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void awaitPostgresCount(long videoId, long expected) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (expected == java.util.Optional.ofNullable(postgresCount(videoId)).orElse(0L)) {
                return;
            }
            Thread.sleep(100);
        }
        assertThat(postgresCount(videoId)).isEqualTo(expected);
    }

    @Test
    void getLikes_returnsZero_forUnknownVideo() {
        ResponseEntity<String> response = restTemplate.getForEntity(likesUrl(100L), String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).contains("\"likeCount\":0");
    }

    @Test
    void likeThenUnlike_updatesCountCorrectly() {
        long videoId = 101L;

        ResponseEntity<String> afterLike = restTemplate.postForEntity(likesUrl(videoId), null, String.class);
        assertThat(afterLike.getBody()).contains("\"likeCount\":1");

        ResponseEntity<String> afterLike2 = restTemplate.postForEntity(likesUrl(videoId), null, String.class);
        assertThat(afterLike2.getBody()).contains("\"likeCount\":2");

        restTemplate.delete(likesUrl(videoId));

        ResponseEntity<String> afterGet = restTemplate.getForEntity(likesUrl(videoId), String.class);
        assertThat(afterGet.getBody()).contains("\"likeCount\":1");
    }

    @Test
    void unlike_neverGoesNegative() {
        long videoId = 102L;

        restTemplate.delete(likesUrl(videoId));
        restTemplate.delete(likesUrl(videoId));

        ResponseEntity<String> response = restTemplate.getForEntity(likesUrl(videoId), String.class);
        assertThat(response.getBody()).contains("\"likeCount\":0");
    }

    @Test
    void invalidVideoId_returnsBadRequest() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/api/videos/-1/likes", String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void increment_isMirroredInRedisCache() {
        long videoId = 104L;

        restTemplate.postForEntity(likesUrl(videoId), null, String.class);
        restTemplate.postForEntity(likesUrl(videoId), null, String.class);

        assertThat(redisTemplate.opsForValue().get(redisKey(videoId))).isEqualTo("2");
    }

    @Test
    void cacheMiss_fallsBackToPostgresAndReseedsRedis() {
        long videoId = 105L;

        // Build up a count, then simulate a cold cache (e.g. after a Redis restart/flush).
        restTemplate.postForEntity(likesUrl(videoId), null, String.class);
        restTemplate.postForEntity(likesUrl(videoId), null, String.class);
        restTemplate.postForEntity(likesUrl(videoId), null, String.class);
        redisTemplate.delete(redisKey(videoId));

        ResponseEntity<String> response = restTemplate.getForEntity(likesUrl(videoId), String.class);

        assertThat(response.getBody()).contains("\"likeCount\":3");
        assertThat(redisTemplate.opsForValue().get(redisKey(videoId))).isEqualTo("3");
    }

    @Test
    void concurrentLikes_areCountedAtomically() throws Exception {
        long videoId = 103L;
        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        List<Runnable> tasks = java.util.stream.IntStream.range(0, threads)
                .<Runnable>mapToObj(i -> () -> restTemplate.postForEntity(likesUrl(videoId), null, String.class))
                .toList();

        tasks.forEach(executor::submit);
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        ResponseEntity<String> response = restTemplate.getForEntity(likesUrl(videoId), String.class);
        JsonNode body = objectMapper.readTree(response.getBody());

        assertThat(body.get("likeCount").asLong()).isEqualTo(threads);
        assertThat(redisTemplate.opsForValue().get(redisKey(videoId))).isEqualTo(String.valueOf(threads));
    }

    @Test
    void likeRequest_eventuallyPersistsToPostgresViaKafkaConsumer() throws Exception {
        long videoId = 201L;

        ResponseEntity<String> response = restTemplate.postForEntity(likesUrl(videoId), null, String.class);

        // The response is immediate (Redis), PostgreSQL durability lags slightly behind
        // since it's applied asynchronously by the batch Kafka consumer.
        assertThat(response.getBody()).contains("\"likeCount\":1");
        awaitPostgresCount(videoId, 1L);
    }

    @Test
    void duplicateKafkaEvent_isAppliedExactlyOnce() throws Exception {
        long videoId = 202L;
        UUID eventId = UUID.randomUUID();
        VideoLikeEvent event = new VideoLikeEvent(eventId, VideoLikeEvent.TYPE_INCREMENTED, videoId, 1L, Instant.now());

        // Publish the exact same event twice, simulating an at-least-once redelivery.
        kafkaTemplate.send("video-like-events", String.valueOf(videoId), event).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("video-like-events", String.valueOf(videoId), event).get(5, TimeUnit.SECONDS);

        awaitPostgresCount(videoId, 1L);
    }
}

