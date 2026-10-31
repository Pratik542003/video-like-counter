package com.example.likecounter.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisLikeCounterGatewayTest {

    @Mock
    private org.springframework.data.redis.core.StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private RedisLikeCounterGateway gateway;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        gateway = new RedisLikeCounterGateway(redisTemplate, true);
    }

    @Test
    void increment_seedsFromPostgres_whenKeyMissing() {
        when(redisTemplate.hasKey("video:likes:1")).thenReturn(false);
        when(valueOperations.increment("video:likes:1")).thenReturn(11L);

        Optional<Long> result = gateway.increment(1L, () -> 10L);

        assertThat(result).contains(11L);
        verify(valueOperations).setIfAbsent("video:likes:1", "10");
    }

    @Test
    void increment_skipsSeeding_whenKeyAlreadyExists() {
        when(redisTemplate.hasKey("video:likes:1")).thenReturn(true);
        when(valueOperations.increment("video:likes:1")).thenReturn(6L);

        Optional<Long> result = gateway.increment(1L, () -> {
            throw new AssertionError("postgres fallback should not be called on cache hit");
        });

        assertThat(result).contains(6L);
        verify(valueOperations, never()).setIfAbsent(anyString(), anyString());
    }

    @Test
    void get_readsCurrentValue_fromRedis() {
        when(redisTemplate.hasKey("video:likes:1")).thenReturn(true);
        when(valueOperations.get("video:likes:1")).thenReturn("42");

        Optional<Long> result = gateway.get(1L, () -> 0L);

        assertThat(result).contains(42L);
    }

    @Test
    void decrementBoundedAtZero_delegatesToLuaScript() {
        when(redisTemplate.hasKey("video:likes:1")).thenReturn(true);
        when(redisTemplate.execute(any(RedisScript.class), eq(List.of("video:likes:1")))).thenReturn(4L);

        Optional<Long> result = gateway.decrementBoundedAtZero(1L, () -> 5L);

        assertThat(result).contains(4L);
    }

    @Test
    void allOperations_returnEmpty_whenRedisUnavailable() {
        when(redisTemplate.hasKey(anyString())).thenThrow(new RedisSystemException("down", new RuntimeException()));

        assertThat(gateway.increment(1L, () -> 0L)).isEmpty();
        assertThat(gateway.get(1L, () -> 0L)).isEmpty();
        assertThat(gateway.decrementBoundedAtZero(1L, () -> 0L)).isEmpty();
    }

    @Test
    void operations_returnEmpty_whenDisabledViaConfig() {
        RedisLikeCounterGateway disabledGateway = new RedisLikeCounterGateway(redisTemplate, false);

        assertThat(disabledGateway.increment(1L, () -> 0L)).isEmpty();
        verify(redisTemplate, never()).hasKey(anyString());
    }
}
