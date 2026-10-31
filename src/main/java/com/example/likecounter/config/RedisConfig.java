package com.example.likecounter.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

    @Bean
    public LettuceConnectionFactory redisConnectionFactory(
            @Value("${REDIS_HOST:localhost}") String host,
            @Value("${REDIS_PORT:6379}") int port,
            @Value("${REDIS_PASSWORD:}") String password) {
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration(host, port);
        boolean authenticated = password != null && !password.isBlank();
        if (authenticated) {
            configuration.setPassword(password);
        }
        log.info("Configuring Redis connection factory host={} port={} authenticated={}", host, port, authenticated);
        return new LettuceConnectionFactory(configuration);
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(LettuceConnectionFactory redisConnectionFactory) {
        return new StringRedisTemplate(redisConnectionFactory);
    }
}
