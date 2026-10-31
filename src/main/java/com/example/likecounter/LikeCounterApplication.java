package com.example.likecounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Map;

@SpringBootApplication
@EnableScheduling
public class LikeCounterApplication {

    private static final Logger log = LoggerFactory.getLogger(LikeCounterApplication.class);

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(LikeCounterApplication.class);
        // Set here (rather than application.yml) so health/shutdown config stays visible in code.
        app.setDefaultProperties(Map.of(
                "management.endpoints.web.exposure.include", "health,info",
                "management.endpoint.health.show-details", "always",
                "management.health.redis.enabled", "true",
                "server.shutdown", "graceful",
                "spring.lifecycle.timeout-per-shutdown-phase", "20s"));
        ConfigurableApplicationContext context = app.run(args);
        Environment env = context.getEnvironment();
        log.info("{} started on port={} activeProfiles={}",
                env.getProperty("spring.application.name", "like-counter"),
                env.getProperty("server.port", "8080"),
                String.join(",", env.getActiveProfiles()));
    }
}
