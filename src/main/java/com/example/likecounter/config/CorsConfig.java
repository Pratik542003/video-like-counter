package com.example.likecounter.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Arrays;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

    private static final Logger log = LoggerFactory.getLogger(CorsConfig.class);

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        String[] allowedOrigins = System.getProperty("cors.allowed-origins",
                System.getenv().getOrDefault("CORS_ALLOWED_ORIGINS", "http://localhost:5173")).split(",");
        log.info("Registering CORS mapping for /api/** allowedOrigins={}", Arrays.toString(allowedOrigins));
        registry.addMapping("/api/**")
                .allowedOriginPatterns(allowedOrigins)
                .allowedMethods("GET", "POST", "DELETE")
                .allowedHeaders("*");
    }
}
