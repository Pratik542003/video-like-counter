package com.example.likecounter.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI videoLikeCounterOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Video Like-Counter API")
                        .description("Phase 1: atomic per-video like counter, no auth/users.")
                        .version("v1"));
    }
}
