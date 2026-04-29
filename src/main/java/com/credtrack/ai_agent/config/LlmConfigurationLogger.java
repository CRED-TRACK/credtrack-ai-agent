package com.credtrack.ai_agent.config;

import com.credtrack.ai_agent.service.LlmGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LlmConfigurationLogger {

    private static final Logger log = LoggerFactory.getLogger(LlmConfigurationLogger.class);

    @Bean
    ApplicationRunner logLlmConfiguration(LlmProperties properties,
                                          LlmGateway llmGateway,
                                          @Value("${spring.ai.ollama.chat.options.model:unknown}") String ollamaModel) {
        return args -> log.info(
                "llm_config primary_provider={} fallback_provider={} ordered_providers={} gemini_configured={} gemini_model={} ollama_model={}",
                properties.getPrimaryProvider(),
                properties.getFallbackProvider(),
                llmGateway.orderedProvidersForLogging(),
                hasText(properties.getGemini().getApiKey()),
                properties.getGemini().getModel(),
                ollamaModel
        );
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
