package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.config.LlmProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
public class GeminiLlmClient implements LlmClient {

    private static final Logger log = LoggerFactory.getLogger(GeminiLlmClient.class);

    private final LlmProperties properties;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public GeminiLlmClient(LlmProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.webClient = WebClient.builder()
                .baseUrl(properties.getGemini().getBaseUrl())
                .build();
    }

    @Override
    public String providerName() {
        return "gemini";
    }

    @Override
    public boolean isAvailable() {
        String apiKey = properties.getGemini().getApiKey();
        return apiKey != null && !apiKey.isBlank();
    }

    @Override
    public String modelName() {
        return properties.getGemini().getModel();
    }

    @Override
    public String availabilityDetails() {
        return isAvailable() ? "configured" : "missing-api-key";
    }

    @Override
    public String generate(String prompt) {
        if (!isAvailable()) {
            throw new IllegalStateException("Gemini API key is not configured");
        }

        GeminiRequest request = new GeminiRequest(
                new Content[]{new Content(new Part[]{new Part(prompt)})},
                new GenerationConfig(
                        "application/json",
                        properties.getGemini().getTemperature(),
                        properties.getGemini().getMaxOutputTokens()
                )
        );

        String responseBody = webClient.post()
                .uri("/v1beta/models/{model}:generateContent", properties.getGemini().getModel())
                .header("x-goog-api-key", properties.getGemini().getApiKey())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .onStatus(HttpStatusCode::isError, response ->
                        response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> Mono.error(new IllegalStateException(
                                        "Gemini API error " + response.statusCode().value() + ": " + body))))
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(properties.getGemini().getTimeoutSeconds()));

        if (responseBody == null || responseBody.isBlank()) {
            throw new IllegalStateException("Gemini returned an empty response");
        }

        try {
            JsonNode root = objectMapper.readTree(responseBody);
            JsonNode candidate = root.path("candidates").path(0);
            JsonNode textNode = candidate.path("content").path("parts").path(0).path("text");
            if (textNode.isTextual() && !textNode.asText().isBlank()) {
                return textNode.asText();
            }

            JsonNode promptFeedback = root.path("promptFeedback");
            if (!promptFeedback.isMissingNode() && !promptFeedback.isEmpty()) {
                throw new IllegalStateException("Gemini blocked the request: " + promptFeedback);
            }

            throw new IllegalStateException("Gemini response did not contain text: " + responseBody);
        } catch (Exception e) {
            log.error("llm_provider=gemini event=parse_failure model={} error={}",
                    properties.getGemini().getModel(),
                    summarize(e.getMessage()));
            throw new IllegalStateException("Failed to parse Gemini response", e);
        }
    }

    private String summarize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.replaceAll("\\s+", " ").trim();
    }

    private record GeminiRequest(Content[] contents, GenerationConfig generationConfig) {}

    private record Content(Part[] parts) {}

    private record Part(String text) {}

    private record GenerationConfig(String responseMimeType, double temperature, int maxOutputTokens) {}
}
