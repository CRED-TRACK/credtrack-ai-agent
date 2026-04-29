package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.config.LlmProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class LlmGateway {

    private static final Logger log = LoggerFactory.getLogger(LlmGateway.class);

    private final Map<String, LlmClient> clientsByProvider;
    private final LlmProperties properties;

    public LlmGateway(List<LlmClient> clients, LlmProperties properties) {
        this.clientsByProvider = clients.stream()
                .collect(Collectors.toMap(client -> normalize(client.providerName()), client -> client));
        this.properties = properties;
    }

    public String generate(String prompt, String operationName) {
        List<String> providers = orderedProviders();
        List<String> failures = new ArrayList<>();
        int promptChars = prompt == null ? 0 : prompt.length();

        log.info("llm_event=start operation={} providers={} prompt_chars={}",
                operationName, providers, promptChars);

        int attempt = 0;
        for (String provider : providers) {
            attempt++;
            LlmClient client = clientsByProvider.get(provider);
            if (client == null) {
                String reason = "provider-not-registered";
                log.warn("llm_event=skip operation={} attempt={} provider={} reason={}",
                        operationName, attempt, provider, reason);
                failures.add(provider + ": " + reason);
                continue;
            }
            if (!client.isAvailable()) {
                String reason = client.availabilityDetails();
                log.warn("llm_event=skip operation={} attempt={} provider={} model={} reason={}",
                        operationName, attempt, provider, client.modelName(), reason);
                failures.add(provider + ": " + reason);
                continue;
            }

            long startedAt = System.nanoTime();
            try {
                log.info("llm_event=attempt operation={} attempt={} provider={} model={} prompt_chars={}",
                        operationName, attempt, provider, client.modelName(), promptChars);
                String response = client.generate(prompt);
                long durationMs = elapsedMs(startedAt);
                int responseChars = response == null ? 0 : response.length();
                log.info("llm_event=success operation={} attempt={} provider={} model={} duration_ms={} response_chars={}",
                        operationName, attempt, provider, client.modelName(), durationMs, responseChars);
                return response;
            } catch (Exception e) {
                long durationMs = elapsedMs(startedAt);
                String error = summarizeError(e);
                log.warn("llm_event=failure operation={} attempt={} provider={} model={} duration_ms={} error={}",
                        operationName, attempt, provider, client.modelName(), durationMs, error);
                failures.add(provider + ": " + error);
            }
        }

        log.error("llm_event=exhausted operation={} providers={} failures={}",
                operationName, providers, failures);
        throw new IllegalStateException("No LLM provider succeeded for " + operationName + ". Attempts: " + failures);
    }

    public List<String> orderedProvidersForLogging() {
        return orderedProviders();
    }

    private List<String> orderedProviders() {
        LinkedHashSet<String> providers = new LinkedHashSet<>();
        addProvider(providers, properties.getPrimaryProvider());
        addProvider(providers, properties.getFallbackProvider());
        if (providers.isEmpty()) {
            addProvider(providers, "ollama");
        }
        return new ArrayList<>(providers);
    }

    private void addProvider(LinkedHashSet<String> providers, String provider) {
        String normalized = normalize(provider);
        if (!normalized.equals("none")) {
            providers.add(normalized);
        }
    }

    private String normalize(String provider) {
        return provider == null ? "none" : provider.trim().toLowerCase(Locale.ROOT);
    }

    private long elapsedMs(long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000L;
    }

    private String summarizeError(Exception e) {
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            message = e.getClass().getSimpleName();
        }
        return message.replaceAll("\\s+", " ").trim();
    }
}
