package com.credtrack.ai_agent.service;

public interface LlmClient {

    String providerName();

    boolean isAvailable();

    String generate(String prompt);

    default String modelName() {
        return "unknown";
    }

    default String availabilityDetails() {
        return isAvailable() ? "configured" : "not-configured";
    }
}
