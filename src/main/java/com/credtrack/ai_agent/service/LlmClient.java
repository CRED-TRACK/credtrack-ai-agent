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

    /** Embedding vector for the given text. Default = unsupported. */
    default float[] embed(String text) {
        throw new UnsupportedOperationException(providerName() + " does not support embeddings");
    }

    default boolean supportsEmbeddings() { return false; }

    default int embeddingDim() { return 0; }

    default String embedModelName() { return "unsupported"; }
}
