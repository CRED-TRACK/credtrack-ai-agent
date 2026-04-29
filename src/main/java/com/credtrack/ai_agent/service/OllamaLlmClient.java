package com.credtrack.ai_agent.service;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class OllamaLlmClient implements LlmClient {

    private final ChatClient chatClient;
    private final String modelName;

    public OllamaLlmClient(ObjectProvider<ChatClient.Builder> chatClientBuilderProvider,
                           @Value("${spring.ai.ollama.chat.options.model:unknown}") String modelName) {
        ChatClient.Builder builder = chatClientBuilderProvider.getIfAvailable();
        this.chatClient = builder != null ? builder.build() : null;
        this.modelName = modelName;
    }

    @Override
    public String providerName() {
        return "ollama";
    }

    @Override
    public boolean isAvailable() {
        return chatClient != null;
    }

    @Override
    public String modelName() {
        return modelName;
    }

    @Override
    public String availabilityDetails() {
        return chatClient != null ? "configured" : "chat-client-unavailable";
    }

    @Override
    public String generate(String prompt) {
        if (chatClient == null) {
            throw new IllegalStateException("Ollama ChatClient is not configured");
        }
        return chatClient.prompt()
                .user(prompt)
                .call()
                .content();
    }
}
