package com.credtrack.ai_agent.controller;

import com.credtrack.ai_agent.service.LlmGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Internal LLM endpoints used by the backend's chat advisor:
 *   POST /internal/embed  → { text }                 → { embedding: [float, …], dim }
 *   POST /internal/chat   → { prompt, max_tokens? }  → { answer }
 *
 * Auth: nothing in ai-agent currently has a service-key interceptor — backend calls
 * are reachable only via the private VPC. If we ever expose ai-agent publicly, add
 * a shared-secret guard here.
 */
@RestController
@RequestMapping("/internal")
public class LlmController {

    private static final Logger log = LoggerFactory.getLogger(LlmController.class);

    private final LlmGateway llmGateway;

    public LlmController(LlmGateway llmGateway) {
        this.llmGateway = llmGateway;
    }

    public record EmbedRequest(String text) {}
    public record EmbedResponse(List<Float> embedding, int dim) {}

    @PostMapping("/embed")
    public ResponseEntity<EmbedResponse> embed(@RequestBody EmbedRequest req) {
        if (req == null || req.text() == null || req.text().isBlank()) {
            return ResponseEntity.badRequest().body(new EmbedResponse(List.of(), 0));
        }
        float[] vec = llmGateway.embed(req.text(), "chat-query-embed");
        List<Float> out = new java.util.ArrayList<>(vec.length);
        for (float f : vec) out.add(f);
        log.info("embed_event=served chars={} dim={}", req.text().length(), vec.length);
        return ResponseEntity.ok(new EmbedResponse(out, vec.length));
    }

    public record ChatRequest(String prompt, Integer maxTokens) {}
    public record ChatResponse(String answer) {}

    @PostMapping("/chat")
    public ResponseEntity<ChatResponse> chat(@RequestBody ChatRequest req) {
        if (req == null || req.prompt() == null || req.prompt().isBlank()) {
            return ResponseEntity.badRequest().body(new ChatResponse(""));
        }
        String answer = llmGateway.generate(req.prompt(), "chat-advisor");
        log.info("chat_event=served prompt_chars={} answer_chars={}",
                req.prompt().length(), answer == null ? 0 : answer.length());
        return ResponseEntity.ok(new ChatResponse(answer == null ? "" : answer));
    }
}
