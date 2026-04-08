package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.model.GmailUserCredential;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class BackendApiClient {

    private static final Logger log = LoggerFactory.getLogger(BackendApiClient.class);

    private final WebClient    webClient;
    private final ObjectMapper mapper;

    public BackendApiClient(
            @Value("${backend.base-url}") String baseUrl,
            @Value("${backend.service-key}") String serviceKey) {

        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader("X-Service-Key", serviceKey)
                .defaultHeader("Content-Type", "application/json")
                .build();

        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Returns all users who have connected Gmail, each with their registered cards.
     * GET /internal/gmail-credentials
     */
    public List<GmailUserCredential> getAllGmailCredentials() {
        return webClient.get()
                .uri("/internal/gmail-credentials")
                .retrieve()
                .bodyToFlux(GmailUserCredential.class)
                .collectList()
                .block();
    }

    /**
     * Posts an extracted statement to the existing backend.
     * POST /internal/statements — 201 on success, 409 on duplicate.
     */
    public void postStatement(String userId,
                               String gmailMessageId,
                               String cardLastFour,
                               String bankKey,
                               BigDecimal statementBalance,
                               BigDecimal minimumDue,
                               LocalDate  statementDate,
                               LocalDate  dueDate,
                               String     viewStatementUrl,
                               String     makePaymentUrl) {
        try {
            // Backend uses SNAKE_CASE Jackson strategy — Map keys must be snake_case
            // because Jackson does NOT transform Map keys, only POJO field names.
            Map<String, Object> body = new HashMap<>();
            body.put("user_id",              userId);
            body.put("gmail_message_id",     gmailMessageId);
            body.put("card_last_four",       cardLastFour);
            body.put("bank",                 bankKey);
            body.put("statement_balance",    statementBalance);
            body.put("minimum_payment_due",  minimumDue);
            body.put("statement_date",       statementDate != null ? statementDate.toString() : null);
            body.put("due_date",             dueDate != null ? dueDate.toString() : null);
            body.put("view_statement_url",   viewStatementUrl);
            body.put("make_payment_url",     makePaymentUrl);

            webClient.post()
                    .uri("/internal/statements")
                    .bodyValue(body)
                    .retrieve()
                    .toBodilessEntity()
                    .block();

            log.info("Statement saved: {} for user {}", gmailMessageId, userId);

        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("409")) {
                log.debug("Duplicate statement skipped: {}", gmailMessageId);
            } else {
                log.error("Failed to save statement {}: {}", gmailMessageId, e.getMessage());
            }
        }
    }

    /**
     * Asks the backend to refresh the expired access token using the stored refresh token.
     * POST /internal/gmail-credentials/{userId}/refresh
     * Returns the new access token, or null if refresh failed.
     */
    public String refreshAccessToken(String userId) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> resp = webClient.post()
                    .uri("/internal/gmail-credentials/{userId}/refresh", userId)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block();
            if (resp != null && resp.containsKey("access_token")) {
                String newToken = (String) resp.get("access_token");
                log.info("Access token refreshed for user {}", userId);
                return newToken;
            }
        } catch (Exception e) {
            log.error("Failed to refresh access token for user {}: {}", userId, e.getMessage());
        }
        return null;
    }

    /**
     * Updates the Gmail historyId cursor after a successful poll cycle.
     * PATCH /internal/gmail-credentials/{userId}
     */
    public void updateHistoryId(String userId, Long historyId) {
        try {
            Map<String, Object> body = new HashMap<>();
            // Backend uses SNAKE_CASE naming strategy — keys must match field names in snake_case
            body.put("history_id",    String.valueOf(historyId));
            body.put("last_synced_at", java.time.LocalDateTime.now().toString());

            webClient.patch()
                    .uri("/internal/gmail-credentials/{userId}", userId)
                    .bodyValue(body)
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            log.info("historyId updated for user {}: {}", userId, historyId);
        } catch (Exception e) {
            log.error("Failed to update historyId for user {}: {}", userId, e.getMessage());
        }
    }
}
