package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.model.GmailUserCredential;
import com.credtrack.ai_agent.model.UtilityAccountInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
                .clientConnector(new org.springframework.http.client.reactive.ReactorClientHttpConnector(
                        reactor.netty.http.client.HttpClient.create()
                                .responseTimeout(Duration.ofSeconds(30))))
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
     * Posts an extracted statement to the backend.
     * POST /internal/statements — 201 on success, 409 on duplicate.
     * Returns true if newly saved, false if duplicate.
     */
    public boolean postStatement(String userId,
                                 String gmailMessageId,
                                 String cardLastFour,
                                 String bankKey,
                                 BigDecimal statementBalance,
                                 BigDecimal minimumDue,
                                 LocalDate  statementDate,
                                 LocalDate  dueDate,
                                 String     viewStatementUrl,
                                 String     makePaymentUrl) {
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

        try {
            webClient.post()
                    .uri("/internal/statements")
                    .bodyValue(body)
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            log.info("Statement saved: {} for user {}", gmailMessageId, userId);
            return true;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("409")) {
                log.debug("Duplicate statement skipped: {}", gmailMessageId);
                return false;
            }
            throw new RuntimeException("Failed to save statement " + gmailMessageId + ": " + e.getMessage(), e);
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
     * Marks a card's one-time init scan as complete so it is never re-scanned.
     * POST /internal/cards/{cardId}/init-complete
     */
    public void markInitComplete(Long cardId) {
        try {
            webClient.post()
                    .uri("/internal/cards/{cardId}/init-complete", cardId)
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            log.info("Init scan marked complete for card {}", cardId);
        } catch (Exception e) {
            log.error("Failed to mark init-complete for card {}: {}", cardId, e.getMessage());
        }
    }

    /**
     * Updates a card's lastFour — called when a bank displays more digits than stored
     * (e.g. Amex shows "51006" but user registered "1006").
     * PATCH /internal/cards/{cardId}/last-four
     */
    public void updateCardLastFour(Long cardId, String lastFour) {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("last_four", lastFour);
            webClient.patch()
                    .uri("/internal/cards/{cardId}/last-four", cardId)
                    .bodyValue(body)
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            log.info("Card {} lastFour updated to {}", cardId, lastFour);
        } catch (Exception e) {
            log.error("Failed to update lastFour for card {}: {}", cardId, e.getMessage());
        }
    }

    /**
     * Posts an extracted Chase payment confirmation to the backend.
     * POST /internal/payments — 204 on success, 409 on duplicate gmailMessageId.
     */
    public void postPayment(String userId,
                             String gmailMessageId,
                             String cardLastFour,
                             String bank,
                             BigDecimal amount,
                             LocalDate  paymentDate,
                             LocalDate  effectiveDate) {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("user_id",          userId);
            body.put("gmail_message_id", gmailMessageId);
            body.put("card_last_four",   cardLastFour);
            body.put("bank",             bank);
            body.put("amount",           amount);
            body.put("payment_date",     paymentDate  != null ? paymentDate.toString()  : null);
            body.put("effective_date",   effectiveDate != null ? effectiveDate.toString() : null);

            webClient.post()
                    .uri("/internal/payments")
                    .bodyValue(body)
                    .retrieve()
                    .toBodilessEntity()
                    .block();

            log.info("Payment saved: {} for user {} amount={}", gmailMessageId, userId, amount);

        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("409")) {
                log.debug("Duplicate payment skipped: {}", gmailMessageId);
            } else {
                log.error("Failed to save payment {}: {}", gmailMessageId, e.getMessage());
            }
        }
    }

    /**
     * Posts an extracted transaction to the backend.
     * POST /internal/transactions — 201 on success, 409 on duplicate gmailMessageId.
     */
    public void postTransaction(String userId,
                                String gmailMessageId,
                                String cardLastFour,
                                String bankKey,
                                String merchantName,
                                String merchantCategory,
                                BigDecimal amount,
                                String currency,
                                LocalDate transactionDate,
                                String transactionType,
                                String description,
                                double confidence) {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("user_id",          userId);
            body.put("gmail_message_id", gmailMessageId);
            body.put("card_last_four",   cardLastFour);
            body.put("bank_key",         bankKey);
            body.put("merchant_name",    merchantName);
            body.put("merchant_category", merchantCategory);
            body.put("amount",           amount);
            body.put("currency",         currency != null ? currency : "USD");
            body.put("transaction_date", transactionDate != null ? transactionDate.toString() : null);
            body.put("transaction_type", transactionType);
            body.put("status",           "PENDING");
            body.put("description",      description);
            body.put("llm_confidence",   String.valueOf(confidence));

            webClient.post()
                    .uri("/internal/transactions")
                    .bodyValue(body)
                    .retrieve()
                    .toBodilessEntity()
                    .block();

            log.info("Transaction saved: {} for user {} amount={}", gmailMessageId, userId, amount);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("409")) {
                log.debug("Duplicate transaction skipped: {}", gmailMessageId);
            } else {
                log.error("Failed to save transaction {}: {}", gmailMessageId, e.getMessage());
            }
        }
    }

    /**
     * Records the current timestamp as the last transaction scan time for a card.
     * PATCH /internal/cards/{cardId}/transaction-scan
     * The coordinator uses this timestamp to decide whether to run a scan again
     * (controlled by TRANSACTION_SCAN_INTERVAL_MINUTES env var, default 1440 = 1 day).
     */
    public void updateLastTransactionScanAt(Long cardId) {
        try {
            webClient.patch()
                    .uri("/internal/cards/{cardId}/transaction-scan", cardId)
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            log.info("Transaction scan timestamp updated for card {}", cardId);
        } catch (Exception e) {
            log.error("Failed to update transaction scan timestamp for card {}: {}", cardId, e.getMessage());
        }
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
            body.put("last_synced_at", LocalDateTime.now().toString());

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

    /**
     * Returns all registered utility accounts across all users.
     * GET /internal/utility-accounts
     */
    public List<UtilityAccountInfo> getUtilityAccounts() {
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> raw = (List<Map<String, Object>>) (List<?>) webClient.get()
                    .uri("/internal/utility-accounts")
                    .retrieve()
                    .bodyToFlux(Map.class)
                    .collectList()
                    .block();
            if (raw == null) return List.of();
            return raw.stream()
                    .map(m -> {
                        Long id = m.get("id") instanceof Number n ? n.longValue() : null;
                        boolean initDone = Boolean.TRUE.equals(m.get("utilityInitComplete"));
                        return new UtilityAccountInfo(
                                id,
                                (String) m.get("userId"),
                                (String) m.get("billerName"),
                                (String) m.get("accountLastFour"),
                                initDone);
                    })
                    .toList();
        } catch (Exception e) {
            log.error("getUtilityAccounts failed: {}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Marks a utility account's one-time historical init scan as complete.
     * POST /internal/utility-accounts/{accountId}/init-complete
     */
    public void markUtilityAccountInitComplete(Long accountId) {
        try {
            webClient.post()
                    .uri("/internal/utility-accounts/" + accountId + "/init-complete")
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            log.info("Utility account {} marked init-complete", accountId);
        } catch (Exception e) {
            log.error("markUtilityAccountInitComplete failed for {}: {}", accountId, e.getMessage());
        }
    }

    /**
     * Posts an extracted utility bill to the backend.
     * POST /internal/utility-bills — 201 on success, 409 on duplicate.
     * Returns true if newly saved, false if duplicate.
     */
    public boolean postUtilityBill(String userId,
                                    String gmailMessageId,
                                    String billerName,
                                    String accountLastFour,
                                    BigDecimal amountDue,
                                    LocalDate dueDate,
                                    LocalDate billDate,
                                    LocalDate billingPeriodStart,
                                    LocalDate billingPeriodEnd) {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("user_id",               userId);
            body.put("gmail_message_id",      gmailMessageId);
            body.put("biller_name",           billerName);
            body.put("account_last_four",     accountLastFour);
            body.put("amount_due",            amountDue);
            body.put("due_date",              dueDate != null ? dueDate.toString() : null);
            body.put("bill_date",             billDate != null ? billDate.toString() : null);
            body.put("billing_period_start",  billingPeriodStart != null ? billingPeriodStart.toString() : null);
            body.put("billing_period_end",    billingPeriodEnd != null ? billingPeriodEnd.toString() : null);

            webClient.post().uri("/internal/utility-bills").bodyValue(body)
                    .retrieve().toBodilessEntity().block();
            log.info("Utility bill saved: {} for user {} biller={}", gmailMessageId, userId, billerName);
            return true;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("409")) {
                log.debug("Duplicate utility bill skipped: {}", gmailMessageId);
                return false;
            }
            throw new RuntimeException("Failed to save utility bill " + gmailMessageId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Posts an extracted utility payment to the backend.
     * POST /internal/utility-payments — 204 on success, 409 on duplicate.
     */
    public void postUtilityPayment(String userId,
                                    String gmailMessageId,
                                    String billerName,
                                    String accountLastFour,
                                    BigDecimal paymentAmount,
                                    LocalDate paymentDate) {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("user_id",           userId);
            body.put("gmail_message_id",  gmailMessageId);
            body.put("biller_name",       billerName);
            body.put("account_last_four", accountLastFour);
            body.put("payment_amount",    paymentAmount);
            body.put("payment_date",      paymentDate != null ? paymentDate.toString() : null);

            webClient.post().uri("/internal/utility-payments").bodyValue(body)
                    .retrieve().toBodilessEntity().block();
            log.info("Utility payment saved: {} for user {} biller={} amount={}",
                    gmailMessageId, userId, billerName, paymentAmount);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("409")) {
                log.debug("Duplicate utility payment skipped: {}", gmailMessageId);
            } else {
                log.error("Failed to save utility payment {}: {}", gmailMessageId, e.getMessage());
            }
        }
    }

    // ── Raw analytics data records ─────────────────────────────────────────────

    public record RawMonthCardRow(Long cardId, String bankKey, String lastFour, double amount) {}

    public record RawMonthData(String month, List<RawMonthCardRow> cards) {}

    /** Statement-based monthly data — replaces the old transaction-aggregated flat structure. */
    public record RawCardStatementData(List<RawMonthData> monthlyData) {}

    public record RawBillRow(String billerName, String accountLastFour,
                              double amountDue, String billDate) {}

    public record RawUtilityData(List<RawBillRow> bills) {}

    /**
     * Fetches statement-based monthly card data for the analytics actor.
     * GET /internal/analytics/card-data?userId=X&months=6
     * Backend groups CardStatement rows by YYYY-MM, applying COALESCE(statementBalance, paidAmount).
     * Returns snake_case JSON; fields are extracted by key name.
     */
    @SuppressWarnings("unchecked")
    public RawCardStatementData getRawCardData(String userId, int months) {
        Map<String, Object> raw = webClient.get()
                .uri("/internal/analytics/card-data?userId={uid}&months={m}", userId, months)
                .retrieve()
                .bodyToMono(Map.class)
                .block();
        if (raw == null) return new RawCardStatementData(List.of());

        List<Map<String, Object>> monthsRaw =
                (List<Map<String, Object>>) raw.getOrDefault("monthly_data", List.of());

        List<RawMonthData> monthlyData = monthsRaw.stream().map(m -> {
            String month = (String) m.get("month");
            List<Map<String, Object>> cardsRaw =
                    (List<Map<String, Object>>) m.getOrDefault("cards", List.of());
            List<RawMonthCardRow> cards = cardsRaw.stream().map(c -> new RawMonthCardRow(
                    ((Number) c.get("card_id")).longValue(),
                    (String) c.get("bank_key"),
                    (String) c.get("last_four"),
                    ((Number) c.get("amount")).doubleValue()
            )).toList();
            return new RawMonthData(month, cards);
        }).toList();

        return new RawCardStatementData(monthlyData);
    }

    /**
     * Fetches raw utility bill history for the analytics actor.
     * GET /internal/analytics/utility-data?userId=X
     */
    @SuppressWarnings("unchecked")
    public RawUtilityData getRawUtilityData(String userId) {
        Map<String, Object> raw = webClient.get()
                .uri("/internal/analytics/utility-data?userId={uid}", userId)
                .retrieve()
                .bodyToMono(Map.class)
                .block();
        if (raw == null) return new RawUtilityData(List.of());

        List<Map<String, Object>> billsRaw = (List<Map<String, Object>>) raw.getOrDefault("bills", List.of());
        List<RawBillRow> bills = billsRaw.stream().map(m -> new RawBillRow(
                (String) m.get("biller_name"),
                (String) m.get("account_last_four"),
                m.get("amount_due") instanceof Number n ? n.doubleValue() : 0.0,
                (String) m.get("bill_date")
        )).toList();

        return new RawUtilityData(bills);
    }

    /**
     * Returns the sum of all unbilled transactions for a card since the last statement date.
     * GET /statements/unbilled?cardId={id}
     */
    public double getUnbilledTotal(Long userCardId) {
        try {
            Map<?, ?> response = webClient.get()
                    .uri("/statements/unbilled?cardId=" + userCardId)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block();
            if (response != null && response.get("unbilledTotal") instanceof Number n) {
                return n.doubleValue();
            }
        } catch (Exception e) {
            log.warn("getUnbilledTotal failed for card {}: {}", userCardId, e.getMessage());
        }
        return 0.0;
    }

    /**
     * GET /internal/card-products-to-scrape — returns card_products with terms_url
     * and ≥1 active user_card. Each row includes the current document's content hash
     * so the scraper can short-circuit unchanged pages.
     */
    public java.util.List<com.credtrack.ai_agent.model.CardProductToScrape> getCardProductsToScrape() {
        return webClient.get()
                .uri("/internal/card-products-to-scrape")
                .retrieve()
                .bodyToFlux(com.credtrack.ai_agent.model.CardProductToScrape.class)
                .collectList()
                .block();
    }

    /**
     * POST /internal/card-terms-documents — records the scraped HTML doc + hash.
     * Returns the document id (used as source_document_id on rule rows) and a flag
     * indicating whether the hash matched the existing current doc (idempotent skip).
     */
    public DocumentSaveResult postCardTermsDocument(Long cardProductId,
                                                    String sourceUrl,
                                                    String contentHash,
                                                    String cleanedText,
                                                    Integer httpStatus,
                                                    String extractorModel,
                                                    Integer extractedRulesCount) {
        Map<String, Object> body = new HashMap<>();
        body.put("card_product_id", cardProductId);
        body.put("source_url", sourceUrl);
        body.put("content_hash", contentHash);
        body.put("cleaned_text", cleanedText);
        body.put("http_status", httpStatus);
        body.put("extractor_model", extractorModel);
        body.put("extracted_rules_count", extractedRulesCount);
        try {
            return webClient.post()
                    .uri("/internal/card-terms-documents")
                    .bodyValue(body)
                    .retrieve()
                    .bodyToMono(DocumentSaveResult.class)
                    .block();
        } catch (Exception e) {
            log.error("postCardTermsDocument failed product_id={} hash={} error={}",
                    cardProductId, contentHash, e.getMessage());
            return null;
        }
    }

    /** Result of POST /internal/card-terms-documents. */
    public record DocumentSaveResult(
            @com.fasterxml.jackson.annotation.JsonProperty("id") Long id,
            @com.fasterxml.jackson.annotation.JsonProperty("duplicate_of_current") boolean duplicateOfCurrent
    ) {}

    /**
     * POST /internal/card-reward-rules — replace-all for LLM_SCRAPED rules
     * of one card_product. SEED + USER_OVERRIDE rules untouched server-side.
     */
    public Map<String, Object> postRewardRules(Long cardProductId,
                                               Long documentId,
                                               Float minConfidence,
                                               java.util.List<Map<String, Object>> rules) {
        Map<String, Object> body = new HashMap<>();
        body.put("card_product_id", cardProductId);
        body.put("document_id", documentId);
        body.put("min_confidence", minConfidence);
        body.put("rules", rules);
        try {
            return webClient.post()
                    .uri("/internal/card-reward-rules")
                    .bodyValue(body)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .map(m -> (Map<String, Object>) m)
                    .block();
        } catch (Exception e) {
            log.error("postRewardRules failed product_id={} error={}",
                    cardProductId, e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }
}
