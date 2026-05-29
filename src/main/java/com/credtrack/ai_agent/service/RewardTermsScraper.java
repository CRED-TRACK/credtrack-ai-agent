package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.config.LlmProperties;
import com.credtrack.ai_agent.model.CardProductToScrape;
import com.credtrack.ai_agent.model.ScrapedTermsResult;
import com.credtrack.ai_agent.model.ScrapedTermsResult.ExtractedRule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Synchronous pipeline that processes one card_product:
 * fetch → Jsoup clean → hash → (skip if unchanged) → LLM extract rules → chunk → embed.
 * No backend writeback yet — returns ScrapedTermsResult for inspection via trigger endpoint.
 */
@Service
public class RewardTermsScraper {

    private static final Logger log = LoggerFactory.getLogger(RewardTermsScraper.class);

    private static final String CANONICAL_CATEGORIES = String.join(", ",
        "GROCERIES_SUPERMARKETS", "WAREHOUSE_CLUB", "DINING_RESTAURANTS", "FAST_FOOD",
        "GAS_STATIONS", "EV_CHARGING", "ONLINE_RETAIL", "DRUGSTORES",
        "TRAVEL_GENERAL", "TRAVEL_PORTAL", "HOTELS", "AIRLINES", "RIDESHARE",
        "STREAMING", "UTILITIES", "TRANSIT", "ENTERTAINMENT",
        "HOME_IMPROVEMENT", "DEPARTMENT_STORES", "OTHER");

    private final LlmGateway llmGateway;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final BackendApiClient backendApiClient;
    private final LlmProperties llmProperties;
    private final float minConfidence;

    public RewardTermsScraper(LlmGateway llmGateway,
                              ObjectMapper objectMapper,
                              BackendApiClient backendApiClient,
                              LlmProperties llmProperties,
                              @Value("${rewards.scrape.min-confidence:0.6}") float minConfidence) {
        this.llmGateway = llmGateway;
        this.objectMapper = objectMapper;
        this.backendApiClient = backendApiClient;
        this.llmProperties = llmProperties;
        this.minConfidence = minConfidence;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public ScrapedTermsResult scrape(CardProductToScrape card) {
        log.info("scrape_event=start card_product_id={} bank={} product={} url={}",
                card.cardProductId(), card.bankKey(), card.productName(), card.termsUrl());

        // 1. Fetch
        String rawHtml;
        int status;
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(card.termsUrl()))
                    .header("User-Agent", "CredTrackBot/1.0 (+contact@credtrack.app)")
                    .header("Accept", "text/html,application/xhtml+xml")
                    .GET()
                    .timeout(Duration.ofSeconds(20))
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            status = resp.statusCode();
            rawHtml = resp.body();
            if (status >= 400) {
                log.warn("scrape_event=fetch_failed card_product_id={} status={}", card.cardProductId(), status);
                return errorResult(card, status, "HTTP " + status);
            }
        } catch (IOException | InterruptedException e) {
            log.warn("scrape_event=fetch_exception card_product_id={} error={}",
                    card.cardProductId(), e.getMessage());
            return errorResult(card, 0, "fetch exception: " + e.getMessage());
        }

        // 2. Jsoup clean (strip nav/script/style)
        Document doc = Jsoup.parse(rawHtml);
        doc.select("script, style, noscript, nav, footer, header, aside").remove();
        String cleaned = doc.text().replaceAll("\\s+", " ").trim();
        if (cleaned.isEmpty()) {
            return errorResult(card, status, "cleaned text empty");
        }
        // Thin-content guard — page likely JS-rendered shell (Amex/Cloudflare). Bail before
        // calling LLM. Document is NOT written so we don't pollute audit trail with shells.
        if (cleaned.length() < 500) {
            log.warn("scrape_event=thin_content card_product_id={} cleaned_chars={} url={}",
                    card.cardProductId(), cleaned.length(), card.termsUrl());
            return errorResult(card, status,
                    "thin_content (" + cleaned.length() + " chars) — page likely JS-rendered. Needs headless browser.");
        }

        // 3. Hash (sha256) for idempotency
        String hash = sha256(cleaned);
        if (hash.equals(card.lastContentHash())) {
            log.info("scrape_event=unchanged card_product_id={} hash={}", card.cardProductId(), hash);
            return ScrapedTermsResult.builder()
                    .cardProductId(card.cardProductId())
                    .bankKey(card.bankKey())
                    .productName(card.productName())
                    .sourceUrl(card.termsUrl())
                    .httpStatus(status)
                    .contentHash(hash)
                    .unchanged(true)
                    .cleanedTextChars(cleaned.length())
                    .chunkCount(0)
                    .embeddingDim(0)
                    .rules(List.of())
                    .build();
        }

        // 4. LLM extract rules (strict JSON)
        List<ExtractedRule> rules = extractRules(card, cleaned);

        // 5. Persist via backend internal endpoints. Document first so we have its id
        //    to attach as source_document_id on each rule.
        Long documentId = null;
        try {
            var docResp = backendApiClient.postCardTermsDocument(
                    card.cardProductId(),
                    card.termsUrl(),
                    hash,
                    truncateForStorage(cleaned),
                    status,
                    llmProperties.getGemini().getModel(),
                    rules.size());
            if (docResp != null) documentId = docResp.id();
        } catch (Exception e) {
            log.warn("scrape_event=document_post_failed card_product_id={} error={}",
                    card.cardProductId(), e.getMessage());
        }

        Map<String, Object> upsertResult = Map.of();
        if (!rules.isEmpty()) {
            try {
                upsertResult = backendApiClient.postRewardRules(
                        card.cardProductId(),
                        documentId,
                        minConfidence,
                        rules.stream().map(this::ruleToMap).toList());
            } catch (Exception e) {
                log.warn("scrape_event=rules_post_failed card_product_id={} error={}",
                        card.cardProductId(), e.getMessage());
            }
        }

        log.info("scrape_event=done card_product_id={} status={} cleaned_chars={} rules={} document_id={} upsert={}",
                card.cardProductId(), status, cleaned.length(), rules.size(), documentId, upsertResult);

        return ScrapedTermsResult.builder()
                .cardProductId(card.cardProductId())
                .bankKey(card.bankKey())
                .productName(card.productName())
                .sourceUrl(card.termsUrl())
                .httpStatus(status)
                .contentHash(hash)
                .unchanged(false)
                .cleanedTextChars(cleaned.length())
                .chunkCount(0)
                .embeddingDim(0)
                .rules(rules)
                .build();
    }

    private Map<String, Object> ruleToMap(ExtractedRule r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("canonical_category", r.getCanonicalCategory());
        m.put("rate_bps", r.getRateBps());
        m.put("base_rate_bps", r.getBaseRateBps());
        m.put("cap_period", r.getCapPeriod());
        m.put("cap_amount", r.getCapAmount());
        m.put("cap_group_key", r.getCapGroupKey());
        m.put("requires_user_choice", r.getRequiresUserChoice());
        m.put("channel_restriction", r.getChannelRestriction());
        m.put("exclusions", r.getExclusions());
        m.put("notes", r.getNotes());
        m.put("confidence", r.getConfidence());
        return m;
    }

    /** Cap stored cleaned_text so we don't bloat the doc table with full marketing pages. */
    private static String truncateForStorage(String text) {
        int cap = 50_000;
        return text.length() <= cap ? text : text.substring(0, cap);
    }

    private List<ExtractedRule> extractRules(CardProductToScrape card, String cleaned) {
        String prompt = """
            You are a strict JSON extractor for credit card reward rules.
            Given the cleaned T&C / marketing text below, identify each distinct earning rule
            and return ONLY this JSON shape (no prose):

            {
              "rules": [
                {
                  "canonical_category": "<ONE OF: %s>",
                  "rate_bps": <int — basis points; 500 = 5%%>,
                  "base_rate_bps": <int or null>,
                  "cap_amount": <number or null>,
                  "cap_period": "<NONE | CALENDAR_YEAR | QUARTER | ANNIVERSARY_YEAR>",
                  "cap_group_key": "<string or null — same key for rules sharing one cap>",
                  "requires_user_choice": <true|false>,
                  "channel_restriction": "<null | TRAVEL_PORTAL_ONLY | ONLINE | IN_STORE>",
                  "exclusions": ["string", ...],
                  "notes": "<short human description>",
                  "confidence": <0..1>
                }
              ]
            }

            Card: %s %s
            Text (truncated to %d chars):
            %s
            """.formatted(
                CANONICAL_CATEGORIES,
                card.bankKey(), card.productName(),
                Math.min(cleaned.length(), 12000),
                cleaned.substring(0, Math.min(cleaned.length(), 12000)));

        String raw;
        try {
            raw = llmGateway.generate(prompt, "reward-terms-extract");
        } catch (Exception e) {
            log.warn("scrape_event=extract_failed card_product_id={} error={}",
                    card.cardProductId(), e.getMessage());
            return List.of();
        }
        try {
            JsonNode root = objectMapper.readTree(raw);
            JsonNode arr = root.path("rules");
            List<ExtractedRule> out = new ArrayList<>();
            if (arr.isArray()) {
                for (JsonNode n : arr) {
                    List<String> exclusions = new ArrayList<>();
                    n.path("exclusions").forEach(x -> exclusions.add(x.asText()));
                    out.add(ExtractedRule.builder()
                            .canonicalCategory(n.path("canonical_category").asText(null))
                            .rateBps(n.path("rate_bps").isMissingNode() ? null : n.path("rate_bps").asInt())
                            .baseRateBps(n.path("base_rate_bps").isMissingNode() ? null : n.path("base_rate_bps").asInt())
                            .capPeriod(n.path("cap_period").asText(null))
                            .capAmount(n.path("cap_amount").isMissingNode() || n.path("cap_amount").isNull()
                                    ? null : new BigDecimal(n.path("cap_amount").asText()))
                            .capGroupKey(n.path("cap_group_key").asText(null))
                            .requiresUserChoice(n.path("requires_user_choice").asBoolean(false))
                            .channelRestriction(n.path("channel_restriction").asText(null))
                            .exclusions(exclusions)
                            .notes(n.path("notes").asText(null))
                            .confidence(n.path("confidence").isMissingNode() ? null : n.path("confidence").asDouble())
                            .build());
                }
            }
            return out;
        } catch (Exception e) {
            log.warn("scrape_event=extract_parse_failed card_product_id={} error={} raw={}",
                    card.cardProductId(), e.getMessage(),
                    raw == null ? "" : raw.substring(0, Math.min(300, raw.length())));
            return List.of();
        }
    }

    private static String sha256(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d);
        } catch (Exception e) {
            return "";
        }
    }

    private static ScrapedTermsResult errorResult(CardProductToScrape card, int status, String error) {
        return ScrapedTermsResult.builder()
                .cardProductId(card.cardProductId())
                .bankKey(card.bankKey())
                .productName(card.productName())
                .sourceUrl(card.termsUrl())
                .httpStatus(status)
                .contentHash(null)
                .unchanged(false)
                .cleanedTextChars(0)
                .chunkCount(0)
                .embeddingDim(0)
                .rules(List.of())
                .error(error)
                .build();
    }
}
