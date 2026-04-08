package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.model.StatementExtraction;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calls the LLM (via Spring AI ChatClient) to extract statement data from
 * a raw bank email body.
 *
 * Pre-processing pipeline before the LLM call:
 *   1. Strip HTML tags with Jsoup (bank emails are almost always HTML-only)
 *   2. Collapse whitespace
 *   3. Truncate to 4 000 chars so the prompt fits the model's context window
 *
 * The bank is NOT extracted by the LLM — it is resolved from the sender domain
 * before this call and injected by the caller (StatementExtractorActor).
 */
@Service
public class ExtractionService {

    private static final Logger log = LoggerFactory.getLogger(ExtractionService.class);

    /** Max characters of email body sent to the LLM. */
    private static final int MAX_BODY_CHARS = 4_000;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // Matches a JSON object anywhere in the LLM response (handles extra prose/markdown)
    private static final Pattern JSON_BLOCK = Pattern.compile("\\{[\\s\\S]*}", Pattern.DOTALL);

    private final ChatClient chatClient;

    public ExtractionService(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder.build();
    }

    /**
     * Extracts statement information from an email body.
     *
     * @param bankKey  resolved from sender domain before LLM call (e.g. "CHASE")
     * @param emailBody raw email text (may be HTML)
     * @return StatementExtraction — check isStatement + confidence before using
     */
    public StatementExtraction extract(String bankKey, String emailBody) {
        String cleanBody = prepareBody(emailBody);
        log.debug("Cleaned body ({} chars) for bank {}: [{}]",
                cleanBody.length(), bankKey,
                cleanBody.length() > 500 ? cleanBody.substring(0, 500) + "..." : cleanBody);

        String prompt = """
                You are a JSON-only financial data extractor. Respond with ONLY a valid JSON object — no explanations, no markdown, no code blocks.

                Bank: %s

                If this is NOT a statement email (promotional, alert, payment confirmation, etc), respond with exactly:
                {"isStatement":false,"confidence":0.0,"cardDigits":null,"statementBalance":null,"minimumPaymentDue":null,"statementDate":null,"dueDate":null,"viewStatementUrl":null,"makePaymentUrl":null}

                If it IS a monthly statement email, extract these fields:
                - isStatement: true
                - cardDigits: last 4-5 digits of the card number shown in the email (string, digits only)
                - statementBalance: total balance as a number (no $ symbol)
                - minimumPaymentDue: minimum payment due as a number (no $ symbol)
                - statementDate: statement closing date in YYYY-MM-DD format
                - dueDate: payment due date in YYYY-MM-DD format
                - viewStatementUrl: full URL to view the statement online (null if not found)
                - makePaymentUrl: full URL to make a payment (null if not found)
                - confidence: your confidence from 0.0 to 1.0

                Email:
                %s

                JSON response:
                """.formatted(bankKey, cleanBody);

        try {
            String raw = chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();

            log.debug("LLM raw response for bank {}: {}", bankKey, raw);

            String json = extractJson(raw);
            StatementExtraction result = MAPPER.readValue(json, StatementExtraction.class);
            log.info("Extraction for bank {} — isStatement={}, confidence={}",
                    bankKey, result.isStatement(), result.getConfidence());
            return result;

        } catch (Exception e) {
            log.error("LLM extraction failed for bank {}: {}", bankKey, e.getMessage());
            StatementExtraction failed = new StatementExtraction();
            failed.setStatement(false);
            failed.setConfidence(0.0);
            return failed;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Strips HTML, extracts all link URLs separately (never truncated), truncates
     * body text to MAX_BODY_CHARS, then appends the full links at the end.
     *
     * This guarantees URLs like view-statement / make-payment are always complete
     * even when their accountId token makes them hundreds of characters long.
     */
    private String prepareBody(String raw) {
        if (raw == null || raw.isBlank()) return "";

        if (raw.contains("<") && raw.contains(">")) {
            org.jsoup.nodes.Document doc = Jsoup.parse(raw);

            // ── 1. Extract all named links first (never truncated) ──────────────
            StringBuilder links = new StringBuilder();
            for (org.jsoup.nodes.Element a : doc.select("a[href]")) {
                String href = a.attr("abs:href");
                if (href == null || href.isBlank()) href = a.attr("href");
                String linkText = a.text().trim();
                if (!linkText.isBlank() && href != null && !href.isBlank()
                        && (href.startsWith("http") || href.startsWith("/"))) {
                    links.append(linkText).append(": ").append(href).append("\n");
                }
            }

            // ── 2. Get plain text (truncated) ────────────────────────────────────
            String text = doc.text().replaceAll("\\s+", " ").trim();
            if (text.length() > MAX_BODY_CHARS) text = text.substring(0, MAX_BODY_CHARS);

            // ── 3. Append full links after the truncated text ────────────────────
            return links.length() > 0
                    ? text + "\n\nLinks:\n" + links.toString().trim()
                    : text;
        } else {
            String text = raw.replaceAll("\\s+", " ").trim();
            return text.length() > MAX_BODY_CHARS ? text.substring(0, MAX_BODY_CHARS) : text;
        }
    }

    /**
     * Extracts the first JSON object from the LLM response.
     * Handles cases where the model adds prose or wraps JSON in code fences.
     */
    private String extractJson(String raw) {
        if (raw == null || raw.isBlank()) throw new IllegalArgumentException("Empty LLM response");

        // Strip markdown code fences if present
        String cleaned = raw.replaceAll("```json", "").replaceAll("```", "").trim();

        Matcher m = JSON_BLOCK.matcher(cleaned);
        if (m.find()) return m.group();

        throw new IllegalArgumentException("No JSON object found in LLM response: " + raw);
    }
}
