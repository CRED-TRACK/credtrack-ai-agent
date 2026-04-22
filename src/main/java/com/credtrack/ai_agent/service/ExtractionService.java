package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.model.StatementExtraction;
import com.credtrack.ai_agent.model.TransactionExtraction;
import com.credtrack.ai_agent.model.UtilityBillExtraction;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
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
        log.info("Statement extraction input for bank {} ({} chars)", bankKey, cleanBody.length());

        String prompt = """
                You are a JSON-only financial data extractor. Respond with ONLY a valid JSON object — no explanations, no markdown, no code blocks.

                Bank: %s

                A statement email is any email that notifies the user that their monthly credit card statement is ready, OR that contains their statement balance/minimum payment. This includes:
                - "Your statement is ready" / "Your statement is now available" notifications (common for Amex, Citi)
                - Emails with a statement balance and minimum payment amount
                - Emails with a link to "View Statement" or "View your statement"
                These are ALL valid statement emails even if they do NOT include the dollar balance directly.

                If this is NOT a statement email (promotional offer, fraud alert, payment confirmation, transaction alert, account security notice, etc), respond with exactly:
                {"isStatement":false,"confidence":0.0,"cardDigits":null,"statementBalance":null,"minimumPaymentDue":null,"statementDate":null,"dueDate":null,"viewStatementUrl":null,"makePaymentUrl":null}

                If it IS a monthly statement email, extract these fields:
                - isStatement: true
                - cardDigits: last 4-5 digits of the card number shown in the email (string, digits only, null if not found)
                - statementBalance: total balance as a number (no $ symbol), null if not present in the email
                - minimumPaymentDue: minimum payment due as a number (no $ symbol), null if not present in the email
                - statementDate: statement closing date in YYYY-MM-DD format, null if not found
                - dueDate: payment due date in YYYY-MM-DD format, null if not found
                - viewStatementUrl: full URL to view the statement online (null if not found)
                - makePaymentUrl: full URL to make a payment (null if not found)
                - confidence: your confidence from 0.0 to 1.0 (use 0.85+ for clear statement-ready notifications)

                Email:
                %s

                JSON response:
                """.formatted(bankKey, cleanBody);

        try {
            String raw = chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();

            log.info("LLM raw response for bank {}: {}", bankKey, raw);

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

    /**
     * Extracts transaction data from a bank transaction alert email using the LLM.
     *
     * @param bankKey   resolved from sender domain (e.g. "CHASE")
     * @param emailBody raw email text (may be HTML)
     * @return TransactionExtraction — check isTransaction + confidence before using
     */
    public TransactionExtraction extractTransaction(String bankKey, String emailBody) {
        String cleanBody = prepareBody(emailBody);
        log.info("Transaction extraction input for bank {} ({} chars)", bankKey, cleanBody.length());

        String prompt = """
                You are a JSON-only financial data extractor. Respond with ONLY a valid JSON object — no explanations, no markdown, no code blocks.

                Bank: %s

                A transaction alert email notifies the user of a single debit or credit on their credit card account.
                Examples: "You made a $86.67 transaction at IC* COSTCO BY INSTAC", "A $120.46 charge was made at AMAZON", "A credit of $40.00 was posted".

                If this is NOT a transaction alert (statement ready, payment scheduled, credit limit change, promotional offer, security alert, etc.), respond with exactly:
                {"isTransaction":false,"merchantName":null,"merchantCategory":null,"amount":0.0,"currency":"USD","transactionDate":null,"transactionType":"DEBIT","description":null,"confidence":0.0}

                If it IS a transaction alert, extract:
                - isTransaction: true
                - merchantName: the merchant name exactly as shown (e.g. "IC* COSTCO BY INSTAC"), null if not found
                - merchantCategory: one of [dining, travel, grocery, gas, entertainment, shopping, health, utilities, payment, other] — infer from merchant name if not explicit, null if unknown
                - amount: transaction amount as a number (no $ symbol), 0.0 if not found
                - currency: currency code, default "USD"
                - transactionDate: date of transaction in YYYY-MM-DD format, null if not found
                - transactionType: "CREDIT" if money is coming back to the card (refund, cashback, payment), "DEBIT" for purchases/charges
                - description: any extra context (optional, usually null)
                - confidence: 0.0–1.0, use 0.90+ when merchant and amount are clearly present

                Email:
                %s

                JSON response:
                """.formatted(bankKey, cleanBody);

        try {
            String raw = chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();

            log.info("LLM transaction response for bank {}: {}", bankKey, raw);

            String json = extractJson(raw);
            TransactionDto dto = MAPPER.readValue(json, TransactionDto.class);

            LocalDate txDate = null;
            if (dto.transactionDate != null && !dto.transactionDate.isBlank()) {
                try { txDate = LocalDate.parse(dto.transactionDate); } catch (Exception ignored) {}
            }

            return new TransactionExtraction(
                    dto.isTransaction,
                    dto.merchantName,
                    dto.merchantCategory,
                    dto.amount,
                    dto.currency != null ? dto.currency : "USD",
                    txDate,
                    dto.transactionType != null ? dto.transactionType : "DEBIT",
                    dto.description,
                    dto.confidence
            );

        } catch (Exception e) {
            log.error("LLM transaction extraction failed for bank {}: {}", bankKey, e.getMessage());
            return new TransactionExtraction(false, null, null, 0.0, "USD", null, "DEBIT", null, 0.0);
        }
    }

    /** Private DTO for deserializing the LLM's transaction JSON response. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TransactionDto {
        public boolean isTransaction;
        public String  merchantName;
        public String  merchantCategory;
        public double  amount;
        public String  currency;
        public String  transactionDate;   // YYYY-MM-DD string from LLM
        public String  transactionType;
        public String  description;
        public double  confidence;
    }

    /**
     * Extracts utility bill data from an Eversource or National Grid bill email.
     *
     * @param billerKey  EVERSOURCE or NATIONAL_GRID
     * @param emailBody  raw email text (may be HTML)
     * @return UtilityBillExtraction — check isBill + confidence before using
     */
    public UtilityBillExtraction extractUtilityBill(String billerKey, String emailBody) {
        // Use text-only preparation (no link appending) — bill data is always in the plain text,
        // and National Grid tracking URLs would bloat the prompt to 10k+ chars causing hallucinations.
        String cleanBody = prepareBodyTextOnly(emailBody);
        log.info("Utility bill extraction input for biller {} ({} chars)", billerKey, cleanBody.length());

        String prompt = """
                You are a JSON-only financial data extractor. Respond with ONLY a valid JSON object — no explanations, no markdown, no code blocks.

                Biller: %s

                A utility bill email notifies the customer that their monthly bill is ready and shows the amount due, due date, and account number.

                If this is NOT a utility bill (payment confirmation, outage notice, marketing email, etc.), respond with exactly:
                {"isBill":false,"accountLastFour":null,"amountDue":0.0,"dueDate":null,"billDate":null,"billingPeriodStart":null,"billingPeriodEnd":null,"confidence":0.0}

                If it IS a utility bill, extract:
                - isBill: true
                - accountLastFour: last 4 digits of the account number shown (e.g. "8616" from "******8616"), null if not found
                - amountDue: total amount due as a number (no $ symbol), 0.0 if not found
                - dueDate: payment due date in YYYY-MM-DD format, null if not found
                - billDate: bill/statement date in YYYY-MM-DD format, null if not found. IMPORTANT: dates in the email may appear as MM/DD/YYYY (e.g. "09/17/2025") — always convert them to YYYY-MM-DD (e.g. "2025-09-17"). Never return the string "null"; use JSON null.
                - billingPeriodStart: start of billing period in YYYY-MM-DD, null if not shown. Convert MM/DD/YYYY to YYYY-MM-DD if needed.
                - billingPeriodEnd: end of billing period in YYYY-MM-DD, null if not shown. Convert MM/DD/YYYY to YYYY-MM-DD if needed.
                - confidence: 0.0–1.0, use 0.85+ when amount and due date are clearly present

                Email:
                %s

                JSON response:
                """.formatted(billerKey, cleanBody);

        try {
            String raw = chatClient.prompt().user(prompt).call().content();
            log.info("LLM utility bill response for biller {}: {}", billerKey, raw);

            String json = extractJson(raw);
            var dto = MAPPER.readValue(json, UtilityBillDto.class);
            log.info("Utility bill extraction for biller {} — isBill={}, confidence={}",
                    billerKey, dto.isBill, dto.confidence);

            return new UtilityBillExtraction(dto.isBill, dto.accountLastFour, dto.amountDue,
                    dto.dueDate, dto.billDate, dto.billingPeriodStart, dto.billingPeriodEnd,
                    dto.confidence);

        } catch (Exception e) {
            log.error("Utility bill extraction failed for biller {}: {}", billerKey, e.getMessage());
            return new UtilityBillExtraction(false, null, 0.0, null, null, null, null, 0.0);
        }
    }

    /** Private DTO for deserializing the LLM's utility bill JSON response. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class UtilityBillDto {
        public boolean isBill;
        public String  accountLastFour;
        public double  amountDue;
        public String  dueDate;
        public String  billDate;
        public String  billingPeriodStart;
        public String  billingPeriodEnd;
        public double  confidence;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Strips HTML to plain text and truncates to MAX_BODY_CHARS.
     * Does NOT append link URLs — used for utility bill extraction where all
     * the relevant data (amount, due date, account number) is in the body text
     * and appending the many tracking URLs would bloat the prompt unnecessarily.
     */
    private String prepareBodyTextOnly(String raw) {
        if (raw == null || raw.isBlank()) return "";
        String text;
        if (raw.contains("<") && raw.contains(">")) {
            text = Jsoup.parse(raw).text().replaceAll("\\s+", " ").trim();
        } else {
            text = raw.replaceAll("\\s+", " ").trim();
        }
        return text.length() > MAX_BODY_CHARS ? text.substring(0, MAX_BODY_CHARS) : text;
    }

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
