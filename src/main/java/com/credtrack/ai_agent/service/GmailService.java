package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.model.EmailMessage;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.History;
import com.google.api.services.gmail.model.ListHistoryResponse;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.gmail.model.MessagePart;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

@Service
public class GmailService {

    private static final Logger log = LoggerFactory.getLogger(GmailService.class);
    private static final String APP_NAME = "CredTrack-AI";
    private static final String USER     = "me";

    /**
     * Known bank sender domains → only emails from these are passed to the LLM.
     * Key = domain suffix to match, Value = bankKey matching CardProduct.bankKey.
     */
    public static final Map<String, String> SENDER_DOMAIN_TO_BANK = Map.of(
            "chase.com",             "CHASE",
            "discover.com",          "DISCOVER",
            "americanexpress.com",   "AMEX",
            "bankofamerica.com",     "BOA",
            "citibank.com",          "CITI",
            "capitalone.com",        "CAPITAL_ONE",
            "wellsfargo.com",        "WELLS_FARGO",
            "usbank.com",            "US_BANK"
    );

    /**
     * Fetches statement emails for a user using three strategies:
     *
     * 1. historyId == null (very first poll ever):
     *    Full historical search for all registered cards.
     *
     * 2. historyId != null + card has gmailScanComplete=false (new card added after first poll):
     *    Targeted historical search just for that card's digits, regardless of historyId cursor.
     *    The card ID is added to completedCardScans so the caller can mark it done.
     *
     * 3. historyId != null + card has gmailScanComplete=true + today is within
     *    [lastStatementDate+1month, lastStatementDate+1month+3days]:
     *    Targeted safety-net search for that specific card's upcoming statement.
     *    Catches emails the agent may have missed (e.g. agent was down when the email arrived).
     *
     * The incremental historyId poll always runs when historyId != null,
     * so new emails are caught even without the safety-net search.
     */
    public FetchResult fetchNewEmails(String accessToken, Long historyId,
                                      List<com.credtrack.ai_agent.model.CardInfo> cards) throws Exception {
        Gmail gmail = buildClient(accessToken);
        List<EmailMessage> emails = new ArrayList<>();
        Set<Long> completedCardScans = new HashSet<>();
        Long newHistoryId = historyId;

        if (historyId == null) {
            // ── Strategy 1: very first poll — full historical search ──────────
            String fromClause = "from:(" + String.join(" OR ", SENDER_DOMAIN_TO_BANK.keySet()) + ")";

            // Separate Amex 4-digit cards — Gmail tokenizes "51006" as one token, so
            // searching "1006" won't find emails that contain "51006". Search Amex
            // without card digits; the extractor will discover and auto-save the 5-digit value.
            List<com.credtrack.ai_agent.model.CardInfo> normalCards = cards == null ? List.of() :
                    cards.stream()
                            .filter(c -> !("AMEX".equals(c.bankKey()) && c.lastFour().length() == 4))
                            .toList();
            List<com.credtrack.ai_agent.model.CardInfo> amex4DigitCards = cards == null ? List.of() :
                    cards.stream()
                            .filter(c -> "AMEX".equals(c.bankKey()) && c.lastFour().length() == 4)
                            .toList();

            // Broad search for all non-Amex cards (and any Amex with 5-digit lastFour)
            if (!normalCards.isEmpty()) {
                String cardClause = buildCardClause(normalCards);
                String query = fromClause + " subject:statement" + cardClause;
                log.info("First poll Gmail query: {}", query);
                newHistoryId = searchAndCollect(gmail, query, emails, newHistoryId);
            }

            // Separate no-digit search per Amex 4-digit card — avoids Gmail tokenization mismatch
            for (com.credtrack.ai_agent.model.CardInfo amexCard : amex4DigitCards) {
                String query = "from:americanexpress.com subject:statement";
                log.info("First poll Amex no-digit search for card {} ({}): {}",
                        amexCard.cardId(), amexCard.lastFour(), query);
                searchAndCollect(gmail, query, emails, null);
            }

            // Also search for bank payment confirmation emails — these have different subjects
            // and are missed by the subject:statement filter
            boolean hasChaseCard = cards != null && cards.stream().anyMatch(c -> "CHASE".equals(c.bankKey()));
            if (hasChaseCard) {
                String paymentQuery = "from:chase.com (\"payment is scheduled\" OR \"payment scheduled\")";
                log.info("First poll Chase payment query: {}", paymentQuery);
                searchAndCollect(gmail, paymentQuery, emails, null);
            }
            boolean hasBoaCard = cards != null && cards.stream().anyMatch(c -> "BOA".equals(c.bankKey()));
            if (hasBoaCard) {
                String paymentQuery = "from:ealerts.bankofamerica.com \"received your credit card payment\"";
                log.info("First poll BOA payment query: {}", paymentQuery);
                searchAndCollect(gmail, paymentQuery, emails, null);
            }
            boolean hasDiscoverCard = cards != null && cards.stream().anyMatch(c -> "DISCOVER".equals(c.bankKey()));
            if (hasDiscoverCard) {
                String paymentQuery = "from:services.discover.com \"scheduled payment\"";
                log.info("First poll Discover payment query: {}", paymentQuery);
                searchAndCollect(gmail, paymentQuery, emails, null);
            }
            boolean hasAmexCard = cards != null && cards.stream().anyMatch(c -> "AMEX".equals(c.bankKey()));
            if (hasAmexCard) {
                String paymentQuery = "from:welcome.americanexpress.com \"received your payment\"";
                log.info("First poll Amex payment query: {}", paymentQuery);
                searchAndCollect(gmail, paymentQuery, emails, null);
            }

            if (newHistoryId == null) {
                newHistoryId = gmail.users().getProfile(USER).execute().getHistoryId().longValue();
            }
            // All cards scanned — mark them all complete
            if (cards != null) cards.forEach(c -> completedCardScans.add(c.cardId()));

        } else {
            // ── Strategy 2 & 3: targeted searches for individual cards ────────
            if (cards != null) {
                for (com.credtrack.ai_agent.model.CardInfo card : cards) {
                    if (!card.gmailScanComplete()) {
                        // New card — historical scan.
                        // Use ONLY this card's bank domain (not all domains) to narrow results,
                        // and drop card digits because Gmail tokenizes "51006" as one token so
                        // searching "1006" won't match it as a substring.
                        // The StatementExtractorActor validates extracted digits against registered
                        // cards anyway, so non-matching statements are rejected downstream.
                        String bankDomain = SENDER_DOMAIN_TO_BANK.entrySet().stream()
                                .filter(e -> e.getValue().equals(card.bankKey()))
                                .map(Map.Entry::getKey)
                                .findFirst().orElse(null);
                        if (bankDomain == null) {
                            log.warn("No domain mapping for bankKey {} — skipping historical scan for card {}",
                                    card.bankKey(), card.cardId());
                            completedCardScans.add(card.cardId());
                            continue;
                        }
                        // If lastFour is 4 digits and this is AMEX, search without digits:
                        // Amex displays 5 digits (e.g. "51006") so the 4-digit token "1006"
                        // won't match. The extractor will discover and save the full 5 digits.
                        // For all other banks (or if 5 digits already stored), include the token.
                        boolean needsDigitDiscovery = "AMEX".equals(card.bankKey())
                                && card.lastFour().length() == 4;
                        String query = needsDigitDiscovery
                                ? "from:" + bankDomain + " subject:statement"
                                : "from:" + bankDomain + " subject:statement " + card.lastFour();
                        log.info("Historical scan for new card {} ({}, {}): {}", card.cardId(), card.lastFour(), card.bankKey(), query);
                        searchAndCollect(gmail, query, emails, null);

                        // Also fetch payment confirmation emails — not caught by subject:statement filter
                        if ("CHASE".equals(card.bankKey())) {
                            String paymentQuery = "from:chase.com (\"payment is scheduled\" OR \"payment scheduled\") " + card.lastFour();
                            log.info("Historical payment scan for new Chase card {}: {}", card.cardId(), paymentQuery);
                            searchAndCollect(gmail, paymentQuery, emails, null);
                        } else if ("BOA".equals(card.bankKey())) {
                            String paymentQuery = "from:ealerts.bankofamerica.com \"received your credit card payment\" " + card.lastFour();
                            log.info("Historical payment scan for new BOA card {}: {}", card.cardId(), paymentQuery);
                            searchAndCollect(gmail, paymentQuery, emails, null);
                        } else if ("DISCOVER".equals(card.bankKey())) {
                            String paymentQuery = "from:services.discover.com \"scheduled payment\" " + card.lastFour();
                            log.info("Historical payment scan for new Discover card {}: {}", card.cardId(), paymentQuery);
                            searchAndCollect(gmail, paymentQuery, emails, null);
                        } else if ("AMEX".equals(card.bankKey())) {
                            String paymentQuery = "from:welcome.americanexpress.com \"received your payment\" " + card.lastFour();
                            log.info("Historical payment scan for new Amex card {}: {}", card.cardId(), paymentQuery);
                            searchAndCollect(gmail, paymentQuery, emails, null);
                        }
                        completedCardScans.add(card.cardId());

                    } else if (isInStatementWindow(card.lastStatementDate())) {
                        // Existing card in its expected statement window — safety-net search
                        java.time.LocalDate windowStart = card.lastStatementDate().plusMonths(1);
                        java.time.LocalDate windowEnd   = windowStart.plusDays(3);
                        String bankDomain2 = SENDER_DOMAIN_TO_BANK.entrySet().stream()
                                .filter(e -> e.getValue().equals(card.bankKey()))
                                .map(Map.Entry::getKey)
                                .findFirst().orElse(null);
                        if (bankDomain2 == null) continue;
                        String dateRange  = "after:" + windowStart.minusDays(1)
                                          + " before:" + windowEnd.plusDays(1);
                        String query = "from:" + bankDomain2 + " subject:statement " + card.lastFour() + " " + dateRange;
                        log.info("Statement window search for card {} ({}, {}): {}", card.cardId(), card.lastFour(), card.bankKey(), query);
                        searchAndCollect(gmail, query, emails, null);
                    }
                }
            }

            // ── Strategy 4: Payment catch-up for cards with unpaid statements ────────
            // For any supported card with hasUnpaidStatements=true, search for payment
            // confirmation emails that may have been sent before the historyId cursor was set.
            if (cards != null) {
                for (com.credtrack.ai_agent.model.CardInfo card : cards) {
                    if (!card.hasUnpaidStatements()) continue;
                    if ("CHASE".equals(card.bankKey())) {
                        String paymentQuery = "from:chase.com (\"payment is scheduled\" OR \"payment scheduled\") " + card.lastFour();
                        log.info("Chase payment catch-up for card {} ({}): {}", card.cardId(), card.lastFour(), paymentQuery);
                        searchAndCollect(gmail, paymentQuery, emails, null);
                    } else if ("BOA".equals(card.bankKey())) {
                        String paymentQuery = "from:ealerts.bankofamerica.com \"received your credit card payment\" " + card.lastFour();
                        log.info("BOA payment catch-up for card {} ({}): {}", card.cardId(), card.lastFour(), paymentQuery);
                        searchAndCollect(gmail, paymentQuery, emails, null);
                    } else if ("DISCOVER".equals(card.bankKey())) {
                        String paymentQuery = "from:services.discover.com \"scheduled payment\" " + card.lastFour();
                        log.info("Discover payment catch-up for card {} ({}): {}", card.cardId(), card.lastFour(), paymentQuery);
                        searchAndCollect(gmail, paymentQuery, emails, null);
                    } else if ("AMEX".equals(card.bankKey())) {
                        String paymentQuery = "from:welcome.americanexpress.com \"received your payment\" " + card.lastFour();
                        log.info("Amex payment catch-up for card {} ({}): {}", card.cardId(), card.lastFour(), paymentQuery);
                        searchAndCollect(gmail, paymentQuery, emails, null);
                    }
                }
            }

            // ── Always run incremental poll for new emails since last historyId ─
            ListHistoryResponse histResp = gmail.users().history()
                    .list(USER)
                    .setStartHistoryId(BigInteger.valueOf(historyId))
                    .setHistoryTypes(List.of("messageAdded"))
                    .execute();

            if (histResp.getHistory() != null) {
                for (History h : histResp.getHistory()) {
                    if (h.getMessagesAdded() != null) {
                        for (var added : h.getMessagesAdded()) {
                            try {
                                Message msg = gmail.users().messages()
                                        .get(USER, added.getMessage().getId())
                                        .setFormat("full")
                                        .execute();
                                toEmailMessage(msg).ifPresent(emails::add);
                            } catch (GoogleJsonResponseException e) {
                                if (e.getStatusCode() == 404) {
                                    log.debug("Message {} not found (deleted?), skipping",
                                            added.getMessage().getId());
                                } else {
                                    throw e;
                                }
                            }
                        }
                    }
                }
            }
            newHistoryId = histResp.getHistoryId() != null
                    ? histResp.getHistoryId().longValue()
                    : historyId;
        }

        // Deduplicate by messageId (targeted searches may overlap with incremental)
        Map<String, EmailMessage> seen = new java.util.LinkedHashMap<>();
        for (EmailMessage e : emails) seen.put(e.messageId(), e);

        List<EmailMessage> bankEmails = seen.values().stream()
                .filter(e -> resolveBankKey(e.senderDomain()) != null)
                .toList();

        log.info("Fetched {} unique bank emails, newHistoryId={}, completedScans={}",
                bankEmails.size(), newHistoryId, completedCardScans);

        return new FetchResult(bankEmails, newHistoryId, completedCardScans);
    }

    /** Runs a Gmail search query and appends results to {@code emails}. Returns highest historyId seen. */
    private Long searchAndCollect(Gmail gmail, String query,
                                  List<EmailMessage> emails, Long currentMax) throws Exception {
        var listResp = gmail.users().messages()
                .list(USER)
                .setMaxResults(50L)
                .setQ(query)
                .execute();

        if (listResp.getMessages() == null) return currentMax;

        for (var ref : listResp.getMessages()) {
            Message msg = gmail.users().messages()
                    .get(USER, ref.getId())
                    .setFormat("full")
                    .execute();
            toEmailMessage(msg).ifPresent(emails::add);
            if (msg.getHistoryId() != null) {
                long hid = msg.getHistoryId().longValue();
                if (currentMax == null || hid > currentMax) currentMax = hid;
            }
        }
        return currentMax;
    }

    /** Returns true if today falls in [lastStatementDate+1month, lastStatementDate+1month+3days]. */
    private boolean isInStatementWindow(java.time.LocalDate lastStatementDate) {
        if (lastStatementDate == null) return false;
        java.time.LocalDate windowStart = lastStatementDate.plusMonths(1);
        java.time.LocalDate windowEnd   = windowStart.plusDays(3);
        java.time.LocalDate today       = java.time.LocalDate.now(ZoneOffset.UTC);
        return !today.isBefore(windowStart) && !today.isAfter(windowEnd);
    }

    /** Builds " (\"1234\" OR \"5678\")" clause from card list. Empty string if no cards. */
    private String buildCardClause(List<com.credtrack.ai_agent.model.CardInfo> cards) {
        if (cards == null || cards.isEmpty()) return "";
        // Quoted — exact match needed for broad first-poll to avoid false positives
        // across all banks + subject:statement filter.
        String digits = cards.stream()
                .map(c -> "\"" + c.lastFour() + "\"")
                .collect(java.util.stream.Collectors.joining(" OR "));
        return " (" + digits + ")";
    }

    /**
     * Returns the bankKey for a sender domain, or null if not a known bank.
     */
    public static String resolveBankKey(String senderDomain) {
        if (senderDomain == null) return null;
        String lower = senderDomain.toLowerCase();
        for (Map.Entry<String, String> entry : SENDER_DOMAIN_TO_BANK.entrySet()) {
            if (lower.endsWith(entry.getKey())) return entry.getValue();
        }
        return null;
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private Gmail buildClient(String accessToken) throws Exception {
        AccessToken token = new AccessToken(accessToken, new Date(Long.MAX_VALUE));
        GoogleCredentials creds = GoogleCredentials.create(token);
        HttpCredentialsAdapter adapter = new HttpCredentialsAdapter(creds);

        // Increase timeout: default NetHttpTransport is 20s which times out under load
        com.google.api.client.http.HttpRequestInitializer timedInitializer = request -> {
            adapter.initialize(request);
            request.setConnectTimeout(60_000);
            request.setReadTimeout(60_000);
        };

        return new Gmail.Builder(
                GoogleNetHttpTransport.newTrustedTransport(),
                GsonFactory.getDefaultInstance(),
                timedInitializer)
                .setApplicationName(APP_NAME)
                .build();
    }

    private Optional<EmailMessage> toEmailMessage(Message msg) {
        if (msg.getPayload() == null) return Optional.empty();

        String subject      = "";
        String senderDomain = "";

        if (msg.getPayload().getHeaders() != null) {
            for (var h : msg.getPayload().getHeaders()) {
                if ("Subject".equalsIgnoreCase(h.getName())) subject = h.getValue();
                if ("From".equalsIgnoreCase(h.getName()))    senderDomain = extractDomain(h.getValue());
            }
        }

        String body = extractBody(msg.getPayload());

        // internalDate is epoch-millis in UTC — this is the statement closing date
        LocalDate sentDate = msg.getInternalDate() != null
                ? Instant.ofEpochMilli(msg.getInternalDate()).atZone(ZoneOffset.UTC).toLocalDate()
                : null;

        // Extract view/pay URLs directly from HTML — bypass LLM to avoid token-limit truncation
        String viewStatementUrl = null;
        String makePaymentUrl   = null;
        if (body != null && body.contains("<a")) {
            org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(body);
            for (org.jsoup.nodes.Element a : doc.select("a[href]")) {
                String text = a.text().toLowerCase().trim();
                String href = a.attr("abs:href");
                if (href == null || href.isBlank()) href = a.attr("href");
                if (href == null || href.isBlank()) continue;
                if (viewStatementUrl == null
                        && (text.contains("view statement") || text.contains("view your statement"))) {
                    viewStatementUrl = href;
                }
                if (makePaymentUrl == null
                        && (text.contains("make a payment") || text.contains("make payment"))) {
                    makePaymentUrl = href;
                }
            }
        }

        return Optional.of(new EmailMessage(msg.getId(), subject, body, msg.getSnippet(),
                senderDomain, sentDate, viewStatementUrl, makePaymentUrl));
    }

    private String extractDomain(String fromHeader) {
        // From header: "Chase <no.reply.alerts@chase.com>" or "alerts@chase.com"
        if (fromHeader == null) return "";
        int at = fromHeader.lastIndexOf('@');
        if (at < 0) return "";
        String rest = fromHeader.substring(at + 1);
        // Strip trailing ">" if present
        int angle = rest.indexOf('>');
        return angle >= 0 ? rest.substring(0, angle).trim() : rest.trim();
    }

    private String extractBody(MessagePart part) {
        if (part == null) return "";
        // Prefer plain text
        if ("text/plain".equals(part.getMimeType())
                && part.getBody() != null && part.getBody().getData() != null) {
            return decodeBase64(part.getBody().getData());
        }
        // Recurse into parts
        if (part.getParts() != null) {
            for (MessagePart child : part.getParts()) {
                String r = extractBody(child);
                if (!r.isEmpty()) return r;
            }
        }
        // Fallback to HTML
        if ("text/html".equals(part.getMimeType())
                && part.getBody() != null && part.getBody().getData() != null) {
            return decodeBase64(part.getBody().getData());
        }
        return "";
    }

    private String decodeBase64(String data) {
        try {
            return new String(Base64.getUrlDecoder().decode(data), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "";
        }
    }

    public record FetchResult(List<EmailMessage> emails, Long newHistoryId, Set<Long> completedCardScans) {}
}
