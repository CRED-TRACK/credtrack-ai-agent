package com.credtrack.ai_agent.service;

import com.credtrack.ai_agent.model.CardInfo;
import com.credtrack.ai_agent.model.EmailMessage;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.History;
import com.google.api.services.gmail.model.ListHistoryResponse;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.gmail.model.MessagePart;
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

    // ── Normal poll (pure incremental) ─────────────────────────────────────────

    /**
     * Fetches emails that arrived since the last historyId cursor.
     *
     * This is the ONLY method used by the normal poll path (EmailFetcherActor).
     * It does NOT perform any keyword search — only the Gmail History API is used,
     * so it is fast and never re-processes old emails.
     *
     * If historyId is null (first time after init), the method records the current
     * historyId as the new cursor and returns an empty list.  All historical emails
     * are handled by InitCardScanActor, not here.
     */
    public FetchResult fetchNewEmails(String accessToken, Long historyId) throws Exception {
        Gmail gmail = buildClient(accessToken);
        List<EmailMessage> emails = new ArrayList<>();
        Long newHistoryId = historyId;

        if (historyId == null) {
            // No cursor yet — record current position and return.
            // Historical scan is handled by InitCardScanActor.
            newHistoryId = gmail.users().getProfile(USER).execute().getHistoryId().longValue();
            log.info("No historyId cursor — recording current position: {}", newHistoryId);
        } else {
            // Incremental: only messages added since the last cursor.
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

        List<EmailMessage> bankEmails = emails.stream()
                .filter(e -> resolveBankKey(e.senderDomain()) != null)
                .toList();

        log.info("Incremental poll: {} bank emails, newHistoryId={}", bankEmails.size(), newHistoryId);
        return new FetchResult(bankEmails, newHistoryId);
    }

    // ── Init scan (one-shot historical search, called by InitCardScanActor) ───

    /**
     * Searches Gmail for all historical statement emails for a single card.
     * Called by InitCardScanActor during the one-time init scan for a new card.
     *
     * Uses Gmail search API (keyword-based), not the History API.
     * AMEX 4-digit cards omit card digits because Gmail tokenizes "51006" as a
     * single token — searching "1006" would miss those emails.  StatementExtractorActor
     * discovers and persists the 5-digit value automatically.
     */
    public List<EmailMessage> searchStatementEmailsForCard(String accessToken, CardInfo card)
            throws Exception {
        String bankDomain = getBankDomain(card.bankKey());
        if (bankDomain == null) {
            log.warn("No domain mapping for bankKey {} — skipping statement search for card {}",
                    card.bankKey(), card.cardId());
            return List.of();
        }
        boolean needsDigitDiscovery = "AMEX".equals(card.bankKey()) && card.lastFour().length() == 4;
        String query = needsDigitDiscovery
                ? "from:" + bankDomain + " subject:statement"
                : "from:" + bankDomain + " subject:statement " + card.lastFour();
        log.info("Init statement search for card {} ({}, {}): {}",
                card.cardId(), card.lastFour(), card.bankKey(), query);

        Gmail gmail = buildClient(accessToken);
        List<EmailMessage> emails = new ArrayList<>();
        searchAndCollect(gmail, query, emails);
        return emails;
    }

    /**
     * Searches Gmail for all historical payment confirmation emails for a single card.
     * Called by InitCardScanActor after the statement phase completes.
     *
     * All payment emails are searched without date filter — the backend's
     * gmailMessageId uniqueness constraint makes duplicate posts idempotent.
     */
    public List<EmailMessage> searchPaymentEmailsForCard(String accessToken, CardInfo card)
            throws Exception {
        Gmail gmail = buildClient(accessToken);
        List<EmailMessage> emails = new ArrayList<>();
        String lastFour = card.lastFour();

        switch (card.bankKey()) {
            case "CHASE" -> {
                String q = "from:chase.com (\"payment is scheduled\" OR \"payment scheduled\") " + lastFour;
                log.info("Init payment search for Chase card {}: {}", card.cardId(), q);
                searchAndCollect(gmail, q, emails);
            }
            case "BOA" -> {
                String q = "from:ealerts.bankofamerica.com \"received your credit card payment\" " + lastFour;
                log.info("Init payment search for BOA card {}: {}", card.cardId(), q);
                searchAndCollect(gmail, q, emails);
            }
            case "DISCOVER" -> {
                String q = "from:services.discover.com \"scheduled payment\" " + lastFour;
                log.info("Init payment search for Discover card {}: {}", card.cardId(), q);
                searchAndCollect(gmail, q, emails);
            }
            case "AMEX" -> {
                // 4-digit stored lastFour: omit digits — Gmail tokenizes "51006" as one token
                // so searching "1006" won't match. PaymentExtractorActor validates digits anyway.
                boolean noDigit = card.lastFour().length() == 4;
                String q = noDigit
                        ? "from:welcome.americanexpress.com \"received your payment\""
                        : "from:welcome.americanexpress.com \"received your payment\" " + lastFour;
                log.info("Init payment search for Amex card {}: {}", card.cardId(), q);
                searchAndCollect(gmail, q, emails);
            }
            default -> log.debug("No payment search configured for bankKey {}", card.bankKey());
        }
        return emails;
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

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

    /** Returns the sender domain for a given bankKey, or null if unknown. */
    private String getBankDomain(String bankKey) {
        return SENDER_DOMAIN_TO_BANK.entrySet().stream()
                .filter(e -> e.getValue().equals(bankKey))
                .map(Map.Entry::getKey)
                .findFirst().orElse(null);
    }

    /** Runs a Gmail search query and appends matching emails to {@code out}. */
    private void searchAndCollect(Gmail gmail, String query, List<EmailMessage> out) throws Exception {
        var listResp = gmail.users().messages()
                .list(USER)
                .setMaxResults(50L)
                .setQ(query)
                .execute();

        if (listResp.getMessages() == null) return;

        for (var ref : listResp.getMessages()) {
            Message msg = gmail.users().messages()
                    .get(USER, ref.getId())
                    .setFormat("full")
                    .execute();
            toEmailMessage(msg).ifPresent(out::add);
        }
    }

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

        // internalDate is epoch-millis in UTC
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
        if (fromHeader == null) return "";
        int at = fromHeader.lastIndexOf('@');
        if (at < 0) return "";
        String rest = fromHeader.substring(at + 1);
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

    public record FetchResult(List<EmailMessage> emails, Long newHistoryId) {}
}
