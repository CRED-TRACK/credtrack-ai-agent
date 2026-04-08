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
     * Fetches new emails since historyId, filtered to known bank senders only.
     * First poll (historyId = null): searches specifically for statement emails
     * matching the user's registered card last-four digits — no promos, no alerts.
     */
    public FetchResult fetchNewEmails(String accessToken, Long historyId,
                                      List<com.credtrack.ai_agent.model.CardInfo> cards) throws Exception {
        Gmail gmail       = buildClient(accessToken);
        List<EmailMessage> emails = new ArrayList<>();
        Long newHistoryId = historyId;

        if (historyId == null) {
            // Build a targeted query: bank senders + "statement" keyword + card digits
            // e.g. from:(chase.com OR discover.com) subject:statement ("5058" OR "8379")
            String fromClause = "from:(" + String.join(" OR ", SENDER_DOMAIN_TO_BANK.keySet()) + ")";

            String cardClause = "";
            if (cards != null && !cards.isEmpty()) {
                String digits = cards.stream()
                        .map(c -> "\"" + c.lastFour() + "\"")
                        .collect(java.util.stream.Collectors.joining(" OR "));
                cardClause = " (" + digits + ")";
            }

            String query = fromClause + " subject:statement" + cardClause;
            log.info("First poll Gmail query: {}", query);

            var listResp = gmail.users().messages()
                    .list(USER)
                    .setMaxResults(50L)
                    .setQ(query)
                    .execute();

            if (listResp.getMessages() != null) {
                for (var ref : listResp.getMessages()) {
                    Message msg = gmail.users().messages()
                            .get(USER, ref.getId())
                            .setFormat("full")
                            .execute();
                    toEmailMessage(msg).ifPresent(emails::add);
                    if (msg.getHistoryId() != null) {
                        long hid = msg.getHistoryId().longValue();
                        if (newHistoryId == null || hid > newHistoryId) newHistoryId = hid;
                    }
                }
            }
            if (newHistoryId == null) {
                newHistoryId = gmail.users().getProfile(USER)
                        .execute().getHistoryId().longValue();
            }

        } else {
            // Incremental poll — only what changed since last historyId
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
                            } catch (com.google.api.client.googleapis.json.GoogleJsonResponseException e) {
                                if (e.getStatusCode() == 404) {
                                    // Message was deleted/auto-archived after it appeared in history
                                    log.debug("Message {} not found (deleted?), skipping",
                                            added.getMessage().getId());
                                } else {
                                    throw e;  // re-throw unexpected errors
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

        // Filter — only keep emails from known bank sender domains
        List<EmailMessage> bankEmails = emails.stream()
                .filter(e -> resolveBankKey(e.senderDomain()) != null)
                .toList();

        log.info("Fetched {} total, {} bank emails, newHistoryId={}",
                emails.size(), bankEmails.size(), newHistoryId);

        return new FetchResult(bankEmails, newHistoryId);
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

    public record FetchResult(List<EmailMessage> emails, Long newHistoryId) {}
}
