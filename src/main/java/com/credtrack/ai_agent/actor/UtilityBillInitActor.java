package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.model.UtilityAccountInfo;
import com.credtrack.ai_agent.model.UtilityBillExtraction;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import com.credtrack.ai_agent.service.GmailService;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One-shot actor spawned when a newly registered utility account needs its
 * historical Gmail inbox scanned for the first time.
 *
 * Sequential scan order (mirrors InitCardScanActor pattern):
 *   1. Search bill emails (last {@link #INIT_LOOKBACK_DAYS} days).
 *   2. Extract + POST each bill to the backend (LLM-based).
 *      Bills are posted BEFORE payments so the payment-matching logic can find them.
 *   3. Search payment emails (same lookback window).
 *   4. Extract + POST each payment to the backend (regex-based).
 *   5. Mark utility account init-complete via the backend.
 *   6. Tell coordinator {@link UtilityBillCoordinatorActor.UtilityInitComplete}.
 *
 * Failure → {@link UtilityBillCoordinatorActor.UtilityInitFailed} so the
 * coordinator retries automatically on the next poll cycle.
 *
 * All blocking I/O (Gmail, Ollama, backend REST) runs on virtual threads so
 * the Akka dispatcher is never blocked.
 */
public class UtilityBillInitActor extends AbstractBehavior<UtilityBillInitActor.Command> {

    // ── Constants ─────────────────────────────────────────────────────────────

    private static final Logger log = LoggerFactory.getLogger(UtilityBillInitActor.class);

    /** Historical lookback window for the one-time init scan: 2 years. */
    static final int INIT_LOOKBACK_DAYS = 730;

    /** Minimum LLM confidence to accept a bill extraction. */
    private static final double CONFIDENCE_THRESHOLD = 0.7;

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /** Sent by UtilityBillCoordinatorActor to start the init scan. */
    public record RunInit(
            UtilityAccountInfo                            account,
            String                                        accessToken,
            ActorRef<UtilityBillCoordinatorActor.Command> coordinator
    ) implements Command {}

    /** Internal — result of the async init task piped back to this mailbox. */
    private record InitTaskResult(boolean success, String error) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(GmailService gmailService,
                                           ExtractionService extractionService,
                                           BackendApiClient backendApiClient,
                                           Executor executor) {
        return Behaviors.setup(ctx ->
                new UtilityBillInitActor(ctx, gmailService, extractionService, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final GmailService      gmailService;
    private final ExtractionService extractionService;
    private final BackendApiClient  backendApiClient;
    private final Executor          executor;

    // Populated when RunInit is received; used when InitTaskResult arrives
    private ActorRef<UtilityBillCoordinatorActor.Command> coordinatorRef;
    private String accountKey;   // userId:billerName:accountLastFour

    private UtilityBillInitActor(ActorContext<Command> ctx,
                                  GmailService gmailService,
                                  ExtractionService extractionService,
                                  BackendApiClient backendApiClient,
                                  Executor executor) {
        super(ctx);
        this.gmailService      = gmailService;
        this.extractionService = extractionService;
        this.backendApiClient  = backendApiClient;
        this.executor          = executor;
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RunInit.class,        this::onRun)
                .onMessage(InitTaskResult.class, this::onResult)
                .build();
    }

    private Behavior<Command> onRun(RunInit msg) {
        this.coordinatorRef = msg.coordinator();
        this.accountKey     = msg.account().userId() + ":"
                            + msg.account().billerName() + ":"
                            + msg.account().accountLastFour();

        UtilityAccountInfo account     = msg.account();
        String             accessToken = msg.accessToken();

        getContext().getLog().info(
                "Utility init scan started — billerName={} acct={} user={}",
                account.billerName(), account.accountLastFour(), account.userId());

        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(
                        () -> runInit(account, accessToken), executor),
                (result, ex) -> ex != null ? new InitTaskResult(false, ex.getMessage()) : result
        );
        return this;
    }

    private Behavior<Command> onResult(InitTaskResult msg) {
        if (msg.success()) {
            getContext().getLog().info("Utility init succeeded — key={}", accountKey);
            coordinatorRef.tell(new UtilityBillCoordinatorActor.UtilityInitComplete(accountKey));
        } else {
            getContext().getLog().warn(
                    "Utility init failed (will retry on next poll cycle) — key={} error={}",
                    accountKey, msg.error());
            coordinatorRef.tell(new UtilityBillCoordinatorActor.UtilityInitFailed(accountKey));
        }
        return Behaviors.stopped();
    }

    // ── Core init logic (runs entirely on a virtual thread) ───────────────────

    private InitTaskResult runInit(UtilityAccountInfo account, String accessToken) {
        LocalDate since = LocalDate.now().minusDays(INIT_LOOKBACK_DAYS);
        try {
            // Phase 1 — bills (LLM extraction)
            log.info("Init phase 1 — bill scan since={}", since);
            List<EmailMessage> bills = gmailService.searchUtilityBillEmails(
                    accessToken, account.billerName(), account.accountLastFour(), since);
            log.info("Found {} bill email(s) — extracting…", bills.size());
            for (EmailMessage email : bills) {
                extractAndPostBill(account, email);
            }

            // Phase 2 — payments (regex extraction), only after bills are posted
            log.info("Init phase 2 — payment scan since={}", since);
            List<EmailMessage> payments = gmailService.searchUtilityPaymentEmails(
                    accessToken, account.billerName(), account.accountLastFour(), since);
            log.info("Found {} payment email(s) — extracting…", payments.size());
            for (EmailMessage email : payments) {
                extractAndPostPayment(account, email);
            }

            // Mark init complete
            if (account.id() != null) {
                backendApiClient.markUtilityAccountInitComplete(account.id());
            }

            return new InitTaskResult(true, null);

        } catch (Exception e) {
            return new InitTaskResult(false, e.getMessage());
        }
    }

    // ── Bill extraction (same logic as UtilityBillExtractorActor) ────────────

    private void extractAndPostBill(UtilityAccountInfo account, EmailMessage email) {
        try {
            UtilityBillExtraction ex =
                    extractionService.extractUtilityBill(account.billerName(), email.body());

            if (!ex.isBill() || ex.confidence() < CONFIDENCE_THRESHOLD) {
                log.info(
                        "Skipping non-bill email — isBill={} confidence={} msgId={} subject={}",
                        ex.isBill(), ex.confidence(), email.messageId(), email.subject());
                return;
            }
            if (ex.amountDue() <= 0) {
                log.warn("Zero amountDue — skipping msgId={}", email.messageId());
                return;
            }

            String extractedLast4 = ex.accountLastFour() != null && ex.accountLastFour().length() >= 4
                    ? ex.accountLastFour().substring(ex.accountLastFour().length() - 4)
                    : ex.accountLastFour();
            // Only reject if the LLM extracted a last four AND it doesn't match.
            // If null, trust the Gmail search query which already filtered by account number.
            if (extractedLast4 != null && !extractedLast4.equals(account.accountLastFour())) {
                log.debug(
                        "Acct last four mismatch extracted={} registered={} — skipping msgId={}",
                        extractedLast4, account.accountLastFour(), email.messageId());
                return;
            }
            String last4ToPost = extractedLast4 != null ? extractedLast4 : account.accountLastFour();

            // If LLM didn't extract a billDate, fall back to the email's sent date
            // (Gmail internalDate) so Eversource bills always have a recognisable month heading.
            LocalDate billDate = parseDate(ex.billDate());
            if (billDate == null) {
                billDate = email.sentDate();
            }

            backendApiClient.postUtilityBill(
                    account.userId(),
                    email.messageId(),
                    account.billerName(),
                    last4ToPost,
                    BigDecimal.valueOf(ex.amountDue()),
                    parseDate(ex.dueDate()),
                    billDate,
                    parseDate(ex.billingPeriodStart()),
                    parseDate(ex.billingPeriodEnd()));

        } catch (Exception e) {
            log.warn("Bill extraction failed for msgId={}: {}",
                    email.messageId(), e.getMessage());
        }
    }

    // ── Payment extraction (same logic as UtilityPaymentExtractorActor) ───────
    // Regex duplicated here so the init actor is self-contained and doesn't
    // depend on a long-lived actor during the synchronous init scan.

    private static final Pattern EVERSOURCE_ACCT =
            Pattern.compile("Eversource Account Number Ending in:\\s*\\*([0-9]+)",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern NATIONAL_GRID_ACCT =
            Pattern.compile("Account Number with National Grid:\\s*[0-9]*([0-9]{4})\\*",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern PAYMENT_AMOUNT =
            Pattern.compile("Payment Amount:\\s*\\$([0-9,]+\\.[0-9]{2})",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern PAYMENT_DATE =
            Pattern.compile("Scheduled Payment Date:\\s*(\\d{2}/\\d{2}/\\d{4})",
                    Pattern.CASE_INSENSITIVE);
    /** MM/DD/YYYY formatter shared by payment-date regex parsing AND LLM date fallback. */
    private static final DateTimeFormatter MDY_FMT =
            DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.US);

    private void extractAndPostPayment(UtilityAccountInfo account, EmailMessage email) {
        try {
            String body = toPlainText(email.body());

            String acctLast4 = "EVERSOURCE".equals(account.billerName())
                    ? extractGroup(EVERSOURCE_ACCT, body)
                    : extractGroup(NATIONAL_GRID_ACCT, body);
            String amountStr = extractGroup(PAYMENT_AMOUNT, body);
            String dateStr   = extractGroup(PAYMENT_DATE,   body);

            if (amountStr == null || dateStr == null) {
                log.warn(
                        "Missing amount/date in payment email — msgId={}", email.messageId());
                return;
            }

            String lastFour = acctLast4 != null && acctLast4.length() >= 4
                    ? acctLast4.substring(acctLast4.length() - 4) : acctLast4;
            if (lastFour == null || !lastFour.equals(account.accountLastFour())) {
                log.debug(
                        "Payment acct mismatch extracted={} registered={} — msgId={}",
                        lastFour, account.accountLastFour(), email.messageId());
                return;
            }

            backendApiClient.postUtilityPayment(
                    account.userId(),
                    email.messageId(),
                    account.billerName(),
                    lastFour,
                    new BigDecimal(amountStr.replace(",", "")),
                    LocalDate.parse(dateStr, MDY_FMT));

        } catch (Exception e) {
            log.warn("Payment extraction failed for msgId={}: {}",
                    email.messageId(), e.getMessage());
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private LocalDate parseDate(String s) {
        if (s == null || s.isBlank() || s.equalsIgnoreCase("null")) return null;
        String v = s.trim();
        // Try ISO YYYY-MM-DD first (LLM is instructed to use this)
        try { return LocalDate.parse(v); } catch (Exception ignored) {}
        // Fall back to MM/DD/YYYY (LLM occasionally ignores the conversion instruction)
        try { return LocalDate.parse(v, MDY_FMT); } catch (Exception ignored) {}
        return null;
    }

    private String toPlainText(String body) {
        if (body == null) return "";
        return (body.contains("<") && body.contains(">")) ? Jsoup.parse(body).text() : body;
    }

    private String extractGroup(Pattern pattern, String text) {
        Matcher m = pattern.matcher(text);
        return m.find() ? m.group(1).trim() : null;
    }
}
