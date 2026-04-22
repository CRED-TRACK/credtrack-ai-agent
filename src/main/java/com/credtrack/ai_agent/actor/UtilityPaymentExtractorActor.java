package com.credtrack.ai_agent.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.service.BackendApiClient;
import org.jsoup.Jsoup;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Long-lived extractor for utility payment confirmation emails from Speedpay.
 * No LLM needed — Speedpay emails have a consistent structure across both billers.
 *
 * Eversource payment (from eversource_noreply@speedpay.com):
 *   Eversource Account Number Ending in: *8616
 *   Scheduled Payment Date: 04/17/2026
 *   Payment Amount: $66.14
 *
 * National Grid payment (from noreply@speedpay.com):
 *   Account Number with National Grid: 9364377069*
 *   Scheduled Payment Date: 04/16/2026
 *   Payment Amount: $13.18
 *
 */
public class UtilityPaymentExtractorActor
        extends AbstractBehavior<UtilityBillRouterActor.Command> {

    // ── Regex patterns ────────────────────────────────────────────────────────

    // Eversource: "Eversource Account Number Ending in: *8616"
    private static final Pattern EVERSOURCE_ACCT =
            Pattern.compile("Eversource Account Number Ending in:\\s*\\*([0-9]+)", Pattern.CASE_INSENSITIVE);

    // National Grid: "Account Number with National Grid: 9364377069*" → capture last 4 before *
    private static final Pattern NATIONAL_GRID_ACCT =
            Pattern.compile("Account Number with National Grid:\\s*[0-9]*([0-9]{4})\\*", Pattern.CASE_INSENSITIVE);

    // Shared: "Payment Amount: $66.14"
    private static final Pattern PAYMENT_AMOUNT =
            Pattern.compile("Payment Amount:\\s*\\$([0-9,]+\\.[0-9]{2})", Pattern.CASE_INSENSITIVE);

    // Shared: "Scheduled Payment Date: 04/17/2026"
    private static final Pattern PAYMENT_DATE =
            Pattern.compile("Scheduled Payment Date:\\s*(\\d{2}/\\d{2}/\\d{4})", Pattern.CASE_INSENSITIVE);

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.US);

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<UtilityBillRouterActor.Command> create(BackendApiClient backendApiClient,
                                                                   Executor executor) {
        return Behaviors.setup(ctx -> new UtilityPaymentExtractorActor(ctx, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final BackendApiClient backendApiClient;
    private final Executor         executor;

    private UtilityPaymentExtractorActor(ActorContext<UtilityBillRouterActor.Command> ctx,
                                          BackendApiClient backendApiClient,
                                          Executor executor) {
        super(ctx);
        this.backendApiClient = backendApiClient;
        this.executor         = executor;
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<UtilityBillRouterActor.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(UtilityBillRouterActor.RouteUtilityEmail.class, this::onRoute)
                .build();
    }

    private Behavior<UtilityBillRouterActor.Command> onRoute(UtilityBillRouterActor.RouteUtilityEmail msg) {
        CompletableFuture.runAsync(() -> {
            try {
                extractAndPost(msg);
            } catch (Exception ex) {
                getContext().getLog().warn("Utility payment extraction failed for {}: {}",
                        msg.email().messageId(), ex.getMessage());
            }
        }, executor);
        return this;
    }

    private void extractAndPost(UtilityBillRouterActor.RouteUtilityEmail msg) {
        String body = toPlainText(msg.email().body());

        // Extract account last four based on biller
        String accountLastFour = "EVERSOURCE".equals(msg.billerKey())
                ? extractGroup(EVERSOURCE_ACCT, body)
                : extractGroup(NATIONAL_GRID_ACCT, body);

        String amountStr  = extractGroup(PAYMENT_AMOUNT, body);
        String dateStr    = extractGroup(PAYMENT_DATE, body);

        if (amountStr == null || dateStr == null) {
            getContext().getLog().warn("Missing required fields in {} payment email — messageId={}",
                    msg.billerKey(), msg.email().messageId());
            return;
        }

        // Validate account last four matches the registered account
        String lastFour = accountLastFour != null && accountLastFour.length() >= 4
                ? accountLastFour.substring(accountLastFour.length() - 4) : accountLastFour;
        if (lastFour == null || !lastFour.equals(msg.accountLastFour())) {
            getContext().getLog().debug(
                    "Account last four mismatch for {} payment: extracted={} registered={} — skipping",
                    msg.billerKey(), lastFour, msg.accountLastFour());
            return;
        }

        BigDecimal amount = new BigDecimal(amountStr.replace(",", ""));
        LocalDate  date   = LocalDate.parse(dateStr, DATE_FMT);

        backendApiClient.postUtilityPayment(
                msg.userId(),
                msg.email().messageId(),
                msg.billerKey(),
                lastFour,
                amount,
                date);
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
