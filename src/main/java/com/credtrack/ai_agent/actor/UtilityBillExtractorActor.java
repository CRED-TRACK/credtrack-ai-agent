package com.credtrack.ai_agent.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.UtilityBillExtraction;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Long-lived extractor for utility bill emails (Eversource / National Grid).
 * Uses the LLM via {@link ExtractionService#extractUtilityBill} — bill emails
 * are HTML-heavy with variable layouts, making regex unreliable.
 *
 */
public class UtilityBillExtractorActor
        extends AbstractBehavior<UtilityBillRouterActor.Command> {

    private static final double CONFIDENCE_THRESHOLD = 0.7;

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<UtilityBillRouterActor.Command> create(ExtractionService extractionService,
                                                                    BackendApiClient backendApiClient,
                                                                    Executor executor) {
        return Behaviors.setup(ctx ->
                new UtilityBillExtractorActor(ctx, extractionService, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final ExtractionService extractionService;
    private final BackendApiClient  backendApiClient;
    private final Executor          executor;

    private UtilityBillExtractorActor(ActorContext<UtilityBillRouterActor.Command> ctx,
                                       ExtractionService extractionService,
                                       BackendApiClient backendApiClient,
                                       Executor executor) {
        super(ctx);
        this.extractionService = extractionService;
        this.backendApiClient  = backendApiClient;
        this.executor          = executor;
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
                getContext().getLog().warn("Utility bill extraction failed for {}: {}",
                        msg.email().messageId(), ex.getMessage());
            }
        }, executor);
        return this;
    }

    private void extractAndPost(UtilityBillRouterActor.RouteUtilityEmail msg) {
        UtilityBillExtraction ex = extractionService.extractUtilityBill(
                msg.billerKey(), msg.email().body());

        if (!ex.isBill() || ex.confidence() < CONFIDENCE_THRESHOLD) {
            getContext().getLog().info(
                    "Skipping non-bill email — isBill={} confidence={} messageId={} subject={}",
                    ex.isBill(), ex.confidence(), msg.email().messageId(), msg.email().subject());
            return;
        }

        if (ex.amountDue() <= 0) {
            getContext().getLog().warn("Utility bill has zero amountDue — skipping messageId={}",
                    msg.email().messageId());
            return;
        }

        // Validate extracted account last four against the registered account.
        // If null, the LLM couldn't find it in the email body — trust the Gmail
        // search query which already filtered by account number.
        String extractedLast4 = ex.accountLastFour() != null && ex.accountLastFour().length() >= 4
                ? ex.accountLastFour().substring(ex.accountLastFour().length() - 4)
                : ex.accountLastFour();
        if (extractedLast4 != null && !extractedLast4.equals(msg.accountLastFour())) {
            getContext().getLog().debug(
                    "Account last four mismatch for {} bill: extracted={} registered={} — skipping",
                    msg.billerKey(), extractedLast4, msg.accountLastFour());
            return;
        }
        String last4ToPost = extractedLast4 != null ? extractedLast4 : msg.accountLastFour();

        backendApiClient.postUtilityBill(
                msg.userId(),
                msg.email().messageId(),
                msg.billerKey(),
                last4ToPost,
                BigDecimal.valueOf(ex.amountDue()),
                parseDate(ex.dueDate()),
                parseDate(ex.billDate()),
                parseDate(ex.billingPeriodStart()),
                parseDate(ex.billingPeriodEnd()));
    }

    private static final DateTimeFormatter MDY_FMT =
            DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.US);

    private LocalDate parseDate(String s) {
        if (s == null || s.isBlank() || s.equalsIgnoreCase("null")) return null;
        String v = s.trim();
        // Try ISO YYYY-MM-DD first (LLM is instructed to use this)
        try { return LocalDate.parse(v); } catch (Exception ignored) {}
        // Fall back to MM/DD/YYYY (LLM occasionally ignores the conversion instruction)
        try { return LocalDate.parse(v, MDY_FMT); } catch (Exception ignored) {}
        return null;
    }
}
