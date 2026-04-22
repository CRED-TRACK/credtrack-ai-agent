package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;

import java.util.concurrent.Executor;

/**
 * Routes utility emails to the correct child extractor using the forward pattern:
 * the router receives {@link RouteUtilityEmail} and tells the same message object
 * to the appropriate child, preserving {@code userId}, {@code billerKey}, and
 * {@code accountLastFour} so the extractor has full context.
 *
 * Two persistent child actors are pre-spawned:
 * - {@link UtilityBillExtractorActor}    — LLM-based, for bill emails
 * - {@link UtilityPaymentExtractorActor} — regex-based, for Speedpay payment confirmations
 */
public class UtilityBillRouterActor extends AbstractBehavior<UtilityBillRouterActor.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /**
     * Route a utility email to the correct extractor.
     * The router forwards this message unchanged — both child extractors
     * handle {@link RouteUtilityEmail} directly.
     */
    public record RouteUtilityEmail(
            String       userId,
            String       billerKey,       // EVERSOURCE or NATIONAL_GRID
            String       accountLastFour,
            String       emailType,       // "BILL" or "PAYMENT" — determined by coordinator
            EmailMessage email
    ) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(ExtractionService extractionService,
                                            BackendApiClient backendApiClient,
                                            Executor executor) {
        return Behaviors.setup(ctx ->
                new UtilityBillRouterActor(ctx, extractionService, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final ActorRef<Command> billExtractor;
    private final ActorRef<Command> paymentExtractor;

    private UtilityBillRouterActor(ActorContext<Command> ctx,
                                    ExtractionService extractionService,
                                    BackendApiClient backendApiClient,
                                    Executor executor) {
        super(ctx);
        this.billExtractor = ctx.spawn(
                UtilityBillExtractorActor.create(extractionService, backendApiClient, executor),
                "utility-bill-extractor");
        this.paymentExtractor = ctx.spawn(
                UtilityPaymentExtractorActor.create(backendApiClient, executor),
                "utility-payment-extractor");
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RouteUtilityEmail.class, this::onRoute)
                .build();
    }

    private Behavior<Command> onRoute(RouteUtilityEmail msg) {
        switch (msg.emailType()) {
            case "BILL"    -> billExtractor.tell(msg);
            case "PAYMENT" -> paymentExtractor.tell(msg);
            default -> getContext().getLog().warn(
                    "Unknown emailType '{}' for messageId={} — skipping",
                    msg.emailType(), msg.email().messageId());
        }
        return this;
    }
}
