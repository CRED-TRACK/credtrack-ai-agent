package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.CardInfo;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Routes transaction emails to the correct bank-specific extractor child.
 *
 * Satisfies Akka requirement: <b>Forward pattern</b>.
 * Akka Typed has no {@code context.forward()} in the Java DSL; the equivalent
 * is passing the exact same message object to the child via {@code tell()}.
 * The {@code replyTo} embedded in {@link RouteTransaction} is preserved, so
 * the extractor replies directly to {@link TransactionInitActor} — the router
 * never touches the reply path, which is the definition of the Forward pattern.
 *
 * Child extractors are spawned once and reused for all routed messages.
 */
public class TransactionRouterActor extends AbstractBehavior<TransactionRouterActor.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /**
     * Route a transaction email to the appropriate bank-specific extractor.
     * The router forwards this message unchanged; the extractor replies to
     * {@code replyTo} directly without going back through the router.
     */
    public record RouteTransaction(
            String                      userId,
            String                      bankKey,
            String                      cardLastFour,
            List<CardInfo>              registeredCards,
            EmailMessage                email,
            ActorRef<TransactionResult> replyTo
    ) implements Command {}

    /** Sent by the child extractor directly to the original replyTo. */
    public record TransactionResult(boolean failed) {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(ExtractionService extractionService,
                                            BackendApiClient backendApiClient,
                                            Executor executor) {
        return Behaviors.setup(ctx ->
                new TransactionRouterActor(ctx, extractionService, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    /** Single LLM-based extractor handles all banks. */
    private final ActorRef<Command> llmExtractor;

    private TransactionRouterActor(ActorContext<Command> context,
                                   ExtractionService extractionService,
                                   BackendApiClient backendApiClient,
                                   Executor executor) {
        super(context);
        this.llmExtractor = context.spawn(
                LlmTransactionExtractorActor.create(extractionService, backendApiClient, executor),
                "llm-transaction-extractor");
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RouteTransaction.class, this::onRoute)
                .build();
    }

    private Behavior<Command> onRoute(RouteTransaction msg) {
        switch (msg.bankKey()) {
            case "CHASE", "BOA", "DISCOVER", "CITI", "CAPITAL_ONE", "WELLS_FARGO", "US_BANK" -> {
                // ── Forward pattern (Akka Typed) ──────────────────────────────
                // In Akka Typed, context.forward() does not exist in the Java DSL.
                // The equivalent is telling the child the exact same message object —
                // the replyTo embedded in msg is preserved, so LlmTransactionExtractorActor
                // replies directly to TransactionInitActor, bypassing this router entirely.
                // This is semantically identical to forward() in Classic Akka.
                llmExtractor.tell(msg);
            }
            case "AMEX" -> {
                // Amex sends weekly snapshot emails, not per-transaction alerts.
                // Per-transaction extraction is not applicable; skip gracefully.
                getContext().getLog().debug(
                        "Skipping Amex email {} — weekly snapshot, no per-transaction extraction",
                        msg.email().messageId());
                msg.replyTo().tell(new TransactionResult(false));
            }
            default -> {
                getContext().getLog().debug("No extractor configured for bank {}", msg.bankKey());
                msg.replyTo().tell(new TransactionResult(false));
            }
        }
        return this;
    }
}
