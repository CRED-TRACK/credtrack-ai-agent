package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.actor.AnalyticsAgentHolder;
import com.credtrack.ai_agent.service.BackendApiClient;

import java.time.Duration;
import java.time.LocalDate;
import java.util.concurrent.Executor;

/**
 * Computes current unbilled spend for a card by:
 *   1. Asking PersistentStatementLedgerActor for the latest statement closing date (Ask pattern).
 *   2. Calling the backend to sum transactions since that date.
 *
 * Satisfies requirement: Ask pattern (actor-to-actor).
 */
public class AnalyticsAggregatorActor extends AbstractBehavior<AnalyticsAggregatorActor.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record ComputeUnbilledSpend(
            String                        userId,
            Long                          userCardId,
            ActorRef<UnbilledSpendReply>  replyTo
    ) implements Command {}

    public record UnbilledSpendReply(Long userCardId, LocalDate since, double unbilledTotal) {}

    // Internal — piped back from Ask to the ledger
    private record LedgerDateReceived(
            Long                         userCardId,
            LocalDate                    closingDate,
            ActorRef<UnbilledSpendReply> originalReplyTo,
            String                       userId
    ) implements Command {}

    private record LedgerAskFailed(
            Long                         userCardId,
            ActorRef<UnbilledSpendReply> originalReplyTo
    ) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(
            ActorRef<PersistentStatementLedgerActor.Command> ledger,
            BackendApiClient backendApiClient,
            Executor executor,
            ActorRef<AnalyticsAgentHolder.Command> agentHolder) {
        return Behaviors.setup(ctx ->
                new AnalyticsAggregatorActor(ctx, ledger, backendApiClient, executor, agentHolder));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final ActorRef<PersistentStatementLedgerActor.Command> ledger;
    private final BackendApiClient                                   backendApiClient;
    private final Executor                                           executor;
    private final ActorRef<AnalyticsAgentHolder.Command>             agentHolder;

    private AnalyticsAggregatorActor(ActorContext<Command> context,
                                     ActorRef<PersistentStatementLedgerActor.Command> ledger,
                                     BackendApiClient backendApiClient,
                                     Executor executor,
                                     ActorRef<AnalyticsAgentHolder.Command> agentHolder) {
        super(context);
        this.ledger           = ledger;
        this.backendApiClient = backendApiClient;
        this.executor         = executor;
        this.agentHolder      = agentHolder;
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ComputeUnbilledSpend.class,  this::onCompute)
                .onMessage(LedgerDateReceived.class,    this::onDateReceived)
                .onMessage(LedgerAskFailed.class,       this::onAskFailed)
                .build();
    }

    private Behavior<Command> onCompute(ComputeUnbilledSpend msg) {
        // ── Ask pattern: actor-to-actor ───────────────────────────────────────
        // Query the persistent ledger for the latest closing date for this card.
        // The reply is piped back as LedgerDateReceived so this actor stays non-blocking.
        getContext().pipeToSelf(
                AskPattern.<PersistentStatementLedgerActor.Command,
                            PersistentStatementLedgerActor.LatestStatementDateReply>ask(
                        ledger,
                        (ActorRef<PersistentStatementLedgerActor.LatestStatementDateReply> replyTo) ->
                                new PersistentStatementLedgerActor.QueryLatestStatementDate(
                                        msg.userCardId(), replyTo),
                        Duration.ofSeconds(3),
                        getContext().getSystem().scheduler()
                ),
                (reply, ex) -> ex != null
                        ? new LedgerAskFailed(msg.userCardId(), msg.replyTo())
                        : new LedgerDateReceived(msg.userCardId(), reply.latestStatementDate(),
                                                 msg.replyTo(), msg.userId())
        );
        return this;
    }

    private Behavior<Command> onDateReceived(LedgerDateReceived msg) {
        LocalDate since = msg.closingDate();
        double total = 0.0;

        if (since != null) {
            try {
                total = backendApiClient.getUnbilledTotal(msg.userCardId());
            } catch (Exception e) {
                getContext().getLog().warn("Failed to fetch unbilled total for card {}: {}",
                        msg.userCardId(), e.getMessage());
            }
        }

        msg.originalReplyTo().tell(new UnbilledSpendReply(msg.userCardId(), since, total));

        // Update the AtomicReference-backed agent so any thread can read the latest total
        agentHolder.tell(new AnalyticsAgentHolder.UpdateSnapshot(msg.userCardId(), total));
        return this;
    }

    private Behavior<Command> onAskFailed(LedgerAskFailed msg) {
        getContext().getLog().warn("Ledger ask timed out for card {} — returning zero", msg.userCardId());
        msg.originalReplyTo().tell(new UnbilledSpendReply(msg.userCardId(), null, 0.0));
        return this;
    }
}
