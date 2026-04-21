package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Akka Agents substitute for Java.
 *
 * {@code akka.agent.Agent} was removed in Akka 2.5+.  This actor fulfills the
 * same role: a shared, concurrency-safe holder of a mutable {@link SpendSnapshot}
 * value.
 *
 * Satisfies Akka requirement: <b>Akka Agents</b>.
 * Pattern: actor serialises writes; {@code AtomicReference.get()} provides
 * lock-free reads from any thread without going through the actor mailbox.
 *
 * Supervised with {@code SupervisorStrategy.restart()} — the snapshot is
 * recomputable from backend data, so loss on crash is acceptable.
 */
public class AnalyticsAgentHolder extends AbstractBehavior<AnalyticsAgentHolder.Command> {

    // ── Snapshot model ────────────────────────────────────────────────────────

    /**
     * Latest unbilled totals per card, keyed by userCardId.
     * Immutable record — the AtomicReference is swapped atomically on update.
     */
    public record SpendSnapshot(Map<Long, Double> unbilledByCard) {
        public static SpendSnapshot empty() {
            return new SpendSnapshot(Map.of());
        }

        public SpendSnapshot withUpdate(Long cardId, Double amount) {
            Map<Long, Double> updated = new HashMap<>(unbilledByCard);
            updated.put(cardId, amount);
            return new SpendSnapshot(Map.copyOf(updated));
        }

        public double get(Long cardId) {
            return unbilledByCard.getOrDefault(cardId, 0.0);
        }
    }

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /** Tell the agent to update its cached unbilled total for a card. */
    public record UpdateSnapshot(Long cardId, double unbilledTotal) implements Command {}

    /** Ask the agent for the current snapshot. Reply is sent to replyTo. */
    public record ReadSnapshot(ActorRef<SpendSnapshot> replyTo) implements Command {}

    // ── Static read API (lock-free, no actor roundtrip needed) ────────────────

    /**
     * Lock-free read path — any thread can call this without sending a message.
     * Returns the last atomically written SpendSnapshot.
     */
    public static SpendSnapshot currentSnapshot() {
        return SNAPSHOT_REF.get();
    }

    private static final AtomicReference<SpendSnapshot> SNAPSHOT_REF =
            new AtomicReference<>(SpendSnapshot.empty());

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create() {
        return Behaviors.supervise(
                Behaviors.setup(AnalyticsAgentHolder::new))
                .onFailure(Exception.class, SupervisorStrategy.restart());
    }

    // ── Constructor ───────────────────────────────────────────────────────────

    private AnalyticsAgentHolder(ActorContext<Command> context) {
        super(context);
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(UpdateSnapshot.class, this::onUpdate)
                .onMessage(ReadSnapshot.class,   this::onRead)
                .build();
    }

    private Behavior<Command> onUpdate(UpdateSnapshot msg) {
        // Actor serialises writes — only one UpdateSnapshot processed at a time,
        // so the CAS loop is effectively uncontested.
        SNAPSHOT_REF.updateAndGet(snap -> snap.withUpdate(msg.cardId(), msg.unbilledTotal()));
        getContext().getLog().debug("Snapshot updated: card {} → {}", msg.cardId(), msg.unbilledTotal());
        return this;
    }

    private Behavior<Command> onRead(ReadSnapshot msg) {
        msg.replyTo().tell(SNAPSHOT_REF.get());
        return this;
    }
}
