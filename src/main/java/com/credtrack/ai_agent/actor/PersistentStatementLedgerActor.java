package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;

import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Durable ledger of the latest statement closing date per card.
 * Uses Akka Persistence (EventSourcedBehavior) so the state survives restarts
 * without re-querying the database.
 *
 * Satisfies requirement: Akka Persistence.
 */
public class PersistentStatementLedgerActor
        extends EventSourcedBehavior<
                PersistentStatementLedgerActor.Command,
                PersistentStatementLedgerActor.StatementClosedEvent,
                PersistentStatementLedgerActor.LedgerState> {

    // ── Commands ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record RecordStatementClosed(
            String userId,
            Long   userCardId,
            LocalDate statementDate
    ) implements Command {}

    public record QueryLatestStatementDate(
            Long                           userCardId,
            ActorRef<LatestStatementDateReply> replyTo
    ) implements Command {}

    // ── Reply ─────────────────────────────────────────────────────────────────

    public record LatestStatementDateReply(Long userCardId, LocalDate latestStatementDate) {}

    // ── Event ─────────────────────────────────────────────────────────────────

    public record StatementClosedEvent(
            String    userId,
            Long      userCardId,
            LocalDate statementDate
    ) implements java.io.Serializable {}

    // ── State ─────────────────────────────────────────────────────────────────

    public record LedgerState(Map<Long, LocalDate> latestDates) {
        public static LedgerState empty() {
            return new LedgerState(Collections.emptyMap());
        }

        public LedgerState withEvent(StatementClosedEvent e) {
            LocalDate existing = latestDates.get(e.userCardId());
            if (existing != null && !e.statementDate().isAfter(existing)) {
                return this; // keep existing if newer or equal
            }
            Map<Long, LocalDate> updated = new HashMap<>(latestDates);
            updated.put(e.userCardId(), e.statementDate());
            return new LedgerState(Collections.unmodifiableMap(updated));
        }
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create() {
        return new PersistentStatementLedgerActor();
    }

    private PersistentStatementLedgerActor() {
        super(PersistenceId.ofUniqueId("statement-ledger"));
    }

    // ── Initial state ─────────────────────────────────────────────────────────

    @Override
    public LedgerState emptyState() {
        return LedgerState.empty();
    }

    // ── Command handler ───────────────────────────────────────────────────────

    @Override
    public CommandHandler<Command, StatementClosedEvent, LedgerState> commandHandler() {
        return newCommandHandlerBuilder()
                .forAnyState()
                .onCommand(RecordStatementClosed.class, (state, cmd) ->
                        Effect().persist(new StatementClosedEvent(
                                cmd.userId(), cmd.userCardId(), cmd.statementDate()))
                )
                .onCommand(QueryLatestStatementDate.class, (state, cmd) ->
                        Effect().none().thenReply(cmd.replyTo(), s ->
                                new LatestStatementDateReply(
                                        cmd.userCardId(),
                                        s.latestDates().get(cmd.userCardId())))
                )
                .build();
    }

    // ── Event handler ─────────────────────────────────────────────────────────

    @Override
    public EventHandler<LedgerState, StatementClosedEvent> eventHandler() {
        return newEventHandlerBuilder()
                .forAnyState()
                .onEvent(StatementClosedEvent.class, LedgerState::withEvent)
                .build();
    }
}
