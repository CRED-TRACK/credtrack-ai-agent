package com.credtrack.ai_agent.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.service.BackendApiClient;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Singleton actor — receives fully resolved statement data and writes it
 * to the existing backend via POST /internal/statements.
 * Fire-and-forget: does not block the pipeline waiting for confirmation.
 */
public class StatementWriterActor extends AbstractBehavior<StatementWriterActor.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record WriteStatement(
            String     userId,
            String     gmailMessageId,
            String     cardLastFour,
            String     bankKey,
            BigDecimal statementBalance,
            BigDecimal minimumDue,
            LocalDate  statementDate,
            LocalDate  dueDate,
            String     viewStatementUrl,
            String     makePaymentUrl
    ) implements Command {}

    private record WriteDone(String gmailMessageId, boolean isNew) implements Command {}
    private record WriteFailed(String gmailMessageId, String reason) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(BackendApiClient backendApiClient, Executor executor) {
        return Behaviors.setup(ctx -> new StatementWriterActor(ctx, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final BackendApiClient backendApiClient;
    private final Executor         executor;

    private StatementWriterActor(ActorContext<Command> context,
                                 BackendApiClient backendApiClient,
                                 Executor executor) {
        super(context);
        this.backendApiClient = backendApiClient;
        this.executor         = executor;
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WriteStatement.class, this::onWrite)
                .onMessage(WriteDone.class,      msg -> {
                    if (msg.isNew()) getContext().getLog().info("Saved: {}", msg.gmailMessageId());
                    else            getContext().getLog().debug("Duplicate skipped: {}", msg.gmailMessageId());
                    return this;
                })
                .onMessage(WriteFailed.class,    msg -> { getContext().getLog().error("Save failed {}: {}", msg.gmailMessageId(), msg.reason()); return this; })
                .build();
    }

    private Behavior<Command> onWrite(WriteStatement msg) {
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    boolean isNew = backendApiClient.postStatement(
                            msg.userId(), msg.gmailMessageId(), msg.cardLastFour(),
                            msg.bankKey(), msg.statementBalance(), msg.minimumDue(),
                            msg.statementDate(), msg.dueDate(),
                            msg.viewStatementUrl(), msg.makePaymentUrl());
                    return isNew;
                }, executor),
                (result, ex) -> ex != null
                        ? new WriteFailed(msg.gmailMessageId(), ex.getMessage())
                        : new WriteDone(msg.gmailMessageId(), result)
        );
        return this;
    }
}
