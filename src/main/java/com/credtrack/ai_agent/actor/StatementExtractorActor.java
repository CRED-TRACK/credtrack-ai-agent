package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.model.StatementExtraction;
import com.credtrack.ai_agent.service.ExtractionService;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Spawned once per eligible email. Calls the LLM via ExtractionService,
 * then forwards a fully resolved WriteStatement to StatementWriterActor.
 * Stops itself when done.
 *
 * Bank and card are resolved BEFORE this actor is spawned —
 * the LLM only extracts numbers, dates, and URLs.
 */
public class StatementExtractorActor extends AbstractBehavior<StatementExtractorActor.Command> {

    private static final double CONFIDENCE_THRESHOLD = 0.7;

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record ExtractStatement(
            String  userId,
            String  bankKey,
            String  cardLastFour,          // resolved from backend cards list
            EmailMessage email,
            ActorRef<StatementWriterActor.Command> writer
    ) implements Command {}

    private record ExtractionDone(StatementExtraction result, ExtractStatement original) implements Command {}
    private record ExtractionFailed(String messageId, String reason) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(ExtractionService extractionService, Executor executor) {
        return Behaviors.setup(ctx -> new StatementExtractorActor(ctx, extractionService, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final ExtractionService extractionService;
    private final Executor          executor;

    private StatementExtractorActor(ActorContext<Command> context,
                                    ExtractionService extractionService,
                                    Executor executor) {
        super(context);
        this.extractionService = extractionService;
        this.executor          = executor;
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ExtractStatement.class,  this::onExtract)
                .onMessage(ExtractionDone.class,    this::onDone)
                .onMessage(ExtractionFailed.class,  this::onFailed)
                .build();
    }

    private Behavior<Command> onExtract(ExtractStatement msg) {
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(() ->
                        extractionService.extract(msg.bankKey(), msg.email().body()), executor
                ),
                (result, ex) -> ex != null
                        ? new ExtractionFailed(msg.email().messageId(), ex.getMessage())
                        : new ExtractionDone(result, msg)
        );
        return this;
    }

    private Behavior<Command> onDone(ExtractionDone msg) {
        StatementExtraction ex  = msg.result();
        ExtractStatement    src = msg.original();

        if (!ex.isStatement() || ex.getConfidence() == null || ex.getConfidence() < CONFIDENCE_THRESHOLD) {
            getContext().getLog().debug(
                    "Skipping {} — isStatement={} confidence={}",
                    src.email().messageId(), ex.isStatement(), ex.getConfidence());
            return Behaviors.stopped();
        }

        // Take last 4 of whatever digits the email showed (handles Amex 5-digit display)
        String digits = ex.getCardDigits() != null ? ex.getCardDigits().replaceAll("\\D", "") : "";
        String lastFour = digits.length() >= 4
                ? digits.substring(digits.length() - 4)
                : src.cardLastFour();   // fallback to card matched before LLM call

        // statementDate = email sent date (Chase sends the email on the statement closing date)
        // Fall back to LLM-extracted statementDate only if sentDate is unavailable
        java.time.LocalDate statementDate = src.email().sentDate() != null
                ? src.email().sentDate()
                : parseDate(ex.getStatementDate());

        // Prefer URLs extracted directly from HTML (full, untruncated) over LLM output
        String viewUrl = src.email().viewStatementUrl() != null
                ? src.email().viewStatementUrl() : ex.getViewStatementUrl();
        String payUrl  = src.email().makePaymentUrl() != null
                ? src.email().makePaymentUrl()  : ex.getMakePaymentUrl();

        src.writer().tell(new StatementWriterActor.WriteStatement(
                src.userId(),
                src.email().messageId(),
                lastFour,
                src.bankKey(),
                toBigDecimal(ex.getStatementBalance()),
                toBigDecimal(ex.getMinimumPaymentDue()),
                statementDate,
                parseDate(ex.getDueDate()),
                viewUrl,
                payUrl
        ));

        return Behaviors.stopped();
    }

    private Behavior<Command> onFailed(ExtractionFailed msg) {
        getContext().getLog().error("Extraction failed for {}: {}", msg.messageId(), msg.reason());
        return Behaviors.stopped();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private BigDecimal toBigDecimal(Double value) {
        return value != null ? BigDecimal.valueOf(value) : null;
    }

    private LocalDate parseDate(String dateStr) {
        if (dateStr == null || dateStr.isBlank()) return null;
        try {
            return LocalDate.parse(dateStr);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
