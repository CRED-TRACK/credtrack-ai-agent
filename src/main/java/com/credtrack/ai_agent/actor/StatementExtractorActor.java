package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.CardInfo;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.model.StatementExtraction;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Spawned once per eligible email. Calls the LLM via ExtractionService,
 * then forwards a fully resolved WriteStatement to StatementWriterActor.
 * Stops itself when done.
 *
 * Bank and card are resolved BEFORE this actor is spawned —
 * the LLM only extracts numbers, dates, and URLs.
 *
 * Before stopping, sends an {@link EmailFetcherActor.LlmExtractorResult} back to
 * the parent EmailFetcherActor so it knows whether the LLM call succeeded.
 * If the LLM was unavailable (connection refused, timeout, etc.), {@code llmFailed=true}
 * is reported and the parent will suppress historyId advancement — causing the
 * email to be retried on the next poll cycle.
 */
public class StatementExtractorActor extends AbstractBehavior<StatementExtractorActor.Command> {

    private static final double CONFIDENCE_THRESHOLD = 0.7;

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record ExtractStatement(
            String         userId,
            String         bankKey,
            String         cardLastFour,      // resolved from backend cards list
            List<CardInfo> registeredCards,   // all registered cards for this user+bank — used to validate extracted digits
            EmailMessage   email,
            ActorRef<StatementWriterActor.Command>          writer,
            ActorRef<EmailFetcherActor.LlmExtractorResult> replyTo  // receives LlmExtractorResult before this actor stops
    ) implements Command {}

    private record ExtractionDone(StatementExtraction result, ExtractStatement original) implements Command {}
    private record ExtractionFailed(String messageId, String reason) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(ExtractionService extractionService,
                                           BackendApiClient backendApiClient,
                                           Executor executor) {
        return Behaviors.setup(ctx -> new StatementExtractorActor(ctx, extractionService, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final ExtractionService extractionService;
    private final BackendApiClient  backendApiClient;
    private final Executor          executor;

    // Stored on first message so onFailed can reply even though it only has messageId.
    private ActorRef<EmailFetcherActor.LlmExtractorResult> replyTo;

    private StatementExtractorActor(ActorContext<Command> context,
                                    ExtractionService extractionService,
                                    BackendApiClient backendApiClient,
                                    Executor executor) {
        super(context);
        this.extractionService = extractionService;
        this.backendApiClient  = backendApiClient;
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
        // Capture replyTo so onFailed can use it (it only receives messageId + reason).
        this.replyTo = msg.replyTo();

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
            // Legitimate skip — LLM responded normally, just not a statement email.
            src.replyTo().tell(new EmailFetcherActor.LlmExtractorResult(false));
            return Behaviors.stopped();
        }

        // Take last 4 of whatever digits the email showed (handles Amex 5-digit display)
        String digits = ex.getCardDigits() != null ? ex.getCardDigits().replaceAll("\\D", "") : "";
        String lastFour = digits.length() >= 4
                ? digits.substring(digits.length() - 4)
                : src.cardLastFour();   // fallback to card matched before LLM call

        // Reject if the extracted card digits don't match any registered card.
        // Uses endsWith to handle banks that display extra leading digits (e.g. Amex
        // shows 5 digits "51006" but registered lastFour may be stored as "51006" —
        // LLM extracts "51006" → lastFour="1006", and "51006".endsWith("1006") = true).
        CardInfo matchedCard = src.registeredCards() == null ? null :
                src.registeredCards().stream()
                        .filter(c -> c.lastFour().endsWith(lastFour))
                        .findFirst().orElse(null);
        if (matchedCard == null) {
            getContext().getLog().info(
                    "Skipping {} — card digits {} not in registered cards",
                    src.email().messageId(), lastFour);
            // LLM responded fine — we just don't have a matching card.
            src.replyTo().tell(new EmailFetcherActor.LlmExtractorResult(false));
            return Behaviors.stopped();
        }
        // Use the registered card's lastFour by default; upgrade to full extracted
        // digits if the bank displays more (e.g. Amex "51006" vs stored "1006").
        String cardLastFourForWrite = matchedCard.lastFour();
        if (digits.length() > matchedCard.lastFour().length()
                && digits.endsWith(matchedCard.lastFour())) {
            // Auto-save the 5-digit display number so future Gmail searches use it
            cardLastFourForWrite = digits;
            Long cardIdToUpdate = matchedCard.cardId();
            CompletableFuture.runAsync(
                    () -> backendApiClient.updateCardLastFour(cardIdToUpdate, digits), executor);
            getContext().getLog().info("Auto-updating card {} lastFour {} → {}",
                    cardIdToUpdate, matchedCard.lastFour(), digits);
        }

        // Prefer LLM-extracted statementDate — it reflects the actual closing date printed
        // in the email. Some banks (e.g. BOA) send the notification 2-4 days after closing,
        // so using the email's sentDate would record the wrong date.
        // Fall back to sentDate only when the LLM didn't extract a date.
        java.time.LocalDate llmDate = parseDate(ex.getStatementDate());
        java.time.LocalDate statementDate = llmDate != null ? llmDate : src.email().sentDate();

        // Prefer URLs extracted directly from HTML (full, untruncated) over LLM output
        String viewUrl = src.email().viewStatementUrl() != null
                ? src.email().viewStatementUrl() : ex.getViewStatementUrl();
        String payUrl  = src.email().makePaymentUrl() != null
                ? src.email().makePaymentUrl()  : ex.getMakePaymentUrl();

        src.writer().tell(new StatementWriterActor.WriteStatement(
                src.userId(),
                src.email().messageId(),
                cardLastFourForWrite,
                src.bankKey(),
                toBigDecimal(ex.getStatementBalance()),
                toBigDecimal(ex.getMinimumPaymentDue()),
                statementDate,
                parseDate(ex.getDueDate()),
                viewUrl,
                payUrl,
                matchedCard.cardId()   // passed to ledger for unbilled-spend tracking
        ));

        // LLM responded and data was forwarded for writing — success.
        src.replyTo().tell(new EmailFetcherActor.LlmExtractorResult(false));
        return Behaviors.stopped();
    }

    private Behavior<Command> onFailed(ExtractionFailed msg) {
        getContext().getLog().error("LLM extraction failed for {}: {}", msg.messageId(), msg.reason());
        // Signal that the LLM was unavailable — parent will suppress historyId advance.
        if (replyTo != null) {
            replyTo.tell(new EmailFetcherActor.LlmExtractorResult(true));
        }
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
