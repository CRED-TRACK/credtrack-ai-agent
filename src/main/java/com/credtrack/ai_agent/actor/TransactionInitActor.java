package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.CardInfo;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import com.credtrack.ai_agent.service.GmailService;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Performs a one-time transaction email backfill for a user after their init scan completes.
 *
 * For each card, searches Gmail for transaction alert emails covering:
 *   - The current unbilled cycle (since last statement date)
 *   - The previous billing cycle (one month prior)
 *
 * Uses a supervised {@link TransactionRouterActor} to route each email to the
 * correct bank-specific extractor via the Forward pattern.
 *
 * Satisfies Akka requirements:
 *   - <b>Supervision</b>: TransactionRouterActor is supervised with restartWithBackoff
 *     so transient failures (network, backend timeout) are retried automatically.
 *   - <b>Forward</b>: TransactionRouterActor forwards RouteTransaction to child
 *     extractors preserving the original replyTo.
 *
 * Cards are processed sequentially to respect Gmail API rate limits.
 * Stops itself when all cards are processed.
 */
public class TransactionInitActor extends AbstractBehavior<TransactionInitActor.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /** Sent by EmailPipelineCoordinator to start the backfill. */
    public record RunTransactionBackfill(
            String                                     userId,
            String                                     accessToken,
            List<CardInfo>                             allCards,
            ActorRef<EmailPipelineCoordinator.Command> coordinator
    ) implements Command {}

    private record TransactionEmailsFetched(List<EmailMessage> emails)  implements Command {}
    private record SearchFailed(String reason)                          implements Command {}

    // Adapter: TransactionRouterActor.TransactionResult → TransactionDone
    private record TransactionDone(boolean failed) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(GmailService gmailService,
                                           BackendApiClient backendApiClient,
                                           ExtractionService extractionService,
                                           Executor executor,
                                           long scanIntervalMinutes) {
        return Behaviors.setup(ctx ->
                new TransactionInitActor(ctx, gmailService, backendApiClient, extractionService, executor, scanIntervalMinutes));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final GmailService      gmailService;
    private final BackendApiClient  backendApiClient;
    private final ExtractionService extractionService;
    private final Executor          executor;
    private final long              scanIntervalMinutes;

    /**
     * Supervised router — restarted with backoff on any RuntimeException so
     * transient failures don't abort the whole backfill.
     *
     * Satisfies Akka Supervision requirement.
     */
    private final ActorRef<TransactionRouterActor.Command> router;

    /** Message adapter to receive TransactionResult from the router/extractor. */
    private final ActorRef<TransactionRouterActor.TransactionResult> resultAdapter;

    // Saved on RunTransactionBackfill
    private String                                     userId;
    private String                                     accessToken;
    private ActorRef<EmailPipelineCoordinator.Command> coordinator;

    // Per-card processing state
    private Queue<CardInfo> pendingCards;
    private CardInfo        currentCard;
    private int             pendingRoutes;

    // ── Constructor ───────────────────────────────────────────────────────────

    private TransactionInitActor(ActorContext<Command> context,
                                 GmailService gmailService,
                                 BackendApiClient backendApiClient,
                                 ExtractionService extractionService,
                                 Executor executor,
                                 long scanIntervalMinutes) {
        super(context);
        this.gmailService        = gmailService;
        this.backendApiClient    = backendApiClient;
        this.extractionService   = extractionService;
        this.executor            = executor;
        this.scanIntervalMinutes = scanIntervalMinutes;

        // Supervised router: restart with backoff on RuntimeException
        this.router = context.spawn(
                Behaviors.supervise(TransactionRouterActor.create(extractionService, backendApiClient, executor))
                         .onFailure(RuntimeException.class,
                                 SupervisorStrategy.restartWithBackoff(
                                         Duration.ofSeconds(1), Duration.ofSeconds(30), 0.2)),
                "transaction-router");

        // Adapt TransactionResult → TransactionDone so this actor handles it
        this.resultAdapter = context.messageAdapter(
                TransactionRouterActor.TransactionResult.class,
                r -> new TransactionDone(r.failed()));
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RunTransactionBackfill.class,    this::onRun)
                .onMessage(TransactionEmailsFetched.class,  this::onEmailsFetched)
                .onMessage(SearchFailed.class,              this::onSearchFailed)
                .onMessage(TransactionDone.class,           this::onTransactionDone)
                .onSignal(Terminated.class,                 sig -> Behaviors.same())
                .build();
    }

    private Behavior<Command> onRun(RunTransactionBackfill msg) {
        this.userId       = msg.userId();
        this.accessToken  = msg.accessToken();
        this.coordinator  = msg.coordinator();
        // Only process cards for banks that send per-transaction alerts
        this.pendingCards = new ArrayDeque<>(
                msg.allCards().stream()
                   .filter(c -> isTransactionAlertBank(c.bankKey()))
                   .toList());

        getContext().getLog().info(
                "Transaction backfill started for user {} — {} card(s)",
                userId, pendingCards.size());
        return nextCard();
    }

    private Behavior<Command> onEmailsFetched(TransactionEmailsFetched msg) {
        List<EmailMessage> emails = msg.emails();
        getContext().getLog().info(
                "Found {} transaction email(s) for card {} user {}",
                emails.size(), currentCard.cardId(), userId);

        if (emails.isEmpty()) {
            // No emails found — still mark as scanned so we respect the interval
            Long cardId = currentCard.cardId();
            CompletableFuture.runAsync(
                    () -> backendApiClient.updateLastTransactionScanAt(cardId), executor);
            return nextCard();
        }

        List<CardInfo> bankCards = pendingCards.stream()
                .filter(c -> currentCard.bankKey().equals(c.bankKey()))
                .toList();
        // Also include currentCard itself (it's already removed from pendingCards)
        var allBankCards = new java.util.ArrayList<CardInfo>(bankCards);
        allBankCards.add(currentCard);

        pendingRoutes = emails.size();
        for (EmailMessage email : emails) {
            router.tell(new TransactionRouterActor.RouteTransaction(
                    userId,
                    currentCard.bankKey(),
                    currentCard.lastFour(),
                    allBankCards,
                    email,
                    resultAdapter));
        }
        return this;
    }

    private Behavior<Command> onTransactionDone(TransactionDone msg) {
        pendingRoutes--;
        if (pendingRoutes <= 0) {
            // Update scan timestamp for this card before moving on
            Long cardId = currentCard.cardId();
            CompletableFuture.runAsync(
                    () -> backendApiClient.updateLastTransactionScanAt(cardId), executor);
            return nextCard();
        }
        return this;
    }

    private Behavior<Command> onSearchFailed(SearchFailed msg) {
        getContext().getLog().warn(
                "Transaction search failed for card {} user {}: {} — continuing with next card",
                currentCard != null ? currentCard.cardId() : "?", userId, msg.reason());
        return nextCard();
    }

    // ── Phase transitions ─────────────────────────────────────────────────────

    private Behavior<Command> nextCard() {
        // Skip cards scanned too recently according to the configured interval
        while (!pendingCards.isEmpty()) {
            CardInfo candidate = pendingCards.peek();
            if (needsScan(candidate.lastTransactionScanAt())) break;
            pendingCards.poll();
            getContext().getLog().debug(
                    "Skipping card {} ({}) — scanned within last {} min",
                    candidate.cardId(), candidate.lastFour(), scanIntervalMinutes);
        }

        if (pendingCards.isEmpty()) {
            getContext().getLog().info(
                    "Transaction scan complete for user {}", userId);
            coordinator.tell(new EmailPipelineCoordinator.TransactionBackfillComplete(userId));
            return Behaviors.stopped();
        }

        currentCard   = pendingCards.poll();
        pendingRoutes = 0;

        // ── since: use last scan timestamp as cursor ───────────────────────────
        // If we scanned before, only fetch emails since that date (incremental).
        // If null (first ever scan), fall back to last statement date - 1 month.
        LocalDate since = parseSinceDate(currentCard.lastTransactionScanAt(),
                currentCard.lastStatementDate());
        LocalDate until = LocalDate.now().plusDays(1); // inclusive of today

        getContext().getLog().info(
                "Scanning transactions for card {} ({} {}) from {} to {} (last scan: {})",
                currentCard.cardId(), currentCard.lastFour(), currentCard.bankKey(),
                since, until, currentCard.lastTransactionScanAt());

        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return gmailService.searchTransactionEmailsForCard(
                                accessToken, currentCard, since, until);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor),
                (result, ex) -> ex != null
                        ? new SearchFailed(ex.getMessage())
                        : new TransactionEmailsFetched(result)
        );
        return this;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * True if the card has never been scanned OR was last scanned more than
     * {@code scanIntervalMinutes} ago.
     */
    private boolean needsScan(String lastScanAt) {
        if (lastScanAt == null) return true;
        try {
            java.time.LocalDateTime last = java.time.LocalDateTime.parse(lastScanAt,
                    java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return last.isBefore(java.time.LocalDateTime.now().minusMinutes(scanIntervalMinutes));
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Determines the Gmail search start date for a card:
     *   - If previously scanned: use the scan date (incremental — only new emails)
     *   - If never scanned: cover last statement date - 1 month (initial backfill)
     *   - If no statement either: fall back to last 90 days
     */
    private LocalDate parseSinceDate(String lastScanAt, LocalDate lastStatementDate) {
        if (lastScanAt != null) {
            try {
                return java.time.LocalDateTime.parse(lastScanAt,
                        java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME).toLocalDate();
            } catch (Exception ignored) {}
        }
        if (lastStatementDate != null) {
            return lastStatementDate.minusMonths(1);
        }
        return LocalDate.now().minusDays(90);
    }

    /** Only these banks send per-transaction alert emails we can parse with regex. */
    private boolean isTransactionAlertBank(String bankKey) {
        return switch (bankKey) {
            case "CHASE", "BOA", "DISCOVER", "CITI", "CAPITAL_ONE", "WELLS_FARGO", "US_BANK" -> true;
            default -> false;
        };
    }
}
