package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
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

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Spawned once per user when one or more registered cards have not yet been
 * historically scanned ({@code gmailScanComplete=false}).
 *
 * Performs a one-shot init scan for each unscanned card, sequentially
 * (to respect Gmail API rate limits):
 *
 *   For each unscanned card:
 *     1. Search Gmail for all historical statement emails for that card.
 *     2. Feed each email through a StatementExtractorActor (LLM extraction).
 *     3. Wait for all statement extractors to finish.
 *        → If any LLM call fails, abort immediately without marking any card
 *          complete; the coordinator removes this user from activeUsers and the
 *          next poll cycle retries automatically.
 *     4. Search Gmail for all historical payment emails for that card.
 *     5. Feed each email through a PaymentExtractorActor (regex, no LLM).
 *     6. Wait for all payment extractors to finish.
 *     7. POST /internal/cards/{cardId}/init-complete (marks gmailScanComplete=true).
 *
 * On success: sends {@link EmailPipelineCoordinator.InitScanComplete} to coordinator.
 * On failure: sends {@link EmailPipelineCoordinator.InitScanFailed} — no card is
 *   marked complete so the next poll automatically retries the full init.
 */
public class InitCardScanActor extends AbstractBehavior<InitCardScanActor.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /** Sent by EmailPipelineCoordinator to start the scan. */
    public record RunInitScan(
            String         userId,
            String         accessToken,
            List<CardInfo> unscannedCards,   // cards with gmailScanComplete=false
            List<CardInfo> allCards,          // all active cards for this user (for digit validation)
            ActorRef<StatementWriterActor.Command>     writer,
            ActorRef<EmailPipelineCoordinator.Command> coordinator
    ) implements Command {}

    /** Delivered via messageAdapter from StatementExtractorActor. */
    public record InitLlmResult(boolean llmFailed) implements Command {}

    private record StatementEmailsFetched(List<EmailMessage> emails) implements Command {}
    private record PaymentEmailsFetched(List<EmailMessage> emails)   implements Command {}
    private record SearchFailed(String reason)                       implements Command {}

    private enum Phase { STATEMENTS, PAYMENTS }

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(GmailService gmailService,
                                           ExtractionService extractionService,
                                           BackendApiClient backendApiClient,
                                           Executor executor) {
        return Behaviors.setup(ctx ->
                new InitCardScanActor(ctx, gmailService, extractionService, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final GmailService      gmailService;
    private final ExtractionService extractionService;
    private final BackendApiClient  backendApiClient;
    private final Executor          executor;

    /**
     * Message adapter: converts EmailFetcherActor.LlmExtractorResult → InitLlmResult
     * so this actor can receive LLM success/failure signals from StatementExtractorActor
     * (which types its replyTo as ActorRef<EmailFetcherActor.LlmExtractorResult>).
     */
    private final ActorRef<EmailFetcherActor.LlmExtractorResult> llmResultAdapter;

    // Saved on RunInitScan
    private String                                     userId;
    private String                                     accessToken;
    private List<CardInfo>                             allCards;
    private ActorRef<StatementWriterActor.Command>     writer;
    private ActorRef<EmailPipelineCoordinator.Command> coordinator;

    // Per-card state (reset for each card in the queue)
    private Queue<CardInfo>    pendingCards;
    private CardInfo            currentCard;
    private int                 pendingExtractors;
    private boolean             anyLlmFailure;
    private Phase               phase;

    // Payment emails are processed one at a time (sequential) to avoid concurrent
    // writes racing on the same statement row in the backend.
    private Queue<EmailMessage> pendingPaymentEmails;

    // ── Constructor ───────────────────────────────────────────────────────────

    private InitCardScanActor(ActorContext<Command> context,
                               GmailService gmailService,
                               ExtractionService extractionService,
                               BackendApiClient backendApiClient,
                               Executor executor) {
        super(context);
        this.gmailService      = gmailService;
        this.extractionService = extractionService;
        this.backendApiClient  = backendApiClient;
        this.executor          = executor;
        this.llmResultAdapter  = context.messageAdapter(
                EmailFetcherActor.LlmExtractorResult.class,
                r -> new InitLlmResult(r.llmFailed()));
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RunInitScan.class,            this::onRun)
                .onMessage(StatementEmailsFetched.class, this::onStatementEmailsFetched)
                .onMessage(PaymentEmailsFetched.class,   this::onPaymentEmailsFetched)
                .onMessage(SearchFailed.class,           this::onSearchFailed)
                .onMessage(InitLlmResult.class,          this::onLlmResult)
                .onSignal(Terminated.class,              this::onExtractorTerminated)
                .build();
    }

    private Behavior<Command> onRun(RunInitScan msg) {
        this.userId       = msg.userId();
        this.accessToken  = msg.accessToken();
        this.allCards     = msg.allCards();
        this.writer       = msg.writer();
        this.coordinator  = msg.coordinator();
        this.pendingCards = new ArrayDeque<>(msg.unscannedCards());

        getContext().getLog().info("Init scan started for user {} — {} card(s) to scan",
                userId, pendingCards.size());
        return nextCard();
    }

    private Behavior<Command> onStatementEmailsFetched(StatementEmailsFetched msg) {
        getContext().getLog().info("Found {} statement email(s) for card {} user {}",
                msg.emails().size(), currentCard.cardId(), userId);

        for (EmailMessage email : msg.emails()) {
            String bankKey = GmailService.resolveBankKey(email.senderDomain());
            if (bankKey == null) continue;

            List<CardInfo> bankCards = allCards.stream()
                    .filter(c -> bankKey.equals(c.bankKey()))
                    .toList();

            ActorRef<StatementExtractorActor.Command> extractor =
                    getContext().spawnAnonymous(
                            StatementExtractorActor.create(extractionService, backendApiClient, executor));
            getContext().watch(extractor);
            pendingExtractors++;

            extractor.tell(new StatementExtractorActor.ExtractStatement(
                    userId, bankKey, currentCard.lastFour(), bankCards, email, writer, llmResultAdapter));
        }

        if (pendingExtractors == 0) {
            return afterStatements();
        }
        return this;
    }

    private Behavior<Command> onPaymentEmailsFetched(PaymentEmailsFetched msg) {
        getContext().getLog().info("Found {} payment email(s) for card {} user {}",
                msg.emails().size(), currentCard.cardId(), userId);

        // Sort oldest-first so earlier payments are processed first.
        // This is critical for Tier 1 (amount-exact) matching: if a newer payment
        // with a non-matching amount is processed first it grabs the statement via
        // Tier 2 (date-based), leaving the older exact-match payment with no home.
        // Gmail returns emails newest-first by default, so we must reverse.
        List<EmailMessage> sorted = msg.emails().stream()
                .filter(e -> e.sentDate() != null)
                .sorted(java.util.Comparator.comparing(EmailMessage::sentDate))
                .toList();
        // Append any emails with null sentDate at the end (shouldn't happen in practice)
        List<EmailMessage> nullDates = msg.emails().stream()
                .filter(e -> e.sentDate() == null)
                .toList();
        List<EmailMessage> ordered = new java.util.ArrayList<>(sorted);
        ordered.addAll(nullDates);

        // Queue all payment emails and process them one at a time — sequential
        // processing prevents concurrent DB writes from racing on the same statement row.
        pendingPaymentEmails = new ArrayDeque<>(ordered);
        pendingExtractors    = 0;
        return processNextPayment();
    }

    /**
     * Spawns a single PaymentExtractorActor for the next email in the queue.
     * Called initially and again each time a payment extractor terminates.
     */
    private Behavior<Command> processNextPayment() {
        if (pendingPaymentEmails.isEmpty()) {
            return afterPayments();
        }

        EmailMessage email   = pendingPaymentEmails.poll();
        String       bankKey = GmailService.resolveBankKey(email.senderDomain());
        if (bankKey == null) {
            // Not a recognised bank — skip and move on immediately
            return processNextPayment();
        }

        List<CardInfo> bankCards = allCards.stream()
                .filter(c -> bankKey.equals(c.bankKey()))
                .toList();

        ActorRef<PaymentExtractorActor.Command> paymentExtractor =
                getContext().spawnAnonymous(
                        PaymentExtractorActor.create(backendApiClient, executor));
        getContext().watch(paymentExtractor);
        pendingExtractors = 1;

        paymentExtractor.tell(new PaymentExtractorActor.ExtractPayment(
                userId, bankKey, bankCards, email));

        return this;
    }

    private Behavior<Command> onSearchFailed(SearchFailed msg) {
        getContext().getLog().error("Gmail search failed for card {} user {}: {}",
                currentCard != null ? currentCard.cardId() : "?", userId, msg.reason());
        coordinator.tell(new EmailPipelineCoordinator.InitScanFailed(userId));
        return Behaviors.stopped();
    }

    private Behavior<Command> onLlmResult(InitLlmResult msg) {
        if (msg.llmFailed()) {
            anyLlmFailure = true;
            getContext().getLog().warn(
                    "LLM failure during init scan for card {} user {} — " +
                    "will not mark complete; coordinator will retry on next poll.",
                    currentCard.cardId(), userId);
        }
        return this;
    }

    private Behavior<Command> onExtractorTerminated(Terminated sig) {
        pendingExtractors--;
        if (pendingExtractors <= 0) {
            // Statements: wait for all (parallel LLM calls) then transition.
            // Payments: sequential — one extractor at a time, so advance the queue here.
            return phase == Phase.STATEMENTS ? afterStatements() : processNextPayment();
        }
        return this;
    }

    // ── Phase transitions ─────────────────────────────────────────────────────

    /**
     * Called when all statement extractors for the current card have terminated.
     * Aborts on LLM failure; otherwise begins the payment search phase.
     */
    private Behavior<Command> afterStatements() {
        if (anyLlmFailure) {
            getContext().getLog().warn(
                    "Aborting init scan for user {} due to LLM failure. " +
                    "Will retry automatically on next poll cycle.", userId);
            coordinator.tell(new EmailPipelineCoordinator.InitScanFailed(userId));
            return Behaviors.stopped();
        }

        phase             = Phase.PAYMENTS;
        pendingExtractors = 0;

        getContext().getLog().info("Scanning payments for card {} ({}) user {}",
                currentCard.cardId(), currentCard.lastFour(), userId);

        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return gmailService.searchPaymentEmailsForCard(accessToken, currentCard);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor),
                (result, ex) -> ex != null
                        ? new SearchFailed(ex.getMessage())
                        : new PaymentEmailsFetched(result)
        );
        return this;
    }

    /**
     * Called when all payment extractors for the current card have terminated.
     * Marks the card init-complete (async, fire-and-forget) then moves to the next card.
     */
    private Behavior<Command> afterPayments() {
        Long cardId = currentCard.cardId();
        CompletableFuture.runAsync(() -> backendApiClient.markInitComplete(cardId), executor);
        getContext().getLog().info("Card {} init-complete queued for user {}", cardId, userId);
        return nextCard();
    }

    /**
     * Advances to the next unscanned card, or completes the scan if the queue is empty.
     */
    private Behavior<Command> nextCard() {
        if (pendingCards.isEmpty()) {
            getContext().getLog().info("Init scan finished for all cards — user {}", userId);
            coordinator.tell(new EmailPipelineCoordinator.InitScanComplete(userId));
            return Behaviors.stopped();
        }

        currentCard       = pendingCards.poll();
        pendingExtractors = 0;
        anyLlmFailure     = false;
        phase             = Phase.STATEMENTS;

        getContext().getLog().info("Scanning statements for card {} ({}) user {}",
                currentCard.cardId(), currentCard.lastFour(), userId);

        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return gmailService.searchStatementEmailsForCard(accessToken, currentCard);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor),
                (result, ex) -> ex != null
                        ? new SearchFailed(ex.getMessage())
                        : new StatementEmailsFetched(result)
        );
        return this;
    }
}
