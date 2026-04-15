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
import com.credtrack.ai_agent.model.GmailUserCredential;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import com.credtrack.ai_agent.service.GmailService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Spawned once per user per poll cycle.
 * 1. Fetches emails from Gmail (bank sender filter applied inside GmailService)
 * 2. For each email: resolves bankKey + cardLastFour from credential cards list
 * 3. Skips emails with no matching registered card (saves LLM cost)
 * 4a. Chase payment emails → spawns PaymentExtractorActor (regex, no LLM)
 * 4b. All other bank emails → spawns StatementExtractorActor (LLM extraction)
 * 5. Reports FetchComplete to coordinator immediately (historyId updated regardless
 *    of whether individual extractions succeed — avoids reprocessing same emails)
 * 6. Stops itself
 */
public class EmailFetcherActor extends AbstractBehavior<EmailFetcherActor.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record FetchEmails(
            GmailUserCredential credential,
            ActorRef<EmailPipelineCoordinator.Command> coordinator,
            ActorRef<StatementWriterActor.Command>     writer
    ) implements Command {}

    private record EmailsFetched(
            GmailUserCredential credential,
            List<EmailMessage>  emails,
            Long                newHistoryId,
            Set<Long>           completedCardScans,
            ActorRef<EmailPipelineCoordinator.Command> coordinator,
            ActorRef<StatementWriterActor.Command>     writer
    ) implements Command {}

    private record FetchFailed(
            String userId,
            String reason,
            ActorRef<EmailPipelineCoordinator.Command> coordinator
    ) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(GmailService gmailService,
                                           ExtractionService extractionService,
                                           BackendApiClient backendApiClient,
                                           Executor executor) {
        return Behaviors.setup(ctx ->
                new EmailFetcherActor(ctx, gmailService, extractionService, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final GmailService      gmailService;
    private final ExtractionService extractionService;
    private final BackendApiClient  backendApiClient;
    private final Executor          executor;
    private int pendingExtractors = 0;

    private EmailFetcherActor(ActorContext<Command> context,
                              GmailService gmailService,
                              ExtractionService extractionService,
                              BackendApiClient backendApiClient,
                              Executor executor) {
        super(context);
        this.gmailService      = gmailService;
        this.extractionService = extractionService;
        this.backendApiClient  = backendApiClient;
        this.executor          = executor;
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(FetchEmails.class,   this::onFetch)
                .onMessage(EmailsFetched.class, this::onFetched)
                .onMessage(FetchFailed.class,   this::onFailed)
                .onSignal(Terminated.class,     this::onExtractorTerminated)
                .build();
    }

    private Behavior<Command> onFetch(FetchEmails msg) {
        String userId = msg.credential().getUserId();
        getContext().getLog().info("Fetching emails for user {} (historyId={})",
                userId, msg.credential().getHistoryId());

        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return gmailService.fetchNewEmails(
                                msg.credential().getAccessToken(),
                                msg.credential().getHistoryId(),
                                msg.credential().getCards());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor),
                (result, ex) -> ex != null
                        ? new FetchFailed(userId, ex.getMessage(), msg.coordinator())
                        : new EmailsFetched(msg.credential(), result.emails(),
                                            result.newHistoryId(), result.completedCardScans(),
                                            msg.coordinator(), msg.writer())
        );
        return this;
    }

    private Behavior<Command> onFetched(EmailsFetched msg) {
        GmailUserCredential cred = msg.credential();
        getContext().getLog().info("Fetched {} bank emails for user {}",
                msg.emails().size(), cred.getUserId());

        for (EmailMessage email : msg.emails()) {
            // Resolve bankKey from sender domain
            String bankKey = GmailService.resolveBankKey(email.senderDomain());
            if (bankKey == null) continue;

            // Match to a registered card by bankKey + last 4 digits shown in email
            // We spawn the extractor and let it confirm the digits after LLM extraction.
            // Here we find the best candidate card for this bank.
            CardInfo matchedCard = cred.getCards() == null ? null :
                    cred.getCards().stream()
                            .filter(c -> bankKey.equals(c.bankKey()))
                            .findFirst()
                            .orElse(null);

            if (matchedCard == null) {
                getContext().getLog().debug(
                        "No registered {} card for user {} — skipping email {}",
                        bankKey, cred.getUserId(), email.messageId());
                continue;
            }

            // All registered cards for this bank — used by extractors to validate digits
            List<CardInfo> bankCards = cred.getCards().stream()
                    .filter(c -> bankKey.equals(c.bankKey()))
                    .toList();

            if (isPaymentEmail(email, bankKey)) {
                // ── Chase payment confirmation email — regex parse, no LLM ──────
                ActorRef<PaymentExtractorActor.Command> paymentExtractor =
                        getContext().spawnAnonymous(
                                PaymentExtractorActor.create(backendApiClient, executor));
                getContext().watch(paymentExtractor);
                pendingExtractors++;

                paymentExtractor.tell(new PaymentExtractorActor.ExtractPayment(
                        cred.getUserId(),
                        bankKey,
                        bankCards,
                        email
                ));
            } else {
                // ── Statement email — LLM extraction ─────────────────────────────
                ActorRef<StatementExtractorActor.Command> extractor =
                        getContext().spawnAnonymous(
                                StatementExtractorActor.create(extractionService, backendApiClient, executor));
                getContext().watch(extractor);
                pendingExtractors++;

                extractor.tell(new StatementExtractorActor.ExtractStatement(
                        cred.getUserId(),
                        bankKey,
                        matchedCard.lastFour(),
                        bankCards,
                        email,
                        msg.writer()
                ));
            }
        }

        // Mark historical scans complete for any newly scanned cards
        if (msg.completedCardScans() != null && !msg.completedCardScans().isEmpty()) {
            for (Long cardId : msg.completedCardScans()) {
                CompletableFuture.runAsync(
                        () -> backendApiClient.markGmailScanComplete(cardId), executor);
            }
        }

        // Update historyId immediately — don't wait for extractions to complete
        msg.coordinator().tell(
                new EmailPipelineCoordinator.FetchComplete(cred.getUserId(), msg.newHistoryId())
        );

        // If no extractors were spawned we can stop right away; otherwise wait
        // for all children to terminate (via onExtractorTerminated) before stopping.
        if (pendingExtractors == 0) {
            return Behaviors.stopped();
        }
        return this;
    }

    private Behavior<Command> onExtractorTerminated(Terminated sig) {
        pendingExtractors--;
        getContext().getLog().debug("Extractor terminated, {} still pending", pendingExtractors);
        if (pendingExtractors <= 0) {
            getContext().getLog().info("All extractors finished — stopping EmailFetcherActor");
            return Behaviors.stopped();
        }
        return this;
    }

    private Behavior<Command> onFailed(FetchFailed msg) {
        getContext().getLog().error("Gmail fetch failed for user {}: {}", msg.userId(), msg.reason());
        msg.coordinator().tell(
                new EmailPipelineCoordinator.FetchFailed(msg.userId(), msg.reason())
        );
        return Behaviors.stopped();
    }

    /**
     * Returns true if this email is a bank payment confirmation (not a statement).
     * Supported: Chase ("payment is scheduled") and BOA ("received your credit card payment").
     */
    private boolean isPaymentEmail(EmailMessage email, String bankKey) {
        String subject = email.subject();
        if (subject == null) return false;
        String lower = subject.toLowerCase();
        return switch (bankKey) {
            case "CHASE"    -> lower.contains("payment is scheduled")
                    || lower.contains("payment scheduled")
                    || lower.contains("payment has been scheduled");
            case "BOA"      -> lower.contains("received your credit card payment");
            case "DISCOVER" -> lower.contains("scheduled payment");
            case "AMEX"    -> lower.contains("received your payment");
            default        -> false;
        };
    }
}
