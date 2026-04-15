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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Spawned once per user per poll cycle — handles the NORMAL (incremental) poll path only.
 *
 * Historical / init scans for new cards are handled exclusively by InitCardScanActor.
 * This actor only processes emails that arrived since the last historyId cursor.
 *
 * Flow:
 *   1. Calls GmailService.fetchNewEmails() — pure incremental, no keyword searches.
 *   2. For each bank email: resolves bankKey + best candidate card.
 *   3a. Payment emails → spawns PaymentExtractorActor (regex, no LLM).
 *   3b. Statement emails → spawns StatementExtractorActor (LLM extraction).
 *   4. Waits for ALL extractors before advancing the historyId cursor.
 *      If any LLM call fails, historyId is NOT advanced so the next poll retries.
 */
public class EmailFetcherActor extends AbstractBehavior<EmailFetcherActor.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record FetchEmails(
            GmailUserCredential credential,
            ActorRef<EmailPipelineCoordinator.Command> coordinator,
            ActorRef<StatementWriterActor.Command>     writer
    ) implements Command {}

    /**
     * Sent by StatementExtractorActor before it stops.
     * {@code llmFailed=true} means the LLM call failed (e.g. Ollama connection refused).
     * {@code llmFailed=false} means the email was handled normally (saved, skipped as
     * non-statement, or rejected because card digits didn't match).
     */
    public record LlmExtractorResult(boolean llmFailed) implements Command {}

    private record EmailsFetched(
            GmailUserCredential credential,
            List<EmailMessage>  emails,
            Long                newHistoryId,
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

    // Message adapter: receives LlmExtractorResult from StatementExtractorActor and delivers
    // it back into this actor's mailbox as a LlmExtractorResult (which implements Command).
    private final ActorRef<LlmExtractorResult> llmResultRef;

    // Counts active child extractors (both statement and payment).
    private int pendingExtractors = 0;

    // Set to true if any StatementExtractorActor reports an LLM failure.
    // When true we do NOT advance historyId so the next poll retries the failed emails.
    private boolean anyLlmFailure = false;

    // Stored from EmailsFetched; consumed when all extractors finish.
    private String                                     pendingUserId;
    private Long                                       pendingNewHistoryId;
    private ActorRef<EmailPipelineCoordinator.Command> pendingCoordinator;

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
        // Adapter: LlmExtractorResult → LlmExtractorResult (identity — it already implements Command)
        this.llmResultRef = context.messageAdapter(LlmExtractorResult.class, r -> r);
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(FetchEmails.class,        this::onFetch)
                .onMessage(EmailsFetched.class,      this::onFetched)
                .onMessage(FetchFailed.class,        this::onFailed)
                .onMessage(LlmExtractorResult.class, this::onLlmResult)
                .onSignal(Terminated.class,          this::onExtractorTerminated)
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
                                msg.credential().getHistoryId());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor),
                (result, ex) -> ex != null
                        ? new FetchFailed(userId, ex.getMessage(), msg.coordinator())
                        : new EmailsFetched(msg.credential(), result.emails(),
                                            result.newHistoryId(),
                                            msg.coordinator(), msg.writer())
        );
        return this;
    }

    private Behavior<Command> onFetched(EmailsFetched msg) {
        GmailUserCredential cred = msg.credential();
        getContext().getLog().info("Fetched {} bank emails for user {}",
                msg.emails().size(), cred.getUserId());

        pendingUserId      = cred.getUserId();
        pendingNewHistoryId = msg.newHistoryId();
        pendingCoordinator = msg.coordinator();

        for (EmailMessage email : msg.emails()) {
            String bankKey = GmailService.resolveBankKey(email.senderDomain());
            if (bankKey == null) continue;

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

            List<CardInfo> bankCards = cred.getCards().stream()
                    .filter(c -> bankKey.equals(c.bankKey()))
                    .toList();

            if (isPaymentEmail(email, bankKey)) {
                // ── Payment confirmation email — regex parse, no LLM ─────────
                ActorRef<PaymentExtractorActor.Command> paymentExtractor =
                        getContext().spawnAnonymous(
                                PaymentExtractorActor.create(backendApiClient, executor));
                getContext().watch(paymentExtractor);
                pendingExtractors++;

                paymentExtractor.tell(new PaymentExtractorActor.ExtractPayment(
                        cred.getUserId(), bankKey, bankCards, email));
            } else {
                // ── Statement email — LLM extraction ─────────────────────────
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
                        msg.writer(),
                        llmResultRef   // typed as ActorRef<LlmExtractorResult>
                ));
            }
        }

        if (pendingExtractors == 0) {
            return onAllExtractorsFinished();
        }
        return this;
    }

    /**
     * Received from StatementExtractorActor before it stops.
     * Tracks whether any LLM call failed so we can suppress historyId advancement.
     */
    private Behavior<Command> onLlmResult(LlmExtractorResult msg) {
        if (msg.llmFailed()) {
            anyLlmFailure = true;
            getContext().getLog().warn(
                    "LLM extraction failure reported — historyId will NOT be advanced " +
                    "this cycle; affected emails will be retried on next poll.");
        }
        return this;
    }

    private Behavior<Command> onExtractorTerminated(Terminated sig) {
        pendingExtractors--;
        getContext().getLog().debug("Extractor terminated, {} still pending", pendingExtractors);
        if (pendingExtractors <= 0) {
            return onAllExtractorsFinished();
        }
        return this;
    }

    /**
     * Called when every spawned child extractor has terminated.
     *
     * If any LLM call failed: skip historyId advance so the next poll retries.
     * If all completed normally: advance historyId via the coordinator.
     */
    private Behavior<Command> onAllExtractorsFinished() {
        getContext().getLog().info("All extractors finished for user {}", pendingUserId);

        if (anyLlmFailure) {
            getContext().getLog().warn(
                    "Suppressing historyId advance for user {} due to LLM failure(s). " +
                    "Emails will be retried on next poll cycle.", pendingUserId);
        } else {
            if (pendingCoordinator != null) {
                pendingCoordinator.tell(
                        new EmailPipelineCoordinator.FetchComplete(pendingUserId, pendingNewHistoryId));
            }
        }
        return Behaviors.stopped();
    }

    private Behavior<Command> onFailed(FetchFailed msg) {
        getContext().getLog().error("Gmail fetch failed for user {}: {}", msg.userId(), msg.reason());
        msg.coordinator().tell(
                new EmailPipelineCoordinator.FetchFailed(msg.userId(), msg.reason()));
        return Behaviors.stopped();
    }

    /**
     * Returns true if this email is a bank payment confirmation (not a statement).
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
            case "AMEX"     -> lower.contains("received your payment");
            default         -> false;
        };
    }
}
