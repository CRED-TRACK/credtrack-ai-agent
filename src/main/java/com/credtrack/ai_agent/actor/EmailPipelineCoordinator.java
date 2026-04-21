package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.time.Duration;
import com.credtrack.ai_agent.model.CardInfo;
import com.credtrack.ai_agent.model.GmailUserCredential;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import com.credtrack.ai_agent.service.GmailService;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Root coordinator of the email ingestion pipeline.
 *
 * Two execution paths, decided per-user on every poll cycle:
 *
 * ── Init path (new card) ──────────────────────────────────────────────────────
 *   If any of the user's cards has {@code gmailScanComplete=false} AND the user
 *   is not already being initialised → spawn InitCardScanActor.
 *   The actor performs a one-time historical Gmail search for that card's
 *   statements and payments, then marks the card complete.
 *   On success: sends InitScanComplete → user removed from activeUsers.
 *   On failure (e.g. Ollama down): sends InitScanFailed → user removed from
 *   activeUsers so the NEXT poll cycle automatically retries.
 *
 * ── Normal poll path ──────────────────────────────────────────────────────────
 *   If all cards are initialised → spawn EmailFetcherActor for purely incremental
 *   polling via the Gmail History API.  historyId is advanced on success.
 *
 * ── Mutual exclusion ──────────────────────────────────────────────────────────
 *   {@code activeUsers} tracks users currently undergoing init.  Normal polling
 *   is skipped for these users so the two paths never interfere.
 *   Because coordinator messages are delivered to a single-threaded mailbox,
 *   no locks are needed — the set is always accessed serially.
 */
public class EmailPipelineCoordinator extends AbstractBehavior<EmailPipelineCoordinator.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /** Sent by EmailPollScheduler every N seconds. */
    public enum Poll implements Command { INSTANCE }

    /** Sent by EmailFetcherActor after a successful incremental fetch. */
    public record FetchComplete(String userId, Long newHistoryId) implements Command {}

    /** Sent by EmailFetcherActor when the Gmail API call fails. */
    public record FetchFailed(String userId, String reason) implements Command {}

    /** Sent by InitCardScanActor when the one-time init scan for a user succeeds. */
    public record InitScanComplete(String userId) implements Command {}

    /** Sent by InitCardScanActor when the init scan fails (e.g. LLM unavailable). */
    public record InitScanFailed(String userId) implements Command {}

    /** Sent by TransactionInitActor when the post-init transaction backfill finishes. */
    public record TransactionBackfillComplete(String userId) implements Command {}

    private record CredentialsFetched(List<GmailUserCredential> credentials) implements Command {}
    private record CredentialsFetchFailed(String reason)                     implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(BackendApiClient backendApiClient,
                                           GmailService gmailService,
                                           ExtractionService extractionService,
                                           Executor executor,
                                           ActorRef<PersistentStatementLedgerActor.Command> ledger,
                                           ActorRef<AnalyticsAgentHolder.Command> agentHolder,
                                           long transactionScanIntervalMinutes) {
        return Behaviors.setup(ctx ->
                new EmailPipelineCoordinator(ctx, backendApiClient, gmailService, extractionService,
                        executor, ledger, agentHolder, transactionScanIntervalMinutes));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final BackendApiClient  backendApiClient;
    private final GmailService      gmailService;
    private final ExtractionService extractionService;
    private final Executor          executor;

    /** Singleton statement writer shared across all fetch workers. */
    private final ActorRef<StatementWriterActor.Command> writer;

    /** Analytics agent holder — updated after each unbilled spend computation. */
    private final ActorRef<AnalyticsAgentHolder.Command> agentHolder;

    /** Ledger ref — used here to seed from backend data on credentials fetch. */
    private final ActorRef<PersistentStatementLedgerActor.Command> statementLedger;

    /**
     * Users currently undergoing a one-time init scan.
     * Normal polling is suppressed for these users until init completes or fails.
     * Single-threaded mailbox ensures this set needs no external synchronisation.
     */
    private final Set<String> activeUsers = new HashSet<>();

    /**
     * Last fetched credentials per userId, used to spawn TransactionInitActor
     * immediately after init scan completes (without re-fetching from backend).
     */
    private final Map<String, GmailUserCredential> lastCredentials = new HashMap<>();

    /** Configurable interval — default 1440 min (24h), set to 5 for testing. */
    private final long transactionScanIntervalMinutes;

    private EmailPipelineCoordinator(ActorContext<Command> context,
                                     BackendApiClient backendApiClient,
                                     GmailService gmailService,
                                     ExtractionService extractionService,
                                     Executor executor,
                                     ActorRef<PersistentStatementLedgerActor.Command> ledger,
                                     ActorRef<AnalyticsAgentHolder.Command> agentHolder,
                                     long transactionScanIntervalMinutes) {
        super(context);
        this.backendApiClient  = backendApiClient;
        this.gmailService      = gmailService;
        this.extractionService = extractionService;
        this.executor          = executor;
        this.statementLedger               = ledger;
        this.agentHolder                   = agentHolder;
        this.transactionScanIntervalMinutes = transactionScanIntervalMinutes;
        this.writer = context.spawn(
                StatementWriterActor.create(backendApiClient, executor, ledger), "statement-writer");
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessageEquals(Poll.INSTANCE,                  this::onPoll)
                .onMessage(CredentialsFetched.class,             this::onCredentialsFetched)
                .onMessage(CredentialsFetchFailed.class,         this::onCredentialsFetchFailed)
                .onMessage(FetchComplete.class,                  this::onFetchComplete)
                .onMessage(FetchFailed.class,                    this::onFetchFailed)
                .onMessage(InitScanComplete.class,               this::onInitScanComplete)
                .onMessage(InitScanFailed.class,                 this::onInitScanFailed)
                .onMessage(TransactionBackfillComplete.class,    this::onTransactionBackfillComplete)
                .build();
    }

    private Behavior<Command> onPoll() {
        getContext().getLog().info("Poll started at {}",
                LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(backendApiClient::getAllGmailCredentials, executor),
                (result, ex) -> ex != null
                        ? new CredentialsFetchFailed(ex.getMessage())
                        : new CredentialsFetched(result)
        );
        return this;
    }

    private Behavior<Command> onCredentialsFetched(CredentialsFetched msg) {
        List<GmailUserCredential> credentials = msg.credentials();
        getContext().getLog().info("Starting pipeline for {} Gmail users", credentials.size());

        // ── Seed the in-memory ledger from backend data ────────────────────────
        // The journal is in-memory and resets on restart; re-seeding each poll
        // cycle from the backend's CardInfo.lastStatementDate keeps the ledger
        // consistent with the source of truth (the CardStatement table).
        // RecordStatementClosed is idempotent — it only updates if newer.
        for (GmailUserCredential cred : credentials) {
            lastCredentials.put(cred.getUserId(), cred);
            if (cred.getCards() != null) {
                for (CardInfo card : cred.getCards()) {
                    if (card.lastStatementDate() != null) {
                        statementLedger.tell(new PersistentStatementLedgerActor.RecordStatementClosed(
                                cred.getUserId(), card.cardId(), card.lastStatementDate()));
                    }
                }
            }
        }

        // ── Transaction scan for already-initialised users ─────────────────────
        // Run if all cards are initialised AND at least one card's lastTransactionScanAt
        // is null or older than the configured interval.
        // The interval (default 24h, TRANSACTION_SCAN_INTERVAL_MINUTES=5 for testing)
        // prevents unnecessary Gmail API calls on every poll cycle.
        java.time.LocalDateTime cutoff =
                java.time.LocalDateTime.now().minusMinutes(transactionScanIntervalMinutes);

        for (GmailUserCredential cred : credentials) {
            String userId = cred.getUserId();
            if (activeUsers.contains(userId)) continue;
            if (cred.getCards() == null || cred.getCards().isEmpty()) continue;
            boolean allInitialised = cred.getCards().stream().allMatch(CardInfo::gmailScanComplete);
            if (!allInitialised) continue;

            boolean anyCardNeedsScan = cred.getCards().stream().anyMatch(card ->
                    needsTransactionScan(card.lastTransactionScanAt(), cutoff));
            if (!anyCardNeedsScan) continue;

            getContext().getLog().info(
                    "Triggering transaction scan for user {} (interval={}min)",
                    userId, transactionScanIntervalMinutes);
            ActorRef<TransactionInitActor.Command> transInit =
                    getContext().spawnAnonymous(
                            TransactionInitActor.create(gmailService, backendApiClient, extractionService, executor,
                                    transactionScanIntervalMinutes));
            transInit.tell(new TransactionInitActor.RunTransactionBackfill(
                    userId, cred.getAccessToken(), cred.getCards(), getContext().getSelf()));
        }

        for (GmailUserCredential cred : credentials) {
            String userId = cred.getUserId();

            // Auto-refresh if token is expired or expires within the next 5 minutes
            if (isTokenExpiredOrSoon(cred.getTokenExpiryUtc())) {
                getContext().getLog().info("Access token expired/expiring for user {}, refreshing...", userId);
                String newToken = backendApiClient.refreshAccessToken(userId);
                if (newToken == null) {
                    getContext().getLog().warn("Skipping user {} — token refresh failed", userId);
                    continue;
                }
                cred.setAccessToken(newToken);
            }

            if (cred.getCards() == null || cred.getCards().isEmpty()) {
                getContext().getLog().debug("Skipping user {} — no registered cards", userId);
                continue;
            }

            // ── Mutual exclusion: skip users whose init scan is still running ──
            if (activeUsers.contains(userId)) {
                getContext().getLog().debug(
                        "Skipping user {} — init scan is in progress", userId);
                continue;
            }

            // ── Decide: init scan or normal incremental poll? ──────────────────
            List<CardInfo> unscannedCards = cred.getCards().stream()
                    .filter(c -> !c.gmailScanComplete())
                    .toList();

            if (!unscannedCards.isEmpty()) {
                // ── Init path: one or more cards have never been historically scanned ─
                getContext().getLog().info(
                        "Spawning init scan for user {} — {} unscanned card(s)",
                        userId, unscannedCards.size());
                activeUsers.add(userId);

                ActorRef<InitCardScanActor.Command> initActor =
                        getContext().spawnAnonymous(
                                InitCardScanActor.create(gmailService, extractionService, backendApiClient, executor));
                initActor.tell(new InitCardScanActor.RunInitScan(
                        userId,
                        cred.getAccessToken(),
                        unscannedCards,
                        cred.getCards(),
                        writer,
                        getContext().getSelf()));

            } else {
                // ── Normal path: all cards initialised → incremental poll ──────────
                getContext().getLog().info("User {} historyId from backend: {}", userId, cred.getHistoryId());

                ActorRef<EmailFetcherActor.Command> fetcher =
                        getContext().spawnAnonymous(
                                EmailFetcherActor.create(gmailService, extractionService, backendApiClient, executor));
                fetcher.tell(new EmailFetcherActor.FetchEmails(cred, getContext().getSelf(), writer));
            }
        }
        return this;
    }

    private Behavior<Command> onCredentialsFetchFailed(CredentialsFetchFailed msg) {
        getContext().getLog().error("Failed to fetch Gmail credentials: {}", msg.reason());
        return this;
    }

    private Behavior<Command> onFetchComplete(FetchComplete msg) {
        getContext().getLog().info("Incremental fetch complete for user {}, updating historyId={}",
                msg.userId(), msg.newHistoryId());
        CompletableFuture.runAsync(() ->
                backendApiClient.updateHistoryId(msg.userId(), msg.newHistoryId()), executor);
        return this;
    }

    private Behavior<Command> onFetchFailed(FetchFailed msg) {
        getContext().getLog().error("Incremental fetch failed for user {}: {}", msg.userId(), msg.reason());
        return this;
    }

    private Behavior<Command> onInitScanComplete(InitScanComplete msg) {
        getContext().getLog().info(
                "Init scan complete for user {} — spawning transaction backfill, " +
                "normal poll resumes next cycle", msg.userId());
        activeUsers.remove(msg.userId());

        // ── Post-init transaction backfill ────────────────────────────────────
        // Now that all cards are historically scanned for statements + payments,
        // kick off a one-time transaction email backfill covering the last 2 cycles.
        GmailUserCredential cred = lastCredentials.get(msg.userId());
        if (cred != null && cred.getCards() != null && !cred.getCards().isEmpty()) {
            ActorRef<TransactionInitActor.Command> transInit =
                    getContext().spawnAnonymous(
                            TransactionInitActor.create(gmailService, backendApiClient, extractionService, executor,
                                    transactionScanIntervalMinutes));
            transInit.tell(new TransactionInitActor.RunTransactionBackfill(
                    msg.userId(),
                    cred.getAccessToken(),
                    cred.getCards(),
                    getContext().getSelf()));
        }
        return this;
    }

    private Behavior<Command> onTransactionBackfillComplete(TransactionBackfillComplete msg) {
        getContext().getLog().info("Transaction backfill complete for user {}", msg.userId());
        return this;
    }

    private Behavior<Command> onInitScanFailed(InitScanFailed msg) {
        getContext().getLog().warn(
                "Init scan failed for user {} (likely Ollama unavailable) — " +
                "will retry automatically on next poll cycle.", msg.userId());
        activeUsers.remove(msg.userId());
        return this;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns true if the card needs a transaction scan:
     *   - Never scanned (lastScanAt is null), OR
     *   - Last scan was before the cutoff (now - interval).
     */
    private boolean needsTransactionScan(String lastScanAt, java.time.LocalDateTime cutoff) {
        if (lastScanAt == null) return true;
        try {
            java.time.LocalDateTime last = java.time.LocalDateTime.parse(lastScanAt,
                    java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return last.isBefore(cutoff);
        } catch (Exception e) {
            return true; // unparseable → treat as never scanned
        }
    }

    /**
     * Returns true if the token is null, already expired, or expires within 5 minutes.
     * tokenExpiryUtc is stored in UTC — compare with UTC now to avoid timezone mismatch.
     */
    private boolean isTokenExpiredOrSoon(String tokenExpiryUtc) {
        if (tokenExpiryUtc == null) return true;
        try {
            LocalDateTime expiry = LocalDateTime.parse(tokenExpiryUtc, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return LocalDateTime.now(ZoneOffset.UTC).isAfter(expiry.minusMinutes(5));
        } catch (Exception e) {
            return true;
        }
    }
}
