package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.GmailUserCredential;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import com.credtrack.ai_agent.service.GmailService;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Root coordinator of the email ingestion pipeline.
 *
 * Poll cycle:
 *   1. Fetch all Gmail credentials + user cards from backend
 *   2. Skip users with expired access tokens (log warning)
 *   3. Spawn one EmailFetcherActor per eligible user
 *   4. Each fetcher reports FetchComplete → update historyId cursor
 */
public class EmailPipelineCoordinator extends AbstractBehavior<EmailPipelineCoordinator.Command> {

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /** Sent by EmailPollScheduler every N seconds. */
    public enum Poll implements Command { INSTANCE }

    /** Sent by EmailFetcherActor after successfully fetching emails. */
    public record FetchComplete(String userId, Long newHistoryId) implements Command {}

    /** Sent by EmailFetcherActor when Gmail API call fails. */
    public record FetchFailed(String userId, String reason) implements Command {}

    private record CredentialsFetched(List<GmailUserCredential> credentials) implements Command {}
    private record CredentialsFetchFailed(String reason) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(BackendApiClient backendApiClient,
                                           GmailService gmailService,
                                           ExtractionService extractionService,
                                           Executor executor) {
        return Behaviors.setup(ctx ->
                new EmailPipelineCoordinator(ctx, backendApiClient, gmailService, extractionService, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final BackendApiClient  backendApiClient;
    private final GmailService      gmailService;
    private final ExtractionService extractionService;
    private final Executor          executor;

    /** Singleton writer shared across all fetch workers. */
    private final ActorRef<StatementWriterActor.Command> writer;

    private EmailPipelineCoordinator(ActorContext<Command> context,
                                     BackendApiClient backendApiClient,
                                     GmailService gmailService,
                                     ExtractionService extractionService,
                                     Executor executor) {
        super(context);
        this.backendApiClient  = backendApiClient;
        this.gmailService      = gmailService;
        this.extractionService = extractionService;
        this.executor          = executor;
        this.writer = context.spawn(
                StatementWriterActor.create(backendApiClient, executor), "statement-writer");
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessageEquals(Poll.INSTANCE,              this::onPoll)
                .onMessage(CredentialsFetched.class,         this::onCredentialsFetched)
                .onMessage(CredentialsFetchFailed.class,     this::onCredentialsFetchFailed)
                .onMessage(FetchComplete.class,              this::onFetchComplete)
                .onMessage(FetchFailed.class,                this::onFetchFailed)
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

        for (GmailUserCredential cred : credentials) {
            // Auto-refresh if token is expired or expires within the next 5 minutes
            if (isTokenExpiredOrSoon(cred.getTokenExpiryUtc())) {
                getContext().getLog().info(
                        "Access token expired/expiring for user {}, refreshing...", cred.getUserId());
                String newToken = backendApiClient.refreshAccessToken(cred.getUserId());
                if (newToken == null) {
                    getContext().getLog().warn(
                            "Skipping user {} — token refresh failed", cred.getUserId());
                    continue;
                }
                cred.setAccessToken(newToken);
            }

            getContext().getLog().info("User {} historyId from backend: {}",
                    cred.getUserId(), cred.getHistoryId());

            if (cred.getCards() == null || cred.getCards().isEmpty()) {
                getContext().getLog().debug(
                        "Skipping user {} — no registered cards", cred.getUserId());
                continue;
            }

            ActorRef<EmailFetcherActor.Command> fetcher =
                    getContext().spawnAnonymous(
                            EmailFetcherActor.create(gmailService, extractionService, executor));

            fetcher.tell(new EmailFetcherActor.FetchEmails(
                    cred, getContext().getSelf(), writer));
        }
        return this;
    }

    private Behavior<Command> onCredentialsFetchFailed(CredentialsFetchFailed msg) {
        getContext().getLog().error("Failed to fetch Gmail credentials: {}", msg.reason());
        return this;
    }

    private Behavior<Command> onFetchComplete(FetchComplete msg) {
        getContext().getLog().info("Fetch complete for user {}, updating historyId={}",
                msg.userId(), msg.newHistoryId());

        CompletableFuture.runAsync(() ->
                backendApiClient.updateHistoryId(msg.userId(), msg.newHistoryId()), executor);
        return this;
    }

    private Behavior<Command> onFetchFailed(FetchFailed msg) {
        getContext().getLog().error("Fetch failed for user {}: {}", msg.userId(), msg.reason());
        return this;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Returns true if the token is null, already expired, or expires within 5 minutes.
     *  tokenExpiryUtc is stored in UTC by the backend — compare with UTC now to avoid
     *  timezone mismatch on machines running in non-UTC local time. */
    private boolean isTokenExpiredOrSoon(String tokenExpiryUtc) {
        if (tokenExpiryUtc == null) return true;  // no expiry stored → treat as expired
        try {
            LocalDateTime expiry = LocalDateTime.parse(tokenExpiryUtc, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return LocalDateTime.now(ZoneOffset.UTC).isAfter(expiry.minusMinutes(5));
        } catch (Exception e) {
            return true;  // parse failure → assume expired, try to refresh
        }
    }
}
