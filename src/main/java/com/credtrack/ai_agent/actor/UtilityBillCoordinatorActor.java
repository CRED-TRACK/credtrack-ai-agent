package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.model.GmailUserCredential;
import com.credtrack.ai_agent.model.UtilityAccountInfo;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import com.credtrack.ai_agent.service.GmailService;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Top-level coordinator for utility bill email ingestion.
 *
 * Two execution paths per utility account, decided on every poll:
 *
 * ── Init path (new account, utilityInitComplete=false) ────────────────────────
 *   Spawns {@link UtilityBillInitActor} which performs a one-time historical
 *   scan covering the last {@link UtilityBillInitActor#INIT_LOOKBACK_DAYS} days:
 *     1. All bill emails → LLM extraction → backend
 *     2. All payment emails → regex extraction → backend  (only AFTER bills)
 *   On success: backend marks utilityInitComplete=true; coordinator removes account
 *   from activeAccounts so normal polling resumes on the next cycle.
 *   On failure: coordinator removes from activeAccounts so the NEXT poll retries.
 *
 * ── Normal path (utilityInitComplete=true) ────────────────────────────────────
 *   Searches the last {@link #NORMAL_LOOKBACK_DAYS} days for new emails and
 *   routes each through {@link UtilityBillRouterActor}.
 *
 * ── Mutual exclusion ──────────────────────────────────────────────────────────
 *   {@code activeAccounts} tracks accounts currently being initialised.
 *   Normal polling is skipped for these until init completes or fails.
 *   The single-threaded Akka mailbox means the set needs no locking.
 *
 */
public class UtilityBillCoordinatorActor extends AbstractBehavior<UtilityBillCoordinatorActor.Command> {

    /** Lookback window for incremental normal-cycle polls (45 days = ~1.5 billing cycles). */
    private static final int NORMAL_LOOKBACK_DAYS = 45;

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    /** Sent by EmailPollScheduler on each poll cycle. */
    public enum Poll implements Command { INSTANCE }

    /** Sent by UtilityBillInitActor on successful init. */
    public record UtilityInitComplete(String accountKey) implements Command {}

    /** Sent by UtilityBillInitActor on init failure (will retry next poll). */
    public record UtilityInitFailed(String accountKey) implements Command {}

    private record AccountsFetched(List<UtilityAccountInfo> accounts,
                                   Map<String, String> accessTokenByUserId) implements Command {}
    private record FetchFailed(String reason) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(BackendApiClient backendApiClient,
                                            GmailService gmailService,
                                            ExtractionService extractionService,
                                            Executor executor) {
        return Behaviors.setup(ctx ->
                new UtilityBillCoordinatorActor(ctx, backendApiClient, gmailService,
                        extractionService, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final BackendApiClient backendApiClient;
    private final GmailService     gmailService;
    private final ExtractionService extractionService;
    private final Executor         executor;

    /**
     * Router child — supervised with per-exception strategies (Akka requirement):
     *   IOException             → restartWithBackoff (transient Gmail/network error)
     *   IllegalArgumentException → stop (bad config, unrecoverable)
     */
    private final ActorRef<UtilityBillRouterActor.Command> router;

    /**
     * Accounts currently undergoing the one-time init scan.
     * Key format: userId:billerName:accountLastFour
     * Normal polling is suppressed for these accounts until init completes.
     */
    private final Set<String> activeAccounts = new HashSet<>();

    private UtilityBillCoordinatorActor(ActorContext<Command> ctx,
                                         BackendApiClient backendApiClient,
                                         GmailService gmailService,
                                         ExtractionService extractionService,
                                         Executor executor) {
        super(ctx);
        this.backendApiClient  = backendApiClient;
        this.gmailService      = gmailService;
        this.extractionService = extractionService;
        this.executor          = executor;

        // Supervised router: IOException → restartWithBackoff, IllegalArgumentException → stop
        this.router = ctx.spawn(
                Behaviors.supervise(
                        Behaviors.<UtilityBillRouterActor.Command>supervise(
                                UtilityBillRouterActor.create(extractionService, backendApiClient, executor))
                                .onFailure(IllegalArgumentException.class, SupervisorStrategy.stop()))
                        .onFailure(IOException.class,
                                SupervisorStrategy.restartWithBackoff(
                                        Duration.ofSeconds(1), Duration.ofSeconds(30), 0.1)),
                "utility-bill-router");
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Poll.class,                this::onPoll)
                .onMessage(AccountsFetched.class,     this::onAccountsFetched)
                .onMessage(FetchFailed.class,         this::onFetchFailed)
                .onMessage(UtilityInitComplete.class, this::onInitComplete)
                .onMessage(UtilityInitFailed.class,   this::onInitFailed)
                .build();
    }

    // ── Poll ──────────────────────────────────────────────────────────────────

    private Behavior<Command> onPoll(Poll ignored) {
        getContext().getLog().info("Utility bill poll cycle started");
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    List<UtilityAccountInfo> accounts = backendApiClient.getUtilityAccounts();
                    List<GmailUserCredential> creds   = backendApiClient.getAllGmailCredentials();

                    for (GmailUserCredential cred : creds) {
                        if (cred.getAccessToken() == null) continue;
                        if (!isTokenExpiredOrSoon(cred.getTokenExpiryUtc())) continue;

                        getContext().getLog().info(
                                "Utility pipeline refreshing expiring Gmail token for user {}",
                                cred.getUserId());
                        String newToken = backendApiClient.refreshAccessToken(cred.getUserId());
                        if (newToken != null) {
                            cred.setAccessToken(newToken);
                        } else {
                            getContext().getLog().warn(
                                    "Utility pipeline token refresh failed for user {}",
                                    cred.getUserId());
                            cred.setAccessToken(null);
                        }
                    }

                    Map<String, String> tokenMap = creds.stream()
                            .filter(c -> c.getAccessToken() != null)
                            .collect(Collectors.toMap(
                                    GmailUserCredential::getUserId,
                                    GmailUserCredential::getAccessToken,
                                    (a, b) -> a));

                    return new AccountsFetched(accounts, tokenMap);
                }, executor),
                (result, ex) -> ex != null ? new FetchFailed(ex.getMessage()) : result
        );
        return this;
    }

    // ── Accounts fetched ──────────────────────────────────────────────────────

    private Behavior<Command> onAccountsFetched(AccountsFetched msg) {
        if (msg.accounts().isEmpty()) {
            getContext().getLog().debug("No utility accounts registered — skipping");
            return this;
        }

        for (UtilityAccountInfo account : msg.accounts()) {
            String token = msg.accessTokenByUserId().get(account.userId());
            if (token == null) {
                getContext().getLog().debug(
                        "No Gmail token for userId={} — skipping", account.userId());
                continue;
            }

            String key = accountKey(account);

            if (activeAccounts.contains(key)) {
                getContext().getLog().debug("Init already running for {} — skipping", key);
                continue;
            }

            if (!account.utilityInitComplete()) {
                // ── Init path: first time this account is seen ────────────────
                getContext().getLog().info(
                        "Spawning utility init scan for billerName={} acct={} user={}",
                        account.billerName(), account.accountLastFour(), account.userId());
                activeAccounts.add(key);

                ActorRef<UtilityBillInitActor.Command> initActor =
                        getContext().spawnAnonymous(
                                UtilityBillInitActor.create(
                                        gmailService, extractionService, backendApiClient, executor));
                initActor.tell(new UtilityBillInitActor.RunInit(
                        account, token, getContext().getSelf()));

            } else {
                // ── Normal path: incremental scan for the last 45 days ────────
                LocalDate since = LocalDate.now().minusDays(NORMAL_LOOKBACK_DAYS);
                CompletableFuture.runAsync(
                        () -> normalScan(account, token, since), executor);
            }
        }
        return this;
    }

    // ── Init result handlers ──────────────────────────────────────────────────

    private Behavior<Command> onInitComplete(UtilityInitComplete msg) {
        getContext().getLog().info("Utility init complete — key={}", msg.accountKey());
        activeAccounts.remove(msg.accountKey());
        return this;
    }

    private Behavior<Command> onInitFailed(UtilityInitFailed msg) {
        getContext().getLog().warn(
                "Utility init failed — will retry on next poll cycle. key={}", msg.accountKey());
        activeAccounts.remove(msg.accountKey());
        return this;
    }

    private Behavior<Command> onFetchFailed(FetchFailed msg) {
        getContext().getLog().error("Failed to fetch utility accounts: {}", msg.reason());
        return this;
    }

    // ── Normal incremental scan (runs on virtual thread) ─────────────────────

    private void normalScan(UtilityAccountInfo account, String accessToken, LocalDate since) {
        try {
            List<EmailMessage> billEmails = gmailService.searchUtilityBillEmails(
                    accessToken, account.billerName(), account.accountLastFour(), since);
            getContext().getLog().info("Normal scan — {} bill email(s) for {} acct={}",
                    billEmails.size(), account.billerName(), account.accountLastFour());
            for (EmailMessage email : billEmails) {
                router.tell(new UtilityBillRouterActor.RouteUtilityEmail(
                        account.userId(), account.billerName(),
                        account.accountLastFour(), "BILL", email));
            }

            List<EmailMessage> paymentEmails = gmailService.searchUtilityPaymentEmails(
                    accessToken, account.billerName(), account.accountLastFour(), since);
            getContext().getLog().info("Normal scan — {} payment email(s) for {} acct={}",
                    paymentEmails.size(), account.billerName(), account.accountLastFour());
            for (EmailMessage email : paymentEmails) {
                router.tell(new UtilityBillRouterActor.RouteUtilityEmail(
                        account.userId(), account.billerName(),
                        account.accountLastFour(), "PAYMENT", email));
            }

        } catch (Exception e) {
            getContext().getLog().error("Normal scan failed for {} acct={}: {}",
                    account.billerName(), account.accountLastFour(), e.getMessage());
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String accountKey(UtilityAccountInfo a) {
        return a.userId() + ":" + a.billerName() + ":" + a.accountLastFour();
    }

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
