package com.credtrack.ai_agent.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import com.credtrack.ai_agent.service.BackendApiClient;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Cluster-sharded analytics actor — one entity per userId.
 *
 * Card analytics are statement-based: groups CardStatement rows by month,
 * using statementBalance with paidAmount as fallback (covers AMEX which
 * omits statementBalance). Results cached for CACHE_TTL.
 */
public class AnalyticsWorkerActor extends AbstractBehavior<AnalyticsWorkerActor.Command> {

    public static final EntityTypeKey<Command> TYPE_KEY =
            EntityTypeKey.create(Command.class, "analytics-worker");

    private static final Duration CACHE_TTL = Duration.ofMinutes(15);

    // ── Public commands ────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record ComputeCardSpending(
            int months,
            ActorRef<CardSpendingResult> replyTo
    ) implements Command {}

    public record ComputeUtilityAnalytics(
            ActorRef<UtilityAnalyticsResult> replyTo
    ) implements Command {}

    // ── Internal commands (piped from async backend calls) ─────────────────────

    private record CardDataFetched(
            int months,
            BackendApiClient.RawCardStatementData data,
            ActorRef<CardSpendingResult> replyTo
    ) implements Command {}

    private record UtilityDataFetched(
            BackendApiClient.RawUtilityData data,
            ActorRef<UtilityAnalyticsResult> replyTo
    ) implements Command {}

    private record CardFetchFailed(
            int months,
            Throwable cause,
            ActorRef<CardSpendingResult> replyTo
    ) implements Command {}

    private record UtilityFetchFailed(
            Throwable cause,
            ActorRef<UtilityAnalyticsResult> replyTo
    ) implements Command {}

    // ── Result types ───────────────────────────────────────────────────────────

    public record CardSpendingResult(CardSpendingData data) {}
    public record UtilityAnalyticsResult(UtilityData data) {}

    // ── Computed data models ───────────────────────────────────────────────────

    public record CardSpendingData(
            double totalSpend,
            int totalTransactions,
            int months,
            List<CardSummaryData> cards,
            List<CategoryClusterData> categories,
            List<MonthlyBreakdownData> monthlyBreakdown) {}

    public record CardSummaryData(
            Long cardId,
            String bankKey,
            String lastFour,
            double totalSpend,
            int transactionCount) {}

    public record CategoryClusterData(
            String cluster,
            double amount,
            double percentage,
            int transactionCount) {}

    public record MonthlyBreakdownData(
            String month,
            double totalSpend,
            List<CardMonthData> cards) {}

    public record CardMonthData(
            Long cardId,
            String bankKey,
            String lastFour,
            double amount) {}

    public record UtilityData(List<AccountData> accounts) {}

    public record AccountData(
            String billerName,
            String accountLastFour,
            List<BillPointData> bills,
            double averageAmount,
            Double latestAmount,
            Double changePercent) {}

    public record BillPointData(String billDate, double amountDue) {}

    // ── Cache entry ────────────────────────────────────────────────────────────

    private record CacheEntry<T>(T value, Instant computedAt) {
        boolean isStale() {
            return Instant.now().isAfter(computedAt.plus(CACHE_TTL));
        }
    }

    // ── Factory ────────────────────────────────────────────────────────────────

    public static Behavior<Command> create(String userId,
                                           BackendApiClient backendApiClient,
                                           Executor executor) {
        return Behaviors.setup(ctx ->
                new AnalyticsWorkerActor(ctx, userId, backendApiClient, executor));
    }

    // ── State ──────────────────────────────────────────────────────────────────

    private final String userId;
    private final BackendApiClient backendApiClient;
    private final Executor executor;

    private final Map<Integer, CacheEntry<CardSpendingData>> cardCache = new HashMap<>();
    private CacheEntry<UtilityData> utilityCache = null;

    private AnalyticsWorkerActor(ActorContext<Command> ctx,
                                  String userId,
                                  BackendApiClient backendApiClient,
                                  Executor executor) {
        super(ctx);
        this.userId           = userId;
        this.backendApiClient = backendApiClient;
        this.executor         = executor;
    }

    // ── Behavior ───────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ComputeCardSpending.class,    this::onComputeCard)
                .onMessage(ComputeUtilityAnalytics.class, this::onComputeUtility)
                .onMessage(CardDataFetched.class,        this::onCardDataFetched)
                .onMessage(UtilityDataFetched.class,     this::onUtilityDataFetched)
                .onMessage(CardFetchFailed.class,        this::onCardFetchFailed)
                .onMessage(UtilityFetchFailed.class,     this::onUtilityFetchFailed)
                .build();
    }

    private Behavior<Command> onComputeCard(ComputeCardSpending msg) {
        CacheEntry<CardSpendingData> entry = cardCache.get(msg.months());
        if (entry != null && !entry.isStale()) {
            getContext().getLog().debug("Card analytics cache hit for user {} months={}", userId, msg.months());
            msg.replyTo().tell(new CardSpendingResult(entry.value()));
            return this;
        }

        getContext().getLog().info("Card analytics cache miss for user {} months={} — fetching", userId, msg.months());
        String uid = this.userId;
        int months = msg.months();
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(
                        () -> backendApiClient.getRawCardData(uid, months), executor),
                (data, ex) -> ex != null
                        ? new CardFetchFailed(months, ex, msg.replyTo())
                        : new CardDataFetched(months, data, msg.replyTo())
        );
        return this;
    }

    private Behavior<Command> onComputeUtility(ComputeUtilityAnalytics msg) {
        if (utilityCache != null && !utilityCache.isStale()) {
            getContext().getLog().debug("Utility analytics cache hit for user {}", userId);
            msg.replyTo().tell(new UtilityAnalyticsResult(utilityCache.value()));
            return this;
        }

        getContext().getLog().info("Utility analytics cache miss for user {} — fetching", userId);
        String uid = this.userId;
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(
                        () -> backendApiClient.getRawUtilityData(uid), executor),
                (data, ex) -> ex != null
                        ? new UtilityFetchFailed(ex, msg.replyTo())
                        : new UtilityDataFetched(data, msg.replyTo())
        );
        return this;
    }

    private Behavior<Command> onCardDataFetched(CardDataFetched msg) {
        CardSpendingData result = computeCardSpending(msg.data(), msg.months());
        cardCache.put(msg.months(), new CacheEntry<>(result, Instant.now()));
        msg.replyTo().tell(new CardSpendingResult(result));
        return this;
    }

    private Behavior<Command> onUtilityDataFetched(UtilityDataFetched msg) {
        UtilityData result = computeUtilityAnalytics(msg.data());
        utilityCache = new CacheEntry<>(result, Instant.now());
        msg.replyTo().tell(new UtilityAnalyticsResult(result));
        return this;
    }

    private Behavior<Command> onCardFetchFailed(CardFetchFailed msg) {
        getContext().getLog().error("Failed to fetch card analytics for user {} months={}: {}",
                userId, msg.months(), msg.cause().getMessage());
        msg.replyTo().tell(new CardSpendingResult(
                new CardSpendingData(0, 0, msg.months(), List.of(), List.of(), List.of())));
        return this;
    }

    private Behavior<Command> onUtilityFetchFailed(UtilityFetchFailed msg) {
        getContext().getLog().error("Failed to fetch utility analytics for user {}: {}",
                userId, msg.cause().getMessage());
        msg.replyTo().tell(new UtilityAnalyticsResult(new UtilityData(List.of())));
        return this;
    }

    // ── Computation ────────────────────────────────────────────────────────────

    private CardSpendingData computeCardSpending(BackendApiClient.RawCardStatementData raw, int months) {
        // Per-card totals across all months (for the summary list)
        Map<Long, double[]> cardTotals = new LinkedHashMap<>();
        Map<Long, String[]> cardMeta  = new HashMap<>(); // cardId → [bankKey, lastFour]
        double totalSpend = 0;

        List<MonthlyBreakdownData> monthlyBreakdown = new ArrayList<>();

        for (BackendApiClient.RawMonthData monthData : raw.monthlyData()) {
            double monthTotal = 0;
            List<CardMonthData> monthCards = new ArrayList<>();

            for (BackendApiClient.RawMonthCardRow card : monthData.cards()) {
                cardMeta.put(card.cardId(), new String[]{card.bankKey(), card.lastFour()});
                double[] totals = cardTotals.computeIfAbsent(card.cardId(), k -> new double[]{0});
                totals[0] += card.amount();
                monthTotal += card.amount();
                monthCards.add(new CardMonthData(
                        card.cardId(), card.bankKey(), card.lastFour(), round(card.amount())));
            }

            monthlyBreakdown.add(new MonthlyBreakdownData(monthData.month(), round(monthTotal), monthCards));
            totalSpend += monthTotal;
        }

        List<CardSummaryData> cards = cardTotals.entrySet().stream().map(e -> {
            String[] meta = cardMeta.get(e.getKey());
            return new CardSummaryData(e.getKey(), meta[0], meta[1], round(e.getValue()[0]), 0);
        }).sorted(Comparator.comparingDouble(CardSummaryData::totalSpend).reversed()).toList();

        return new CardSpendingData(round(totalSpend), 0, months, cards, List.of(), monthlyBreakdown);
    }

    private UtilityData computeUtilityAnalytics(BackendApiClient.RawUtilityData raw) {
        Map<String, List<BackendApiClient.RawBillRow>> grouped = new LinkedHashMap<>();
        for (BackendApiClient.RawBillRow bill : raw.bills()) {
            String key = bill.billerName() + "|" + bill.accountLastFour();
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(bill);
        }

        List<AccountData> accounts = grouped.values().stream().map(bills -> {
            BackendApiClient.RawBillRow sample = bills.get(0);

            List<BillPointData> points = bills.stream()
                    .filter(b -> b.billDate() != null)
                    .sorted(Comparator.comparing(BackendApiClient.RawBillRow::billDate))
                    .map(b -> new BillPointData(b.billDate(), round(b.amountDue())))
                    .toList();

            double average = points.stream().mapToDouble(BillPointData::amountDue).average().orElse(0);
            Double latest  = points.isEmpty() ? null : points.get(points.size() - 1).amountDue();
            Double prev    = points.size() >= 2 ? points.get(points.size() - 2).amountDue() : null;
            Double change  = (latest != null && prev != null && prev > 0)
                    ? round((latest - prev) / prev * 100) : null;

            return new AccountData(
                    sample.billerName(), sample.accountLastFour(),
                    points, round(average),
                    latest != null ? round(latest) : null, change);
        }).toList();

        return new UtilityData(accounts);
    }

    private double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
