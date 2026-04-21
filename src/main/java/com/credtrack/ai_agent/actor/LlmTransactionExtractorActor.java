package com.credtrack.ai_agent.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.CardInfo;
import com.credtrack.ai_agent.model.TransactionExtraction;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * LLM-based transaction extractor for all supported banks.
 *
 * Receives {@link TransactionRouterActor.RouteTransaction} messages forwarded by
 * {@link TransactionRouterActor} and delegates extraction to
 * {@link ExtractionService#extractTransaction(String, String)}, which calls the LLM.
 *
 * Each extraction runs on the virtual-thread executor to avoid blocking the mailbox.
 */
public class LlmTransactionExtractorActor
        extends AbstractBehavior<TransactionRouterActor.Command> {

    private static final Logger log = LoggerFactory.getLogger(LlmTransactionExtractorActor.class);

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<TransactionRouterActor.Command> create(ExtractionService extractionService,
                                                                   BackendApiClient backendApiClient,
                                                                   Executor executor) {
        return Behaviors.setup(ctx ->
                new LlmTransactionExtractorActor(ctx, extractionService, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final ExtractionService extractionService;
    private final BackendApiClient  backendApiClient;
    private final Executor          executor;

    private LlmTransactionExtractorActor(ActorContext<TransactionRouterActor.Command> context,
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
    public Receive<TransactionRouterActor.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(TransactionRouterActor.RouteTransaction.class, this::onRoute)
                .build();
    }

    private Behavior<TransactionRouterActor.Command> onRoute(TransactionRouterActor.RouteTransaction msg) {
        CompletableFuture.runAsync(() -> {
            try {
                extractAndPost(msg);
                msg.replyTo().tell(new TransactionRouterActor.TransactionResult(false));
            } catch (Exception ex) {
                log.error("LLM transaction extraction failed for {}: {}",
                        msg.email().messageId(), ex.getMessage());
                msg.replyTo().tell(new TransactionRouterActor.TransactionResult(true));
            }
        }, executor);
        return this;
    }

    // ── Extraction logic ──────────────────────────────────────────────────────

    private void extractAndPost(TransactionRouterActor.RouteTransaction msg) {
        TransactionExtraction ex = extractionService.extractTransaction(
                msg.bankKey(), msg.email().body());

        if (!ex.isTransaction() || ex.amount() <= 0) {
            log.debug("Not a transaction email (isTransaction={}, amount={}): {}",
                    ex.isTransaction(), ex.amount(), msg.email().messageId());
            return;
        }

        // Resolve the correct card last four from registered cards if body mentions one
        String lastFour = resolveCardLastFour(
                msg.registeredCards(), msg.cardLastFour(), msg.email().body());

        backendApiClient.postTransaction(
                msg.userId(),
                msg.email().messageId(),
                lastFour,
                msg.bankKey(),
                ex.merchantName(),
                ex.merchantCategory(),
                BigDecimal.valueOf(ex.amount()),
                ex.currency(),
                ex.transactionDate(),
                ex.transactionType(),
                ex.description(),
                ex.confidence());
    }

    private String resolveCardLastFour(List<CardInfo> cards, String defaultLastFour, String body) {
        if (cards == null || cards.isEmpty() || body == null) return defaultLastFour;
        for (CardInfo card : cards) {
            if (body.contains(card.lastFour())) return card.lastFour();
        }
        return defaultLastFour;
    }
}
