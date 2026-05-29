package com.credtrack.ai_agent.controller;

import akka.actor.typed.ActorRef;
import com.credtrack.ai_agent.actor.EmailPipelineCoordinator;
import com.credtrack.ai_agent.model.CardProductToScrape;
import com.credtrack.ai_agent.model.ScrapedTermsResult;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.RewardTermsScraper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Internal trigger endpoint — called by the backend when a new card is added.
 * NOT user-facing; secured by network isolation (localhost only in prod).
 */
@RestController
@RequestMapping("/internal")
public class TriggerController {

    private static final Logger log = LoggerFactory.getLogger(TriggerController.class);

    private final ActorRef<EmailPipelineCoordinator.Command> coordinator;
    private final BackendApiClient backendApiClient;
    private final RewardTermsScraper rewardTermsScraper;
    private final Executor executor;

    public TriggerController(ActorRef<EmailPipelineCoordinator.Command> coordinator,
                             BackendApiClient backendApiClient,
                             RewardTermsScraper rewardTermsScraper,
                             Executor virtualThreadExecutor) {
        this.coordinator = coordinator;
        this.backendApiClient = backendApiClient;
        this.rewardTermsScraper = rewardTermsScraper;
        this.executor = virtualThreadExecutor;
    }

    /**
     * POST /internal/trigger-scan
     * Fires an immediate Gmail poll cycle.
     * Returns 202 Accepted right away — the scan runs asynchronously via Akka.
     */
    @PostMapping("/trigger-scan")
    public ResponseEntity<Void> triggerScan() {
        log.info("Immediate Gmail scan triggered via API");
        coordinator.tell(EmailPipelineCoordinator.Poll.INSTANCE);
        return ResponseEntity.accepted().build();
    }

    /**
     * Fetch the scrape list from the backend without executing the pipeline.
     * Useful for verifying connectivity + endpoint shape before triggering a real run.
     */
    @GetMapping("/reward-scrape/cards")
    public ResponseEntity<List<CardProductToScrape>> listScrapeTargets() {
        List<CardProductToScrape> cards = backendApiClient.getCardProductsToScrape();
        log.info("reward-scrape list_size={}", cards == null ? 0 : cards.size());
        return ResponseEntity.ok(cards == null ? List.of() : cards);
    }

    /**
     * Run the reward-terms scraper synchronously across all scrape targets and
     * return the per-card results inline. v1 does NOT write back to the backend —
     * this is for manual inspection only.
     */
    @PostMapping("/trigger-reward-scrape")
    public ResponseEntity<List<ScrapedTermsResult>> triggerRewardScrape() {
        log.info("reward-scrape triggered via API");
        List<CardProductToScrape> cards = backendApiClient.getCardProductsToScrape();
        if (cards == null || cards.isEmpty()) {
            log.info("reward-scrape no_cards_to_scrape");
            return ResponseEntity.ok(List.of());
        }
        List<ScrapedTermsResult> results = new ArrayList<>();
        for (CardProductToScrape card : cards) {
            try {
                results.add(rewardTermsScraper.scrape(card));
            } catch (Exception e) {
                log.error("reward-scrape unexpected_error card_product_id={} error={}",
                        card.cardProductId(), e.getMessage());
            }
            try { Thread.sleep(5000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
        return ResponseEntity.ok(results);
    }
}
