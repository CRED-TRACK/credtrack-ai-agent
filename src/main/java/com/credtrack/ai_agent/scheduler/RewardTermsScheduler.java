package com.credtrack.ai_agent.scheduler;

import com.credtrack.ai_agent.model.CardProductToScrape;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.RewardTermsScraper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Weekly reward-terms scrape. Default = Mon 03:00 America/New_York.
 * Override the cron with the REWARDS_SCRAPE_CRON env var.
 *
 * Sequential per-card with 5s spacing — see RewardTermsScraper for the per-card flow:
 *   fetch → Jsoup clean → SHA-256 hash → (skip if unchanged) → LLM extract → POST back.
 */
@Component
public class RewardTermsScheduler {

    private static final Logger log = LoggerFactory.getLogger(RewardTermsScheduler.class);

    private final BackendApiClient backendApiClient;
    private final RewardTermsScraper rewardTermsScraper;
    private final Executor executor;

    public RewardTermsScheduler(BackendApiClient backendApiClient,
                                RewardTermsScraper rewardTermsScraper,
                                Executor virtualThreadExecutor) {
        this.backendApiClient = backendApiClient;
        this.rewardTermsScraper = rewardTermsScraper;
        this.executor = virtualThreadExecutor;
    }

    @Scheduled(cron = "${rewards.scrape.cron:0 0 3 ? * MON}",
               zone = "${rewards.scrape.zone:America/New_York}")
    public void weeklyScrape() {
        log.info("reward_scrape_event=cron_start");
        executor.execute(this::runOnce);
    }

    private void runOnce() {
        List<CardProductToScrape> cards;
        try {
            cards = backendApiClient.getCardProductsToScrape();
        } catch (Exception e) {
            log.error("reward_scrape_event=list_failed error={}", e.getMessage());
            return;
        }
        if (cards == null || cards.isEmpty()) {
            log.info("reward_scrape_event=no_targets");
            return;
        }
        log.info("reward_scrape_event=run_start targets={}", cards.size());
        for (CardProductToScrape card : cards) {
            try {
                rewardTermsScraper.scrape(card);
            } catch (Exception e) {
                log.error("reward_scrape_event=card_failed card_product_id={} error={}",
                        card.cardProductId(), e.getMessage());
            }
            try { Thread.sleep(5_000); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
        log.info("reward_scrape_event=run_done");
    }
}
