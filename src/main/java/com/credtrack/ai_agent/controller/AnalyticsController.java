package com.credtrack.ai_agent.controller;

import akka.actor.typed.ActorRef;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import com.credtrack.ai_agent.actor.AnalyticsWorkerActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Internal analytics endpoint — called by the backend, NOT by the iOS app.
 * Secured with X-Service-Key header (same pattern as TriggerController).
 *
 * Routes each request to the AnalyticsWorkerActor entity for the given userId.
 * Cluster Sharding ensures the entity lives on exactly one cluster node,
 * where its in-actor cache avoids repeated backend DB round-trips.
 */
@RestController
@RequestMapping("/internal/analytics")
public class AnalyticsController {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsController.class);

    private final ClusterSharding sharding;
    private final String serviceKey;

    public AnalyticsController(ClusterSharding sharding,
                                @Value("${backend.service-key}") String serviceKey) {
        this.sharding   = sharding;
        this.serviceKey = serviceKey;
    }

    /**
     * GET /internal/analytics/cards?userId=X&months=6
     * Returns card spending analytics computed by the sharded actor.
     */
    @GetMapping("/cards")
    public ResponseEntity<AnalyticsWorkerActor.CardSpendingData> cardAnalytics(
            @RequestHeader("X-Service-Key") String key,
            @RequestParam String userId,
            @RequestParam(defaultValue = "6") int months) {

        if (!serviceKey.equals(key)) return ResponseEntity.status(403).build();

        EntityRef<AnalyticsWorkerActor.Command> ref =
                sharding.entityRefFor(AnalyticsWorkerActor.TYPE_KEY, userId);
        try {
            AnalyticsWorkerActor.CardSpendingResult result = ref
                    .ask((ActorRef<AnalyticsWorkerActor.CardSpendingResult> replyTo) ->
                                    new AnalyticsWorkerActor.ComputeCardSpending(months, replyTo),
                            Duration.ofSeconds(30))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            return ResponseEntity.ok(result.data());
        } catch (Exception e) {
            log.error("Card analytics ask failed for user {} months={}: {}", userId, months, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * GET /internal/analytics/utilities?userId=X
     * Returns utility bill analytics computed by the sharded actor.
     */
    @GetMapping("/utilities")
    public ResponseEntity<AnalyticsWorkerActor.UtilityData> utilityAnalytics(
            @RequestHeader("X-Service-Key") String key,
            @RequestParam String userId) {

        if (!serviceKey.equals(key)) return ResponseEntity.status(403).build();

        EntityRef<AnalyticsWorkerActor.Command> ref =
                sharding.entityRefFor(AnalyticsWorkerActor.TYPE_KEY, userId);
        try {
            AnalyticsWorkerActor.UtilityAnalyticsResult result = ref
                    .ask((ActorRef<AnalyticsWorkerActor.UtilityAnalyticsResult> replyTo) ->
                                    new AnalyticsWorkerActor.ComputeUtilityAnalytics(replyTo),
                            Duration.ofSeconds(30))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            return ResponseEntity.ok(result.data());
        } catch (Exception e) {
            log.error("Utility analytics ask failed for user {}: {}", userId, e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
}
