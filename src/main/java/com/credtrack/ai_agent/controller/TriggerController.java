package com.credtrack.ai_agent.controller;

import akka.actor.typed.ActorRef;
import com.credtrack.ai_agent.actor.EmailPipelineCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal trigger endpoint — called by the backend when a new card is added.
 * NOT user-facing; secured by network isolation (localhost only in prod).
 */
@RestController
@RequestMapping("/internal")
public class TriggerController {

    private static final Logger log = LoggerFactory.getLogger(TriggerController.class);

    private final ActorRef<EmailPipelineCoordinator.Command> coordinator;

    public TriggerController(ActorRef<EmailPipelineCoordinator.Command> coordinator) {
        this.coordinator = coordinator;
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
}
