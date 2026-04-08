package com.credtrack.ai_agent.scheduler;

import akka.actor.typed.ActorRef;
import com.credtrack.ai_agent.actor.EmailPipelineCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Triggers the Akka email pipeline on a fixed interval.
 * Default: every 1 hour (statements arrive monthly — no need to poll faster).
 * Override via GMAIL_POLL_INTERVAL_MS env var for testing.
 */
@Component
public class EmailPollScheduler {

    private static final Logger log = LoggerFactory.getLogger(EmailPollScheduler.class);

    private final ActorRef<EmailPipelineCoordinator.Command> coordinator;

    public EmailPollScheduler(ActorRef<EmailPipelineCoordinator.Command> coordinator) {
        this.coordinator = coordinator;
    }

    @Scheduled(fixedRateString = "${gmail.poll-interval-ms:3600000}")
    public void poll() {
        log.info("Triggering Gmail poll");
        coordinator.tell(EmailPipelineCoordinator.Poll.INSTANCE);
    }
}
