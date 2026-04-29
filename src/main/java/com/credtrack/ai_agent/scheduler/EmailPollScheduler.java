package com.credtrack.ai_agent.scheduler;

import akka.actor.typed.ActorRef;
import com.credtrack.ai_agent.actor.EmailPipelineCoordinator;
import com.credtrack.ai_agent.actor.UtilityBillCoordinatorActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Triggers the Akka email pipelines on a fixed interval.
 * Default: every 3 hours.
 * Override via GMAIL_POLL_INTERVAL_MS env var for testing.
 */
@Component
public class EmailPollScheduler {

    private static final Logger log = LoggerFactory.getLogger(EmailPollScheduler.class);

    private final ActorRef<EmailPipelineCoordinator.Command>      coordinator;
    private final ActorRef<UtilityBillCoordinatorActor.Command>   utilityCoordinator;

    public EmailPollScheduler(ActorRef<EmailPipelineCoordinator.Command> coordinator,
                               ActorRef<UtilityBillCoordinatorActor.Command> utilityCoordinator) {
        this.coordinator        = coordinator;
        this.utilityCoordinator = utilityCoordinator;
    }

    @Scheduled(fixedRateString = "${gmail.poll-interval-ms:10800000}")
    public void poll() {
        log.info("Triggering Gmail poll");
        coordinator.tell(EmailPipelineCoordinator.Poll.INSTANCE);
        utilityCoordinator.tell(UtilityBillCoordinatorActor.Poll.INSTANCE);
    }
}
