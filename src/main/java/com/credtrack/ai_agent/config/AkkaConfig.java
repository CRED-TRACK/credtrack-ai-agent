package com.credtrack.ai_agent.config;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import akka.actor.typed.javadsl.AskPattern;
import com.credtrack.ai_agent.actor.EmailPipelineCoordinator;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import com.credtrack.ai_agent.service.GmailService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
public class AkkaConfig {

    /**
     * Virtual-thread-per-task executor used for all blocking I/O inside actors
     * (Gmail API, Ollama, backend REST calls).
     *
     * Java 21+ virtual threads are extremely cheap — each blocking call gets its
     * own virtual thread and the platform thread is never blocked, so we can have
     * hundreds of concurrent Ollama/Gmail calls without saturating the ForkJoinPool.
     */
    @Bean
    public Executor virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Root ActorSystem using SpawnProtocol so we can spawn top-level actors
     * as Spring beans via AskPattern.
     */
    @Bean
    public ActorSystem<SpawnProtocol.Command> actorSystem() {
        return ActorSystem.create(SpawnProtocol.create(), "credtrack-ai");
    }

    /**
     * EmailPipelineCoordinator exposed as a Spring bean so the scheduler
     * can inject it directly and send Poll messages.
     */
    @Bean
    public ActorRef<EmailPipelineCoordinator.Command> emailPipelineCoordinator(
            ActorSystem<SpawnProtocol.Command> system,
            BackendApiClient backendApiClient,
            GmailService gmailService,
            ExtractionService extractionService,
            Executor virtualThreadExecutor) {

        return AskPattern.<SpawnProtocol.Command, ActorRef<EmailPipelineCoordinator.Command>>ask(
                system,
                replyTo -> new SpawnProtocol.Spawn<>(
                        EmailPipelineCoordinator.create(
                                backendApiClient, gmailService, extractionService, virtualThreadExecutor),
                        "email-pipeline-coordinator",
                        akka.actor.typed.Props.empty(),
                        replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        ).toCompletableFuture().join();
    }
}
